from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple, Optional, Any, Dict

from openai import OpenAI

from .schemas import TemplateInput, TemplateOutput, Candidate
from .utils.io import read_text, read_yaml


class TemplateAgent:
    """
    Template Agent (MVP)
    - rules/prompts/rag 로드
    - LLM으로 후보 생성(JSON object 강제)
    - LLM 응답을 계약(TemplateOutput)에 맞게 정규화(normalize)
    - Pydantic 검증 + 룰 기반 필터링
    """

    def __init__(self, model: str, temperature: float, max_output_tokens: int, candidate_count: int):
        self.client = OpenAI()
        self.model = model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.candidate_count = candidate_count

        base = Path(__file__).parent

        # prompts
        self.system_prompt = read_text(str(base / "prompt" / "system.md"))
        self.fewshot = read_text(str(base / "prompt" / "fewshot.md"))

        # rules (YAML -> dict)
        self.copy_rules = read_yaml(str(base / "rules" / "copy_rules.yml"))
        self.slot_schema = read_yaml(str(base / "rules" / "slot_schema.yml"))
        self.recipe_strategy = read_yaml(str(base / "rules" / "recipe_strategy.yml"))

        # rag (TEXT)
        self.brand_guide = read_text(str(base / "rag" / "brand_guide.md"))
        self.product_usps = read_text(str(base / "rag" / "product_usps.md"))

        # banned list (TEXT)
        banned_path = base / "rag" / "banned_phrases.txt"
        self.banned_phrases = [
            line.strip()
            for line in read_text(str(banned_path)).splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    def run(self, inp: TemplateInput) -> TemplateOutput:
        allowed_slots = self._get_allowed_slots(inp.campaign_goal, inp.channel)
        strategy = self._get_strategy(inp.campaign_goal, inp.step_id)
        channel_rules = self._get_channel_rules(inp.channel, inp.constraints)

        user_prompt = self._build_user_prompt(
            inp=inp,
            allowed_slots=allowed_slots,
            strategy=strategy,
            channel_rules=channel_rules,
        )

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.fewshot + "\n\n" + user_prompt},
        ]

        # JSON object 강제 호출
        data = self._call_llm_json(messages)

        # LLM 응답을 계약 스키마에 맞게 정규화
        normalized = self._normalize_to_contract(
            raw=data,
            inp=inp,
            allowed_slots=allowed_slots,
        )

        # 계약 검증
        out = TemplateOutput.model_validate(normalized)

        # 룰 기반 필터링
        filtered, warnings = self._validate_and_filter(out, allowed_slots, inp)
        out.candidates = filtered

        # warnings는 기존 + 필터링 경고 합치기
        out.warnings = (out.warnings or []) + (warnings or [])

        return out

    # LLM 호출: JSON object 강제
    def _call_llm_json(self, messages: List[dict]) -> dict:
        """
        New SDK면 responses.create(text.format=json_object),
        아니면 chat.completions.create(response_format=json_object)로 fallback.
        """
        if hasattr(self.client, "responses"):
            resp = self.client.responses.create(
                model=self.model,
                input=messages,
                text={"format": {"type": "json_object"}},
                temperature=self.temperature,
                max_output_tokens=self.max_output_tokens,
            )
            return json.loads(resp.output_text)

        # Older SDK fallback
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content)

    # Normalize: raw JSON -> TemplateOutput contract
    def _normalize_to_contract(self, raw: Any, inp: TemplateInput, allowed_slots: List[str]) -> Dict[str, Any]:
        """
        LLM이 반환한 JSON이 candidates/warnings만 있어도,
        TemplateOutput 스키마(required)를 만족하도록 메타/필수필드/타입을 보정한다.
        """
        data: Dict[str, Any] = raw if isinstance(raw, dict) else {}
        candidates = data.get("candidates")
        warnings = data.get("warnings")

        if not isinstance(candidates, list):
            candidates = []
        if not isinstance(warnings, list):
            warnings = []

        # TemplateOutput required fields 채우기
        data["campaign_goal"] = inp.campaign_goal
        data["channel"] = inp.channel
        data["step_id"] = inp.step_id
        data["persona_id"] = inp.persona.persona_id
        data["tone_id"] = inp.tone.tone_id
        data["allowed_slots"] = allowed_slots
        data["warnings"] = warnings

        # 스키마가 urgency_level <= 2 라서 0~2로 매핑
        urg_map = {"low": 0, "mid": 1, "high": 2}

        fixed_candidates: List[Dict[str, Any]] = []
        for i, c in enumerate(candidates):
            if not isinstance(c, dict):
                c = {}

            # slot_map 보정
            slot_map = c.get("slot_map")
            if not isinstance(slot_map, dict):
                slot_map = {}

            # allowed_slots 밖 키 제거
            slot_map = {k: v for k, v in slot_map.items() if k in allowed_slots}

            # tags 보정
            tags = c.get("tags")
            if not isinstance(tags, dict):
                tags = {}

            # urgency_level 보정 (str -> int, 그리고 0~2 clamp)
            ul = tags.get("urgency_level")
            if isinstance(ul, str):
                tags["urgency_level"] = urg_map.get(ul.lower(), 1)
            elif isinstance(ul, int):
                tags["urgency_level"] = ul
            else:
                tags["urgency_level"] = 1  # default mid

            # clamp to 0~2
            try:
                tags["urgency_level"] = int(tags["urgency_level"])
            except Exception:
                tags["urgency_level"] = 1
            tags["urgency_level"] = max(0, min(2, tags["urgency_level"]))

            # 스키마에서 필요할 수 있는 태그 기본값들
            tags.setdefault("length_hint", "medium")
            tags.setdefault("benefit_claim", True)

            fixed_candidates.append(
                {
                    "candidate_id": c.get("candidate_id") or f"C{i+1}",
                    "slot_map": slot_map,
                    "tags": tags,
                    "rationale": c.get("rationale") or "",
                    "variant_tag": c.get("variant_tag") or "direct",
                }
            )

        data["candidates"] = fixed_candidates
        return data

    # Prompt Builder
    def _build_user_prompt(
        self,
        inp: TemplateInput,
        allowed_slots: List[str],
        strategy: dict,
        channel_rules: dict,
    ) -> str:
        return f"""
[CONTEXT]
- campaign_goal: {inp.campaign_goal}
- channel: {inp.channel}
- step_id: {inp.step_id}
- persona_id: {inp.persona.persona_id}
- persona_traits: {inp.persona.traits}
- tone_id: {inp.tone.tone_id}
- tone_do: {inp.tone.do}
- tone_dont: {inp.tone.dont}
- product: {inp.product.model_dump()}
- benefit_hint: {inp.benefit_hint}

[BRAND_GUIDE]
{self.brand_guide}

[PRODUCT_USPS]
{self.product_usps}

[CHANNEL_RULES]
{json.dumps(channel_rules, ensure_ascii=False)}

[STRATEGY]
{json.dumps(strategy, ensure_ascii=False)}

[ALLOWED_SLOTS]
{allowed_slots}

[TASK]
- 후보 메시지 {self.candidate_count}개를 생성하세요.
- 출력은 반드시 "단 하나의 JSON 객체"만 반환하세요. (설명/문장/마크다운 금지)
- 각 candidate.slot_map 안에는 allowed_slots에 포함된 키만 사용하세요. (그 외 키 금지)
- 채널 제약을 준수하세요. (max_chars, emoji_max)
- 금지 문구를 포함하지 마세요.
- 검증되지 않은 효능/의학적·확정적 표현(치료/완치/보장/100% 등)은 쓰지 마세요.

[OUTPUT_JSON_SHAPE]
{{
  "candidates": [
    {{
      "slot_map": {{"<slot_key>": "<text>"}},
      "tags": {{"length_hint": "short|medium|long", "urgency_level": "low|mid|high", "benefit_claim": true}},
      "variant_tag": "direct|question|empathy",
      "rationale": "optional"
    }}
  ],
  "warnings": []
}}
""".strip()

    # Rules lookup helpers
    def _get_allowed_slots(self, campaign_goal: str, channel: str) -> List[str]:
        key = f"{campaign_goal}:{channel}"

        slots = None
        if isinstance(self.slot_schema, dict):
            slots = self.slot_schema.get(key)

        if not slots and isinstance(self.slot_schema, dict):
            slots = self.slot_schema.get(campaign_goal) or self.slot_schema.get("default")

        if not slots:
            return ["headline", "body", "cta"]

        if isinstance(slots, str):
            return [s.strip() for s in slots.split(",") if s.strip()]
        if isinstance(slots, list):
            return slots

        return ["headline", "body", "cta"]

    def _get_strategy(self, campaign_goal: str, step_id: str) -> dict:
        key = f"{campaign_goal}:{step_id}"
        if isinstance(self.recipe_strategy, dict):
            return self.recipe_strategy.get(key) or self.recipe_strategy.get(campaign_goal) or {}
        return {}

    def _get_channel_rules(self, channel: str, constraints: Any) -> dict:
        base_rules = {}
        if isinstance(self.copy_rules, dict):
            base_rules = self.copy_rules.get(channel) or self.copy_rules.get("default") or {}

        merged = dict(base_rules)
        if constraints:
            merged.update({k: v for k, v in constraints.model_dump().items() if v is not None})
        return merged

    # Validate & Filter
    def _validate_and_filter(
        self,
        out: TemplateOutput,
        allowed_slots: List[str],
        inp: TemplateInput,
    ) -> Tuple[List[Candidate], List[str]]:
        warnings: List[str] = []
        kept: List[Candidate] = []

        def _contains_banned(text: str) -> Optional[str]:
            t = (text or "").lower()
            for b in self.banned_phrases:
                if b.lower() in t:
                    return b
            return None

        def _count_emoji(text: str) -> int:
            text = text or ""
            cnt = 0
            for ch in text:
                cp = ord(ch)
                if (
                    0x1F300 <= cp <= 0x1FAFF
                    or 0x2600 <= cp <= 0x26FF
                    or 0x2700 <= cp <= 0x27BF
                    or 0x1F1E6 <= cp <= 0x1F1FF
                ):
                    cnt += 1
            return cnt

        max_chars = inp.constraints.max_chars
        emoji_max = inp.constraints.emoji_max

        for c in out.candidates or []:
            slot_map = dict(c.slot_map or {})
            slot_map = {k: v for k, v in slot_map.items() if k in allowed_slots}

            if not slot_map:
                continue

            combined = " ".join(str(v) for v in slot_map.values())

            banned = _contains_banned(combined)
            if banned:
                warnings.append(f"Removed candidate due to banned phrase: {banned}")
                continue

            if max_chars is not None and len(combined) > max_chars:
                warnings.append("Removed candidate due to max_chars limit")
                continue

            if emoji_max is not None and _count_emoji(combined) > emoji_max:
                warnings.append("Removed candidate due to emoji_max limit")
                continue

            c.slot_map = slot_map
            kept.append(c)

        if len(kept) < 3:
            warnings.append("Candidate count after filtering is less than 3. Consider relaxing rules or regenerating.")

        return kept, warnings
