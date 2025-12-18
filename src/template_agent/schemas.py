from __future__ import annotations
from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field

Channel = Literal["SMS", "KAKAO", "PUSH", "EMAIL"]
Goal = Literal["cart_recovery", "browse_abandon", "repurchase", "back_in_stock"]
VariantTag = Literal["question", "direct", "empathy"]

class ProductContext(BaseModel):
    name: str = Field(..., description="상품명")
    category: Optional[str] = Field(default=None, description="카테고리")
    usp_keywords: List[str] = Field(default=None, description="상품 url")

class PersonaContext(BaseModel):
    persona_id: str = Field(..., description="페르소나 ID")
    traits: List[str] = Field(default_factory=list, description="페르소나 특성 키워드")

class TONEContext(BaseModel):
    tone_id: str = Field(..., description="브랜드 톤 ID")
    do: List[str] = Field(default_factory=list, description="권장 표현/말투")
    dont: List[str] = Field(default_factory=list, description="금지 표현/말투")

class Constraints(BaseModel):
    max_chars: int = Field(default=90, description="권장 최대 글자수")
    emoji_max: int = Field(default=1, description="최대 이모지 개수")
    must_not_claim_unverified_benefit: bool =True

class TemplateInput(BaseModel):
    campaign_goal: Goal
    channel: Channel
    step_id: str = Field(..., description="S1, S2 등")
    persona: PersonaContext
    tone: TONEContext
    product: ProductContext
    benefit_hint: Optional[str] = Field(default=None, description="혜택이 '있을 수도' 있음을 알려주는 힌트(검증은 Execution에서)")
    constraints: Constraints

class CandidateTags(BaseModel):
    benefit_claim: bool = Field(..., description="혜택을 주장/암지하는 문구가 포함되는지")
    urgency_level: int = Field(0, ge=0, le=2, description="0(없음)~2(강함)")
    length_hint: Literal["short", "normal"] = "normal"

class Candidate(BaseModel):
    candidate_id: str
    variant_tag: VariantTag
    slot_map: Dict[str, str] = Field(..., description="슬롯별 문장 조각.키는 allowed_slots 내여야 함.")
    tags: CandidateTags
    rationale: str = Field(..., description="내부용 근거/의도 요약(짧게)")

class TemplateOutput(BaseModel):
    campaign_goal: Goal
    channel: Channel
    step_id: str
    persona_id: str
    tone_id: str
    allowed_slots: List[str]
    candidates: List[Candidate] = Field(..., min_length=3, max_length=6)
    warnings: List[str] = Field(default_factory=list, description="검증/필터 과정에서 발생한 경고")



