[FEW-SHOT EXAMPLES]

예시1) cart_recovery / SMS / S1(혜택이 있을 수도 있음)
{
    "campagin_goal": "cart_recovery",
    "channel": "SMS",
    "step_id": "S1",
    "persona_id": "value_seeker",
    "tone_id": "Brand_Default_Friendly",
    "allowed_slots": ["greeting", "product_name", "benefit_line", "cta", "short_link"],
    "candidates": [
        {
            "candidate_id": "c1",
            "variant_tag": "direct",
            "slot_map": {
                "greeting": "{{ user.name | default:'고객님' }}",
                "product_name": "{{ product.name }},
                "benefit_line": "{% if benefit.exists %}{{ benefit.text }}{% endif %}",
                "cta": "장바구니 지금 확인"
                "short_link": "{{ short_linke }}"
            },
            "tags": { "benefit_claim": true, "urgency_level": 0, "length_hint": "short" },
            "rationale": "가벼운 리마인드 + CTA 간결"
        }
    ],
    "warnings": []
}

예시2) browse_abandon / SMS / S1 (혜택 언급 없음)
{
  "campaign_goal": "browse_abandon",
  "channel": "SMS",
  "step_id": "S1",
  "persona_id": "ingredient_care",
  "tone_id": "Brand_Default_Friendly",
  "allowed_slots": ["greeting","product_name","usp_line","cta","short_link"],
  "candidates": [
    {
      "candidate_id": "c1",
      "variant_tag": "question",
      "slot_map": {
        "greeting": "{{ user.name | default:'고객님' }}",
        "product_name": "{{ product.name }}",
        "usp_line": "{{ usp.one_liner }}",
        "cta": "상품 다시 보기",
        "short_link": "{{ short_link }}"
      },
      "tags": { "benefit_claim": false, "urgency_level": 0, "length_hint": "short" },
      "rationale": "USP 한 줄로 관심 복귀 유도"
    }
  ],
  "warnings": []
}