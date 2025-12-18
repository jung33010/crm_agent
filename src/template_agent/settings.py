from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()
@dataclass(frozen=True)
class Settings:
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    model : str = os.getenv("TEMPLATE_AGENT_MODEL", "gpt-4o-mini")
    candidate_count: int = int(os.getenv("CANDIDATE_COUNT", 5))
    temperature: float = float(os.getenv("TEMPERATURE", "0.7"))
    max_output_tokens: int = int(os.getenv("MAX_OUTPUT_TOKENS", "1200"))

def get_settings() -> Settings:
    s = Settings()
    if not s.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다. .env파일을 확인하세요")
    return s