import json
from pathlib import Path
from typing import Any, Dict
import yaml
def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")

def read_yaml(path: str) -> Dict[str, Any]:
    return yaml.safe_load(read_text(path))

def read_json(path: str) -> Any:
    return json.loads(read_text(path))

def write_json(path: str, data: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")