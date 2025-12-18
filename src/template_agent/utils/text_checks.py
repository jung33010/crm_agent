from __future__ import annotations
from typing import Iterable

def is_emoji_char(ch: str) -> bool:
    """
    안정적인 '대략 이모지' 판별.
    정규식 이모지 범위는 OS/파이썬 버전별로 깨질 수 있어 코드로 처리.
    """
    if not ch:
        return False
    cp = ord(ch)

    # 주요 이모지 블록(대략)
    # Emoticons, Misc Symbols, Dingbats, Transport, Supplemental Symbols, etc.
    return (
        0x1F300 <= cp <= 0x1FAFF  # Misc symbols & pictographs + Supplemental Symbols
        or 0x2600 <= cp <= 0x26FF  # Misc symbols
        or 0x2700 <= cp <= 0x27BF  # Dingbats
        or 0x1F1E6 <= cp <= 0x1F1FF  # Flags
    )

def count_emoji(s: str) -> int:
    s = s or ""
    return sum(1 for ch in s if is_emoji_char(ch))

def contains_banned_phrase(text: str, banned_list: Iterable[str]) -> str | None:
    t = (text or "").lower()
    for b in banned_list:
        b2 = b.strip().lower()
        if not b2:
            continue
        if b2 in t:
            return b.strip()
    return None

def rough_len(text: str) -> int:
    return len(text or "")
