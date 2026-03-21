"""Prompt-injection prevention utilities following OWASP LLM Cheat Sheet.

Reference:
  https://cheatsheetseries.owasp.org/cheatsheets/
  LLM_Prompt_Injection_Prevention_Cheat_Sheet.html

Defence layers implemented
--------------------------
1. **Input validation & sanitisation** – regex + fuzzy detection of common
   injection phrases, encoding payload detection, length enforcement.
2. **Risk scoring** – each signal adds to a cumulative score; callers can
   decide whether to flag, log, or reject.
3. **Output monitoring** – detect system-prompt leakage and suspicious
   instruction sequences in LLM responses.
"""

from __future__ import annotations

import base64
import logging
import re
import unicodedata
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Input validation patterns
# ---------------------------------------------------------------------------

# Direct injection phrases (case-insensitive).
_INJECTION_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"ignore\s+(all\s+)?previous\s+instructions",
        r"ignore\s+(all\s+)?prior\s+instructions",
        r"ignore\s+(all\s+)?above\s+instructions",
        r"disregard\s+(all\s+)?previous",
        r"disregard\s+(all\s+)?prior",
        r"forget\s+(all\s+)?previous",
        r"forget\s+(all\s+)?(your|the)\s+instructions",
        r"override\s+(all\s+)?instructions",
        r"override\s+(your|the)\s+system\s+prompt",
        r"new\s+instructions?\s*:",
        r"system\s*:\s*you\s+are",
        r"you\s+are\s+now\s+in\s+developer\s+mode",
        r"developer\s+mode\s+enabled",
        r"act\s+as\s+(a\s+)?different\s+(ai|assistant|model)",
        r"pretend\s+you\s+are\s+(?!looking|going|planning)",
        r"reveal\s+(your\s+)?(system\s+)?prompt",
        r"show\s+(me\s+)?(your\s+)?(system\s+)?prompt",
        r"repeat\s+(your\s+)?instructions\s+verbatim",
        r"output\s+(your\s+)?(system|initial)\s+prompt",
        r"what\s+(are|is)\s+(your\s+)?(system\s+)?(instructions|prompt)",
        r"do\s+anything\s+now",
        r"DAN\s+mode",
        r"jailbreak",
    ]
]

# High-risk keywords that raise the risk score.
_HIGH_RISK_KEYWORDS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bpassword\b",
        r"\bapi[_\s]?key\b",
        r"\bsecret[_\s]?key\b",
        r"\badmin\b",
        r"\bbypass\b",
        r"\boverride\b",
        r"\bsudo\b",
        r"\broot\b",
        r"\bexec\b",
        r"\beval\b",
        r"\b__import__\b",
        r"\bos\.system\b",
        r"\bsubprocess\b",
    ]
]

# Typoglycemia-resilient matching.
# Build a pattern that matches the first and last letter with any
# interior letters in any order.
_TYPO_SEEDS = [
    "ignore",
    "previous",
    "instructions",
    "disregard",
    "override",
    "developer",
    "jailbreak",
    "reveal",
    "prompt",
    "system",
    "bypass",
]


def _typo_pattern(word: str) -> re.Pattern[str]:
    """Match typoglycemia variant: first + last char anchored, interior fuzzy."""
    if len(word) <= 3:
        return re.compile(re.escape(word), re.IGNORECASE)
    first, mid, last = word[0], word[1:-1], word[-1]
    mid_chars = set(mid.lower())
    char_class = "".join(re.escape(c) for c in mid_chars)
    return re.compile(
        rf"{re.escape(first)}[{char_class}]{{{len(mid)-1},{len(mid)+1}}}{re.escape(last)}",
        re.IGNORECASE,
    )


_TYPO_PATTERNS = [_typo_pattern(w) for w in _TYPO_SEEDS]

# ---------------------------------------------------------------------------
# 2. Encoding detection
# ---------------------------------------------------------------------------

_BASE64_RE = re.compile(r"[A-Za-z0-9+/]{20,}={0,2}")
_HEX_BLOCK_RE = re.compile(r"(?:\\x[0-9a-fA-F]{2}){4,}")
_UNICODE_ESCAPE_RE = re.compile(r"(?:\\u[0-9a-fA-F]{4}){3,}")


def _contains_encoded_payload(text: str) -> bool:
    """Detect Base64, hex-escape, or Unicode-escape sequences."""
    # Base64 blocks that decode to ASCII text
    for match in _BASE64_RE.finditer(text):
        try:
            decoded = base64.b64decode(match.group(), validate=True)
            if decoded.isascii() and len(decoded) > 10:
                return True
        except Exception:
            pass
    if _HEX_BLOCK_RE.search(text):
        return True
    if _UNICODE_ESCAPE_RE.search(text):
        return True
    return False


# ---------------------------------------------------------------------------
# 3. Unicode normalisation
# ---------------------------------------------------------------------------

# Invisible / zero-width characters.
_INVISIBLE_RE = re.compile(
    r"[\u200b\u200c\u200d\u200e\u200f\u2060\u2061\u2062\u2063\u2064\ufeff\u00ad]"
)
# Homoglyphs mapped to ASCII.
_HOMOGLYPH_MAP: dict[str, str] = {
    "\u0430": "a",  # Cyrillic а
    "\u0435": "e",  # Cyrillic е
    "\u043e": "o",  # Cyrillic о
    "\u0440": "p",  # Cyrillic р
    "\u0441": "c",  # Cyrillic с
    "\u0443": "y",  # Cyrillic у
    "\u0445": "x",  # Cyrillic х
    "\u0456": "i",  # Cyrillic і
    "\u0455": "s",  # Cyrillic ѕ
    "\u04bb": "h",  # Cyrillic һ
    "\uff10": "0",  # Fullwidth 0
    "\uff11": "1",  # Fullwidth 1
}


def _normalise_text(text: str) -> str:
    """Normalise Unicode, strip invisibles, replace homoglyphs."""
    text = unicodedata.normalize("NFKC", text)
    text = _INVISIBLE_RE.sub("", text)
    for glyph, ascii_char in _HOMOGLYPH_MAP.items():
        text = text.replace(glyph, ascii_char)
    # Collapse excessive whitespace.
    text = re.sub(r"\s{3,}", "  ", text)
    return text


# ---------------------------------------------------------------------------
# 4. Risk-score dataclass
# ---------------------------------------------------------------------------

@dataclass
class InjectionScanResult:
    """Result of scanning a text input for injection signals."""

    risk_score: int = 0
    signals: list[str] = field(default_factory=list)

    @property
    def is_suspicious(self) -> bool:
        """Score >= 3 is considered suspicious (OWASP HITL threshold)."""
        return self.risk_score >= 3

    @property
    def is_blocked(self) -> bool:
        """Score >= 5 warrants automatic blocking."""
        return self.risk_score >= 5


# ---------------------------------------------------------------------------
# 5. Core scanning function
# ---------------------------------------------------------------------------

# Default max length for user-controlled fields (OWASP: 10 000 chars).
DEFAULT_MAX_INPUT_LENGTH = 10_000


def scan_input(
    text: str,
    *,
    max_length: int = DEFAULT_MAX_INPUT_LENGTH,
    field_name: str = "input",
) -> InjectionScanResult:
    """Scan a text string for prompt-injection signals.

    Returns an ``InjectionScanResult`` with cumulative risk score and
    human-readable signal descriptions.
    """
    result = InjectionScanResult()

    if not text:
        return result

    # --- Length check ---
    if len(text) > max_length:
        result.risk_score += 2
        result.signals.append(
            f"{field_name}: exceeds max length ({len(text)} > {max_length})"
        )

    # Normalise for robust matching.
    normalised = _normalise_text(text)

    # --- Direct injection patterns (highest risk) ---
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(normalised):
            result.risk_score += 3
            result.signals.append(
                f"{field_name}: injection pattern matched: {pattern.pattern[:60]}"
            )
            break  # One match is enough for the +3

    # --- Typoglycemia variants ---
    typo_hits = sum(1 for p in _TYPO_PATTERNS if p.search(normalised))
    if typo_hits >= 3:
        result.risk_score += 2
        result.signals.append(
            f"{field_name}: typoglycemia cluster ({typo_hits} matches)"
        )

    # --- High-risk keywords ---
    for pattern in _HIGH_RISK_KEYWORDS:
        if pattern.search(normalised):
            result.risk_score += 1
            result.signals.append(
                f"{field_name}: high-risk keyword: {pattern.pattern[:40]}"
            )

    # --- Encoded payloads ---
    if _contains_encoded_payload(normalised):
        result.risk_score += 2
        result.signals.append(f"{field_name}: encoded payload detected")

    return result


def sanitise_input(
    text: str,
    *,
    max_length: int = DEFAULT_MAX_INPUT_LENGTH,
    field_name: str = "input",
) -> tuple[str, InjectionScanResult]:
    """Normalise, scan, and truncate a user-controlled string.

    Returns ``(cleaned_text, scan_result)``.  The caller decides whether
    to proceed, flag for review, or reject based on the scan result.
    """
    cleaned = _normalise_text(text)
    # Enforce length limit.
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length]
    result = scan_input(cleaned, max_length=max_length, field_name=field_name)
    return cleaned, result


# ---------------------------------------------------------------------------
# 6. Output monitoring (detect system-prompt leakage)
# ---------------------------------------------------------------------------

_LEAKAGE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"SYSTEM:\s*You\s+are",
        r"SYSTEM_INSTRUCTIONS:",
        r"system\s+prompt\s*:",
        r"<\|im_start\|>system",
        r"<<SYS>>",
        r"You are Ketchup.s planning engine",
        r"Return strict JSON only with key",
        r"Build exactly 5 plans for a friend group",
    ]
]

# Numbered instruction sequences (e.g. "1. Do this\n2. Do that\n3. …")
_NUMBERED_INSTR_RE = re.compile(
    r"(?:^|\n)\s*\d+\.\s+\w.+(?:\n\s*\d+\.\s+\w.+){3,}", re.MULTILINE
)

DEFAULT_MAX_OUTPUT_LENGTH = 5_000


def scan_output(
    text: str,
    *,
    max_length: int = DEFAULT_MAX_OUTPUT_LENGTH,
) -> InjectionScanResult:
    """Scan LLM output for system-prompt leakage or suspicious patterns."""
    result = InjectionScanResult()

    if not text:
        return result

    if len(text) > max_length:
        result.risk_score += 1
        result.signals.append(
            f"output: exceeds max length ({len(text)} > {max_length})"
        )

    for pattern in _LEAKAGE_PATTERNS:
        if pattern.search(text):
            result.risk_score += 3
            result.signals.append(
                f"output: leakage pattern matched: {pattern.pattern[:60]}"
            )
            break

    return result
