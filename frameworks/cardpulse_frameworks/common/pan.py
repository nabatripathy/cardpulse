"""Card-number (PAN) detection used by the PCI guard (DQ-02).

CardPulse never stores clear card numbers. Every value in a PAN-guarded column is
checked here before it can reach Bronze, and every quarantined record is redacted
with :func:`redact` before it is written anywhere.

A value is treated as a PAN when it is 13-19 digits (spaces or dashes allowed
between digits) and passes the Luhn checksum. Tokens issued for CardPulse are
Luhn-invalid by construction, so they never trip the guard.
"""

from __future__ import annotations

import re

# 13-19 digits, optionally separated by single spaces or dashes, not touching other digits.
_CANDIDATE = re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")
REDACTION = "[PAN-REDACTED]"


def _luhn_sum_ok(number: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(number)):
        d = ord(ch) - 48
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def luhn_valid(number: str) -> bool:
    """Return True when ``number`` is 13-19 digits and passes the Luhn checksum."""
    return number.isdigit() and 13 <= len(number) <= 19 and _luhn_sum_ok(number)


def luhn_check_digit(partial: str) -> int:
    """Return the digit that makes ``partial + digit`` pass the Luhn checksum."""
    return next(d for d in range(10) if _luhn_sum_ok(f"{partial}{d}"))


def is_pan(value: str | None) -> bool:
    """Exact check: the whole value (ignoring spaces and dashes) is a PAN."""
    if not value:
        return False
    return luhn_valid(value.replace(" ", "").replace("-", ""))


def contains_pan(text: str | None) -> bool:
    """Scan check: any 13-19 digit run inside free text is a PAN."""
    if not text:
        return False
    return any(luhn_valid(re.sub(r"[ -]", "", m.group())) for m in _CANDIDATE.finditer(text))


def redact(text: str) -> str:
    """Replace every Luhn-valid digit run in ``text`` with a redaction marker."""

    def _sub(m: re.Match[str]) -> str:
        return REDACTION if luhn_valid(re.sub(r"[ -]", "", m.group())) else m.group()

    return _CANDIDATE.sub(_sub, text)
