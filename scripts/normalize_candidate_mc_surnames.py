#!/usr/bin/env python3
"""Normalize Mc surname capitalization in serialized candidate fields."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
FIELD_RE = re.compile(r'("(?:dem_candidate|rep_candidate|nameonballot|candidate(?:_name)?|dem_name|rep_name)"\s*:\s*")([^"]*)(")', re.IGNORECASE)
MC_RE = re.compile(r"\b[Mm][Cc][A-Za-z]+\b")


def normalize(value: str) -> str:
    def fix(match: re.Match[str]) -> str:
        token = match.group(0)
        return "Mc" + token[2].upper() + token[3:].lower()

    return MC_RE.sub(fix, value)


def main() -> None:
    changed = 0
    for path in DATA.rglob("*.json"):
        original = path.read_text(encoding="utf-8")
        updated = FIELD_RE.sub(lambda m: m.group(1) + normalize(m.group(2)) + m.group(3), original)
        if updated != original:
            path.write_text(updated, encoding="utf-8")
            changed += 1
            print(path.relative_to(ROOT))
    print(f"updated {changed} JSON files")


if __name__ == "__main__":
    main()
