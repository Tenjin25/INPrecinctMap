#!/usr/bin/env python
"""Give candidate fields consistent capitalization across generated election JSONs."""

import json
import re
from collections import Counter, defaultdict
from pathlib import Path


DATA = Path(__file__).resolve().parents[1] / "data"
NAME_INITIALS = {"JD"}
FILES = sorted((DATA / "contests").glob("*.json")) + sorted((DATA / "district_contests").glob("*.json"))
FILES += [DATA / "in_elections_aggregated.json", DATA / "in_district_results_2022_lines.json"]


def candidate_fields(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key.endswith("_candidate") and isinstance(value, str) and value.strip():
                yield node, key, value
            else:
                yield from candidate_fields(value)
    elif isinstance(node, list):
        for value in node:
            yield from candidate_fields(value)


def display_case(value):
    def fix_word(match):
        word = match.group()
        if word.upper() in NAME_INITIALS:
            return word.upper()
        if word.isupper() and len(word) > 1:
            if word in {"II", "III", "IV", "VI", "VII", "VIII", "IX", "X"}:
                return word
            return word.capitalize()
        return word

    return re.sub(r"[A-Za-z]+", fix_word, value)


def preference(value):
    # Prefer a spelling already written in normal case over a derived one.
    uppercase_words = sum(
        len(word) > 1 and word.isupper() and word not in NAME_INITIALS
        for word in re.findall(r"[A-Za-z]+", value)
    )
    return uppercase_words == 0


def main():
    documents = {path: json.loads(path.read_text(encoding="utf-8")) for path in FILES}
    variants = defaultdict(Counter)
    for payload in documents.values():
        for _node, _key, value in candidate_fields(payload):
            variants[value.casefold()][value] += 1

    canonical = {}
    for name_key, counts in variants.items():
        canonical[name_key] = sorted(counts, key=lambda name: (-preference(name), -counts[name], name))[0]
        canonical[name_key] = re.sub(r"\s+", " ", display_case(canonical[name_key])).strip()

    changed_files = changed_names = 0
    for path, payload in documents.items():
        changes = 0
        for node, key, value in candidate_fields(payload):
            formatted = canonical[value.casefold()]
            if formatted != value:
                node[key] = formatted
                changes += 1
        if changes:
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            changed_files += 1
            changed_names += changes
    print(f"Normalized {changed_names} candidate fields in {changed_files} JSON files.")


if __name__ == "__main__":
    main()
