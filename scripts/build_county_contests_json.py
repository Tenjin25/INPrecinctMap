#!/usr/bin/env python
"""
Build a single county-level contest JSON from Data/contests/*.json slices.

Output:
  - Data/county_contests.json
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
CONTESTS_DIR = DATA_DIR / "contests"
MANIFEST_PATH = CONTESTS_DIR / "manifest.json"
OUT_PATH = DATA_DIR / "county_contests.json"


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def build() -> None:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Missing manifest: {MANIFEST_PATH}")

    manifest_obj = load_json(MANIFEST_PATH)
    files: List[Dict[str, Any]] = list(manifest_obj.get("files", []))
    files_sorted = sorted(
        files,
        key=lambda x: (int(x.get("year", 0)), str(x.get("contest_type", ""))),
    )

    county_results_by_year: Dict[str, Dict[str, Any]] = {}
    total_rows = 0

    for entry in files_sorted:
        year = str(entry.get("year", ""))
        contest_type = str(entry.get("contest_type", "")).strip()
        filename = str(entry.get("file", "")).strip()
        if not year or not contest_type or not filename:
            continue

        contest_path = CONTESTS_DIR / filename
        if not contest_path.exists():
            continue

        payload = load_json(contest_path)
        rows = payload.get("rows", [])
        meta = payload.get("meta", {})
        total_rows += len(rows) if isinstance(rows, list) else 0

        year_node = county_results_by_year.setdefault(year, {})
        year_node[contest_type] = {
            "meta": {
                "contest_type": meta.get("contest_type", contest_type),
                "year": int(meta.get("year", int(year))),
                "rows": int(meta.get("rows", len(rows) if isinstance(rows, list) else 0)),
                "dem_total": int(meta.get("dem_total", 0)),
                "rep_total": int(meta.get("rep_total", 0)),
                "other_total": int(meta.get("other_total", 0)),
                "major_party_contested": bool(meta.get("major_party_contested", True)),
            },
            "rows": rows if isinstance(rows, list) else [],
        }

    out = {
        "meta": {
            "state": "Indiana",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_manifest": str(MANIFEST_PATH.relative_to(ROOT)).replace("\\", "/"),
            "contest_files": len(files_sorted),
            "county_rows": total_rows,
        },
        "manifest": files_sorted,
        "county_results_by_year": county_results_by_year,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(out, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    build()

