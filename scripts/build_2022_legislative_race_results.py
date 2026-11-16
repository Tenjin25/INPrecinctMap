#!/usr/bin/env python3
"""Rebuild 2022 State House/Senate race results from AllOfficeResults.json.

The source rows already identify the legislative district. Precinct and
locality rows are therefore summed directly into that district; locality rows
(including Marion) are not treated as county-wide margins for every district.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
SOURCE = DATA / "AllOfficeResults.json"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["state_house", "state_senate"], required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    office_prefix = "State Representative, District " if args.scope == "state_house" else "State Senator, District "
    totals = defaultdict(lambda: defaultdict(float))
    names = defaultdict(dict)

    for row in json.loads(SOURCE.read_text(encoding="utf-8")):
        office = str(row.get("Office", ""))
        if not office.startswith(office_prefix):
            continue
        match = re.search(r"District\s+(\d+)$", office)
        if not match:
            continue
        district = str(int(match.group(1)))
        party = str(row.get("PoliticalParty", "")).strip().upper()
        bucket = "dem" if party == "DEMOCRATIC" else "rep" if party == "REPUBLICAN" else "other"
        totals[district][bucket] += float(row.get("TotalVotes") or 0)
        if bucket in ("dem", "rep") and row.get("NameonBallot"):
            names[district][bucket] = str(row["NameonBallot"]).replace(" (W/I)", "")

    results = {}
    for district in sorted(totals, key=lambda value: int(value)):
        dem, rep, other = (round(totals[district][party]) for party in ("dem", "rep", "other"))
        total = dem + rep + other
        if total <= 0:
            continue
        margin = rep - dem
        winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        results[district] = {
            "dem_votes": dem, "rep_votes": rep, "other_votes": other, "total_votes": total,
            "dem_candidate": names[district].get("dem", ""), "rep_candidate": names[district].get("rep", ""),
            "margin": margin, "margin_pct": round(margin / total * 100, 4), "winner": winner,
            "color": "#dc2626" if winner == "REP" else "#2563eb" if winner == "DEM" else "#64748b",
        }

    payload = {
        "meta": {"scope": args.scope, "contest_type": args.scope, "year": 2022,
                 "districts": len(results), "match_coverage_pct": 100.0,
                 "allocation": "district-specific_precinct_and_locality_sum",
                 "source": "Data/AllOfficeResults.json", "calibration": {"enabled": False}},
        "general": {"results": results},
    }
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}: {len(results)} districts")


if __name__ == "__main__":
    main()
