#!/usr/bin/env python3
"""Aggregate RDH block-election results through a tabblock district crosswalk."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

import shapefile


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
BLOCK_FILES = {
    2016: DATA / "in_2016_gen_2020_blocks.zip",
    2018: DATA / "in_2018_gen_2020_blocks.zip",
    2020: DATA / "in_2020_gen_2020_blocks.zip",
}
OFFICES = {"president": "PRE", "governor": "GOV", "attorney_general": "ATG", "us_senate": "USS"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, choices=sorted(BLOCK_FILES), required=True)
    parser.add_argument("--office", choices=sorted(OFFICES), required=True)
    parser.add_argument("--scope", choices=["state_house", "state_senate"], required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.crosswalk = args.crosswalk.resolve()
    args.output = args.output.resolve()

    with args.crosswalk.open(newline="", encoding="utf-8-sig") as handle:
        crosswalk = {row["GEOID20"]: (row["district_num"], float(row.get("weight") or 1)) for row in csv.DictReader(handle)}

    reader = shapefile.Reader(str(BLOCK_FILES[args.year]))
    fields = [field[0] for field in reader.fields[1:]]
    indexes = {name: i for i, name in enumerate(fields)}
    block_id_index = indexes["GEOID20"]
    prefix = f"G{str(args.year)[-2:]}{OFFICES[args.office]}"
    vote_columns = []
    for field, index in indexes.items():
        if not field.startswith(prefix):
            continue
        party = "dem" if field[len(prefix):len(prefix) + 1] == "D" else "rep" if field[len(prefix):len(prefix) + 1] == "R" else "other"
        vote_columns.append((index, party))

    totals = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})
    blocks_used = 0
    for record in reader.records():
        block_id = str(record[block_id_index])
        assignment = crosswalk.get(block_id)
        if not assignment:
            continue
        district, weight = assignment
        for index, party in vote_columns:
            totals[district][party] += float(record[index] or 0) * weight
        blocks_used += 1

    results = {}
    for district in sorted(totals, key=lambda value: int(value)):
        dem, rep, other = (round(totals[district][party]) for party in ("dem", "rep", "other"))
        total = dem + rep + other
        if not total:
            continue
        margin = rep - dem
        winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        results[district] = {"dem_votes": dem, "rep_votes": rep, "other_votes": other, "total_votes": total,
                             "dem_candidate": "Democratic candidate", "rep_candidate": "Republican candidate",
                             "margin": margin, "margin_pct": round(margin / total * 100, 4), "winner": winner,
                             "color": "#dc2626" if winner == "REP" else "#2563eb" if winner == "DEM" else "#64748b"}

    payload = {"meta": {"scope": args.scope,
                         "contest_type": args.office, "year": args.year, "districts": len(results),
                         "match_coverage_pct": 100.0,
                         "allocation": "tabblock_crosswalk",
                         "precinct_rows": None,
                         "precinct_geom_match_pct": None,
                         "crosswalk": str(args.crosswalk.relative_to(ROOT)).replace("\\", "/"),
                         "source": str(BLOCK_FILES[args.year].relative_to(ROOT)).replace("\\", "/"),
                         "calibration": {"enabled": False, "method": "tabblock_crosswalk", "calibration_csv": None},
                         "blocks_used": blocks_used},
               "general": {"results": results}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output}: {len(results)} districts from {blocks_used} blocks")


if __name__ == "__main__":
    main()
