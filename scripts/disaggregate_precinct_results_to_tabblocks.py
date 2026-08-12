#!/usr/bin/env python3
"""Allocate precinct election results to tabblocks using an external crosswalk.

This is an exploratory allocation tool, not a replacement for precinct-based
district margins. It preserves each precinct's reported totals and allocates
them to blocks by the supplied weight (normally CVAP, voting-age population,
or registered voters).

Required precinct CSV columns:
  precinct_key, dem, rep
Optional: other, total

Required crosswalk CSV columns:
  block_geoid, precinct_key, block_weight
Optional district columns:
  district_num, district_weight

The crosswalk must contain all blocks that should receive a precinct's votes.
Weights are normalized within each precinct. If district columns are present,
the script also reports modeled district totals after block allocation.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def number(value: str | None) -> float:
    try:
        return float(value or 0)
    except ValueError:
        return 0.0


def read_precinct_results(path: Path) -> dict[str, dict[str, float]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = csv.DictReader(handle)
        required = {"precinct_key", "dem", "rep"}
        missing = required - set(rows.fieldnames or [])
        if missing:
            raise ValueError(f"{path}: missing columns: {', '.join(sorted(missing))}")
        out = {}
        for row in rows:
            key = (row.get("precinct_key") or "").strip()
            if not key:
                continue
            dem = number(row.get("dem"))
            rep = number(row.get("rep"))
            other = number(row.get("other"))
            total = number(row.get("total")) or dem + rep + other
            out[key] = {"dem": dem, "rep": rep, "other": other, "total": total}
        return out


def read_crosswalk(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = csv.DictReader(handle)
        required = {"block_geoid", "precinct_key", "block_weight"}
        missing = required - set(rows.fieldnames or [])
        if missing:
            raise ValueError(f"{path}: missing columns: {', '.join(sorted(missing))}")
        return [row for row in rows if (row.get("block_geoid") or "").strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--precinct-results", type=Path, required=True)
    parser.add_argument("--crosswalk", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    precincts = read_precinct_results(args.precinct_results)
    crosswalk = read_crosswalk(args.crosswalk)
    by_precinct: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in crosswalk:
        by_precinct[(row.get("precinct_key") or "").strip()].append(row)

    blocks: dict[str, dict[str, object]] = {}
    modeled_districts: dict[str, dict[str, float]] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0})
    missing_precincts = []

    for precinct_key, result in precincts.items():
        rows = by_precinct.get(precinct_key, [])
        positive = [(row, max(0.0, number(row.get("block_weight")))) for row in rows]
        weight_total = sum(weight for _, weight in positive)
        if weight_total <= 0:
            missing_precincts.append(precinct_key)
            continue
        for row, weight in positive:
            share = weight / weight_total
            block_id = (row.get("block_geoid") or "").strip()
            allocated = {party: result[party] * share for party in ("dem", "rep", "other", "total")}
            blocks[block_id] = {
                "block_geoid": block_id,
                "precinct_key": precinct_key,
                **allocated,
                "allocation_weight": share,
            }
            district = (row.get("district_num") or "").strip()
            if district:
                district_share = number(row.get("district_weight")) or 1.0
                for party in ("dem", "rep", "other", "total"):
                    modeled_districts[district][party] += allocated[party] * district_share

    payload = {
        "meta": {
            "method": "precinct_results_allocated_to_tabblocks_by_crosswalk_weight",
            "warning": "Modeled block estimates; use precinct-crosswalk totals for official district margins.",
            "precincts_input": len(precincts),
            "blocks_output": len(blocks),
            "precincts_without_crosswalk": missing_precincts,
        },
        "blocks": sorted(blocks.values(), key=lambda row: str(row["block_geoid"])),
    }
    if modeled_districts:
        payload["modeled_districts"] = dict(sorted(modeled_districts.items(), key=lambda item: item[0]))
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
