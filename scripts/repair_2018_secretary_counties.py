#!/usr/bin/env python
"""Repair four 2018 Secretary of State county rows from candidate-level returns.

Source: https://github.com/openelections/openelections-data-in/blob/master/2018/20181106__in__general__county.csv
The county file transcribes Indiana Election Division results. Preserve other
counties because that source's county coverage differs from this snapshot.
"""

import csv
import json
import math
from collections import defaultdict
from pathlib import Path


DATA = Path(__file__).resolve().parents[1] / "data"
CONTEST = DATA / "contests" / "secretary_of_state_2018.json"
MANIFEST = DATA / "contests" / "manifest.json"
AGGREGATE = DATA / "in_elections_aggregated.json"
CORRECTIONS = DATA / "corrections" / "secretary_of_state_2018_counties.csv"
DISTRICT_DIR = DATA / "district_contests"
CROSSWALKS = {
    "congressional": DATA / "crosswalks" / "county_to_cd118.csv",
    "state_house": DATA / "crosswalks" / "county_to_2022_state_house.csv",
    "state_senate": DATA / "crosswalks" / "county_to_2022_state_senate.csv",
}


def update_result(row, counts):
    dem, rep, other = counts
    total = dem + rep + other
    margin = rep - dem
    winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
    row.update({
        "dem_votes": dem,
        "rep_votes": rep,
        "other_votes": other,
        "total_votes": total,
        "margin": margin,
        "margin_pct": round(margin / total * 100, 4) if total else 0.0,
        "winner": winner,
    })
    color = "#dc2626" if winner == "REP" else "#2563eb" if winner == "DEM" else "#64748b"
    if "competitiveness" in row:
        row["competitiveness"]["color"] = color
    else:
        row["color"] = color


def write(path, payload):
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def allocate(total, weights):
    portions = {district: total * weight for district, weight in weights.items()}
    floor = {district: math.floor(value) for district, value in portions.items()}
    remainder = total - sum(floor.values())
    for district in sorted(portions, key=lambda district: (-(portions[district] - floor[district]), district))[:remainder]:
        floor[district] += 1
    return floor


def district_weights(path, county):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        weights = {
            int(row["district_num"]): float(row["area_weight"])
            for row in csv.DictReader(handle) if row["county"] == county and float(row["area_weight"]) > 0
        }
    if not weights:
        raise ValueError(f"Missing district crosswalk for {county}: {path}")
    total = sum(weights.values())
    return {district: weight / total for district, weight in weights.items()}


def main():
    with CORRECTIONS.open(encoding="utf-8", newline="") as handle:
        correction_rows = {row["county"]: row for row in csv.DictReader(handle)}
    corrections = {
        county: tuple(int(row[key]) for key in ("dem_votes", "rep_votes", "other_votes"))
        for county, row in correction_rows.items()
    }
    contest = json.loads(CONTEST.read_text(encoding="utf-8"))
    aggregate = json.loads(AGGREGATE.read_text(encoding="utf-8"))
    rows = {row["county"]: row for row in contest["rows"]}
    agg_rows = aggregate["results_by_year"]["2018"]["secretary_of_state"]["secretary_of_state"]["results"]
    for county, counts in corrections.items():
        update_result(rows[county], counts)
        update_result(agg_rows[county], counts)
    for party in ("dem", "rep", "other"):
        contest["meta"][f"{party}_total"] = sum(row[f"{party}_votes"] for row in contest["rows"])
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    entry = next(row for row in manifest["files"] if row["file"] == CONTEST.name)
    for party in ("dem", "rep", "other"):
        entry[f"{party}_total"] = contest["meta"][f"{party}_total"]
    write(CONTEST, contest)
    write(MANIFEST, manifest)
    write(AGGREGATE, aggregate)

    district_aggregate_path = DATA / "in_district_results_2022_lines.json"
    district_aggregate = json.loads(district_aggregate_path.read_text(encoding="utf-8"))
    district_manifest_path = DISTRICT_DIR / "manifest.json"
    district_manifest = json.loads(district_manifest_path.read_text(encoding="utf-8"))
    for scope, crosswalk_path in CROSSWALKS.items():
        path = DISTRICT_DIR / f"{scope}_secretary_of_state_2018.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not payload["meta"].get("candidate_party_correction_counties"):
            results = payload["general"]["results"]
            for county, row in correction_rows.items():
                weights = district_weights(crosswalk_path, county)
                for party in ("dem", "rep", "other"):
                    new_count = int(row[f"{party}_votes"])
                    old_count = int(row[f"old_{party}_votes"])
                    new_split = allocate(new_count, weights)
                    old_split = allocate(old_count, weights)
                    for district in weights:
                        results[str(district)][f"{party}_votes"] += new_split[district] - old_split[district]
            for result in results.values():
                update_result(result, tuple(result[f"{party}_votes"] for party in ("dem", "rep", "other")))
            payload["meta"]["candidate_party_correction_counties"] = sorted(corrections)
            payload["meta"]["candidate_party_correction_allocation"] = "county_area_weights_for_split_counties"
            write(path, payload)
        district_aggregate["results_by_year"]["2018"][scope]["secretary_of_state"] = payload
        manifest_row = next(row for row in district_manifest["files"] if row["file"] == path.name)
        for party in ("dem", "rep", "other"):
            manifest_row[f"{party}_total"] = sum(r[f"{party}_votes"] for r in payload["general"]["results"].values())
    write(district_aggregate_path, district_aggregate)
    write(district_manifest_path, district_manifest)
    print("Repaired 2018 Secretary of State results:", ", ".join(corrections))


if __name__ == "__main__":
    main()
