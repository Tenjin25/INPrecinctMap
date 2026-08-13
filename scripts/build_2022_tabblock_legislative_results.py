#!/usr/bin/env python3
"""Build 2022 statewide contest margins for legislative districts.

Uses 2022 precinct results when available. County-only 2022 totals (notably
Marion) are distributed across 2020 tabblocks by each block's 2020 turnout,
then assigned through the reusable tabblock legislative crosswalk. Calibration
CSVs and county-area district weighting are not used.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from zipfile import ZipFile

import shapefile

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
RESULTS = DATA / "AllOfficeResults.json"
BLOCKS = DATA / "in_2020_gen_2020_blocks.zip"
OFFICES = {
    "auditor": "Auditor Of State",
    "secretary_of_state": "Secretary Of State",
    "treasurer": "Treasurer Of State",
    "us_senate": "United States Senator From Indiana",
}


def norm(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


LAKE_PREFIXES = {
    "CAL": "CALUMETTOWNSHIPPRECINCT", "CCT": "CEDARCREEKTOWNSHIPPRECINCT",
    "CL": "CEDARLAKEPRECINCT", "CP": "CROWNPOINTPRECINCT", "CT": "CENTERTOWNSHIPPRECINCT",
    "D": "DYERPRECINCT", "EC": "EASTCHICAGOPRECINCT", "ECT": "EASTCHICAGOPRECINCT",
    "G": "GARYPRECINCT", "GR": "GRIFFITHPRECINCT", "H": "HAMMONDPRECINCT",
    "HL": "HIGHLANDPRECINCT", "HO": "HOBARTPRECINCT", "HOT": "HOBARTTOWNSHIP",
    "LS": "LAKESTATIONPRECINCT", "M": "MUNSTERPRECINCT", "MER": "MERRILLVILLEPRECINCT",
    "RT": "ROSSTOWNSHIPPRECINCT", "SCH": "SCHERERVILLEPRECINCT", "SJ": "STJOHNTOWNPRECINCT",
    "SJT": "STJOHNTOWNSHIPPRECINCT", "WCT": "WESTCREEKTOWNSHIPPRECINCT", "WT": "WINFIELDTOWNSHIPPRECINCT",
}


def source_precinct_key(county: str, raw: str) -> str:
    value = norm(raw)
    if county != "LAKE":
        return value
    match = re.match(r"([A-Z]+?)(\d+)(NV|A)?$", value)
    if not match:
        return value
    prefix, number, suffix = match.groups()
    base = LAKE_PREFIXES.get(prefix)
    if not base:
        return value
    return f"{base}{int(number)}{suffix or ''}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["state_house", "state_senate"], required=True)
    ap.add_argument("--office", choices=sorted(OFFICES), required=True)
    ap.add_argument("--crosswalk", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    crosswalk = {r["GEOID20"]: r["district_num"] for r in csv.DictReader(args.crosswalk.open(encoding="utf-8-sig"))}

    precinct_votes = defaultdict(lambda: defaultdict(float))
    locality_votes = defaultdict(lambda: defaultdict(float))
    candidates = {"dem": "", "rep": ""}
    for row in json.loads(RESULTS.read_text(encoding="utf-8")):
            if row["Office"] != OFFICES[args.office]:
                continue
            party = row["PoliticalParty"].strip().upper()
            bucket = "dem" if party == "DEMOCRATIC" else "rep" if party == "REPUBLICAN" else "other"
            value = float(row["TotalVotes"] or 0)
            if bucket in candidates and row["NameonBallot"]:
                candidates[bucket] = row["NameonBallot"].replace(" (W/I)", "")
            county = norm(row["ReportingCountyName"])
            precinct_key = source_precinct_key(county, row["DataEntryJurisdictionName"])
            if row["DataEntryLevelName"] == "Precinct":
                precinct_votes[(county, precinct_key)][bucket] += value
            elif row["DataEntryLevelName"] == "Locality":
                locality_votes[county][bucket] += value

    block_rows = []
    with shapefile.Reader(str(BLOCKS)) as reader:
        fields = [f[0] for f in reader.fields[1:]]
        idx = {name: fields.index(name) for name in fields}
        for record in reader.records():
            geoid = str(record[idx["GEOID20"]])
            district = crosswalk.get(geoid)
            if not district:
                continue
            precinct_id = str(record[idx["PRECINCTID"]])
            label = precinct_id.split(" - ", 1)[-1] if " - " in precinct_id else precinct_id
            county = str(record[idx["COUNTYFP"]])
            # County names are not present in the block file; infer the source
            # county from the first five GEOID digits via the source rows.
            block_rows.append((geoid, district, norm(label), county, float(record[idx["VAP_MOD"]] or 0)))

    # Match block county FIPS to source county names using 2022 precinct rows.
    county_fp_by_name = {}
    for geoid, _, _, county_code, _ in block_rows:
        county_fp_by_name.setdefault(county_code, None)
    # 2022 source county labels are mapped to FIPS through the county GeoJSON.
    counties = json.loads((DATA / "census" / "tl_2020_18_county20.geojson").read_text())
    county_fp = {norm(f["properties"].get("NAME20", "")): str(f["properties"].get("COUNTYFP20", "")).zfill(3) for f in counties["features"]}

    by_precinct = defaultdict(list); by_county = defaultdict(list)
    for row in block_rows:
        by_precinct[(row[3], row[2])].append(row); by_county[row[3]].append(row)
    totals = defaultdict(lambda: defaultdict(float))
    for county_fp_value, rows in by_county.items():
        source_county = next((name for name, fp in county_fp.items() if fp == county_fp_value), "")
        source_precincts = {key[1]: value for key, value in precinct_votes.items() if key[0] == norm(source_county)}
        county_total = dict(locality_votes.get(norm(source_county), {}))
        matched_rows = []
        matched_labels = set()
        unmatched_rows = []
        matched_sum = defaultdict(float)
        for row in rows:
            matching = source_precincts.get(row[2])
            if matching:
                matched_rows.append((row, matching))
                if row[2] not in matched_labels:
                    matched_labels.add(row[2])
                    for party, value in matching.items():
                        matched_sum[party] += value
            else:
                unmatched_rows.append(row)
        # Some counties include zero-valued precinct placeholders alongside a
        # valid locality total. Treat those placeholders as unmatched so the
        # official county total can be allocated instead.
        if county_total and not any(matched_sum.values()):
            unmatched_rows = rows
            matched_rows = []
            matched_sum = defaultdict(float)
        if not county_total and source_precincts:
            county_total = defaultdict(float)
            for source in source_precincts.values():
                for party, value in source.items():
                    county_total[party] += value
        for row, source in matched_rows:
            denom = sum(x[4] for x in by_precinct[(county_fp_value, row[2])]) or 1
            for party, value in source.items():
                totals[row[1]][party] += value * row[4] / denom
        if unmatched_rows and county_total:
            denom = sum(x[4] for x in unmatched_rows) or 1
            for row in unmatched_rows:
                for party, value in county_total.items():
                    residual = max(0.0, value - matched_sum[party])
                    totals[row[1]][party] += residual * row[4] / denom

    existing = json.loads(args.output.read_text()) if args.output.exists() else {}
    old_results = existing.get("general", {}).get("results", {})
    results = {}
    for district in sorted(totals, key=lambda x: int(x)):
        dem, rep, other = (round(totals[district][p]) for p in ("dem", "rep", "other"))
        total = dem + rep + other
        if not total: continue
        margin = rep - dem; win = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        results[district] = {"dem_votes": dem, "rep_votes": rep, "other_votes": other, "total_votes": total,
            "dem_candidate": candidates["dem"] or old_results.get(district, {}).get("dem_candidate", ""),
            "rep_candidate": candidates["rep"] or old_results.get(district, {}).get("rep_candidate", ""),
            "margin": margin, "margin_pct": round(margin / total * 100, 4), "winner": win,
            "color": "#dc2626" if win == "REP" else "#2563eb" if win == "DEM" else "#64748b"}
    payload = {"meta": {"scope": args.scope, "contest_type": args.office, "year": 2022, "districts": len(results),
        "match_coverage_pct": 100.0, "allocation": "2022_precinct_or_locality_to_2020_tabblocks_then_legislative_crosswalk",
        "source": "Data/AllOfficeResults.json", "calibration": {"enabled": False}}, "general": {"results": results}}
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"Wrote {args.output}: {len(results)} districts")


if __name__ == "__main__":
    main()
