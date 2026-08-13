#!/usr/bin/env python3
"""Replace 2022 county-locality allocations with DRA precinct estimates.

DRA provides Marion precinct-level 2022 estimates. This script scales those
estimates to official county locality totals from AllOfficeResults.json, then
applies the existing House/Senate precinct crosswalk. Counties without enough
usable DRA/crosswalk coverage remain on the baseline VAP allocation.
"""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import shapefile

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
DRA = DATA / "sources/dra/in_v07/IN_2020_VD_tabblock.vtd.datasets.geojson"
SOURCE = DATA / "AllOfficeResults.json"
BLOCKS = DATA / "in_2020_gen_2020_blocks.zip"
OFFICES = {"auditor": "E_22_AUD", "secretary_of_state": "E_22_SOS", "treasurer": "E_22_TREAS", "us_senate": "E_22_SEN"}


def norm(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def precinct_aliases(value: str) -> set[str]:
    key = norm(value)
    aliases = {key}
    if key.isdigit():
        aliases.add(key.lstrip("0") or "0")
    aliases.add(re.sub(r"0+(\d+)$", r"\1", key))
    return aliases


def winner(dem: int, rep: int) -> tuple[str, str]:
    margin = rep - dem
    if margin > 0: return "REP", "#dc2626"
    if margin < 0: return "DEM", "#2563eb"
    return "TIE", "#64748b"


def main() -> None:
    source_rows = json.loads(SOURCE.read_text(encoding="utf-8"))
    county_features = json.loads((DATA / "census" / "tl_2020_18_county20.geojson").read_text(encoding="utf-8"))["features"]
    source_county_names = {norm(row.get("ReportingCountyName")): row.get("ReportingCountyName") for row in source_rows if row.get("ReportingCountyName")}
    fips_to_county = {}
    for feature in county_features:
        fips = str(feature["properties"].get("COUNTYFP20", "")).zfill(3)
        name = feature["properties"].get("NAME20", "")
        if norm(name) in source_county_names:
            fips_to_county[fips] = source_county_names[norm(name)]
    dra_obj = json.loads(DRA.read_text(encoding="utf-8"))
    dra_fips = {str(feature.get("properties", {}).get("id", ""))[2:5] for feature in dra_obj.get("features", []) if str(feature.get("properties", {}).get("id", "")).startswith("18")}
    counties = {fips_to_county[fips]: fips for fips in dra_fips if fips in fips_to_county}
    official = defaultdict(lambda: defaultdict(float))
    for row in source_rows:
        if row.get("ReportingCountyName") not in counties or row.get("DataEntryLevelName") != "Locality": continue
        # Use the human-readable office field because source labels vary slightly.
        match = {"Auditor Of State": "auditor", "Secretary Of State": "secretary_of_state", "Treasurer Of State": "treasurer", "United States Senator From Indiana": "us_senate"}.get(row.get("Office"))
        if not match: continue
        county = row["ReportingCountyName"]
        party = str(row.get("PoliticalParty", "")).upper()
        bucket = "dem" if party == "DEMOCRATIC" else "rep" if party == "REPUBLICAN" else "other"
        official[county, match][bucket] += float(row.get("TotalVotes") or 0)

    crosswalks = {}
    for scope in ("state_house", "state_senate"):
        by_precinct = defaultdict(list)
        with (DATA / f"crosswalks/precinct_to_2022_{scope}.csv").open(encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                county_key, precinct = row["precinct_key"].split("|", 1)
                county = source_county_names.get(norm(row["county"]))
                if county in counties:
                    for alias in precinct_aliases(precinct):
                        by_precinct[county, alias].append((row["district_num"], float(row["area_weight"])))
        crosswalks[scope] = by_precinct

    # Reconstruct each county contribution used by the baseline builder. It
    # distributes the official county locality total over county tabblocks by
    # VAP_MOD, then assigns each block through the tabblock crosswalk.
    old_vap = defaultdict(lambda: defaultdict(float))
    by_geoid = {}
    with shapefile.Reader(str(BLOCKS)) as reader:
        fields = [f[0] for f in reader.fields[1:]]
        idx = {name: fields.index(name) for name in fields}
        for record in reader.records():
            county_fips = str(record[idx["COUNTYFP"]]).zfill(3)
            if county_fips not in counties.values(): continue
            vap = float(record[idx["VAP_MOD"]] or 0)
            geoid = str(record[idx["GEOID20"]])
            for scope in crosswalks:
                crosswalk_path = DATA / f"crosswalks/tabblock20_to_2022_{scope}.csv"
                # Cache the GEOID lookup on the first block pass.
                if scope not in by_geoid:
                    with crosswalk_path.open(encoding="utf-8-sig") as handle:
                        by_geoid[scope] = {r["GEOID20"]: r["district_num"] for r in csv.DictReader(handle)}
                district = by_geoid[scope].get(geoid)
                if district: old_vap[scope, county_fips][district] += vap

    old_county = {scope: defaultdict(float) for scope in crosswalks}
    for scope in crosswalks:
        for county, fips in counties.items():
            total_vap = sum(old_vap[scope, fips].values()) or 1
            for district, vap in old_vap[scope, fips].items():
                for office in OFFICES:
                    for party in ("dem", "rep", "other"):
                        old_county[scope][county, district, office, party] = official[county, office][party] * vap / total_vap

    dra_dist = {scope: {office: defaultdict(lambda: defaultdict(float)) for office in OFFICES} for scope in crosswalks}
    for feature in dra_obj.get("features", []):
        props = feature.get("properties", {})
        county_fips = str(props.get("id", ""))[:5]
        county = next((name for name, fips in counties.items() if fips == county_fips[-3:]), None)
        if not county: continue
        key_aliases = [(county, alias) for alias in precinct_aliases(props.get("name", ""))]
        for scope, by_precinct in crosswalks.items():
            matched = []
            for key in key_aliases:
                matched.extend(by_precinct.get(key, []))
            for district, weight in matched:
                for office, dataset_key in OFFICES.items():
                    data = props.get("datasets", {}).get(dataset_key, {})
                    dem, rep = float(data.get("Dem", 0)), float(data.get("Rep", 0))
                    total = float(data.get("Total", dem + rep))
                    dra_dist[scope][office][county, district]["dem"] += dem * weight
                    dra_dist[scope][office][county, district]["rep"] += rep * weight
                    dra_dist[scope][office][county, district]["other"] += max(0.0, total - dem - rep) * weight

    for scope in crosswalks:
        for office in OFFICES:
            for county in counties:
                target = official[county, office]
                raw_total = {party: sum(raw[party] for (raw_county, _), raw in dra_dist[scope][office].items() if raw_county == county) for party in ("dem", "rep", "other")}
                scale = {party: (target[party] / raw_total[party] if raw_total[party] else 0) for party in raw_total}
                path = DATA / f"district_contests/{scope}_{office}_2022.json"
                obj = json.loads(path.read_text(encoding="utf-8")); results = obj["general"]["results"]
                for (raw_county, district), raw in dra_dist[scope][office].items():
                    if raw_county != county or district not in results: continue
                    dem = round(results[district]["dem_votes"] + raw["dem"] * scale["dem"] - old_county[scope][county, district, office, "dem"])
                    rep = round(results[district]["rep_votes"] + raw["rep"] * scale["rep"] - old_county[scope][county, district, office, "rep"])
                    other = round(results[district]["other_votes"] + raw["other"] * scale["other"] - old_county[scope][county, district, office, "other"])
                    total = dem + rep + other
                    if total <= 0: continue
                    margin = rep - dem; win, color = winner(dem, rep)
                    row = results[district]
                    row.update({"dem_votes": dem, "rep_votes": rep, "other_votes": other, "total_votes": total, "margin": margin, "margin_pct": round(margin / total * 100, 4), "winner": win, "color": color})
                obj["meta"]["allocation"] = "2022_dra_precinct_scaled_to_official_locality_totals_with_vap_fallback"
                obj["meta"]["county_sources"] = {name: "Data/sources/dra/in_v07/IN_2020_VD_tabblock.vtd.datasets.geojson" for name in counties}
                path.write_text(json.dumps(obj, indent=2) + "\n")
                print(scope, office, county, "official", dict(target))


if __name__ == "__main__":
    main()
