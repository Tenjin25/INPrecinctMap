#!/usr/bin/env python3
"""Repair Marion's 2022 congressional statewide-contest allocation.

The congressional district files already contain precinct-based statewide
results. This script replaces Marion's county-area contribution in CD-06 and
CD-07 with DRA precinct estimates scaled to Marion's official totals.
"""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
DRA = DATA / "sources/dra/in_v07/IN_2020_VD_tabblock.vtd.datasets.geojson"
SOURCE = DATA / "AllOfficeResults.json"
OFFICES = {"auditor": "Auditor Of State", "secretary_of_state": "Secretary Of State", "treasurer": "Treasurer Of State", "us_senate": "United States Senator From Indiana"}


def norm(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def aliases(value: str) -> set[str]:
    key = norm(value)
    return {key, key.lstrip("0") or "0", re.sub(r"0+(\d+)$", r"\1", key)}


def winner(dem: int, rep: int) -> tuple[str, str]:
    margin = rep - dem
    if margin > 0: return "REP", "#dc2626"
    if margin < 0: return "DEM", "#2563eb"
    return "TIE", "#64748b"


def main() -> None:
    official = defaultdict(float)
    for row in json.loads(SOURCE.read_text(encoding="utf-8")):
        if row.get("ReportingCountyName") != "Marion" or row.get("DataEntryLevelName") != "Locality": continue
        office = next((key for key, label in OFFICES.items() if row.get("Office") == label), None)
        if not office: continue
        party = str(row.get("PoliticalParty", "")).upper()
        bucket = "dem" if party == "DEMOCRATIC" else "rep" if party == "REPUBLICAN" else "other"
        official[office, bucket] += float(row.get("TotalVotes") or 0)

    by_precinct = defaultdict(list)
    with (DATA / "crosswalks/precinct_to_cd118.csv").open(encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if row["county"] == "Marion":
                for alias in aliases(row["precinct_key"].split("|", 1)[1]):
                    by_precinct[alias].append((row["district_num"], float(row["area_weight"])))

    dra = {office: defaultdict(float) for office in OFFICES}
    for feature in json.loads(DRA.read_text(encoding="utf-8")).get("features", []):
        props = feature.get("properties", {})
        if not str(props.get("id", "")).startswith("18097"): continue
        matches = by_precinct.get(norm(props.get("name", "")), [])
        for district, weight in matches:
            if district not in {"6", "7"}: continue
            for office, dataset_key in OFFICES.items():
                data = props.get("datasets", {}).get({"auditor": "E_22_AUD", "secretary_of_state": "E_22_SOS", "treasurer": "E_22_TREAS", "us_senate": "E_22_SEN"}[office], {})
                dem, rep = float(data.get("Dem", 0)), float(data.get("Rep", 0))
                other = max(0.0, float(data.get("Total", dem + rep)) - dem - rep)
                dra[office][district, "dem"] += dem * weight
                dra[office][district, "rep"] += rep * weight
                dra[office][district, "other"] += other * weight

    for office in OFFICES:
        totals = {party: sum(dra[office][district, party] for district in ("6", "7")) for party in ("dem", "rep", "other")}
        scale = {party: official[office, party] / totals[party] if totals[party] else 0 for party in totals}
        path = DATA / f"district_contests/congressional_{office}_2022.json"
        obj = json.loads(path.read_text(encoding="utf-8")); results = obj["general"]["results"]
        # Preserve non-Marion counties; replace the old Marion split using the
        # county's congressional area weights (29.77% / 70.23%).
        old_share = {"6": 0.297731932754, "7": 0.702268035949}
        for district in ("6", "7"):
            row = results[district]
            vals = {}
            for party in ("dem", "rep", "other"):
                vals[party] = round(row[f"{party}_votes"] - official[office, party] * old_share[district] + dra[office][district, party] * scale[party])
            total = sum(vals.values()); margin = vals["rep"] - vals["dem"]; win, color = winner(vals["dem"], vals["rep"])
            row.update({"dem_votes": vals["dem"], "rep_votes": vals["rep"], "other_votes": vals["other"], "total_votes": total, "margin": margin, "margin_pct": round(margin / total * 100, 4), "winner": win, "color": color})
        obj["meta"]["marion_allocation"] = "2022_dra_precinct_scaled_to_official_locality_totals"
        path.write_text(json.dumps(obj, indent=2) + "\n")
        print(office, "CD-06/CD-07 repaired")


if __name__ == "__main__":
    main()
