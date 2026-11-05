#!/usr/bin/env python
"""Build the 2026 county and district contest JSON slices from a precinct CSV.

This is deliberately incremental: it replaces only 2026 entries and preserves
all earlier contest files and aggregates, including any hand-repaired history.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
CONTESTS = DATA / "contests"
DISTRICT_CONTESTS = DATA / "district_contests"
YEAR = 2026
RESULT_STATUS = "unofficial_incomplete"
AS_OF_DATE = "2026-11-05"
NOTE = "Post-election unofficial snapshot; outstanding mail and overseas ballots may not be included."
SOURCE = "Data/AllOfficeResults2026.json"

STATEWIDE_CONTESTS = {"secretary_of_state", "auditor", "treasurer"}
EXPECTED_DISTRICTS = {"congressional": 9, "state_house": 100, "state_senate": 50}
CROSSWALKS = {
    "congressional": (
        DATA / "crosswalks" / "precinct_to_cd118.csv",
        DATA / "crosswalks" / "county_to_cd118.csv",
    ),
    "state_house": (
        DATA / "crosswalks" / "precinct_to_2022_state_house.csv",
        DATA / "crosswalks" / "county_to_2022_state_house.csv",
    ),
    "state_senate": (
        DATA / "crosswalks" / "precinct_to_2022_state_senate.csv",
        DATA / "crosswalks" / "county_to_2022_state_senate.csv",
    ),
}

ORDINALS = {
    "FIRST": "1", "SECOND": "2", "THIRD": "3", "FOURTH": "4", "FIFTH": "5",
    "SIXTH": "6", "SEVENTH": "7", "EIGHTH": "8", "NINTH": "9", "TENTH": "10",
    "ELEVENTH": "11", "TWELFTH": "12", "THIRTEENTH": "13", "FOURTEENTH": "14",
    "FIFTEENTH": "15", "SIXTEENTH": "16", "SEVENTEENTH": "17", "EIGHTEENTH": "18",
    "NINETEENTH": "19", "TWENTIETH": "20", "TWENTY FIRST": "21",
    "TWENTY SECOND": "22", "TWENTY THIRD": "23", "TWENTY FOURTH": "24",
    "TWENTY FIFTH": "25", "TWENTY SIXTH": "26", "TWENTY SEVENTH": "27",
    "TWENTY EIGHTH": "28", "TWENTY NINTH": "29", "THIRTIETH": "30",
    "THIRTY FIRST": "31",
}


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_token(value: str) -> str:
    text = (value or "").upper().replace("_", " ")
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    text = normalize_space(text)
    text = re.sub(r"\bCOUNTY\b", "", text)
    return normalize_space(text)


def canonical_precinct(value: str) -> str:
    text = normalize_token(value)
    for phrase in sorted(ORDINALS, key=len, reverse=True):
        text = re.sub(rf"\b{re.escape(phrase)}\b", ORDINALS[phrase], text)
    text = re.sub(r"^\d+\s+", "", text)
    tokens = [t for t in text.split() if t not in {"PRECINCT", "PCT", "TOWNSHIP", "TWP"}]
    return "".join(str(int(t)) if t.isdigit() else t for t in tokens)


def party_bucket(value: str) -> str:
    party = normalize_token(value)
    if party in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC", "DEMOCRATIC PARTY"} or "DEM" in party:
        return "dem"
    if party in {"R", "REP", "REPUBLICAN", "REPUBLICAN PARTY"} or "REP" in party:
        return "rep"
    return "other"


def statewide_contest(office: str, district: str) -> str | None:
    if district:
        return None
    text = normalize_token(office)
    if "SECRETARY OF STATE" in text:
        return "secretary_of_state"
    if "AUDITOR OF STATE" in text or "STATE AUDITOR" in text or "STATE COMPTROLLER" in text:
        return "auditor"
    if "TREASURER OF STATE" in text or "STATE TREASURER" in text:
        return "treasurer"
    return None


def district_contest(office: str, district: str) -> tuple[str, str, int] | None:
    digits = re.sub(r"[^0-9]", "", district or "")
    if not digits:
        return None
    number = int(digits)
    text = normalize_token(office)
    if "US HOUSE" in text or "U S HOUSE" in text or "UNITED STATES HOUSE" in text:
        return "congressional", "us_house", number
    if "STATE HOUSE" in text or "STATE REPRESENTATIVE" in text:
        return "state_house", "state_house", number
    if "STATE SENATE" in text or "STATE SENATOR" in text:
        return "state_senate", "state_senate", number
    return None


def winner(dem: int, rep: int) -> str:
    return "REP" if rep > dem else "DEM" if dem > rep else "TIE"


def color(result: str) -> str:
    return "#dc2626" if result == "REP" else "#2563eb" if result == "DEM" else "#64748b"


def top_candidate(values: dict[str, int]) -> str:
    return sorted(values.items(), key=lambda item: (-item[1], item[0]))[0][0] if values else ""


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def result_row(dem: int, rep: int, other: int, dem_candidate: str, rep_candidate: str) -> dict[str, Any]:
    total = dem + rep + other
    margin = rep - dem
    result = winner(dem, rep)
    return {
        "dem_votes": dem,
        "rep_votes": rep,
        "other_votes": other,
        "total_votes": total,
        "dem_candidate": dem_candidate,
        "rep_candidate": rep_candidate,
        "margin": margin,
        "margin_pct": round((margin / total) * 100, 4) if total else 0.0,
        "winner": result,
        "color": color(result),
    }


def status_meta() -> dict[str, str]:
    return {
        "result_status": RESULT_STATUS,
        "as_of_date": AS_OF_DATE,
        "note": NOTE,
        "source": SOURCE,
    }


def load_crosswalk(path: Path, key_column: str) -> dict[str, list[tuple[int, float]]]:
    output: dict[str, list[tuple[int, float]]] = defaultdict(list)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = row[key_column] if key_column == "precinct_key" else normalize_token(row[key_column])
            weight = float(row["area_weight"])
            if weight > 0:
                output[key].append((int(row["district_num"]), weight))
    return output


def apportion(values: dict[int, float], target: int) -> dict[int, int]:
    floors = {district: int(math.floor(max(0.0, value))) for district, value in values.items()}
    remainder = target - sum(floors.values())
    fractions = sorted(
        ((max(0.0, values[district]) - floors[district], district) for district in floors),
        key=lambda item: (-item[0], item[1]),
    )
    for _fraction, district in fractions[:max(0, remainder)]:
        floors[district] += 1
    if remainder < 0:
        for _fraction, district in reversed(fractions):
            if remainder == 0:
                break
            take = min(floors[district], -remainder)
            floors[district] -= take
            remainder += take
    return floors


def sorted_manifest(entries: list[dict[str, Any]], *, district: bool = False) -> list[dict[str, Any]]:
    if district:
        return sorted(entries, key=lambda row: (row["scope"], int(row["year"]), row["contest_type"]))
    return sorted(entries, key=lambda row: (int(row["year"]), row["contest_type"]))


def build(input_csv: Path) -> None:
    county_votes: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"dem": 0, "rep": 0, "other": 0})
    precinct_votes: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: {"dem": 0, "rep": 0, "other": 0})
    statewide_candidates: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    district_votes: dict[tuple[str, str, int], dict[str, int]] = defaultdict(lambda: {"dem": 0, "rep": 0, "other": 0})
    district_candidates: dict[tuple[str, str, int, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))

    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            votes = int(row.get("votes") or 0)
            if votes <= 0:
                continue
            county = normalize_space(row.get("county") or "")
            precinct = normalize_space(row.get("precinct") or "")
            candidate = normalize_space(row.get("candidate") or "")
            party = party_bucket(row.get("party") or "")
            office = row.get("office") or ""
            district = row.get("district") or ""

            contest = statewide_contest(office, district)
            if contest in STATEWIDE_CONTESTS:
                county_votes[(contest, county)][party] += votes
                precinct_votes[(contest, county, precinct)][party] += votes
                if candidate:
                    statewide_candidates[(contest, party)][candidate] += votes

            mapped = district_contest(office, district)
            if mapped:
                scope, contest_type, district_num = mapped
                district_votes[(scope, contest_type, district_num)][party] += votes
                if candidate:
                    district_candidates[(scope, contest_type, district_num, party)][candidate] += votes

    contest_manifest_path = CONTESTS / "manifest.json"
    contest_manifest = json.loads(contest_manifest_path.read_text(encoding="utf-8"))["files"]
    contest_manifest = [row for row in contest_manifest if int(row.get("year", 0)) != YEAR]

    election_agg_path = DATA / "in_elections_aggregated.json"
    election_agg = json.loads(election_agg_path.read_text(encoding="utf-8"))
    election_year: dict[str, Any] = {}

    county_payloads: dict[str, dict[str, Any]] = {}
    for contest in sorted(STATEWIDE_CONTESTS):
        by_county = {county: values for (kind, county), values in county_votes.items() if kind == contest}
        dem_candidate = top_candidate(statewide_candidates[(contest, "dem")])
        rep_candidate = top_candidate(statewide_candidates[(contest, "rep")])
        rows = []
        county_results = {}
        for county in sorted(by_county):
            values = by_county[county]
            base = result_row(values["dem"], values["rep"], values["other"], dem_candidate, rep_candidate)
            rows.append({"county": county, **base})
            county_results[county] = {**base, "competitiveness": {"color": base["color"]}}
            county_results[county].pop("color")

        dem_total = sum(row["dem_votes"] for row in rows)
        rep_total = sum(row["rep_votes"] for row in rows)
        other_total = sum(row["other_votes"] for row in rows)
        meta = {
            "contest_type": contest,
            "year": YEAR,
            "rows": len(rows),
            "dem_total": dem_total,
            "rep_total": rep_total,
            "other_total": other_total,
            "match_coverage_pct": round(len(rows) / 92 * 100, 2),
            "major_party_contested": dem_total > 0 and rep_total > 0,
            **status_meta(),
        }
        filename = f"{contest}_{YEAR}.json"
        payload = {"meta": meta, "rows": rows}
        write_json(CONTESTS / filename, payload)
        county_payloads[contest] = payload
        contest_manifest.append({"year": YEAR, "contest_type": contest, "file": filename, **{k: meta[k] for k in (
            "rows", "dem_total", "rep_total", "other_total", "match_coverage_pct", "major_party_contested",
            "result_status", "as_of_date", "note", "source",
        )}})
        election_year[contest] = {contest: {"results": county_results}}

    contest_manifest = sorted_manifest(contest_manifest)
    write_json(contest_manifest_path, {"files": contest_manifest})
    election_agg["results_by_year"][str(YEAR)] = election_year
    write_json(election_agg_path, election_agg)

    district_manifest_path = DISTRICT_CONTESTS / "manifest.json"
    district_manifest = json.loads(district_manifest_path.read_text(encoding="utf-8"))["files"]
    district_manifest = [row for row in district_manifest if int(row.get("year", 0)) != YEAR]
    district_agg_path = DATA / "in_district_results_2022_lines.json"
    district_agg = json.loads(district_agg_path.read_text(encoding="utf-8"))
    district_year: dict[str, Any] = defaultdict(dict)

    crosswalks = {
        scope: (load_crosswalk(paths[0], "precinct_key"), load_crosswalk(paths[1], "county"))
        for scope, paths in CROSSWALKS.items()
    }

    for contest in sorted(STATEWIDE_CONTESTS):
        dem_candidate = top_candidate(statewide_candidates[(contest, "dem")])
        rep_candidate = top_candidate(statewide_candidates[(contest, "rep")])
        totals = {
            party: sum(values[party] for (kind, _county), values in county_votes.items() if kind == contest)
            for party in ("dem", "rep", "other")
        }
        for scope in ("congressional", "state_house", "state_senate"):
            precinct_xwalk, county_xwalk = crosswalks[scope]
            floats: dict[int, dict[str, float]] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})
            matched = 0
            matched_votes = 0
            precinct_count = 0
            for (kind, county, precinct), values in precinct_votes.items():
                if kind != contest:
                    continue
                precinct_count += 1
                key = f"{normalize_token(county)}|{canonical_precinct(precinct)}"
                weights = precinct_xwalk.get(key)
                if weights:
                    matched += 1
                    matched_votes += sum(values.values())
                else:
                    weights = county_xwalk.get(normalize_token(county), [])
                for district_num, weight in weights:
                    for party in ("dem", "rep", "other"):
                        floats[district_num][party] += values[party] * weight

            apportioned = {
                party: apportion({d: values[party] for d, values in floats.items()}, totals[party])
                for party in ("dem", "rep", "other")
            }
            results = {}
            for district_num in sorted(floats):
                results[str(district_num)] = result_row(
                    apportioned["dem"].get(district_num, 0),
                    apportioned["rep"].get(district_num, 0),
                    apportioned["other"].get(district_num, 0),
                    dem_candidate,
                    rep_candidate,
                )
            meta = {
                "scope": scope,
                "contest_type": contest,
                "year": YEAR,
                "districts": len(results),
                "match_coverage_pct": round(len(results) / EXPECTED_DISTRICTS[scope] * 100, 2),
                "allocation": "precinct_area_weighted_with_county_area_fallback",
                "precinct_rows": precinct_count,
                "precinct_geom_match_pct": round(matched / precinct_count * 100, 2) if precinct_count else 0.0,
                "precinct_geom_match_vote_pct": round(matched_votes / sum(totals.values()) * 100, 2) if sum(totals.values()) else 0.0,
                **status_meta(),
            }
            payload = {"meta": meta, "general": {"results": results}}
            filename = f"{scope}_{contest}_{YEAR}.json"
            write_json(DISTRICT_CONTESTS / filename, payload)
            district_manifest.append({
                "scope": scope, "year": YEAR, "contest_type": contest, "file": filename,
                "districts": len(results), "dem_total": totals["dem"], "rep_total": totals["rep"],
                "other_total": totals["other"], "major_party_contested": totals["dem"] > 0 and totals["rep"] > 0,
                "match_coverage_pct": meta["match_coverage_pct"], **status_meta(),
            })
            district_year[scope][contest] = payload

    grouped_districts: dict[tuple[str, str], dict[int, dict[str, int]]] = defaultdict(dict)
    for (scope, contest, district_num), values in district_votes.items():
        grouped_districts[(scope, contest)][district_num] = values

    for (scope, contest), by_district in sorted(grouped_districts.items()):
        results = {}
        for district_num, values in sorted(by_district.items()):
            results[str(district_num)] = result_row(
                values["dem"], values["rep"], values["other"],
                top_candidate(district_candidates[(scope, contest, district_num, "dem")]),
                top_candidate(district_candidates[(scope, contest, district_num, "rep")]),
            )
        totals = {party: sum(values[party] for values in by_district.values()) for party in ("dem", "rep", "other")}
        meta = {
            "scope": scope,
            "contest_type": contest,
            "year": YEAR,
            "districts": len(results),
            "match_coverage_pct": round(len(results) / EXPECTED_DISTRICTS[scope] * 100, 2),
            "allocation": "precinct_sum_by_district_number",
            "source": SOURCE,
            **status_meta(),
        }
        payload = {"meta": meta, "general": {"results": results}}
        filename = f"{scope}_{contest}_{YEAR}.json"
        write_json(DISTRICT_CONTESTS / filename, payload)
        district_manifest.append({
            "scope": scope, "year": YEAR, "contest_type": contest, "file": filename,
            "districts": len(results), "dem_total": totals["dem"], "rep_total": totals["rep"],
            "other_total": totals["other"], "major_party_contested": totals["dem"] > 0 and totals["rep"] > 0,
            "match_coverage_pct": meta["match_coverage_pct"], **status_meta(),
        })
        district_year[scope][contest] = payload

    district_manifest = sorted_manifest(district_manifest, district=True)
    write_json(district_manifest_path, {"files": district_manifest})
    district_agg["results_by_year"][str(YEAR)] = dict(district_year)
    write_json(district_agg_path, district_agg)

    print(f"Wrote {len(county_payloads)} county contest slices and {sum(len(v) for v in district_year.values())} district slices.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build incremental 2026 contest JSONs.")
    parser.add_argument(
        "input_csv",
        nargs="?",
        type=Path,
        default=DATA / "openelections_generated" / "20261103__in__general__precinct.csv",
    )
    args = parser.parse_args()
    build(args.input_csv)


if __name__ == "__main__":
    main()
