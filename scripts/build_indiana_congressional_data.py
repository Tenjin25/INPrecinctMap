#!/usr/bin/env python
"""
Build Indiana statewide contest slices + congressional layer JSONs.

Outputs:
  - Data/contests/*.json + Data/contests/manifest.json
  - Data/in_elections_aggregated.json
  - Data/tileset/in_cd118_tileset.geojson
  - Data/tileset/in_state_house_2022_lines_tileset.geojson
  - Data/tileset/in_state_house_2024_lines_tileset.geojson
  - Data/tileset/in_state_senate_2022_lines_tileset.geojson
  - Data/tileset/in_state_senate_2024_lines_tileset.geojson
  - Data/in_congressional_districts.csv
  - Data/in_state_house_districts.csv
  - Data/in_state_senate_districts.csv
  - Data/crosswalks/precinct_to_cd118.csv   (county-keyed carry crosswalk)
  - Data/crosswalks/precinct_to_2022_state_house.csv
  - Data/crosswalks/precinct_to_2024_state_house.csv
  - Data/crosswalks/precinct_to_2022_state_senate.csv
  - Data/crosswalks/precinct_to_2024_state_senate.csv
  - Data/crosswalks/county_to_cd118.csv
  - Data/crosswalks/county_to_2022_state_house.csv
  - Data/crosswalks/county_to_2022_state_senate.csv
  - Data/district_contests/*.json + Data/district_contests/manifest.json
  - Data/in_district_results_2022_lines.json
"""

from __future__ import annotations

import csv
import io
import json
import re
import shutil
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import shapefile
from shapely.geometry import shape


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
TMP_DIR = ROOT / ".tmp_build_indiana_congressional"

COUNTIES_GEOJSON = DATA_DIR / "census" / "tl_2020_18_county20.geojson"
CD118_ZIP = DATA_DIR / "tl_2022_18_cd118.zip"
SLDL_ZIP = DATA_DIR / "tl_2022_18_sldl.zip"
SLDU_ZIP = DATA_DIR / "tl_2022_18_sldu.zip"
OPENELECTIONS_ROOT = DATA_DIR / "openelections-data-in"
OPENELECTIONS_GENERATED_ROOT = DATA_DIR / "openelections_generated"

OUT_TILESET_DIR = DATA_DIR / "tileset"
OUT_CROSSWALKS_DIR = DATA_DIR / "crosswalks"
OUT_CONTESTS_DIR = DATA_DIR / "contests"
OUT_DISTRICT_CONTESTS_DIR = DATA_DIR / "district_contests"

OUT_CD118_GEOJSON = OUT_TILESET_DIR / "in_cd118_tileset.geojson"
OUT_CD119_GEOJSON = OUT_TILESET_DIR / "in_cd119_tileset.geojson"
OUT_DISTRICTS_INFO_CSV = DATA_DIR / "in_congressional_districts.csv"
OUT_CROSSWALK_PRECINCT_TO_CD = OUT_CROSSWALKS_DIR / "precinct_to_cd118.csv"
OUT_CROSSWALK_COUNTY_TO_CD = OUT_CROSSWALKS_DIR / "county_to_cd118.csv"

OUT_STATE_HOUSE_2022_GEOJSON = OUT_TILESET_DIR / "in_state_house_2022_lines_tileset.geojson"
OUT_STATE_HOUSE_2024_GEOJSON = OUT_TILESET_DIR / "in_state_house_2024_lines_tileset.geojson"
OUT_STATE_SENATE_2022_GEOJSON = OUT_TILESET_DIR / "in_state_senate_2022_lines_tileset.geojson"
OUT_STATE_SENATE_2024_GEOJSON = OUT_TILESET_DIR / "in_state_senate_2024_lines_tileset.geojson"

OUT_STATE_HOUSE_INFO_CSV = DATA_DIR / "in_state_house_districts.csv"
OUT_STATE_SENATE_INFO_CSV = DATA_DIR / "in_state_senate_districts.csv"

OUT_CROSSWALK_PRECINCT_TO_STATE_HOUSE_2022 = OUT_CROSSWALKS_DIR / "precinct_to_2022_state_house.csv"
OUT_CROSSWALK_PRECINCT_TO_STATE_HOUSE_2024 = OUT_CROSSWALKS_DIR / "precinct_to_2024_state_house.csv"
OUT_CROSSWALK_PRECINCT_TO_STATE_SENATE_2022 = OUT_CROSSWALKS_DIR / "precinct_to_2022_state_senate.csv"
OUT_CROSSWALK_PRECINCT_TO_STATE_SENATE_2024 = OUT_CROSSWALKS_DIR / "precinct_to_2024_state_senate.csv"
OUT_CROSSWALK_COUNTY_TO_STATE_HOUSE_2022 = OUT_CROSSWALKS_DIR / "county_to_2022_state_house.csv"
OUT_CROSSWALK_COUNTY_TO_STATE_SENATE_2022 = OUT_CROSSWALKS_DIR / "county_to_2022_state_senate.csv"
OUT_ELECTION_AGG = DATA_DIR / "in_elections_aggregated.json"
OUT_DISTRICT_AGG = DATA_DIR / "in_district_results_2022_lines.json"
SOS_2024_OFFICIAL_CONTESTS = DATA_DIR / "sources" / "in_sos_2024_statewide_county_totals.json"


INDIANA_COUNTY_COUNT = 92
MIN_STATEWIDE_COUNTY_COVERAGE = 70
MIN_IMPUTE_OVERLAP_COUNTIES = 40
SUPPORTED_CONTEST_TYPES = {
    "attorney_general",
    "governor",
    "president",
    "superintendent",
    "us_senate",
    "auditor",
    "secretary_of_state",
    "treasurer",
}

IMPUTATION_DONOR_PREFERENCE: Dict[str, Tuple[str, ...]] = {
    "attorney_general": ("governor", "us_senate", "president"),
    "treasurer": ("governor", "us_senate", "president"),
    "auditor": ("governor", "us_senate", "president"),
    "secretary_of_state": ("governor", "us_senate", "president"),
    "superintendent": ("governor", "us_senate", "president"),
    "governor": ("president", "us_senate"),
    "us_senate": ("governor", "president"),
    "president": ("governor", "us_senate"),
}


@dataclass
class CountyFeature:
    countyfp: str
    name: str
    norm_key: str
    geom: Any


@dataclass
class DistrictFeature:
    district_num: int
    geoid: str
    namelsad: str
    geom: Any
    properties: Dict[str, Any]


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_alnum_token(text: str) -> str:
    t = (text or "").upper().replace("_", " ")
    t = re.sub(r"[^A-Z0-9 ]+", " ", t)
    t = normalize_space(t)
    t = re.sub(r"\bCOUNTY\b", "", t)
    return normalize_space(t)


def normalize_no_space(text: str) -> str:
    return normalize_alnum_token(text).replace(" ", "")


def title_case_county(name: str) -> str:
    # Keep known county stylings.
    lower = (name or "").strip().lower()
    if lower == "laporte":
        return "LaPorte"
    if lower == "lagrange":
        return "LaGrange"
    if lower == "dekalb":
        return "DeKalb"
    if lower == "st. joseph" or lower == "st joseph":
        return "St. Joseph"
    parts = [p.capitalize() for p in re.split(r"\s+", lower) if p]
    return " ".join(parts)


def clean_number(value: Any, default: float = 0.0) -> float:
    try:
        s = str(value).strip().replace(",", "")
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def parse_votes(value: Any) -> int:
    return int(round(clean_number(value, 0.0)))


def party_bucket(party_raw: str) -> str:
    p = normalize_alnum_token(party_raw)
    if not p:
        return "other"
    if p in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC", "DEMOCRATIC PARTY"} or "DEM" in p:
        return "dem"
    if p in {"R", "REP", "REPUBLICAN", "REPUBLICAN PARTY"} or "REP" in p:
        return "rep"
    return "other"


def map_office_to_contest_type(office_raw: str) -> Optional[str]:
    o = normalize_alnum_token(office_raw)
    if not o:
        return None

    if any(
        token in o
        for token in (
            "BALLOTS CAST",
            "REGISTERED VOTERS",
            "VOTERS",
            "STRAIGHT PARTY",
            "PUBLIC QUESTION",
            "AMENDMENT",
        )
    ):
        return None

    if any(token in o for token in ("STATE SENATE", "STATE SENATOR", "STATE HOUSE", "US HOUSE", "CONGRESSIONAL DISTRICT")):
        return None

    if "PRESIDENT" in o:
        return "president"

    if ("SENATE" in o or "SENATOR" in o) and (("US" in o) or ("UNITED STATES" in o) or ("U S" in o)):
        return "us_senate"

    if "GOVERNOR" in o:
        return "governor"

    if "ATTORNEY GENERAL" in o:
        return "attorney_general"

    if "SECRETARY OF STATE" in o or "STATE SECRETARY" in o:
        return "secretary_of_state"

    if "TREASURER OF STATE" in o or "STATE TREASURER" in o or o == "TREASURER":
        return "treasurer"

    if "AUDITOR OF STATE" in o or "STATE AUDITOR" in o or o == "AUDITOR":
        return "auditor"

    if "SUPERINTENDENT" in o and "PUBLIC" in o and "INSTRUCTION" in o:
        return "superintendent"

    if "AGRICULTURE" in o and "COMMISSIONER" in o:
        return "agriculture_commissioner"

    if "INSURANCE" in o and "COMMISSIONER" in o:
        return "insurance_commissioner"

    if "LABOR" in o and "COMMISSIONER" in o:
        return "labor_commissioner"

    return None


def map_office_to_district_race_type(office_raw: str) -> Optional[Tuple[str, str]]:
    """
    Map OpenElections-style office labels to (scope, contest_type) for district-specific races.

    These contests are aggregated by the district number embedded in the precinct rows.
    """
    o = normalize_alnum_token(office_raw)
    if not o:
        return None

    # Congressional districts.
    if "US HOUSE" in o or "U S HOUSE" in o or "UNITED STATES HOUSE" in o:
        return "congressional", "us_house"

    # Indiana legislative districts.
    if "STATE HOUSE" in o or "STATE REPRESENTATIVE" in o:
        return "state_house", "state_house"
    if "STATE SENATE" in o or "STATE SENATOR" in o:
        return "state_senate", "state_senate"

    return None


def winner_from_votes(dem: int, rep: int) -> str:
    if rep > dem:
        return "REP"
    if dem > rep:
        return "DEM"
    return "TIE"


def color_from_winner(winner: str) -> str:
    if winner == "REP":
        return "#dc2626"
    if winner == "DEM":
        return "#2563eb"
    return "#64748b"


def load_counties() -> Tuple[List[CountyFeature], Dict[str, str], Dict[str, str]]:
    obj = json.loads(COUNTIES_GEOJSON.read_text(encoding="utf-8"))
    features = obj.get("features", [])

    out: List[CountyFeature] = []
    county_alias_to_name: Dict[str, str] = {}
    county_name_to_fp: Dict[str, str] = {}

    for f in features:
        props = f.get("properties", {})
        countyfp = str(props.get("COUNTYFP20") or "").zfill(3)
        name = title_case_county(str(props.get("NAME20") or "").strip())
        if not countyfp or not name:
            continue
        geom = shape(f.get("geometry"))
        norm_key = normalize_alnum_token(name)
        out.append(CountyFeature(countyfp=countyfp, name=name, norm_key=norm_key, geom=geom))
        county_name_to_fp[name] = countyfp

        alias_tokens = set()
        alias_tokens.add(normalize_alnum_token(name))
        alias_tokens.add(normalize_no_space(name))
        alias_tokens.add(normalize_alnum_token(name + " County"))
        alias_tokens.add(normalize_no_space(name + " County"))

        # St./Saint variants.
        if norm_key.startswith("ST "):
            saint = norm_key.replace("ST ", "SAINT ", 1)
            alias_tokens.add(saint)
            alias_tokens.add(saint.replace(" ", ""))
        if norm_key.startswith("SAINT "):
            st = norm_key.replace("SAINT ", "ST ", 1)
            alias_tokens.add(st)
            alias_tokens.add(st.replace(" ", ""))

        for token in alias_tokens:
            if token:
                county_alias_to_name[token] = name

    # Known variants seen in OpenElections.
    manual_alias = {
        "STJOSEPH": "St. Joseph",
        "ST JOSEPH": "St. Joseph",
        "SAINTJOSEPH": "St. Joseph",
        "SAINT JOSEPH": "St. Joseph",
        "LAPORTE": "LaPorte",
        "LA PORTE": "LaPorte",
        "LAGRANGE": "LaGrange",
        "LA GRANGE": "LaGrange",
        "DEKALB": "DeKalb",
        "VERMILION": "Vermillion",  # common spelling variant
    }
    for token, name in manual_alias.items():
        if name in county_name_to_fp:
            county_alias_to_name[token] = name

    return out, county_alias_to_name, county_name_to_fp


def canonicalize_county_name(raw: str, alias_map: Dict[str, str]) -> Optional[str]:
    if not raw:
        return None
    candidates = [
        normalize_alnum_token(raw),
        normalize_no_space(raw),
    ]
    for key in candidates:
        if key in alias_map:
            return alias_map[key]
    return None


def extract_shapefile(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    shp = list(out_dir.glob("*.shp"))
    if not shp:
        raise FileNotFoundError(f"No .shp in {zip_path}")
    return shp[0]


def first_nonempty(props: Dict[str, Any], keys: Iterable[str]) -> str:
    for k in keys:
        v = props.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def parse_district_number(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    digits = re.sub(r"[^0-9]", "", s)
    if digits:
        return int(digits)
    try:
        return int(float(s))
    except Exception:
        return None


def load_districts(
    zip_path: Path,
    extract_dir: Path,
    district_field_candidates: List[str],
    geoid_field_candidates: List[str],
    namelsad_field_candidates: List[str],
    fallback_label: str,
) -> List[DistrictFeature]:
    shp_path = extract_shapefile(zip_path, extract_dir)
    reader = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in reader.fields[1:]]
    out: List[DistrictFeature] = []

    for sr in reader.iterShapeRecords():
        props = {k: sr.record[i] for i, k in enumerate(fields)}
        district_raw = first_nonempty(props, district_field_candidates)
        district_num = parse_district_number(district_raw)
        if district_num is None:
            continue
        geom = shape(sr.shape.__geo_interface__)
        if geom.is_empty:
            continue
        geoid = first_nonempty(props, geoid_field_candidates)
        namelsad = first_nonempty(props, namelsad_field_candidates) or f"{fallback_label} District {district_num}"
        props["DISTRICT"] = district_num
        props["district"] = district_num
        out.append(
            DistrictFeature(
                district_num=district_num,
                geoid=geoid,
                namelsad=namelsad,
                geom=geom,
                properties=props,
            )
        )
    return out


def load_congressional_districts(zip_path: Path, extract_dir: Path) -> List[DistrictFeature]:
    return load_districts(
        zip_path=zip_path,
        extract_dir=extract_dir,
        district_field_candidates=["CD118FP", "DISTRICT", "district"],
        geoid_field_candidates=["GEOID20", "GEOID"],
        namelsad_field_candidates=["NAMELSAD20", "NAMELSAD"],
        fallback_label="Congressional",
    )


def load_state_house_districts(zip_path: Path, extract_dir: Path) -> List[DistrictFeature]:
    return load_districts(
        zip_path=zip_path,
        extract_dir=extract_dir,
        district_field_candidates=["SLDLST", "DISTRICT", "district"],
        geoid_field_candidates=["GEOID", "GEOID20"],
        namelsad_field_candidates=["NAMELSAD", "NAMELSAD20"],
        fallback_label="State House",
    )


def load_state_senate_districts(zip_path: Path, extract_dir: Path) -> List[DistrictFeature]:
    return load_districts(
        zip_path=zip_path,
        extract_dir=extract_dir,
        district_field_candidates=["SLDUST", "DISTRICT", "district"],
        geoid_field_candidates=["GEOID", "GEOID20"],
        namelsad_field_candidates=["NAMELSAD", "NAMELSAD20"],
        fallback_label="State Senate",
    )


def write_cd_geojson(districts: List[DistrictFeature], out_path: Path) -> None:
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": d.properties,
                "geometry": d.geom.__geo_interface__,
            }
            for d in sorted(districts, key=lambda x: x.district_num)
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(fc, separators=(",", ":")), encoding="utf-8")


def build_county_to_cd_weights(counties: List[CountyFeature], districts: List[DistrictFeature]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in counties:
        c_area = float(c.geom.area or 0.0)
        if c_area <= 0:
            continue
        subtotal = 0.0
        local_rows: List[Dict[str, Any]] = []
        for d in districts:
            if not c.geom.intersects(d.geom):
                continue
            inter = c.geom.intersection(d.geom)
            area = float(inter.area or 0.0)
            if area <= 0:
                continue
            w = area / c_area
            if w <= 0:
                continue
            subtotal += w
            local_rows.append(
                {
                    "county": c.name,
                    "countyfp20": c.countyfp,
                    "district_num": d.district_num,
                    "district_geoid": d.geoid,
                    "area_weight": w,
                }
            )
        # Normalize residual floating error to 1.0 when close.
        if local_rows and subtotal > 0:
            for r in local_rows:
                r["area_weight"] = r["area_weight"] / subtotal
        rows.extend(local_rows)
    return rows


def iter_general_precinct_files(root: Path, *, allow_flat: bool = False) -> Iterable[Path]:
    for p in root.rglob("*.csv"):
        rel = p.relative_to(root)
        if not rel.parts:
            continue
        # Use only canonical year folders; optionally support top-level OpenElections
        # filenames like 20241105__in__general__<county>__precinct.csv.
        if not re.fullmatch(r"\d{4}", rel.parts[0]):
            if not (allow_flat and re.match(r"^\d{8}__", p.name)):
                continue
        name = p.name.lower()
        if "__general__" not in name:
            continue
        if "precinct" not in name:
            continue
        yield p


def iter_general_county_files(root: Path, *, allow_flat: bool = False) -> Iterable[Path]:
    for p in root.rglob("*.csv"):
        rel = p.relative_to(root)
        if not rel.parts:
            continue
        if not re.fullmatch(r"\d{4}", rel.parts[0]):
            if not (allow_flat and re.match(r"^\d{8}__", p.name)):
                continue
        name = p.name.lower()
        if "__general__" not in name:
            continue
        if "county" not in name:
            continue
        # Keep true county totals files only; avoid county-named precinct files.
        if "precinct" in name:
            continue
        yield p


def choose_imputation_donor_contest(
    *,
    grouped: Dict[Tuple[int, str], Dict[str, Dict[str, int]]],
    year: int,
    contest_type: str,
    missing_counties: Set[str],
) -> Optional[Tuple[str, Dict[str, Dict[str, int]]]]:
    candidates = [ct for (y, ct) in grouped.keys() if y == year and ct != contest_type]
    if not candidates:
        return None

    pref = IMPUTATION_DONOR_PREFERENCE.get(contest_type, ())

    def sort_key(ct: str) -> Tuple[int, int, int, str]:
        donor = grouped[(year, ct)]
        has_all_missing = 0 if missing_counties.issubset(set(donor.keys())) else 1
        pref_rank = pref.index(ct) if ct in pref else len(pref) + 1
        return (has_all_missing, pref_rank, -len(donor), ct)

    for ct in sorted(candidates, key=sort_key):
        donor = grouped[(year, ct)]
        if not missing_counties.issubset(set(donor.keys())):
            continue
        return ct, donor
    return None


def load_sos_2024_official_contests(
    county_alias_map: Dict[str, str],
) -> Tuple[Dict[str, Dict[str, Dict[str, int]]], Dict[str, Dict[str, str]]]:
    if not SOS_2024_OFFICIAL_CONTESTS.exists():
        return {}, {}

    obj = json.loads(SOS_2024_OFFICIAL_CONTESTS.read_text(encoding="utf-8"))
    contests = obj.get("contests") or []

    by_contest: Dict[str, Dict[str, Dict[str, int]]] = {}
    candidates: Dict[str, Dict[str, str]] = {}
    for contest in contests:
        if int(contest.get("year") or 0) != 2024:
            continue
        contest_type = str(contest.get("contest_type") or "").strip()
        if contest_type not in SUPPORTED_CONTEST_TYPES:
            continue

        rows = contest.get("rows") or []
        by_county: Dict[str, Dict[str, int]] = {}
        for r in rows:
            county_name = canonicalize_county_name(str(r.get("county") or ""), county_alias_map)
            if not county_name:
                continue
            by_county[county_name] = {
                "dem": int(round(clean_number(r.get("dem_votes"), 0.0))),
                "rep": int(round(clean_number(r.get("rep_votes"), 0.0))),
                "other": int(round(clean_number(r.get("other_votes"), 0.0))),
            }

        if not by_county:
            continue
        by_contest[contest_type] = by_county
        candidates[contest_type] = {
            "dem_candidate": str(contest.get("dem_candidate") or "").strip(),
            "rep_candidate": str(contest.get("rep_candidate") or "").strip(),
        }

    return by_contest, candidates


def impute_missing_counties_from_donor(
    *,
    target_by_county: Dict[str, Dict[str, int]],
    donor_by_county: Dict[str, Dict[str, int]],
    missing_counties: Set[str],
) -> Dict[str, Dict[str, int]]:
    overlap = [c for c in target_by_county.keys() if c in donor_by_county]
    if len(overlap) < MIN_IMPUTE_OVERLAP_COUNTIES:
        return {}

    scales: Dict[str, float] = {}
    for party in ("dem", "rep", "other"):
        target_sum = sum(int(target_by_county[c].get(party, 0)) for c in overlap)
        donor_sum = sum(int(donor_by_county[c].get(party, 0)) for c in overlap)
        scales[party] = (target_sum / donor_sum) if donor_sum > 0 else 0.0

    out: Dict[str, Dict[str, int]] = {}
    for county in sorted(missing_counties):
        dv = donor_by_county.get(county)
        if not dv:
            continue
        dem = int(round(int(dv.get("dem", 0)) * scales["dem"]))
        rep = int(round(int(dv.get("rep", 0)) * scales["rep"]))
        oth = int(round(int(dv.get("other", 0)) * scales["other"]))
        out[county] = {
            "dem": max(0, dem),
            "rep": max(0, rep),
            "other": max(0, oth),
        }

    return out


def collect_statewide_contests(
    root: Path,
    county_alias_map: Dict[str, str],
) -> Tuple[Dict[Tuple[int, str, str], Dict[str, int]], Dict[Tuple[int, str, str], Dict[str, int]], Dict[Tuple[int, str], set]]:
    # (year, contest_type, county) -> dem/rep/other vote totals
    county_votes: Dict[Tuple[int, str, str], Dict[str, int]] = defaultdict(lambda: {"dem": 0, "rep": 0, "other": 0})
    # (year, contest_type, party) -> candidate -> votes
    candidate_votes: Dict[Tuple[int, str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # contest coverage counties
    coverage: Dict[Tuple[int, str], set] = defaultdict(set)

    seen_rows = set()

    for path in iter_general_precinct_files(root, allow_flat=False):
        m = re.match(r"^(\d{4})", path.name)
        if not m:
            continue
        year = int(m.group(1))
        # Some OpenElections county files are UTF-8 with BOM; use utf-8-sig so
        # the first header still resolves to "county" instead of "\ufeffcounty".
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                district = (row.get("district") or "").strip()
                if district:
                    continue

                contest_type = map_office_to_contest_type(row.get("office") or "")
                if not contest_type:
                    continue
                if contest_type not in SUPPORTED_CONTEST_TYPES:
                    continue

                county_name = canonicalize_county_name(row.get("county") or "", county_alias_map)
                if not county_name:
                    continue

                votes = parse_votes(row.get("votes"))
                if votes <= 0:
                    continue

                precinct = normalize_space((row.get("precinct") or "").upper())
                party = party_bucket(row.get("party") or "")
                candidate = normalize_space(row.get("candidate") or "")

                # Deduplicate row-level duplicates appearing in some year folders.
                row_key = (year, contest_type, county_name, precinct, party, candidate.upper(), votes)
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)

                county_votes[(year, contest_type, county_name)][party] += votes
                if candidate:
                    candidate_votes[(year, contest_type, party)][candidate] += votes
                coverage[(year, contest_type)].add(county_name)

    # Some older years have canonical county-level statewide totals files.
    # Use them only to backfill county/contest pairs missing from precinct parses.
    existing_county_contests = set(county_votes.keys())
    seen_county_rows = set()
    for path in iter_general_county_files(root, allow_flat=False):
        m = re.match(r"^(\d{4})", path.name)
        if not m:
            continue
        year = int(m.group(1))
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                district = (row.get("district") or "").strip()
                if district:
                    continue

                contest_type = map_office_to_contest_type(row.get("office") or "")
                if not contest_type:
                    continue
                if contest_type not in SUPPORTED_CONTEST_TYPES:
                    continue

                county_name = canonicalize_county_name(row.get("county") or "", county_alias_map)
                if not county_name:
                    continue

                key = (year, contest_type, county_name)
                if key in existing_county_contests:
                    continue

                votes = parse_votes(row.get("votes"))
                if votes <= 0:
                    continue

                party = party_bucket(row.get("party") or "")
                candidate = normalize_space(row.get("candidate") or "")

                row_key = (year, contest_type, county_name, party, candidate.upper(), votes)
                if row_key in seen_county_rows:
                    continue
                seen_county_rows.add(row_key)

                county_votes[key][party] += votes
                if candidate:
                    candidate_votes[(year, contest_type, party)][candidate] += votes
                coverage[(year, contest_type)].add(county_name)

    return county_votes, candidate_votes, coverage


def collect_district_race_contests(
    root: Path,
    county_alias_map: Dict[str, str],
    *,
    years: Optional[Set[int]] = None,
) -> Tuple[
    Dict[Tuple[int, str, str, int], Dict[str, int]],
    Dict[Tuple[int, str, str, int, str], Dict[str, int]],
    Dict[Tuple[int, str, str], set],
]:
    # (year, scope, contest_type, district_num) -> dem/rep/other vote totals
    district_votes: Dict[Tuple[int, str, str, int], Dict[str, int]] = defaultdict(lambda: {"dem": 0, "rep": 0, "other": 0})
    # (year, scope, contest_type, district_num, party) -> candidate -> votes
    candidate_votes: Dict[Tuple[int, str, str, int, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    # contest coverage districts
    coverage: Dict[Tuple[int, str, str], set] = defaultdict(set)

    seen_rows = set()

    for path in iter_general_precinct_files(root, allow_flat=True):
        m = re.match(r"^(\d{4})", path.name)
        if not m:
            continue
        year = int(m.group(1))
        if years is not None and year not in years:
            continue

        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                district_raw = (row.get("district") or "").strip()
                if not district_raw:
                    continue

                # District numbers are expected for these races; ignore anything non-numeric.
                digits = re.sub(r"[^0-9]", "", district_raw)
                if not digits:
                    continue
                district_num = int(digits)
                if district_num <= 0:
                    continue

                mapped = map_office_to_district_race_type(row.get("office") or "")
                if not mapped:
                    continue
                scope, contest_type = mapped

                county_name = canonicalize_county_name(row.get("county") or "", county_alias_map)
                if not county_name:
                    continue

                votes = parse_votes(row.get("votes"))
                if votes <= 0:
                    continue

                precinct = normalize_space((row.get("precinct") or "").upper())
                party = party_bucket(row.get("party") or "")
                candidate = normalize_space(row.get("candidate") or "")

                row_key = (year, scope, contest_type, district_num, county_name, precinct, party, candidate.upper(), votes)
                if row_key in seen_rows:
                    continue
                seen_rows.add(row_key)

                district_votes[(year, scope, contest_type, district_num)][party] += votes
                if candidate:
                    candidate_votes[(year, scope, contest_type, district_num, party)][candidate] += votes
                coverage[(year, scope, contest_type)].add(district_num)

    return district_votes, candidate_votes, coverage


def top_candidate(candidate_totals: Dict[str, int]) -> str:
    if not candidate_totals:
        return ""
    return sorted(candidate_totals.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def clear_json_outputs(directory: Path) -> None:
    if not directory.exists():
        return
    for p in directory.glob("*.json"):
        if p.is_file():
            p.unlink()


def build_scope_assets(
    *,
    scope_label: str,
    districts: List[DistrictFeature],
    counties: List[CountyFeature],
    out_geojson_2022: Path,
    out_geojson_2024: Path,
    out_info_csv: Path,
    out_county_crosswalk_csv: Path,
    out_precinct_crosswalk_2022_csv: Path,
    out_precinct_crosswalk_2024_csv: Path,
) -> List[Dict[str, Any]]:
    county_to_district_rows = build_county_to_cd_weights(counties, districts)

    write_cd_geojson(districts, out_geojson_2022)
    # Reuse 2022 lines for 2024 toggle until separate district shapefiles are provided.
    write_cd_geojson(districts, out_geojson_2024)

    info_rows = []
    for d in sorted(districts, key=lambda x: x.district_num):
        info_rows.append(
            {
                "district": d.district_num,
                "name": d.namelsad,
                "district_geoid": d.geoid,
                "aland": d.properties.get("ALAND20", d.properties.get("ALAND", 0)),
                "awater": d.properties.get("AWATER20", d.properties.get("AWATER", 0)),
            }
        )
    write_csv(
        out_info_csv,
        ["district", "name", "district_geoid", "aland", "awater"],
        info_rows,
    )

    county_xwalk_rows = []
    precinct_xwalk_rows = []
    for r in sorted(county_to_district_rows, key=lambda x: (x["county"], x["district_num"])):
        county_xwalk_rows.append(
            {
                "county": r["county"],
                "countyfp20": r["countyfp20"],
                "district_num": r["district_num"],
                "district_geoid": r["district_geoid"],
                "area_weight": f"{float(r['area_weight']):.12f}",
            }
        )
        # App carryover logic expects precinct_key rows.
        precinct_xwalk_rows.append(
            {
                "precinct_key": r["county"].upper(),
                "district_num": r["district_num"],
                "area_weight": f"{float(r['area_weight']):.12f}",
                "county": r["county"],
                "countyfp20": r["countyfp20"],
            }
        )

    write_csv(
        out_county_crosswalk_csv,
        ["county", "countyfp20", "district_num", "district_geoid", "area_weight"],
        county_xwalk_rows,
    )

    write_csv(
        out_precinct_crosswalk_2022_csv,
        ["precinct_key", "district_num", "area_weight", "county", "countyfp20"],
        precinct_xwalk_rows,
    )
    write_csv(
        out_precinct_crosswalk_2024_csv,
        ["precinct_key", "district_num", "area_weight", "county", "countyfp20"],
        precinct_xwalk_rows,
    )

    print(f"Wrote {scope_label} geometry: {out_geojson_2022}")
    print(f"Wrote {scope_label} crosswalk: {out_precinct_crosswalk_2022_csv}")
    return county_to_district_rows


def build_outputs() -> None:
    if not COUNTIES_GEOJSON.exists():
        raise FileNotFoundError(f"Missing counties GeoJSON: {COUNTIES_GEOJSON}")
    if not CD118_ZIP.exists():
        raise FileNotFoundError(f"Missing congressional shapefile ZIP: {CD118_ZIP}")
    if not SLDL_ZIP.exists():
        raise FileNotFoundError(f"Missing state house shapefile ZIP: {SLDL_ZIP}")
    if not SLDU_ZIP.exists():
        raise FileNotFoundError(f"Missing state senate shapefile ZIP: {SLDU_ZIP}")
    if not OPENELECTIONS_ROOT.exists():
        raise FileNotFoundError(f"Missing OpenElections root: {OPENELECTIONS_ROOT}")

    # Keep generated contest directories idempotent across reruns.
    clear_json_outputs(OUT_CONTESTS_DIR)
    clear_json_outputs(OUT_DISTRICT_CONTESTS_DIR)

    counties, county_alias_map, _county_name_to_fp = load_counties()

    congressional_districts = load_congressional_districts(CD118_ZIP, TMP_DIR / "cd118")
    state_house_districts = load_state_house_districts(SLDL_ZIP, TMP_DIR / "sldl")
    state_senate_districts = load_state_senate_districts(SLDU_ZIP, TMP_DIR / "sldu")

    county_to_cd_rows = build_scope_assets(
        scope_label="congressional",
        districts=congressional_districts,
        counties=counties,
        out_geojson_2022=OUT_CD118_GEOJSON,
        out_geojson_2024=OUT_CD119_GEOJSON,
        out_info_csv=OUT_DISTRICTS_INFO_CSV,
        out_county_crosswalk_csv=OUT_CROSSWALK_COUNTY_TO_CD,
        out_precinct_crosswalk_2022_csv=OUT_CROSSWALK_PRECINCT_TO_CD,
        out_precinct_crosswalk_2024_csv=OUT_CROSSWALK_PRECINCT_TO_CD,
    )

    county_to_state_house_rows = build_scope_assets(
        scope_label="state_house",
        districts=state_house_districts,
        counties=counties,
        out_geojson_2022=OUT_STATE_HOUSE_2022_GEOJSON,
        out_geojson_2024=OUT_STATE_HOUSE_2024_GEOJSON,
        out_info_csv=OUT_STATE_HOUSE_INFO_CSV,
        out_county_crosswalk_csv=OUT_CROSSWALK_COUNTY_TO_STATE_HOUSE_2022,
        out_precinct_crosswalk_2022_csv=OUT_CROSSWALK_PRECINCT_TO_STATE_HOUSE_2022,
        out_precinct_crosswalk_2024_csv=OUT_CROSSWALK_PRECINCT_TO_STATE_HOUSE_2024,
    )

    county_to_state_senate_rows = build_scope_assets(
        scope_label="state_senate",
        districts=state_senate_districts,
        counties=counties,
        out_geojson_2022=OUT_STATE_SENATE_2022_GEOJSON,
        out_geojson_2024=OUT_STATE_SENATE_2024_GEOJSON,
        out_info_csv=OUT_STATE_SENATE_INFO_CSV,
        out_county_crosswalk_csv=OUT_CROSSWALK_COUNTY_TO_STATE_SENATE_2022,
        out_precinct_crosswalk_2022_csv=OUT_CROSSWALK_PRECINCT_TO_STATE_SENATE_2022,
        out_precinct_crosswalk_2024_csv=OUT_CROSSWALK_PRECINCT_TO_STATE_SENATE_2024,
    )

    county_votes, candidate_votes, coverage = collect_statewide_contests(OPENELECTIONS_ROOT, county_alias_map)
    official_2024_by_contest, official_2024_candidates = load_sos_2024_official_contests(county_alias_map)
    official_2024_contests = set(official_2024_by_contest.keys())

    if official_2024_contests:
        # Remove pseudo-statewide 2024 contests not present in official statewide SOS feeds.
        for key in list(county_votes.keys()):
            year, contest_type, _county = key
            if year == 2024 and contest_type in SUPPORTED_CONTEST_TYPES and contest_type not in official_2024_contests:
                del county_votes[key]

        for key in list(candidate_votes.keys()):
            year, contest_type, _party = key
            if year == 2024 and contest_type in SUPPORTED_CONTEST_TYPES and contest_type not in official_2024_contests:
                del candidate_votes[key]

        for key in list(coverage.keys()):
            year, contest_type = key
            if year == 2024 and contest_type in SUPPORTED_CONTEST_TYPES and contest_type not in official_2024_contests:
                del coverage[key]

        # Overlay 2024 county totals/candidate names from official SOS feeds.
        for contest_type, by_county in official_2024_by_contest.items():
            for key in list(county_votes.keys()):
                if key[0] == 2024 and key[1] == contest_type:
                    del county_votes[key]
            coverage[(2024, contest_type)] = set(by_county.keys())

            dem_total = 0
            rep_total = 0
            for county, votes in by_county.items():
                county_votes[(2024, contest_type, county)] = {
                    "dem": int(votes.get("dem", 0)),
                    "rep": int(votes.get("rep", 0)),
                    "other": int(votes.get("other", 0)),
                }
                dem_total += int(votes.get("dem", 0))
                rep_total += int(votes.get("rep", 0))

            candidate_votes[(2024, contest_type, "dem")] = defaultdict(int)
            candidate_votes[(2024, contest_type, "rep")] = defaultdict(int)
            candidate_votes[(2024, contest_type, "other")] = defaultdict(int)
            cand_info = official_2024_candidates.get(contest_type, {})
            dem_name = cand_info.get("dem_candidate") or ""
            rep_name = cand_info.get("rep_candidate") or ""
            if dem_name:
                candidate_votes[(2024, contest_type, "dem")][dem_name] = dem_total
            if rep_name:
                candidate_votes[(2024, contest_type, "rep")][rep_name] = rep_total

    # Build contest slices + manifests + aggregated fallback JSON.
    contest_manifest_files: List[Dict[str, Any]] = []
    district_manifest_files: List[Dict[str, Any]] = []
    election_agg: Dict[str, Any] = {"results_by_year": {}}
    district_agg: Dict[str, Any] = {"results_by_year": {}}

    # Pre-index county->district weights by scope.
    county_weights_by_scope: Dict[str, Dict[str, List[Tuple[int, float]]]] = {}
    for scope, rows in {
        "congressional": county_to_cd_rows,
        "state_house": county_to_state_house_rows,
        "state_senate": county_to_state_senate_rows,
    }.items():
        scope_weights: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
        for r in rows:
            scope_weights[r["county"]].append((int(r["district_num"]), float(r["area_weight"])))
        county_weights_by_scope[scope] = scope_weights

    all_county_names = {c.name for c in counties}

    grouped = defaultdict(dict)  # (year, contest_type) -> county -> votes dict
    for (year, contest_type, county), votes in county_votes.items():
        grouped[(year, contest_type)][county] = votes

    for (year, contest_type), by_county in sorted(grouped.items()):
        coverage_counties = len(coverage.get((year, contest_type), set()))
        coverage_pct = round((coverage_counties / INDIANA_COUNTY_COUNT) * 100.0, 2)
        if coverage_counties < MIN_STATEWIDE_COUNTY_COVERAGE:
            continue  # exclude low-coverage pseudo-statewide artifacts

        imputed_counties: List[str] = []
        imputed_from_contest = ""
        missing_counties = all_county_names.difference(set(by_county.keys()))
        if missing_counties:
            donor_info = choose_imputation_donor_contest(
                grouped=grouped,
                year=year,
                contest_type=contest_type,
                missing_counties=missing_counties,
            )
            if donor_info:
                donor_contest_type, donor_by_county = donor_info
                imputed_votes = impute_missing_counties_from_donor(
                    target_by_county=by_county,
                    donor_by_county=donor_by_county,
                    missing_counties=missing_counties,
                )
                if imputed_votes:
                    for county, votes in imputed_votes.items():
                        by_county[county] = votes
                    imputed_counties = sorted(imputed_votes.keys())
                    imputed_from_contest = donor_contest_type

        dem_total = sum(v["dem"] for v in by_county.values())
        rep_total = sum(v["rep"] for v in by_county.values())
        other_total = sum(v["other"] for v in by_county.values())
        if dem_total <= 0 or rep_total <= 0:
            continue  # contested statewide only

        dem_candidate = top_candidate(candidate_votes.get((year, contest_type, "dem"), {}))
        rep_candidate = top_candidate(candidate_votes.get((year, contest_type, "rep"), {}))

        # County contest rows
        contest_rows = []
        county_results_obj = {}
        imputed_set = set(imputed_counties)
        for county in sorted(by_county.keys()):
            v = by_county[county]
            dem = int(v["dem"])
            rep = int(v["rep"])
            oth = int(v["other"])
            total = dem + rep + oth
            if total <= 0:
                continue
            margin = rep - dem
            margin_pct = (margin / total) * 100.0
            winner = winner_from_votes(dem, rep)
            row = {
                "county": county,
                "dem_votes": dem,
                "rep_votes": rep,
                "other_votes": oth,
                "total_votes": total,
                "dem_candidate": dem_candidate,
                "rep_candidate": rep_candidate,
                "margin": int(margin),
                "margin_pct": round(margin_pct, 4),
                "winner": winner,
                "color": color_from_winner(winner),
            }
            if county in imputed_set:
                row["imputed"] = True
            contest_rows.append(row)
            county_payload = {
                "dem_votes": dem,
                "rep_votes": rep,
                "other_votes": oth,
                "total_votes": total,
                "dem_candidate": dem_candidate,
                "rep_candidate": rep_candidate,
                "margin": int(margin),
                "margin_pct": round(margin_pct, 4),
                "winner": winner,
                "competitiveness": {"color": color_from_winner(winner)},
            }
            if county in imputed_set:
                county_payload["imputed"] = True
            county_results_obj[county] = county_payload

        contest_filename = f"{contest_type}_{year}.json"
        contest_path = OUT_CONTESTS_DIR / contest_filename
        contest_meta: Dict[str, Any] = {
            "contest_type": contest_type,
            "year": year,
            "rows": len(contest_rows),
            "dem_total": dem_total,
            "rep_total": rep_total,
            "other_total": other_total,
            "match_coverage_pct": coverage_pct,
            "major_party_contested": True,
        }
        if imputed_counties:
            contest_meta["imputed_count"] = len(imputed_counties)
            contest_meta["imputed_counties"] = imputed_counties
            contest_meta["imputed_from_contest"] = imputed_from_contest
        write_json(
            contest_path,
            {
                "meta": contest_meta,
                "rows": contest_rows,
            },
        )

        manifest_entry: Dict[str, Any] = {
            "year": year,
            "contest_type": contest_type,
            "file": contest_filename,
            "rows": len(contest_rows),
            "dem_total": dem_total,
            "rep_total": rep_total,
            "other_total": other_total,
            "match_coverage_pct": coverage_pct,
            "major_party_contested": True,
        }
        if imputed_counties:
            manifest_entry["imputed_count"] = len(imputed_counties)
            manifest_entry["imputed_from_contest"] = imputed_from_contest
        contest_manifest_files.append(manifest_entry)

        # Aggregated election JSON fallback
        year_node = election_agg["results_by_year"].setdefault(str(year), {})
        office_node = year_node.setdefault(contest_type, {})
        office_node[contest_type] = {"results": county_results_obj}

        # District slices from county-area-weight allocations for each scope.
        for scope in ("congressional", "state_house", "state_senate"):
            scope_weights = county_weights_by_scope.get(scope, {})
            district_float = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0})
            for county, v in by_county.items():
                weights = scope_weights.get(county, [])
                if not weights:
                    continue
                for district_num, w in weights:
                    district_float[district_num]["dem"] += v["dem"] * w
                    district_float[district_num]["rep"] += v["rep"] * w
                    district_float[district_num]["other"] += v["other"] * w

            district_results = {}
            for d in sorted(district_float.keys()):
                dv = district_float[d]
                dem = int(round(dv["dem"]))
                rep = int(round(dv["rep"]))
                oth = int(round(dv["other"]))
                total = dem + rep + oth
                if total <= 0:
                    continue
                margin = rep - dem
                margin_pct = (margin / total) * 100.0
                winner = winner_from_votes(dem, rep)
                district_results[str(d)] = {
                    "dem_votes": dem,
                    "rep_votes": rep,
                    "other_votes": oth,
                    "total_votes": total,
                    "dem_candidate": dem_candidate,
                    "rep_candidate": rep_candidate,
                    "margin": int(margin),
                    "margin_pct": round(margin_pct, 4),
                    "winner": winner,
                    "color": color_from_winner(winner),
                }

            district_meta: Dict[str, Any] = {
                "scope": scope,
                "contest_type": contest_type,
                "year": year,
                "districts": len(district_results),
                "match_coverage_pct": coverage_pct,
                "allocation": "county_area_weighted_from_statewide_contest",
            }
            if imputed_counties:
                district_meta["imputed_count"] = len(imputed_counties)
                district_meta["imputed_from_contest"] = imputed_from_contest
            district_payload = {
                "meta": {
                    **district_meta,
                },
                "general": {
                    "results": district_results,
                },
            }
            district_filename = f"{scope}_{contest_type}_{year}.json"
            write_json(OUT_DISTRICT_CONTESTS_DIR / district_filename, district_payload)
            district_manifest_files.append(
                {
                    "scope": scope,
                    "year": year,
                    "contest_type": contest_type,
                    "file": district_filename,
                    "districts": len(district_results),
                    "dem_total": dem_total,
                    "rep_total": rep_total,
                    "other_total": other_total,
                    "major_party_contested": True,
                    "match_coverage_pct": coverage_pct,
                    "imputed_count": len(imputed_counties),
                    "imputed_from_contest": imputed_from_contest,
                }
            )

            year_d = district_agg["results_by_year"].setdefault(str(year), {})
            scope_d = year_d.setdefault(scope, {})
            scope_d[contest_type] = district_payload

    # District-specific races (U.S. House / State House / State Senate) aggregated
    # directly from precinct results when available.
    scope_to_expected_districts: Dict[str, Set[int]] = {
        "congressional": {d.district_num for d in congressional_districts},
        "state_house": {d.district_num for d in state_house_districts},
        "state_senate": {d.district_num for d in state_senate_districts},
    }

    district_race_root = OPENELECTIONS_GENERATED_ROOT if OPENELECTIONS_GENERATED_ROOT.exists() else OPENELECTIONS_ROOT
    district_votes, district_candidate_votes, district_coverage = collect_district_race_contests(
        district_race_root,
        county_alias_map,
        years={2022, 2024},
    )

    grouped_district = defaultdict(dict)  # (year, scope, contest_type) -> district_num -> votes dict
    for (year, scope, contest_type, district_num), votes in district_votes.items():
        if scope not in scope_to_expected_districts:
            continue
        if district_num not in scope_to_expected_districts[scope]:
            continue
        grouped_district[(year, scope, contest_type)][district_num] = votes

    for (year, scope, contest_type), by_district in sorted(grouped_district.items()):
        expected = scope_to_expected_districts.get(scope, set())
        expected_count = len(expected) or len(by_district)
        covered = district_coverage.get((year, scope, contest_type), set())
        coverage_pct = round((len(covered) / expected_count) * 100.0, 2) if expected_count else 0.0

        dem_total = sum(v["dem"] for v in by_district.values())
        rep_total = sum(v["rep"] for v in by_district.values())
        other_total = sum(v["other"] for v in by_district.values())
        if dem_total <= 0 and rep_total <= 0 and other_total <= 0:
            continue

        district_results = {}
        for d in sorted(by_district.keys()):
            v = by_district[d]
            dem = int(v.get("dem", 0))
            rep = int(v.get("rep", 0))
            oth = int(v.get("other", 0))
            total = dem + rep + oth
            if total <= 0:
                continue
            margin = rep - dem
            margin_pct = (margin / total) * 100.0
            winner = winner_from_votes(dem, rep)
            dem_candidate = top_candidate(district_candidate_votes.get((year, scope, contest_type, d, "dem"), {}))
            rep_candidate = top_candidate(district_candidate_votes.get((year, scope, contest_type, d, "rep"), {}))
            district_results[str(d)] = {
                "dem_votes": dem,
                "rep_votes": rep,
                "other_votes": oth,
                "total_votes": total,
                "dem_candidate": dem_candidate,
                "rep_candidate": rep_candidate,
                "margin": int(margin),
                "margin_pct": round(margin_pct, 4),
                "winner": winner,
                "color": color_from_winner(winner),
            }

        if not district_results:
            continue

        district_meta: Dict[str, Any] = {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "districts": len(district_results),
            "match_coverage_pct": coverage_pct,
            "allocation": "precinct_sum_by_district_number",
            "source": str(district_race_root.name),
        }
        district_payload = {
            "meta": {
                **district_meta,
            },
            "general": {
                "results": district_results,
            },
        }
        district_filename = f"{scope}_{contest_type}_{year}.json"
        write_json(OUT_DISTRICT_CONTESTS_DIR / district_filename, district_payload)
        district_manifest_files.append(
            {
                "scope": scope,
                "year": year,
                "contest_type": contest_type,
                "file": district_filename,
                "districts": len(district_results),
                "dem_total": dem_total,
                "rep_total": rep_total,
                "other_total": other_total,
                "major_party_contested": bool(dem_total > 0 and rep_total > 0),
                "match_coverage_pct": coverage_pct,
            }
        )

        year_d = district_agg["results_by_year"].setdefault(str(year), {})
        scope_d = year_d.setdefault(scope, {})
        scope_d[contest_type] = district_payload

    write_json(OUT_CONTESTS_DIR / "manifest.json", {"files": contest_manifest_files})
    write_json(OUT_DISTRICT_CONTESTS_DIR / "manifest.json", {"files": district_manifest_files})
    write_json(OUT_ELECTION_AGG, election_agg)
    write_json(OUT_DISTRICT_AGG, district_agg)

    print(f"Wrote contests manifest entries: {len(contest_manifest_files)}")
    print(f"Wrote district contest manifest entries: {len(district_manifest_files)}")
    print(f"Wrote geometry: {OUT_CD118_GEOJSON}")
    print(f"Wrote crosswalk: {OUT_CROSSWALK_PRECINCT_TO_CD}")


def main() -> None:
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    try:
        build_outputs()
    finally:
        shutil.rmtree(TMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
