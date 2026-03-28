import argparse
import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path


@dataclass(frozen=True)
class ElectionInfo:
    election_id: str  # YYYYMMDD
    state: str  # two-letter lowercase
    election_type: str  # general|primary|...


ORDINAL_WORDS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
    "eleventh": 11,
    "twelfth": 12,
    "thirteenth": 13,
    "fourteenth": 14,
    "fifteenth": 15,
    "sixteenth": 16,
    "seventeenth": 17,
    "eighteenth": 18,
    "nineteenth": 19,
    "twentieth": 20,
}


OFFICE_CATEGORY_MAP = {
    "us senator": ("U.S. Senate", None),
    "us representative": ("U.S. House", "district_from_office"),
    "state senator": ("State Senate", "district_from_office"),
    "state representative": ("State House", "district_from_office"),
    "presidential electors for us president & vp": ("President", None),
    "secretary of state": ("Secretary of State", None),
    "auditor of state": ("Auditor of State", None),
    "treasurer of state": ("Treasurer of State", None),
    "attorney general": ("Attorney General", None),
    "governor": ("Governor", None),
}


PARTY_MAP = {
    "republican": "REP",
    "democratic": "DEM",
    "libertarian": "LBT",
    "independent": "IND",
    "green": "GRN",
}


OUTPUT_HEADER_BASE = [
    "county",
    "precinct",
    "office",
    "district",
    "seats",
    "party",
    "candidate",
    "votes",
]

OUTPUT_HEADER_WITH_BREAKDOWN = OUTPUT_HEADER_BASE + [
    "early_voting",
    "election_day",
    "absentee",
]


def slugify_county(county: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", county.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unknown"


def _tuesday_after_first_monday(year: int, month: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != 0:  # Monday
        d += timedelta(days=1)
    return d + timedelta(days=1)  # Tuesday after first Monday


def parse_election_info(election_label: str) -> ElectionInfo:
    # Common IDOE export labels: "2022 General Election", "2024 Primary Election", etc.
    m = re.match(r"^\s*(\d{4})\s+([A-Za-z]+)\s+Election\s*$", election_label)
    if not m:
        raise ValueError(f"Unrecognized Election label: {election_label!r}")

    year = int(m.group(1))
    kind = m.group(2).strip().lower()

    if kind == "general":
        d = _tuesday_after_first_monday(year, 11)
    elif kind == "primary":
        # Indiana primary is typically the first Tuesday after the first Monday in May.
        d = _tuesday_after_first_monday(year, 5)
    else:
        raise ValueError(f"Unsupported election kind {kind!r} in {election_label!r}")

    return ElectionInfo(election_id=d.strftime("%Y%m%d"), state="in", election_type=kind)


def normalize_party(party: str, *, force_blank: bool = False) -> str:
    if force_blank:
        return ""
    party = (party or "").strip()
    if not party:
        return ""
    mapped = PARTY_MAP.get(party.lower())
    return mapped if mapped is not None else party


def normalize_candidate(name: str) -> tuple[str, bool]:
    """
    Returns (candidate, is_write_in).
    """
    name = (name or "").strip()
    if not name:
        return "", False

    is_write_in = "(w/i)" in name.lower()
    if is_write_in:
        name = re.sub(r"\s*\(w/i\)\s*", "", name, flags=re.IGNORECASE).strip()
        if not name:
            name = "Write-In"
    return name, is_write_in


def extract_district(office_text: str) -> str:
    t = (office_text or "").strip()
    if not t:
        return ""

    m = re.search(r"\bDistrict\s*([0-9]{1,3})\b", t, flags=re.IGNORECASE)
    if m:
        return str(int(m.group(1)))

    m = re.search(r",\s*([A-Za-z]+)\s+District\b", t)
    if m:
        word = m.group(1).strip().lower()
        if word in ORDINAL_WORDS:
            return str(ORDINAL_WORDS[word])

    return ""


def normalize_office_and_district(office: str, office_category: str) -> tuple[str, str]:
    office = (office or "").strip()
    office_category_norm = (office_category or "").strip().lower()

    mapped = OFFICE_CATEGORY_MAP.get(office_category_norm)
    if mapped is None:
        return office, extract_district(office)

    normalized_office, district_mode = mapped
    if district_mode == "district_from_office":
        return normalized_office, extract_district(office)
    return normalized_office, ""


def int_required(value: str | None, *, field: str) -> str:
    v = (value or "").strip()
    if v == "":
        return "0"
    v = v.replace(",", "")
    try:
        return str(int(v))
    except ValueError as e:
        raise ValueError(f"Invalid integer for {field}: {value!r}") from e


def int_optional(value: str | None, *, field: str) -> str:
    v = (value or "").strip()
    if v == "":
        return ""
    v = v.replace(",", "")
    try:
        return str(int(v))
    except ValueError as e:
        raise ValueError(f"Invalid integer for {field}: {value!r}") from e


def open_writer(path: Path, *, header: list[str]) -> tuple[csv.DictWriter, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    f = path.open("w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=header, lineterminator="\n")
    w.writeheader()
    return w, f


def convert_file(
    input_csv: Path,
    *,
    out_dir: Path,
    split_by_county: bool,
    consolidated: bool,
    include_vote_breakdown: bool,
) -> dict[str, int]:
    """
    Returns counts keyed by output file basename.
    """
    writers_by_path: dict[Path, tuple[csv.DictWriter, object]] = {}
    counts: dict[str, int] = defaultdict(int)

    def get_writer(out_path: Path) -> csv.DictWriter:
        if out_path not in writers_by_path:
            header = OUTPUT_HEADER_WITH_BREAKDOWN if include_vote_breakdown else OUTPUT_HEADER_BASE
            writers_by_path[out_path] = open_writer(out_path, header=header)
        return writers_by_path[out_path][0]

    with input_csv.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {
            "Election",
            "ReportingCountyName",
            "DataEntryJurisdictionName",
            "DataEntryLevelName",
            "Office",
            "OfficeCategory",
            "NameonBallot",
            "PoliticalParty",
            "NumberofOfficeSeats",
            "TotalVotes",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{input_csv}: missing required columns: {', '.join(missing)}")

        for row in reader:
            if (row.get("DataEntryLevelName") or "").strip().lower() != "precinct":
                continue

            election_info = parse_election_info(row["Election"])
            county = (row.get("ReportingCountyName") or "").strip()
            precinct = (row.get("DataEntryJurisdictionName") or "").strip()

            office, district = normalize_office_and_district(row.get("Office"), row.get("OfficeCategory"))
            candidate, is_write_in = normalize_candidate(row.get("NameonBallot"))
            party = normalize_party(row.get("PoliticalParty"), force_blank=is_write_in or candidate in {"Yes", "No"})

            votes = int_required(row.get("TotalVotes"), field="TotalVotes")
            seats = int_optional(row.get("NumberofOfficeSeats"), field="NumberofOfficeSeats")

            out_paths: list[Path] = []
            if consolidated:
                out_paths.append(
                    out_dir
                    / f"{election_info.election_id}__{election_info.state}__{election_info.election_type}__precinct.csv"
                )
            if split_by_county:
                county_slug = slugify_county(county)
                out_paths.append(
                    out_dir
                    / f"{election_info.election_id}__{election_info.state}__{election_info.election_type}__{county_slug}__precinct.csv"
                )

            out_row = {
                "county": county,
                "precinct": precinct,
                "office": office,
                "district": district,
                "seats": seats,
                "party": party,
                "candidate": candidate,
                "votes": votes,
            }
            if include_vote_breakdown:
                out_row["early_voting"] = ""
                out_row["election_day"] = ""
                out_row["absentee"] = ""

            for out_path in out_paths:
                get_writer(out_path).writerow(out_row)
                counts[out_path.name] += 1

    for _, (_, handle) in writers_by_path.items():
        handle.close()

    return dict(counts)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Convert IDOE AllOfficeResults precinct export CSV to OpenElections-style precinct CSVs."
    )
    p.add_argument("inputs", nargs="+", type=Path, help="Input AllOfficeResults CSV file(s).")
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path("Data") / "openelections_generated",
        help="Output directory (default: Data/openelections_generated).",
    )
    p.add_argument(
        "--split-by-county",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write one output file per county (default: enabled).",
    )
    p.add_argument(
        "--consolidated",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also write one consolidated statewide file per election (default: enabled).",
    )
    p.add_argument(
        "--include-vote-breakdown",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Include early_voting/election_day/absentee columns (default: disabled; votes-only).",
    )

    args = p.parse_args()

    all_counts: dict[str, int] = {}
    for input_csv in args.inputs:
        counts = convert_file(
            input_csv,
            out_dir=args.out_dir,
            split_by_county=args.split_by_county,
            consolidated=args.consolidated,
            include_vote_breakdown=args.include_vote_breakdown,
        )
        all_counts.update(counts)

    if all_counts:
        print("Wrote:")
        for name, count in sorted(all_counts.items()):
            print(f"  {name}: {count:,} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
