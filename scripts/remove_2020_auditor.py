#!/usr/bin/env python
"""Remove a county Auditor race misclassified as statewide in 2020."""

import json
from pathlib import Path


DATA = Path(__file__).resolve().parents[1] / "data"


def update(path, action):
    payload = json.loads(path.read_text(encoding="utf-8"))
    action(payload)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main():
    county_file = "auditor_2020.json"
    district_files = {f"{scope}_auditor_2020.json" for scope in ("congressional", "state_house", "state_senate")}
    update(DATA / "contests" / "manifest.json", lambda p: p.update(files=[r for r in p["files"] if r["file"] != county_file]))
    update(DATA / "district_contests" / "manifest.json", lambda p: p.update(files=[r for r in p["files"] if r["file"] not in district_files]))
    update(DATA / "in_elections_aggregated.json", lambda p: p["results_by_year"]["2020"].pop("auditor", None))

    def remove_districts(payload):
        for scope in ("congressional", "state_house", "state_senate"):
            payload["results_by_year"]["2020"][scope].pop("auditor", None)

    update(DATA / "in_district_results_2022_lines.json", remove_districts)

    def remove_county_bundle(payload):
        old = payload["county_results_by_year"]["2020"].pop("auditor", None)
        payload["manifest"] = [row for row in payload["manifest"] if row["file"] != county_file]
        if old is not None:
            payload["meta"]["contest_files"] -= 1
            payload["meta"]["county_rows"] -= 1

    update(DATA / "county_contests.json", remove_county_bundle)
    for path in [DATA / "contests" / county_file, *(DATA / "district_contests" / name for name in sorted(district_files))]:
        path.unlink(missing_ok=True)
    print("Removed 2020 Auditor from contest files, manifests, and aggregates.")


if __name__ == "__main__":
    main()
