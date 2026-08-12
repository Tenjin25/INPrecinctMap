#!/usr/bin/env python3
"""Create reusable 2020-tabblock to 2022 Indiana legislative crosswalks."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import shapefile
from shapely.geometry import Point, shape
from shapely.strtree import STRtree


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
DISTRICTS = {
    "state_house": DATA / "tl_2022_18_sldl.zip",
    "state_senate": DATA / "tl_2022_18_sldu.zip",
}


def load_districts(path: Path):
    reader = shapefile.Reader(str(path))
    names = [field[0] for field in reader.fields[1:]]
    district_field = "SLDLST" if "SLDLST" in names else "SLDUST"
    district_index = names.index(district_field)
    geoms, nums = [], []
    for record, shp in zip(reader.records(), reader.shapes()):
        geoms.append(shape(shp.__geo_interface__))
        nums.append(str(record[district_index]).lstrip("0") or "0")
    return geoms, nums


def build(blocks_path: Path, scope: str, output: Path) -> None:
    district_geoms, district_nums = load_districts(DISTRICTS[scope])
    tree = STRtree(district_geoms)
    reader = shapefile.Reader(str(blocks_path))
    fields = [field[0] for field in reader.fields[1:]]
    geoid_index = fields.index("GEOID20")
    rows = []
    unmatched = 0
    for record, shp in zip(reader.records(), reader.shapes()):
        min_x, min_y, max_x, max_y = shp.bbox
        point = Point((min_x + max_x) / 2.0, (min_y + max_y) / 2.0)
        assigned = None
        for index in tree.query(point):
            index = int(index)
            if district_geoms[index].covers(point):
                assigned = district_nums[index]
                break
        if assigned is None:
            unmatched += 1
            continue
        rows.append({"GEOID20": str(record[geoid_index]), "district_num": assigned, "weight": "1.000000000000"})

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["GEOID20", "district_num", "weight"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {output}: {len(rows)} blocks; unmatched: {unmatched}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blocks", type=Path, default=DATA / "tl_2020_18_tabblock20.zip")
    parser.add_argument("--scope", choices=sorted(DISTRICTS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.blocks, args.scope, args.output)


if __name__ == "__main__":
    main()
