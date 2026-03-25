#!/usr/bin/env python
"""
Build Indiana historical precinct centroids using NHGIS block crosswalks.

Pipeline:
  1) Load 2020 tabblock internal points (INTPTLON20/INTPTLAT20).
  2) Chain NHGIS crosswalks to back-cast block points:
       2020 -> 2010 -> 2000
  3) Spatially assign crosswalked block points to VTD polygons:
       - 2010 VTDs from tl_2012_18_vtd10.zip
       - 2000 VTDs from county tl_2008_*_vtd00.zip files
  4) Emit weighted centroid GeoJSONs for older precinct years.
"""

from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import shapefile
from shapely.geometry import Point, shape
from shapely.strtree import STRtree


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
TMP_DIR = ROOT / ".tmp_historical_centroids"

TABBLOCK_2020_ZIP = DATA_DIR / "tl_2020_18_tabblock20.zip"
VTD10_ZIP = DATA_DIR / "tl_2012_18_vtd10.zip"
VTD00_DIR = DATA_DIR / "TIGER2008_18_IN_counties"

XWALK_2010_2020_ZIP = DATA_DIR / "nhgis_blk2010_blk2020_18.zip"
XWALK_2000_2010_ZIP = DATA_DIR / "nhgis_blk2000_blk2010_18.zip"

OUT_2010 = DATA_DIR / "precinct_centroids_2010_xwalk.geojson"
OUT_2000 = DATA_DIR / "precinct_centroids_2000_xwalk.geojson"

INDIANA_STATEFP = "18"


@dataclass
class VtdFeature:
    countyfp: str
    properties: Dict[str, Any]
    geom: Any


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def extract_zip(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(out_dir)
    shp_files = list(out_dir.glob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp found in {zip_path}")
    return shp_files[0]


def find_csv_in_zip(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                return name
    raise FileNotFoundError(f"No CSV found in {zip_path}")


def parse_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def load_block_points_2020(tabblock_zip: Path, extract_dir: Path) -> Dict[str, Tuple[float, float]]:
    shp_path = extract_zip(tabblock_zip, extract_dir)
    reader = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in reader.fields[1:]]
    idx_geoid = fields.index("GEOID20")
    idx_lon = fields.index("INTPTLON20")
    idx_lat = fields.index("INTPTLAT20")

    out: Dict[str, Tuple[float, float]] = {}
    for rec in reader.iterRecords():
        geoid = str(rec[idx_geoid]).strip()
        if not geoid.startswith(INDIANA_STATEFP):
            continue
        lon = parse_float(rec[idx_lon])
        lat = parse_float(rec[idx_lat])
        if lon is None or lat is None:
            continue
        out[geoid] = (lon, lat)
    return out


def build_crosswalked_points(
    crosswalk_zip: Path,
    src_col: str,
    dst_col: str,
    dst_points: Dict[str, Tuple[float, float] | Tuple[float, float, float]],
) -> Dict[str, Tuple[float, float, float]]:
    csv_name = find_csv_in_zip(crosswalk_zip)
    accum: Dict[str, List[float]] = {}

    with zipfile.ZipFile(crosswalk_zip, "r") as zf:
        with zf.open(csv_name, "r") as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"))
            for row in reader:
                src = (row.get(src_col) or "").strip()
                dst = (row.get(dst_col) or "").strip()
                if not src or not dst:
                    continue
                if not src.startswith(INDIANA_STATEFP):
                    continue
                dst_pt = dst_points.get(dst)
                if not dst_pt:
                    continue
                weight = parse_float(row.get("weight"))
                if weight is None or weight <= 0:
                    continue
                x = float(dst_pt[0])
                y = float(dst_pt[1])
                if src not in accum:
                    accum[src] = [0.0, 0.0, 0.0]
                bucket = accum[src]
                bucket[0] += weight
                bucket[1] += weight * x
                bucket[2] += weight * y

    out: Dict[str, Tuple[float, float, float]] = {}
    for src, (sum_w, sum_x, sum_y) in accum.items():
        if sum_w <= 0:
            continue
        out[src] = (sum_x / sum_w, sum_y / sum_w, sum_w)
    return out


def read_shape_records(shp_path: Path) -> Iterable[Tuple[Dict[str, Any], Any]]:
    reader = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in reader.fields[1:]]
    for sr in reader.iterShapeRecords():
        props = {field: clean_value(val) for field, val in zip(fields, sr.record)}
        geom = shape(sr.shape.__geo_interface__)
        if geom.is_empty:
            continue
        yield props, geom


def load_vtd10_features(vtd10_zip: Path, extract_dir: Path) -> List[VtdFeature]:
    shp_path = extract_zip(vtd10_zip, extract_dir)
    out: List[VtdFeature] = []
    for props, geom in read_shape_records(shp_path):
        county = str(props.get("COUNTYFP10") or "").zfill(3)
        if not county or county == "000":
            continue
        out.append(VtdFeature(countyfp=county, properties=props, geom=geom))
    return out


def load_vtd00_features(vtd00_dir: Path, extract_root: Path) -> List[VtdFeature]:
    out: List[VtdFeature] = []
    for zip_path in sorted(vtd00_dir.glob("tl_2008_*_vtd00.zip")):
        shp_dir = extract_root / zip_path.stem
        shp_path = extract_zip(zip_path, shp_dir)
        for props, geom in read_shape_records(shp_path):
            county = str(props.get("COUNTYFP00") or "").zfill(3)
            if not county or county == "000":
                continue
            out.append(VtdFeature(countyfp=county, properties=props, geom=geom))
    return out


def build_county_index(features: List[VtdFeature]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[int]] = defaultdict(list)
    for i, feature in enumerate(features):
        grouped[feature.countyfp].append(i)

    out: Dict[str, Dict[str, Any]] = {}
    for countyfp, indices in grouped.items():
        geoms = [features[i].geom for i in indices]
        out[countyfp] = {
            "feature_indices": indices,
            "geoms": geoms,
            "tree": STRtree(geoms),
        }
    return out


def assign_block_points_to_vtd(
    block_points: Dict[str, Tuple[float, float, float]],
    features: List[VtdFeature],
    county_index: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[int, List[float]], int]:
    # feature_index -> [sum_weight, sum_weighted_x, sum_weighted_y, block_count]
    agg: Dict[int, List[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
    unmatched = 0

    for block_geoid, (x, y, weight) in block_points.items():
        countyfp = block_geoid[2:5]
        county_ctx = county_index.get(countyfp)
        if county_ctx is None:
            unmatched += 1
            continue

        point = Point(x, y)
        candidate_local = county_ctx["tree"].query(point)
        hit_feature_index = None
        for local_idx in candidate_local:
            li = int(local_idx)
            geom = county_ctx["geoms"][li]
            if geom.covers(point):
                hit_feature_index = county_ctx["feature_indices"][li]
                break

        if hit_feature_index is None:
            unmatched += 1
            continue

        rec = agg[hit_feature_index]
        rec[0] += weight
        rec[1] += weight * x
        rec[2] += weight * y
        rec[3] += 1.0

    return agg, unmatched


def build_centroid_feature_collection(
    features: List[VtdFeature],
    assignment_agg: Dict[int, List[float]],
    src_label: str,
) -> Dict[str, Any]:
    out_features: List[Dict[str, Any]] = []
    for idx, feature in enumerate(features):
        rec = assignment_agg.get(idx)
        if rec and rec[0] > 0:
            lon = rec[1] / rec[0]
            lat = rec[2] / rec[0]
            blocks = int(rec[3])
            weight_sum = rec[0]
            source = "crosswalk_weighted_blocks"
        else:
            rp = feature.geom.representative_point()
            lon = float(rp.x)
            lat = float(rp.y)
            blocks = 0
            weight_sum = 0.0
            source = "polygon_representative_point_fallback"

        props = dict(feature.properties)
        props["CENTROID_SRC"] = source
        props["XWALK_CHAIN"] = src_label
        props["XWALK_WSUM"] = round(weight_sum, 8)
        props["XWALK_BLOCKS"] = blocks

        out_features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
            }
        )

    return {"type": "FeatureCollection", "features": out_features}


def write_geojson(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))


def ensure_inputs() -> None:
    required = [
        TABBLOCK_2020_ZIP,
        VTD10_ZIP,
        XWALK_2010_2020_ZIP,
        XWALK_2000_2010_ZIP,
    ]
    missing = [p for p in required if not p.exists()]
    if not VTD00_DIR.exists():
        missing.append(VTD00_DIR)
    if missing:
        raise FileNotFoundError("Missing required inputs:\n" + "\n".join(f"  - {m}" for m in missing))


def main() -> None:
    ensure_inputs()
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        points_2020 = load_block_points_2020(TABBLOCK_2020_ZIP, TMP_DIR / "tabblock20")
        points_2010 = build_crosswalked_points(
            XWALK_2010_2020_ZIP,
            src_col="blk2010ge",
            dst_col="blk2020ge",
            dst_points=points_2020,
        )
        points_2000 = build_crosswalked_points(
            XWALK_2000_2010_ZIP,
            src_col="blk2000ge",
            dst_col="blk2010ge",
            dst_points=points_2010,
        )

        vtd10_features = load_vtd10_features(VTD10_ZIP, TMP_DIR / "vtd10")
        vtd00_features = load_vtd00_features(VTD00_DIR, TMP_DIR / "vtd00")

        idx10 = build_county_index(vtd10_features)
        idx00 = build_county_index(vtd00_features)

        agg10, unmatched10 = assign_block_points_to_vtd(points_2010, vtd10_features, idx10)
        agg00, unmatched00 = assign_block_points_to_vtd(points_2000, vtd00_features, idx00)

        fc10 = build_centroid_feature_collection(
            vtd10_features,
            agg10,
            src_label="nhgis_blk2010_blk2020_18",
        )
        fc00 = build_centroid_feature_collection(
            vtd00_features,
            agg00,
            src_label="nhgis_blk2000_blk2010_18",
        )

        write_geojson(OUT_2010, fc10)
        write_geojson(OUT_2000, fc00)

        print(f"Loaded 2020 block points: {len(points_2020):,}")
        print(f"Derived 2010 block points from crosswalk: {len(points_2010):,}")
        print(f"Derived 2000 block points from crosswalk: {len(points_2000):,}")
        print(f"2010 VTD features: {len(vtd10_features):,} | unmatched block points: {unmatched10:,}")
        print(f"2000 VTD features: {len(vtd00_features):,} | unmatched block points: {unmatched00:,}")
        print(f"Wrote {OUT_2010}")
        print(f"Wrote {OUT_2000}")
    finally:
        shutil.rmtree(TMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()

