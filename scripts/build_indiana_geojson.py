#!/usr/bin/env python
"""
Build Indiana county/precinct GeoJSON files from TIGER ZIP shapefiles.

Inputs expected in ./Data:
  - tl_2020_18_county20.zip
  - tl_2020_18_vtd20.zip

Outputs written to ./data:
  - census/tl_2020_18_county20.geojson
  - Voting_Precincts.geojson
  - precinct_centroids.geojson
"""

from __future__ import annotations

import json
import zipfile
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import shapefile


ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "Data"
OUTPUT_DIR = ROOT / "data"

COUNTY_ZIP = INPUT_DIR / "tl_2020_18_county20.zip"
PRECINCT_ZIP = INPUT_DIR / "tl_2020_18_vtd20.zip"

COUNTY_OUT = OUTPUT_DIR / "census" / "tl_2020_18_county20.geojson"
PRECINCT_OUT = OUTPUT_DIR / "Voting_Precincts.geojson"
PRECINCT_CENTROIDS_OUT = OUTPUT_DIR / "precinct_centroids.geojson"


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


def _iter_coords(coords: Any) -> Iterable[Tuple[float, float]]:
    if not isinstance(coords, (list, tuple)):
        return
    if len(coords) >= 2 and isinstance(coords[0], (int, float)) and isinstance(coords[1], (int, float)):
        yield (float(coords[0]), float(coords[1]))
        return
    for part in coords:
        yield from _iter_coords(part)


def centroid_from_bbox(geometry: Dict[str, Any] | None) -> List[float] | None:
    if not geometry:
        return None
    coords = list(_iter_coords(geometry.get("coordinates")))
    if not coords:
        return None
    min_x = min(x for x, _ in coords)
    max_x = max(x for x, _ in coords)
    min_y = min(y for _, y in coords)
    max_y = max(y for _, y in coords)
    return [(min_x + max_x) / 2.0, (min_y + max_y) / 2.0]


def extract_shapefile(zip_path: Path, tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tmp_dir)
    shp_files = list(tmp_dir.glob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp file found in {zip_path}")
    return shp_files[0]


def shapefile_to_feature_collection(shp_path: Path) -> Dict[str, Any]:
    reader = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in reader.fields[1:]]
    features: List[Dict[str, Any]] = []
    for shape_record in reader.iterShapeRecords():
        props = {field: clean_value(value) for field, value in zip(fields, shape_record.record)}
        geom = shape_record.shape.__geo_interface__
        features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": geom,
            }
        )
    return {"type": "FeatureCollection", "features": features}


def build_centroid_feature_collection(feature_collection: Dict[str, Any]) -> Dict[str, Any]:
    out_features: List[Dict[str, Any]] = []
    for feature in feature_collection.get("features", []):
        center = centroid_from_bbox(feature.get("geometry"))
        if center is None:
            continue
        out_features.append(
            {
                "type": "Feature",
                "properties": dict(feature.get("properties") or {}),
                "geometry": {"type": "Point", "coordinates": center},
            }
        )
    return {"type": "FeatureCollection", "features": out_features}


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))


def ensure_inputs() -> None:
    missing = [p for p in (COUNTY_ZIP, PRECINCT_ZIP) if not p.exists()]
    if missing:
        lines = "\n".join(f"  - {p}" for p in missing)
        raise FileNotFoundError(f"Missing required input ZIP files:\n{lines}")


def main() -> None:
    ensure_inputs()
    tmp_root = ROOT / ".tmp_geojson_build"
    county_tmp = tmp_root / "county"
    precinct_tmp = tmp_root / "precinct"
    shutil.rmtree(tmp_root, ignore_errors=True)
    tmp_root.mkdir(parents=True, exist_ok=True)

    try:
        county_shp = extract_shapefile(COUNTY_ZIP, county_tmp)
        precinct_shp = extract_shapefile(PRECINCT_ZIP, precinct_tmp)

        county_geojson = shapefile_to_feature_collection(county_shp)
        precinct_geojson = shapefile_to_feature_collection(precinct_shp)
        precinct_centroids = build_centroid_feature_collection(precinct_geojson)

        write_json(COUNTY_OUT, county_geojson)
        write_json(PRECINCT_OUT, precinct_geojson)
        write_json(PRECINCT_CENTROIDS_OUT, precinct_centroids)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    print(f"Wrote {COUNTY_OUT} ({len(county_geojson.get('features', []))} features)")
    print(f"Wrote {PRECINCT_OUT} ({len(precinct_geojson.get('features', []))} features)")
    print(f"Wrote {PRECINCT_CENTROIDS_OUT} ({len(precinct_centroids.get('features', []))} features)")


if __name__ == "__main__":
    main()
