# INPrecinctMap

Indiana election atlas with county, precinct, congressional, and state legislative views.

## UI highlights

- County hover tooltips now use the richer NCMap-style presentation in `index.html`.
- Desktop county hovers support compact previews that can be clicked to pin and expand.
- Mobile and touch interactions keep hover shells non-blocking until a tooltip is pinned.
- Hover cards include contest-aware winner, competitiveness tier, shift/flip summaries, and county detail disclosure.

## What is in this repo

- Frontend app: `index.html`
- Build scripts: `scripts/`
- Generated data: `Data/`

Key generated outputs:

- County contests (per contest): `Data/contests/*.json`
- County contests (single file): `Data/county_contests.json`
- County contest manifest: `Data/contests/manifest.json`
- Congressional + legislative district contests: `Data/district_contests/*.json`
- District contest manifest: `Data/district_contests/manifest.json`
- Congressional tileset GeoJSON: `Data/tileset/in_cd118_tileset.geojson`
- State House/Senate tileset GeoJSON:
  - `Data/tileset/in_state_house_2022_lines_tileset.geojson`
  - `Data/tileset/in_state_senate_2022_lines_tileset.geojson`
- Crosswalks: `Data/crosswalks/*.csv`
- Historical precinct centroids:
  - `Data/precinct_centroids_2010_xwalk.geojson`
  - `Data/precinct_centroids_2000_xwalk.geojson`

## Local setup

1. Create virtual env:

```powershell
py -m venv .venv
```

2. Activate:

```powershell
.\.venv\Scripts\Activate.ps1
```

3. Install deps used by build scripts:

```powershell
pip install pyshp shapely numpy
```

## Rebuild data

Build Indiana geometry, contests, district layers, and crosswalks:

```powershell
.\.venv\Scripts\python.exe scripts\build_indiana_congressional_data.py
```

Build historical centroid carryover files:

```powershell
.\.venv\Scripts\python.exe scripts\build_historical_centroids_from_crosswalks.py
```

Build consolidated county contest JSON:

```powershell
.\.venv\Scripts\python.exe scripts\build_county_contests_json.py
```

## Convert IDOE "AllOfficeResults" exports to OpenElections format

These exports (like `Data/2022AllOfficeResults.csv` / `Data/2024AllOfficeResults.csv`) can be converted into the
OpenElections-style precinct CSV schema:

```powershell
.\.venv\Scripts\python.exe scripts\convert_alloffice_to_openelections.py Data\2022AllOfficeResults.csv Data\2024AllOfficeResults.csv
```

Outputs are written to `Data/openelections_generated/` (ignored by git).
By default the output is votes-only (uses `TotalVotes`); add `--include-vote-breakdown` if you want the extra vote-type columns.

## Mapbox token

`index.html` reads token from:

```js
window.MAPBOX_TOKEN
```

Set it before loading the app (for example in an inline script or your hosting template).

## Notes

- Raw source ZIPs and very large inputs are ignored in `.gitignore`.
- County and district contests in this repo are currently built from contested statewide general-election slices.
