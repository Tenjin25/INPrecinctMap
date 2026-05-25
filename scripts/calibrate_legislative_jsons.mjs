import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CAL_DIR = path.join(DATA_DIR, "Calibration csvs");
const DISTRICT_CONTESTS_DIR = path.join(DATA_DIR, "district_contests");
const DISTRICT_AGG_PATH = path.join(DATA_DIR, "in_district_results_2022_lines.json");
const CALIBRATION_REPORT_PATH = path.join(DATA_DIR, "district_calibration_report.json");
const DRA_VTD_GEOJSON_PATH = path.join(DATA_DIR, "sources", "dra", "in_v07", "IN_2020_VD_tabblock.vtd.datasets.geojson");
const COUNTIES_GEOJSON_PATH = path.join(DATA_DIR, "census", "tl_2020_18_county20.geojson");
const CROSSWALK_BY_SCOPE = {
  state_house: path.join(DATA_DIR, "crosswalks", "precinct_to_2022_state_house.csv"),
  state_senate: path.join(DATA_DIR, "crosswalks", "precinct_to_2022_state_senate.csv"),
  congressional: path.join(DATA_DIR, "crosswalks", "precinct_to_cd118.csv"),
};

const ORDINAL_NUMBER_TOKEN_MAP = {
  FIRST: "1",
  SECOND: "2",
  THIRD: "3",
  FOURTH: "4",
  FIFTH: "5",
  SIXTH: "6",
  SEVENTH: "7",
  EIGHTH: "8",
  NINTH: "9",
  TENTH: "10",
  ELEVENTH: "11",
  TWELFTH: "12",
  THIRTEENTH: "13",
  FOURTEENTH: "14",
  FIFTEENTH: "15",
  SIXTEENTH: "16",
  SEVENTEENTH: "17",
  EIGHTEENTH: "18",
  NINETEENTH: "19",
  TWENTIETH: "20",
  "TWENTY FIRST": "21",
  "TWENTY SECOND": "22",
  "TWENTY THIRD": "23",
  "TWENTY FOURTH": "24",
  "TWENTY FIFTH": "25",
  "TWENTY SIXTH": "26",
  "TWENTY SEVENTH": "27",
  "TWENTY EIGHTH": "28",
  "TWENTY NINTH": "29",
  THIRTIETH: "30",
  "THIRTY FIRST": "31",
};

let draGeojsonCache = null;
let countyFpToKeyCache = null;
const crosswalkCache = new Map();
const draDistrictTurnoutCache = new Map();

function parseCsvLine(line) {
  const out = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    if (ch === ",") {
      out.push(field);
      field = "";
      continue;
    }
    field += ch;
  }
  out.push(field);
  return out;
}

function readCalibrationCsv(csvPath) {
  const raw = fs.readFileSync(csvPath, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return new Map();

  const header = parseCsvLine(lines[0]).map((h) => h.trim());
  const idx = (name) => header.findIndex((h) => h === name);
  const idIdx = idx("ID");
  const demIdx = idx("Dem");
  const repIdx = idx("Rep");
  const othIdx = idx("Oth");

  if (idIdx < 0 || demIdx < 0 || repIdx < 0 || othIdx < 0) {
    throw new Error(`Missing expected columns in ${path.basename(csvPath)}`);
  }

  const byId = new Map();
  for (let i = 1; i < lines.length; i++) {
    const row = parseCsvLine(lines[i]);
    const id = (row[idIdx] ?? "").trim();
    if (!id || !/^\d+$/.test(id)) continue;
    const dem = Number(row[demIdx]);
    const rep = Number(row[repIdx]);
    const oth = Number(row[othIdx]);
    if (!Number.isFinite(dem) || !Number.isFinite(rep) || !Number.isFinite(oth)) continue;
    byId.set(id, { dem, rep, oth });
  }
  return byId;
}

function roundTo(value, digits) {
  const p = 10 ** digits;
  return Math.round((value + Number.EPSILON) * p) / p;
}

function normalizeSpace(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function normalizeAlnumToken(text) {
  let t = String(text || "").toUpperCase().replaceAll("_", " ");
  t = t.replace(/[^A-Z0-9 ]+/g, " ");
  t = normalizeSpace(t);
  t = t.replace(/\bCOUNTY\b/g, "");
  return normalizeSpace(t);
}

function normalizeNumericToken(token) {
  if (/^\d+$/.test(token)) return String(Number(token));
  return token;
}

function normalizePrecinctTokens(raw) {
  let t = normalizeAlnumToken(raw);
  if (!t) return [];
  for (const phrase of Object.keys(ORDINAL_NUMBER_TOKEN_MAP).sort((a, b) => b.length - a.length)) {
    t = t.replace(new RegExp(`\\b${phrase}\\b`, "g"), ORDINAL_NUMBER_TOKEN_MAP[phrase]);
  }
  t = t.replace(/^\d+\s+/, "");
  return normalizeSpace(t).split(" ").filter(Boolean);
}

function canonicalPrecinctKey(raw) {
  const tokens = normalizePrecinctTokens(raw);
  if (!tokens.length) return "";
  return tokens.map(normalizeNumericToken).join("");
}

function parseFloatSafe(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function apportionIntegerVotes(floatVotesByDistrict, targetTotal) {
  const districts = Object.keys(floatVotesByDistrict).sort((a, b) => Number(a) - Number(b));
  if (!districts.length) return {};
  const tgt = Math.max(0, Math.round(targetTotal));
  if (tgt === 0) {
    const out = {};
    for (const d of districts) out[d] = 0;
    return out;
  }

  const floors = {};
  const remainders = [];
  let floorSum = 0;
  for (const d of districts) {
    const v = Math.max(0, parseFloatSafe(floatVotesByDistrict[d], 0));
    const base = Math.floor(v);
    floors[d] = base;
    floorSum += base;
    remainders.push({ d, rem: v - base });
  }

  let diff = tgt - floorSum;
  if (diff > 0) {
    remainders
      .slice()
      .sort((a, b) => (b.rem - a.rem) || (Number(a.d) - Number(b.d)))
      .slice(0, diff)
      .forEach(({ d }) => {
        floors[d] += 1;
      });
  } else if (diff < 0) {
    let remaining = -diff;
    const order = remainders
      .slice()
      .sort((a, b) => (a.rem - b.rem) || (Number(a.d) - Number(b.d)))
      .map((x) => x.d);
    while (remaining > 0) {
      let progressed = false;
      for (const d of order) {
        if (remaining <= 0) break;
        if ((floors[d] || 0) > 0) {
          floors[d] -= 1;
          remaining -= 1;
          progressed = true;
        }
      }
      if (!progressed) break;
    }
  }
  return floors;
}

function winnerForMargin(margin) {
  if (margin < 0) return { winner: "DEM", color: "#2563eb" };
  if (margin > 0) return { winner: "REP", color: "#dc2626" };
  return { winner: "TIE", color: "#64748b" };
}

function getCountyFpToKey() {
  if (countyFpToKeyCache) return countyFpToKeyCache;
  if (!fs.existsSync(COUNTIES_GEOJSON_PATH)) return new Map();
  const obj = JSON.parse(fs.readFileSync(COUNTIES_GEOJSON_PATH, "utf8"));
  const out = new Map();
  for (const f of obj?.features || []) {
    const p = f?.properties || {};
    const cfp = String(p.COUNTYFP20 || "").padStart(3, "0");
    const name = String(p.NAME20 || "").trim();
    if (cfp && name) out.set(cfp, normalizeAlnumToken(name));
  }
  countyFpToKeyCache = out;
  return out;
}

function getDraGeojson() {
  if (draGeojsonCache) return draGeojsonCache;
  if (!fs.existsSync(DRA_VTD_GEOJSON_PATH)) return null;
  draGeojsonCache = JSON.parse(fs.readFileSync(DRA_VTD_GEOJSON_PATH, "utf8"));
  return draGeojsonCache;
}

function readCrosswalk(scope) {
  if (crosswalkCache.has(scope)) return crosswalkCache.get(scope);
  const p = CROSSWALK_BY_SCOPE[scope];
  if (!p || !fs.existsSync(p)) return [];
  const raw = fs.readFileSync(p, "utf8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return [];
  const header = parseCsvLine(lines[0]).map((h) => h.trim());
  const idx = (name) => header.findIndex((h) => h === name);
  const pkIdx = idx("precinct_key");
  const dIdx = idx("district_num");
  const wIdx = idx("area_weight");
  if (pkIdx < 0 || dIdx < 0 || wIdx < 0) return [];
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const row = parseCsvLine(lines[i]);
    const precinctKey = normalizeSpace(row[pkIdx] || "");
    const districtId = normalizeSpace(row[dIdx] || "");
    const w = parseFloatSafe(row[wIdx], 0);
    if (!precinctKey || !districtId || w <= 0) continue;
    rows.push({ precinctKey, districtId, weight: w });
  }
  crosswalkCache.set(scope, rows);
  return rows;
}

function getDraDatasetKey(contestType, year) {
  const y = Number(year);
  const key = `${y}:${contestType}`;
  const map = {
    "2008:president": "E_08_PRES",
    "2012:president": "E_12_PRES",
    "2016:president": "E_16_PRES",
    "2016:us_senate": "E_16_SEN",
    "2016:governor": "E_16_GOV",
    "2016:attorney_general": "E_16_AG",
    "2018:us_senate": "E_18_SEN",
    "2020:president": "E_20_PRES",
    "2020:governor": "E_20_GOV",
    "2020:attorney_general": "E_20_AG",
    "2022:us_senate": "E_22_SEN",
    "2022:auditor": "E_22_AUD",
    "2022:secretary_of_state": "E_22_SOS",
    "2022:treasurer": "E_22_TREAS",
    "2024:president": "E_24_PRES",
    "2024:us_senate": "E_24_SEN",
    "2024:governor": "E_24_GOV",
    "2024:attorney_general": "E_24_AG",
  };
  return map[key] || null;
}

function getDraDistrictTurnout(scope, contestType, year) {
  const datasetKey = getDraDatasetKey(contestType, year);
  if (!datasetKey) return null;
  const cacheKey = `${scope}|${datasetKey}`;
  if (draDistrictTurnoutCache.has(cacheKey)) return draDistrictTurnoutCache.get(cacheKey);

  const dra = getDraGeojson();
  const countyFpToKey = getCountyFpToKey();
  const crosswalk = readCrosswalk(scope);
  if (!dra || !countyFpToKey.size || !crosswalk.length) {
    draDistrictTurnoutCache.set(cacheKey, null);
    return null;
  }

  const precinctVotes = new Map();
  for (const f of dra?.features || []) {
    const p = f?.properties || {};
    const geoid = String(p.id || "");
    if (geoid.length < 5) continue;
    const countyfp = geoid.slice(2, 5);
    const countyKey = countyFpToKey.get(countyfp);
    if (!countyKey) continue;
    const precinctName = String(p.name || "").trim();
    const precinctKeyPart = canonicalPrecinctKey(precinctName);
    if (!precinctKeyPart) continue;
    const dataset = p?.datasets?.[datasetKey];
    if (!dataset || typeof dataset !== "object") continue;
    const dem = parseFloatSafe(dataset.Dem, 0);
    const rep = parseFloatSafe(dataset.Rep, 0);
    const total = parseFloatSafe(dataset.Total, dem + rep);
    if (total <= 0) continue;
    const other = Math.max(0, total - dem - rep);
    const key = `${countyKey}|${precinctKeyPart}`;
    const prev = precinctVotes.get(key) || { dem: 0, rep: 0, other: 0, total: 0 };
    prev.dem += dem;
    prev.rep += rep;
    prev.other += other;
    prev.total += total;
    precinctVotes.set(key, prev);
  }

  const districtFloat = new Map();
  const totalsFloat = { dem: 0, rep: 0, other: 0 };
  for (const row of crosswalk) {
    const pv = precinctVotes.get(row.precinctKey);
    if (!pv) continue;
    const cur = districtFloat.get(row.districtId) || { dem: 0, rep: 0, other: 0 };
    cur.dem += pv.dem * row.weight;
    cur.rep += pv.rep * row.weight;
    cur.other += pv.other * row.weight;
    districtFloat.set(row.districtId, cur);
    totalsFloat.dem += pv.dem * row.weight;
    totalsFloat.rep += pv.rep * row.weight;
    totalsFloat.other += pv.other * row.weight;
  }

  const demTarget = Math.round(totalsFloat.dem);
  const repTarget = Math.round(totalsFloat.rep);
  const othTarget = Math.round(totalsFloat.other);
  const demByDistrict = apportionIntegerVotes(
    Object.fromEntries([...districtFloat.entries()].map(([d, v]) => [d, v.dem])),
    demTarget
  );
  const repByDistrict = apportionIntegerVotes(
    Object.fromEntries([...districtFloat.entries()].map(([d, v]) => [d, v.rep])),
    repTarget
  );
  const othByDistrict = apportionIntegerVotes(
    Object.fromEntries([...districtFloat.entries()].map(([d, v]) => [d, v.other])),
    othTarget
  );

  const byDistrict = new Map();
  const districtIds = new Set([
    ...Object.keys(demByDistrict),
    ...Object.keys(repByDistrict),
    ...Object.keys(othByDistrict),
  ]);
  for (const d of districtIds) {
    const dem = Number(demByDistrict[d] || 0);
    const rep = Number(repByDistrict[d] || 0);
    const oth = Number(othByDistrict[d] || 0);
    const total = dem + rep + oth;
    if (total <= 0) continue;
    byDistrict.set(String(d), { dem_votes: dem, rep_votes: rep, other_votes: oth, total_votes: total });
  }

  const turnoutNode = { datasetKey, byDistrict };
  draDistrictTurnoutCache.set(cacheKey, turnoutNode);
  return turnoutNode;
}

function buildSharesFromDistrictTotals(districtTotalsById) {
  if (!districtTotalsById || !(districtTotalsById instanceof Map)) return new Map();
  const shares = new Map();
  for (const [districtId, totals] of districtTotalsById.entries()) {
    const total = Number(totals?.total_votes || 0);
    if (!Number.isFinite(total) || total <= 0) continue;
    const dem = Number(totals?.dem_votes || 0);
    const rep = Number(totals?.rep_votes || 0);
    const oth = Number(totals?.other_votes || 0);
    shares.set(String(districtId), {
      dem: dem / total,
      rep: rep / total,
      oth: oth / total,
    });
  }
  return shares;
}

function calibrateSliceJson(slice, sharesByDistrictId, calibrationMeta, options = {}) {
  const districtTotalsById = options?.districtTotalsById || null;
  const results =
    slice?.general?.results && typeof slice.general.results === "object" ? slice.general.results : {};
  if (!slice.general || typeof slice.general !== "object") slice.general = {};
  slice.general.results = results;
  if (!sharesByDistrictId || sharesByDistrictId.size === 0) return { calibrated: 0, updated: 0, missing: 0, created: 0 };

  let calibrated = 0;
  let updated = 0;
  let missing = 0;
  let created = 0;

  let fallbackDemCandidate = "";
  let fallbackRepCandidate = "";
  for (const r of Object.values(results)) {
    if (!fallbackDemCandidate && r && typeof r === "object") {
      const c = String(r.dem_candidate || "").trim();
      if (c) fallbackDemCandidate = c;
    }
    if (!fallbackRepCandidate && r && typeof r === "object") {
      const c = String(r.rep_candidate || "").trim();
      if (c) fallbackRepCandidate = c;
    }
    if (fallbackDemCandidate && fallbackRepCandidate) break;
  }

  const districtIds = [...sharesByDistrictId.keys()].sort((a, b) => Number(a) - Number(b));
  for (const districtId of districtIds) {
    const share = sharesByDistrictId.get(String(districtId));
    if (!share) continue;

    const hadRow = !!results[districtId];
    const r = hadRow && typeof results[districtId] === "object" ? results[districtId] : {};
    const turnoutNode = districtTotalsById?.get(String(districtId)) || null;
    const totalVotes = turnoutNode ? Number(turnoutNode.total_votes) : Number(r.total_votes);
    if (!Number.isFinite(totalVotes) || totalVotes <= 0) continue;
    calibrated++;

    const demVotes = Math.round(totalVotes * share.dem);
    const repVotes = Math.round(totalVotes * share.rep);
    const othVotes = Math.max(0, totalVotes - demVotes - repVotes);
    const margin = repVotes - demVotes;
    const marginPct = totalVotes > 0 ? (margin / totalVotes) * 100 : 0;
    const { winner, color } = winnerForMargin(margin);

    const nextMarginPct = roundTo(marginPct, 4);
    const changedNow =
      Number(r.dem_votes) !== demVotes ||
      Number(r.rep_votes) !== repVotes ||
      Number(r.other_votes) !== othVotes ||
      Number(r.total_votes) !== totalVotes ||
      Number(r.margin) !== margin ||
      Number(r.margin_pct) !== nextMarginPct ||
      String(r.winner || "") !== winner ||
      String(r.color || "") !== color;

    r.dem_votes = demVotes;
    r.rep_votes = repVotes;
    r.other_votes = othVotes;
    r.total_votes = totalVotes;
    if (!String(r.dem_candidate || "").trim() && fallbackDemCandidate) r.dem_candidate = fallbackDemCandidate;
    if (!String(r.rep_candidate || "").trim() && fallbackRepCandidate) r.rep_candidate = fallbackRepCandidate;
    r.margin = margin;
    r.margin_pct = nextMarginPct;
    r.winner = winner;
    r.color = color;
    if (!hadRow) {
      results[districtId] = r;
      created++;
    }
    if (changedNow) updated++;
  }

  for (const id of sharesByDistrictId.keys()) {
    if (!results[id] || Number(results[id].total_votes) <= 0) missing++;
  }

  slice.meta = slice.meta && typeof slice.meta === "object" ? slice.meta : {};
  slice.meta.districts = Object.keys(results).length;
  slice.meta.calibration = {
    enabled: true,
    method: "vote_share_from_calibration_csv_rescaled_to_total_votes",
    ...calibrationMeta,
  };

  return { calibrated, updated, missing, created };
}

function mapCsvContestToJsonContest(rawContest) {
  const c = rawContest.trim().toLowerCase();
  if (c === "pres") return "president";
  if (c === "us senate") return "us_senate";
  if (c === "ag") return "attorney_general";
  if (c === "gov" || c === "governor") return "governor";
  if (c === "auditor") return "auditor";
  if (c === "sos") return "secretary_of_state";
  if (c === "treasurer") return "treasurer";
  return null;
}

function mapCsvScopeToJsonScope(rawScope) {
  const s = rawScope.trim().toLowerCase();
  if (s === "state house") return "state_house";
  if (s === "state senate") return "state_senate";
  if (s === "congressional" || s === "congress") return "congressional";
  return null;
}

function main() {
  const csvFiles = fs
    .readdirSync(CAL_DIR, { withFileTypes: true })
    .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".csv"))
    .map((e) => e.name);

  if (csvFiles.length === 0) {
    console.error(`No calibration CSVs found in ${CAL_DIR}`);
    process.exitCode = 1;
    return;
  }

  const touched = [];
  const skippedUnchanged = [];
  const reportRows = [];
  const processedSliceKeys = new Set();

  const aggregateBefore = fs.existsSync(DISTRICT_AGG_PATH)
    ? fs.readFileSync(DISTRICT_AGG_PATH, "utf8")
    : null;
  const aggregateObj = aggregateBefore ? JSON.parse(aggregateBefore) : null;

  for (const csvName of csvFiles) {
    const m = /^district-statistics (state house|state senate|congress(?:ional)?) (\d{4}) (.+)\.csv$/i.exec(csvName);
    if (!m) continue;

    const scope = mapCsvScopeToJsonScope(m[1]);
    if (!scope) continue;

    const year = Number(m[2]);
    const contestType = mapCsvContestToJsonContest(m[3]);
    if (!contestType) continue;

    const jsonName = `${scope}_${contestType}_${year}.json`;
    const jsonPath = path.join(DISTRICT_CONTESTS_DIR, jsonName);
    if (!fs.existsSync(jsonPath)) continue;

    const csvPath = path.join(CAL_DIR, csvName);
    const sharesById = readCalibrationCsv(csvPath);
    if (sharesById.size === 0) continue;
    const draTurnout = getDraDistrictTurnout(scope, contestType, year);

    const beforeText = fs.readFileSync(jsonPath, "utf8");
    const slice = JSON.parse(beforeText);
    const priorGeneratedOn =
      (slice?.meta?.calibration && typeof slice.meta.calibration === "object"
        ? slice.meta.calibration.generated_on
        : null) || null;
    const baseMeta = {
      method: "vote_share_from_calibration_csv_rescaled_to_total_votes",
      calibration_csv: path.posix.join("Data", "Calibration csvs", csvName),
      calibration_scope: scope,
      calibration_year: year,
      calibration_contest_type: contestType,
      generated_on: priorGeneratedOn || new Date().toISOString().slice(0, 10),
      districts_with_shares: sharesById.size,
      turnout_source: draTurnout?.datasetKey ? "dra_vtd_dataset_crosswalked_to_2022_lines" : "existing_slice_total_votes",
      turnout_dataset_key: draTurnout?.datasetKey || null,
    };
    const { calibrated, updated, missing, created } = calibrateSliceJson(slice, sharesById, baseMeta, {
      districtTotalsById: draTurnout?.byDistrict || null,
    });
    if (slice?.meta?.calibration && typeof slice.meta.calibration === "object") {
      slice.meta.calibration.districts_calibrated = calibrated;
      slice.meta.calibration.districts_updated = updated;
      slice.meta.calibration.districts_created = created;
      slice.meta.calibration.districts_missing_shares = missing;
    }

    if (calibrated > 0 && aggregateObj?.results_by_year?.[String(year)]?.[scope]) {
      aggregateObj.results_by_year[String(year)][scope][contestType] = slice;
    }

    const afterText = JSON.stringify(slice, null, 2) + "\n";
    const reportRow = {
      scope,
      year,
      contest_type: contestType,
      json_file: jsonName,
      calibration_csv: csvName,
      districts_with_shares: sharesById.size,
      districts_calibrated: calibrated,
      districts_updated: updated,
      districts_created: created,
      districts_missing_shares: missing,
      districts_without_results: Math.max(0, sharesById.size - calibrated),
      turnout_source: baseMeta.turnout_source,
      turnout_dataset_key: baseMeta.turnout_dataset_key,
      wrote_file: false,
    };

    if (afterText !== beforeText) {
      fs.writeFileSync(jsonPath, afterText, "utf8");
      touched.push({ jsonName, source: csvName, calibrated, updated, created, missing });
      reportRow.wrote_file = true;
    } else if (calibrated > 0) {
      skippedUnchanged.push({ jsonName, source: csvName, calibrated, updated, created, missing });
    }
    if (calibrated > 0) {
      reportRows.push(reportRow);
      processedSliceKeys.add(`${scope}|${year}|${contestType}`);
    }
  }

  const congressionalFiles = fs
    .readdirSync(DISTRICT_CONTESTS_DIR, { withFileTypes: true })
    .filter((e) => e.isFile() && /^congressional_.+_\d{4}\.json$/i.test(e.name))
    .map((e) => e.name);

  for (const jsonName of congressionalFiles) {
    const m = /^congressional_(.+)_(\d{4})\.json$/i.exec(jsonName);
    if (!m) continue;
    const scope = "congressional";
    const contestType = String(m[1] || "").toLowerCase();
    const year = Number(m[2]);
    const sliceKey = `${scope}|${year}|${contestType}`;
    if (processedSliceKeys.has(sliceKey)) continue;

    const draTurnout = getDraDistrictTurnout(scope, contestType, year);
    if (!draTurnout?.datasetKey || !draTurnout?.byDistrict) continue;
    const sharesById = buildSharesFromDistrictTotals(draTurnout.byDistrict);
    if (!sharesById.size) continue;

    const jsonPath = path.join(DISTRICT_CONTESTS_DIR, jsonName);
    const beforeText = fs.readFileSync(jsonPath, "utf8");
    const slice = JSON.parse(beforeText);
    const priorGeneratedOn =
      (slice?.meta?.calibration && typeof slice.meta.calibration === "object"
        ? slice.meta.calibration.generated_on
        : null) || null;
    const baseMeta = {
      method: "dra_dataset_votes_crosswalked_to_cd118_districts",
      calibration_csv: null,
      calibration_scope: scope,
      calibration_year: year,
      calibration_contest_type: contestType,
      generated_on: priorGeneratedOn || new Date().toISOString().slice(0, 10),
      districts_with_shares: sharesById.size,
      turnout_source: "dra_vtd_dataset_crosswalked_to_2022_lines",
      turnout_dataset_key: draTurnout.datasetKey,
    };
    const { calibrated, updated, missing, created } = calibrateSliceJson(slice, sharesById, baseMeta, {
      districtTotalsById: draTurnout.byDistrict,
    });
    if (slice?.meta?.calibration && typeof slice.meta.calibration === "object") {
      slice.meta.calibration.districts_calibrated = calibrated;
      slice.meta.calibration.districts_updated = updated;
      slice.meta.calibration.districts_created = created;
      slice.meta.calibration.districts_missing_shares = missing;
    }

    if (calibrated > 0 && aggregateObj?.results_by_year?.[String(year)]?.[scope]) {
      aggregateObj.results_by_year[String(year)][scope][contestType] = slice;
    }

    const afterText = JSON.stringify(slice, null, 2) + "\n";
    const reportRow = {
      scope,
      year,
      contest_type: contestType,
      json_file: jsonName,
      calibration_csv: null,
      districts_with_shares: sharesById.size,
      districts_calibrated: calibrated,
      districts_updated: updated,
      districts_created: created,
      districts_missing_shares: missing,
      districts_without_results: Math.max(0, sharesById.size - calibrated),
      turnout_source: baseMeta.turnout_source,
      turnout_dataset_key: baseMeta.turnout_dataset_key,
      wrote_file: false,
    };

    if (afterText !== beforeText) {
      fs.writeFileSync(jsonPath, afterText, "utf8");
      touched.push({ jsonName, source: `DRA ${draTurnout.datasetKey}`, calibrated, updated, created, missing });
      reportRow.wrote_file = true;
    } else if (calibrated > 0) {
      skippedUnchanged.push({ jsonName, source: `DRA ${draTurnout.datasetKey}`, calibrated, updated, created, missing });
    }
    if (calibrated > 0) {
      reportRows.push(reportRow);
      processedSliceKeys.add(sliceKey);
    }
  }

  let aggregateUpdated = false;
  if (aggregateObj && aggregateBefore != null) {
    const aggregateAfter = JSON.stringify(aggregateObj, null, 2) + "\n";
    if (aggregateAfter !== aggregateBefore) {
      fs.writeFileSync(DISTRICT_AGG_PATH, aggregateAfter, "utf8");
      aggregateUpdated = true;
    }
  }

  if (touched.length === 0 && skippedUnchanged.length === 0) {
    console.error("No legislative slices were calibrated (no matching CSV/JSON pairs).");
    process.exitCode = 2;
    return;
  }

  for (const t of touched) {
    console.log(
      `${t.jsonName}: calibrated ${t.calibrated} districts, updated ${t.updated}, created ${t.created}, missing shares: ${t.missing} from ${t.source}`
    );
  }
  if (skippedUnchanged.length > 0) {
    console.log(`Skipped ${skippedUnchanged.length} already-calibrated slices (no value changes).`);
  }
  if (aggregateUpdated) {
    console.log(`Updated aggregate district fallback JSON: ${path.relative(ROOT, DISTRICT_AGG_PATH).replaceAll("\\\\", "/")}`);
  }

  const report = {
    generated_at_utc: new Date().toISOString(),
    slices: reportRows.sort((a, b) =>
      a.scope.localeCompare(b.scope) ||
      a.year - b.year ||
      a.contest_type.localeCompare(b.contest_type)
    ),
  };
  fs.writeFileSync(CALIBRATION_REPORT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");
  console.log(`Wrote calibration report: ${path.relative(ROOT, CALIBRATION_REPORT_PATH).replaceAll("\\\\", "/")}`);
}

main();
