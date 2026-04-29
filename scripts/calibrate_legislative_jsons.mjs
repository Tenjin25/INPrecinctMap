import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CAL_DIR = path.join(DATA_DIR, "Calibration csvs");
const DISTRICT_CONTESTS_DIR = path.join(DATA_DIR, "district_contests");

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

function winnerForMargin(margin) {
  if (margin < 0) return { winner: "DEM", color: "#2563eb" };
  if (margin > 0) return { winner: "REP", color: "#dc2626" };
  return { winner: "TIE", color: "#64748b" };
}

function calibrateSliceJson(slice, sharesByDistrictId, calibrationMeta) {
  const results = slice?.general?.results;
  if (!results || typeof results !== "object") return { changed: 0, missing: 0 };

  let changed = 0;
  let missing = 0;

  for (const [districtId, r] of Object.entries(results)) {
    if (!r || typeof r !== "object") continue;
    const totalVotes = Number(r.total_votes);
    if (!Number.isFinite(totalVotes) || totalVotes <= 0) continue;

    const s = sharesByDistrictId.get(String(districtId));
    if (!s) {
      missing++;
      continue;
    }

    const demVotes = Math.round(totalVotes * s.dem);
    const repVotes = Math.round(totalVotes * s.rep);
    const othVotes = Math.max(0, totalVotes - demVotes - repVotes);
    const margin = repVotes - demVotes;
    const marginPct = totalVotes > 0 ? (margin / totalVotes) * 100 : 0;
    const { winner, color } = winnerForMargin(margin);

    r.dem_votes = demVotes;
    r.rep_votes = repVotes;
    r.other_votes = othVotes;
    r.total_votes = totalVotes;
    r.margin = margin;
    r.margin_pct = roundTo(marginPct, 4);
    r.winner = winner;
    r.color = color;
    changed++;
  }

  slice.meta = slice.meta && typeof slice.meta === "object" ? slice.meta : {};
  slice.meta.calibration = {
    enabled: true,
    method: "vote_share_from_calibration_csv_rescaled_to_total_votes",
    ...calibrationMeta,
  };

  return { changed, missing };
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

  for (const csvName of csvFiles) {
    const m = /^district-statistics (state house|state senate) (\d{4}) (.+)\.csv$/i.exec(csvName);
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

    const slice = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
    const baseMeta = {
      calibration_csv: path.posix.join("Data", "Calibration csvs", csvName),
      calibration_scope: scope,
      calibration_year: year,
      calibration_contest_type: contestType,
      generated_on: new Date().toISOString().slice(0, 10),
      districts_with_shares: sharesById.size,
    };
    const { changed, missing } = calibrateSliceJson(slice, sharesById, baseMeta);
    if (slice?.meta?.calibration && typeof slice.meta.calibration === "object") {
      slice.meta.calibration.districts_calibrated = changed;
      slice.meta.calibration.districts_missing_shares = missing;
    }

    if (changed > 0) {
      fs.writeFileSync(jsonPath, JSON.stringify(slice, null, 2) + "\n", "utf8");
      touched.push({ jsonName, csvName, changed, missing });
    }
  }

  if (touched.length === 0) {
    console.error("No legislative slices were calibrated (no matching CSV/JSON pairs).");
    process.exitCode = 2;
    return;
  }

  for (const t of touched) {
    console.log(`${t.jsonName}: calibrated ${t.changed} districts (missing shares: ${t.missing}) from ${t.csvName}`);
  }
}

main();
