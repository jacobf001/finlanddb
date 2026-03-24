/**
 * 1.discover-competitions.ts
 *
 * Fetches all SPL competitions and categories in a single API call,
 * exactly as the tulospalvelu.palloliitto.fi frontend does:
 *
 *   GET getCategories?season_id=2025-26,2026
 *
 * Returns 2000+ categories in one shot — no per-competition loop needed.
 * Each category row already contains competition_id, category_id, gender,
 * sport, organiser etc. so we upsert directly.
 *
 * Usage:
 *   npx tsx 1.discover-competitions.ts [--from 2020] [--to 2026] [--sleep 300] [--dry]
 *
 * Env vars required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

import { createClient } from "@supabase/supabase-js";
import path from "path";
import * as dotenv from "dotenv";

dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

if (!process.env.SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!process.env.SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const BASE_URL = "https://spl.torneopal.net/taso/rest";

// ── CLI args ───────────────────────────────────────────────────────────────
function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : process.argv[i + 1] ?? null;
}
const fromYear = Number(arg("--from") ?? "2020");
const toYear   = Number(arg("--to")   ?? String(new Date().getFullYear()));
const sleepMs  = Number(arg("--sleep") ?? "300");
const dry      = process.argv.includes("--dry");

// ── Types ──────────────────────────────────────────────────────────────────
type DbGender = "Male" | "Female" | "Youth_Male" | "Youth_Female" | "Unknown";

function deriveGender(categoryGroupName: string, categoryGender: string): DbGender {
  // Use category_group_name first (most reliable)
  switch (categoryGroupName) {
    case "Miehet": return "Male";
    case "Naiset": return "Female";
    case "Pojat":  return "Youth_Male";
    case "Tytöt":  return "Youth_Female";
  }
  // Fallback to category_gender field
  switch (categoryGender) {
    case "M": return "Male";
    case "F": return "Female";
  }
  return "Unknown";
}

// Tier 1 = top flight, 99 = cup/plate
const MEN_TIERS: Record<string, number> = {
  VL: 1,                          // Veikkausliiga
  M1: 2, M1L: 2,                  // Ykkönen / Ykkösliiga
  M2: 3,                          // Kakkonen
  M3: 4,                          // Kolmonen
  M4: 5,                          // Nelonen
  M7: 5,                          // Seiska (7-a-side)
  M5: 6,                          // Vitonen
  M6: 7,                          // Kutonen
  // Cups & plates
  LC: 99, MSC: 99, MRC: 99,
  M1LCUP: 99,                     // Ykkösliigacup
  // Promotion playoffs
  VLK: 99, M1LK: 99, M1K: 99, M3K: 99,
  // International / national team
  WCQ: 99, ECQM: 99, UNL: 99, ECQ: 99,
  U21EC: 99, U21ECQ: 99, U21M: 99,
};

const WOMEN_TIERS: Record<string, number> = {
  NL: 1,                          // Naisten Liiga / Kansallinen Liiga
  N1: 2, N1L: 2,                  // Naisten Ykkönen / Kansallinen Ykkönen
  N2: 3,                          // Naisten Kakkonen
  N3: 4,                          // Naisten Kolmonen
  N4: 5,                          // Naisten Nelonen
  // Cups
  NSC: 99,
  // International
  WCQW: 99, ECQW: 99, WWCQ: 99,
};

// Youth tier detection — regex-based since there are hundreds of category IDs
// Pattern: P/T + age + suffix where suffix indicates tier within age group
// P = Pojat (boys), T = Tytöt (girls)
// Age groups we care about for betting: 21, 19, 18, 17, 16, 15, 14, 13
// Tiers within age group: SM/HL/LE/LP = top, 1 = second, 2 = third etc.
// Skip: PI (Peli-Ilta casual), PPK (development), 5v5/8v8 mini formats, age < 12

function deriveYouthTier(categoryId: string): number {
  const id = categoryId.toUpperCase();

  // Skip mini formats regardless of age
  if (/PI$|PPK$|5V5|8V8|PK$|TESTI|PELI/i.test(id)) return 9;

  // Extract age group — handles P21, T18, PU17, P8, P9 etc.
  const ageMatch = id.match(/^[PT]U?(\d{1,2})/);
  if (ageMatch) {
    const age = parseInt(ageMatch[1], 10);
    // Only keep U18 and above (18, 19, 20, 21)
    if (age < 18) return 9;
  }

  // SM = national championship = tier 2
  if (/SM$/.test(id)) return 2;

  // Qualification/playoff
  if (/(SMK|LPK|LK|karsin)/i.test(id)) return 99;

  // Top regional leagues: HL, LE, LP
  if (/(HL|LE|LP)$/.test(id)) return 3;

  // Numbered tiers: P211 = Ykkönen (tier 3), P212 = Kakkonen (tier 4) etc.
  const numMatch = id.match(/^[PT]U?\d{2,}(\d)$/);
  if (numMatch) return parseInt(numMatch[1], 10) + 2;

  // Explicit single-digit suffix
  if (/1$/.test(id)) return 3;
  if (/2$/.test(id)) return 4;
  if (/3$/.test(id)) return 5;
  if (/4$/.test(id)) return 6;
  if (/5$/.test(id)) return 7;

  return 9;
}

function deriveTier(categoryId: string, gender: DbGender): number {
  if (gender === "Male")   return MEN_TIERS[categoryId]   ?? 9;
  if (gender === "Female") return WOMEN_TIERS[categoryId] ?? 9;
  if (gender === "Youth_Male" || gender === "Youth_Female") return deriveYouthTier(categoryId);
  return 9;
}

// ── Build season_id list ───────────────────────────────────────────────────
// The API accepts comma-separated season IDs.
// Football: "2020", "2021" ... "2026"
// Futsal:   "2020-21", "2021-22" ... "2025-26"
function buildSeasonParam(from: number, to: number): string {
  const ids: string[] = [];
  for (let y = from; y <= to; y++) {
    // Always include the preceding futsal season e.g. "2025-26" for year 2026
    ids.push(`${y - 1}-${String(y).slice(2)}`);
    ids.push(String(y));
  }
  return [...new Set(ids)].join(",");
}

// ── Fetch all categories in one call ──────────────────────────────────────
async function fetchAllCategories(seasonParam: string): Promise<any[]> {
  // The site uses all_current=1 — this is what bypasses the API key requirement
  const url = `${BASE_URL}/getCategories?season_id=${seasonParam}`;
  console.log(`GET ${url}`);
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
      "Accept": "json/n9tnjq45uuccbe8nbfy6q7ggmreqntvs",
      "Origin": "https://tulospalvelu.palloliitto.fi",
      "Referer": "https://tulospalvelu.palloliitto.fi/",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return Array.isArray(data?.categories) ? data.categories : [];
}

// ── Main ───────────────────────────────────────────────────────────────────
interface DbRow {
  spl_competition_id: string;
  spl_category_id:    string;
  season_year:        number;
  name:               string;
  category_name:      string;
  gender:             DbGender;
  tier:               number;
  sport:              string;
  organiser:          string;
  status:             string;
}

async function main() {
  console.log("=== 1.discover-competitions (SPL / Torneopal) ===");
  console.log(`Seasons: ${fromYear} → ${toYear}  |  dry=${dry}\n`);

  const seasonParam = buildSeasonParam(fromYear, toYear);
  console.log(`Season param: ${seasonParam}\n`);

  const categories = await fetchAllCategories(seasonParam);
  console.log(`Total categories returned: ${categories.length}`);

  // Build DB rows — only keep official league/cup competitions
  // Exclude: national team pools, friendlies, development events, tournaments
  const rows: DbRow[] = [];
  for (const cat of categories) {
    const seasonYear = parseInt(String(cat.season_id ?? "").split("-")[0], 10);
    if (isNaN(seasonYear) || seasonYear < fromYear || seasonYear > toYear) continue;

    // Skip futsal entirely
    if ((cat.sport ?? "") === "futsal") continue;

    // Skip non-official competitions (friendlies, development, practice)
    const officiality = cat.competition_officiality ?? "";
    if (officiality === "practice" || officiality === "friendly") continue;

    // Skip pure tournament events (national team pools, cups handled separately)
    // Keep: league, cup. Skip: tournament (these are national team/development pools)
    const compType = cat.competition_type ?? "";
    if (compType === "tournament") continue;

    // Skip national team / international competitions (organiser = splmaa)
    if (cat.organiser === "splmaa") continue;
    // Skip Åland competitions
    if (cat.organiser === "splaland") continue;
    // Skip competitions with "pojat" or "tytöt" in category name (youth development)
    const catNameLower = (cat.category_name ?? "").toLowerCase();
    if (catNameLower.includes("pojat") || catNameLower.includes("tytöt") || catNameLower.includes("tytot")) continue;

    const gender = deriveGender(cat.category_group_name ?? "", cat.category_gender ?? "");
    const tier   = deriveTier(cat.category_id ?? "", gender);

    // Skip unclassified youth (under U18, mini formats etc.)
    if (tier === 9 && (gender === "Youth_Male" || gender === "Youth_Female")) continue;

    // Skip practice/harjoitusottelu competitions by name
    const nameLower = (cat.competition_name ?? "").toLowerCase();
    if (nameLower.includes("harjoitus") || nameLower.includes("peli-ilta")) continue;

    rows.push({
      spl_competition_id: cat.competition_id,
      spl_category_id:    cat.category_id,
      season_year:        seasonYear,
      name:               cat.competition_name  ?? "",
      category_name:      cat.category_name     ?? "",
      gender,
      tier,
      sport:              cat.sport             ?? "football",
      organiser:          cat.organiser         ?? "",
      status:             cat.category_status   ?? "",
    });
  }

  console.log(`Rows to upsert: ${rows.length}`);

  // Summary
  const groups = new Map<string, DbRow[]>();
  for (const r of rows) {
    const k = `${r.gender}|T${String(r.tier).padStart(2, "0")}|${r.sport}`;
    const arr = groups.get(k) ?? [];
    arr.push(r);
    groups.set(k, arr);
  }
  console.log("\nGender         | Tier | Sport    | Count | Sample");
  console.log("---------------|------|----------|-------|-------");
  for (const [k, arr] of Array.from(groups.entries()).sort()) {
    const [g, t, s] = k.split("|");
    const tier = t.replace("T0", "").replace("T", "");
    console.log(
      `${g.padEnd(14)} | ${tier.padEnd(4)} | ${s.padEnd(8)} | ${String(arr.length).padEnd(5)} | ${arr[0].category_name}`
    );
  }

  if (dry) {
    console.log("\n[dry run] Skipping upsert.");
    return;
  }

  // Upsert in batches of 200
  console.log("\nUpserting into Supabase...");
  const BATCH = 200;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase
      .from("competitions")
      .upsert(batch, { onConflict: "spl_competition_id,spl_category_id" });
    if (error) {
      console.error(`  ✗ Upsert error (batch ${Math.floor(i / BATCH) + 1}):`, error);
      throw error;
    }
    console.log(`  ✓ Batch ${Math.floor(i / BATCH) + 1}: rows ${i + 1}–${i + batch.length}`);
  }

  console.log("\nDone.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});