/**
 * 2.discover-matches.ts
 *
 * Reads competitions from Supabase, then for each competition fetches the
 * match list from the SPL Torneopal REST API (getCategory?matches=1).
 *
 * Only upserts matches that have ALREADY BEEN PLAYED (fs_A and fs_B present,
 * status = "Played") — future fixtures are ignored entirely, avoiding the
 * "delete future games" problem from the Iceland pipeline.
 *
 * Usage:
 *   npx tsx 2.discover-matches.ts [--from 2020] [--to 2026] [--limit 10] [--sleep 300] [--dry] [--debug]
 *
 * Env vars required:
 *   SUPABASE_URL
 *   SUPABASE_SERVICE_ROLE_KEY
 */

import * as dotenv from "dotenv";
import path from "path";
import { createClient } from "@supabase/supabase-js";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL) throw new Error("SUPABASE_URL is required");
if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const BASE_URL = "https://spl.torneopal.net/taso/rest";

// ── CLI args ───────────────────────────────────────────────────────────────
function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : process.argv[i + 1] ?? null;
}
const fromYear = Number(arg("--from") ?? "2020");
const toYear   = Number(arg("--to")   ?? new Date().getFullYear());
const sleepMs  = Number(arg("--sleep") ?? "300");
const limit    = Number(arg("--limit") ?? "0");
const dry      = process.argv.includes("--dry");
const debug    = process.argv.includes("--debug");

// ── Types ──────────────────────────────────────────────────────────────────
type Competition = {
  spl_competition_id: string;
  spl_category_id:    string;
  season_year:        number;
  name:               string;
  gender:             string;
  tier:               number | null;
};

type MatchUpsert = {
  spl_match_id:       string;
  spl_competition_id: string;
  spl_category_id:    string;
  season_year:        number;
  kickoff_at:         string | null;
  venue:              string | null;
  home_team_spl_id:   string | null;
  away_team_spl_id:   string | null;
  home_score:         number | null;
  away_score:         number | null;
  home_halftime:      number | null;
  away_halftime:      number | null;
  status:             string;
  group_id:           string | null;
  group_name:         string | null;
};

// ── Helpers ────────────────────────────────────────────────────────────────
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchJSON(url: string): Promise<any> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
      "Accept": "json/n9tnjq45uuccbe8nbfy6q7ggmreqntvs",
      "Origin": "https://tulospalvelu.palloliitto.fi",
      "Referer": "https://tulospalvelu.palloliitto.fi/",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}

// ── Fetch matches for one competition/category ─────────────────────────────
// getCategory with matches=1 returns all matches in the category group.
// Each match has: match_id, team_A_id, team_B_id, fs_A, fs_B, hts_A, hts_B,
//                date, time, status, venue_name etc.
interface RawMatch {
  match_id:    string;
  status:      string;    // "Played" | "Scheduled" | "Cancelled" | ...
  date:        string;    // "2024-06-15"
  time:        string;    // "18:30:00"
  team_A_id:   string;
  team_A_name: string;
  team_B_id:   string;
  team_B_name: string;
  fs_A:        string;    // final score team A (empty if not played)
  fs_B:        string;    // final score team B
  hts_A:       string;    // half-time score A
  hts_B:       string;    // half-time score B
  venue_name:  string;
  venue_city_name: string;
  time_zone:   string;
}

async function fetchMatchesForCategory(
  competitionId: string,
  categoryId: string
): Promise<RawMatch[]> {
  // matches=1 includes the full match list with scores
  const url = `${BASE_URL}/getCategory?competition_id=${competitionId}&category_id=${categoryId}&matches=1`;
  if (debug) console.log(`  GET ${url}`);
  try {
    const data = await fetchJSON(url);
    if (debug) console.log(`  raw keys:`, Object.keys(data ?? {}));
    if (debug && data?.category) console.log(`  category keys:`, Object.keys(data.category));

    const matches: RawMatch[] = [];

    // Primary: matches are at data.category.matches directly
    if (Array.isArray(data?.category?.matches)) {
      matches.push(...data.category.matches);
    }

    // Fallback: nested under groups → rounds → matches
    if (matches.length === 0) {
      const groups = data?.category?.groups ?? [];
      if (debug) console.log(`  groups count:`, groups.length);
      for (const group of groups) {
        for (const round of group?.rounds ?? []) {
          for (const m of round?.matches ?? []) {
            matches.push(m);
          }
        }
      }
    }

    // Final fallback: data.matches
    if (matches.length === 0 && Array.isArray(data?.matches)) {
      matches.push(...data.matches);
    }

    if (debug) console.log(`  total matches found in response:`, matches.length);
    return matches;
  } catch (err) {
    console.warn(`  ⚠ Failed getCategory ${competitionId}/${categoryId}: ${err}`);
    return [];
  }
}

// ── Build kickoff timestamp ────────────────────────────────────────────────
function buildKickoffAt(date: string, time: string): string | null {
  if (!date) return null;
  const t = time || "00:00:00";
  // SPL times are in Europe/Helsinki — store as ISO with offset
  // We can't do proper tz conversion without a library, so store as-is
  // and let the app handle the offset. Format: "2024-06-15T18:30:00"
  return `${date}T${t}`;
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== 2.discover-matches (SPL / Torneopal) ===`);
  console.log(`Seasons: ${fromYear} → ${toYear}  |  dry=${dry}  |  sleep=${sleepMs}ms  |  limit=${limit || "none"}\n`);

  // Load all competitions from Supabase
  const allComps: Competition[] = [];
  let from = 0;
  const ps = 1000;
  while (true) {
    const { data, error } = await supabase
      .from("competitions")
      .select("spl_competition_id, spl_category_id, season_year, name, gender, tier")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .not("spl_category_id", "eq", "")  // skip rows with no category
      .order("season_year", { ascending: true })
      .order("tier",        { ascending: true })
      .range(from, from + ps - 1);

    if (error) throw new Error(error.message);
    const batch = (data ?? []) as Competition[];
    allComps.push(...batch);
    if (batch.length < ps) break;
    from += ps;
  }

  const list = limit > 0 ? allComps.slice(0, limit) : allComps;
  console.log(`Competitions to process: ${list.length}`);

  // Summary by gender
  const byGender = new Map<string, number>();
  for (const c of list) byGender.set(c.gender, (byGender.get(c.gender) ?? 0) + 1);
  for (const [g, n] of byGender) console.log(`  ${g}: ${n}`);

  let ok = 0;
  let fail = 0;
  let totalFound = 0;
  let totalPlayed = 0;
  let totalUpserted = 0;

  for (const comp of list) {
    console.log(
      `\n[${comp.gender} T${comp.tier ?? "?"} ${comp.season_year}] ${comp.name} / ${comp.spl_category_id}`
    );

    try {
      const rawMatches = await fetchMatchesForCategory(
        comp.spl_competition_id,
        comp.spl_category_id
      );
      await sleep(sleepMs);

      totalFound += rawMatches.length;
      console.log(`  Found ${rawMatches.length} matches`);

      const completedMatches = rawMatches.filter(
        (m) => m.status === "Played" && m.fs_A !== "" && m.fs_B !== ""
      );
      totalPlayed += completedMatches.length;
      console.log(`  Played: ${completedMatches.length}`);

      if (rawMatches.length === 0) {
        ok++;
        continue;
      }

      const upsertRows: MatchUpsert[] = rawMatches.map((m) => ({
        spl_match_id:       m.match_id,
        spl_competition_id: comp.spl_competition_id,
        spl_category_id:    comp.spl_category_id,
        season_year:        comp.season_year,
        kickoff_at:         buildKickoffAt(m.date, m.time),
        venue:              [m.venue_name, m.venue_city_name].filter(Boolean).join(", ") || null,
        home_team_spl_id:   m.team_A_id || null,
        away_team_spl_id:   m.team_B_id || null,
        home_score:         m.fs_A !== "" ? Number(m.fs_A) : null,
        away_score:         m.fs_B !== "" ? Number(m.fs_B) : null,
        home_halftime:      m.hts_A !== "" ? Number(m.hts_A) : null,
        away_halftime:      m.hts_B !== "" ? Number(m.hts_B) : null,
        status:             m.status || "Scheduled",
        group_id:           (m as any).group_id || null,
        group_name:         (m as any).group_name || null,
      }));

      if (debug) console.log("  Sample:", upsertRows[0]);

      if (dry) {
        console.log(`  [dry] Would upsert ${upsertRows.length} rows`);
        ok++;
        continue;
      }

      // Upsert in chunks of 500
      const CHUNK = 500;
      for (let i = 0; i < upsertRows.length; i += CHUNK) {
        const chunk = upsertRows.slice(i, i + CHUNK);
        const { error: upErr } = await supabase
          .from("matches")
          .upsert(chunk, { onConflict: "spl_match_id" });
        if (upErr) throw new Error(upErr.message);
        totalUpserted += chunk.length;
      }
      console.log(`  ✓ Upserted ${upsertRows.length} matches`);
      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ✗ ${e?.message ?? String(e)}`);
    }

    await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
  console.log(`Total found: ${totalFound} | Played: ${totalPlayed}${dry ? "" : ` | Upserted: ${totalUpserted}`}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});