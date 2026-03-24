/**
 * 3.scrape-match-overview.ts
 *
 * Fills in missing match data (scores, teams, kickoff, venue) for matches
 * that were discovered by script 2 but are missing fields.
 *
 * Iceland used HTML scraping + regex for this. Finland uses getMatch from
 * the Torneopal REST API — the JSON response contains everything we need
 * directly: team_A_id, team_B_id, fs_A, fs_B, hts_A, hts_B, date, time,
 * venue_name, venue_city_name, attendance etc.
 *
 * Also upserts both teams into the `teams` table if not already present.
 *
 * Usage:
 *   npx tsx 3.scrape-match-overview.ts [--from 2020] [--to 2026] [--limit 200] [--sleep 300] [--dry] [--debug]
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
const toYear   = Number(arg("--to")   ?? String(new Date().getFullYear()));
const sleepMs  = Number(arg("--sleep") ?? "300");
const limit    = Number(arg("--limit") ?? "0");
const dry      = process.argv.includes("--dry");
const debug    = process.argv.includes("--debug");

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

// ── Build kickoff ISO string ───────────────────────────────────────────────
// SPL times are in Europe/Helsinki. We store as a naive ISO string and let
// the app/DB handle timezone. Format: "2024-06-15T18:30:00"
function buildKickoffAt(date: string, time: string): string | null {
  if (!date) return null;
  const t = time || "00:00:00";
  return `${date}T${t}`;
}

// ── Team upsert ────────────────────────────────────────────────────────────
interface TeamRow {
  spl_team_id:  string;
  spl_club_id:  string | null;
  team_name:    string | null;
  club_name:    string | null;
  club_crest:   string | null;
}

async function ensureTeamsExist(teams: TeamRow[]) {
  if (teams.length === 0) return;
  const { error } = await supabase
    .from("teams")
    .upsert(teams, { onConflict: "spl_team_id" });
  if (error) throw new Error(`teams upsert failed: ${error.message}`);
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== 3.scrape-match-overview (SPL / Torneopal) ===`);
  console.log(`Seasons: ${fromYear} → ${toYear}  |  dry=${dry}  |  limit=${limit || "none"}  |  sleep=${sleepMs}ms\n`);

  // Fetch matches that are missing team data or scores
  // Script 2 fills scores at discovery time, so this handles any gaps
  const all: any[] = [];
  let from = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select("spl_match_id, season_year, kickoff_at, home_team_spl_id, away_team_spl_id, home_score, away_score")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .or("home_team_spl_id.is.null,home_score.is.null")
      .order("kickoff_at", { ascending: false, nullsFirst: false })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  const target = limit > 0 ? all.slice(0, limit) : all;
  console.log(`Matches needing overview scrape: ${all.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.spl_match_id);
    const url = `${BASE_URL}/getMatch?match_id=${mid}`;

    console.log(`\n[${i + 1}/${target.length}] match ${mid}`);
    if (debug) console.log(`  GET ${url}`);

    try {
      const data = await fetchJSON(url);
      const match = data?.match;

      if (!match) throw new Error("No match object in response");

      // Only process played matches
      if (match.status !== "Played") {
        console.log(`  ⚠ status=${match.status} — skipping`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      const homeTeamId  = match.team_A_id  || null;
      const awayTeamId  = match.team_B_id  || null;
      const homeClubId  = match.club_A_id  || null;
      const awayClubId  = match.club_B_id  || null;
      const homeName    = match.team_A_name || null;
      const awayName    = match.team_B_name || null;
      const homeClub    = match.club_A_name || null;
      const awayClub    = match.club_B_name || null;
      const homeCrest   = match.club_A_crest || null;
      const awayCrest   = match.club_B_crest || null;

      const homeScore   = match.fs_A !== "" && match.fs_A != null ? Number(match.fs_A) : null;
      const awayScore   = match.fs_B !== "" && match.fs_B != null ? Number(match.fs_B) : null;
      const homeHalf    = match.hts_A !== "" && match.hts_A != null ? Number(match.hts_A) : null;
      const awayHalf    = match.hts_B !== "" && match.hts_B != null ? Number(match.hts_B) : null;

      const kickoffAt   = buildKickoffAt(match.date, match.time);
      const venue       = [match.venue_name, match.venue_city_name].filter(Boolean).join(", ") || null;
      const attendance  = match.attendance ? Number(match.attendance) : null;

      if (debug) {
        console.log(`  home: ${homeTeamId} (${homeName}) | away: ${awayTeamId} (${awayName})`);
        console.log(`  score: ${homeScore}-${awayScore} (HT: ${homeHalf}-${awayHalf})`);
        console.log(`  kickoff: ${kickoffAt} | venue: ${venue}`);
      }

      const teamsToUpsert: TeamRow[] = [];
      if (homeTeamId) teamsToUpsert.push({
        spl_team_id: homeTeamId,
        spl_club_id: homeClubId,
        team_name:   homeName,
        club_name:   homeClub,
        club_crest:  homeCrest,
      });
      if (awayTeamId) teamsToUpsert.push({
        spl_team_id: awayTeamId,
        spl_club_id: awayClubId,
        team_name:   awayName,
        club_name:   awayClub,
        club_crest:  awayCrest,
      });

      const patch = {
        home_team_spl_id:  homeTeamId,
        away_team_spl_id:  awayTeamId,
        home_score:        homeScore,
        away_score:        awayScore,
        home_halftime:     homeHalf,
        away_halftime:     awayHalf,
        kickoff_at:        kickoffAt,
        venue,
        attendance,
        scraped_overview_at: new Date().toISOString(),
      };

      if (dry) {
        console.log("  [dry] teams:", teamsToUpsert.map((t) => `${t.spl_team_id} ${t.team_name}`));
        console.log("  [dry] patch:", patch);
      } else {
        if (teamsToUpsert.length) await ensureTeamsExist(teamsToUpsert);
        const { error: upErr } = await supabase
          .from("matches")
          .update(patch)
          .eq("spl_match_id", mid);
        if (upErr) throw new Error(upErr.message);
        console.log(`  ✓ updated (${homeName} ${homeScore}-${awayScore} ${awayName})`);
      }

      ok++;
    } catch (e: any) {
      fail++;
      console.error(`  ✗ ${e?.message ?? String(e)}`);
    }

    await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok} FAIL=${fail}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});