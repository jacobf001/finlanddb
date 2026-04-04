/**
 * 4.scrape-match-lineups.ts
 *
 * Fetches lineups for all played matches that haven't been scraped yet.
 *
 * Iceland parsed complex HTML grids with Cheerio to find "Byrjunarlið" /
 * "Varamenn" sections. Finland gets lineups directly from the Torneopal
 * getMatch JSON response — the `lineups` array is already structured with
 * player_id, shirt_number, start (1=starter, 0=bench), captain, position,
 * birthyear, goals, warnings, suspensions, playing_time_min etc.
 *
 * One API call per match gives us both teams' full lineup.
 *
 * Usage:
 *   npx tsx 4.scrape-match-lineups.ts [--from 2020] [--to 2026] [--limit 200] [--sleep 300] [--dry] [--debug]
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

// ── Lineup row shape ───────────────────────────────────────────────────────
// Mirrors the Iceland `match_lineups` table structure as closely as possible,
// adapted to SPL field names.
//
// Raw API lineup entry (from the confirmed getMatch response):
// {
//   lineup_id, match_id, team_id, player_id, player_name,
//   first_name, last_name, shirt_number, start ("1"=starter, "0"=bench),
//   captain ("C" | ""), position, playing_time_min,
//   goals, assists, warnings, suspensions,
//   birthyear, overage, ...
// }

type Squad = "xi" | "bench";
type Side  = "home" | "away";

interface LineupRow {
  spl_match_id:    string;
  lineup_idx:      number;
  spl_team_id:     string | null;
  spl_player_id:   string | null;
  player_name:     string | null;
  first_name:      string | null;
  last_name:       string | null;
  shirt_number:    number | null;
  squad:           Squad;
  side:            Side;
  captain:         boolean;
  position:        string | null;
  playing_time_min: number | null;
  goals:           number | null;
  assists:         number | null;
  warnings:        number | null;   // yellow cards
  suspensions:     number | null;   // red cards
  birth_year:      number | null;
}

function parseLineupEntry(
  raw: any,
  matchId: string,
  side: Side,
  idx: number
): LineupRow {
  const start = String(raw.start ?? "0");
  const squad: Squad = start === "1" ? "xi" : "bench";

  const shirtRaw = raw.shirt_number;
  const shirt = shirtRaw !== "" && shirtRaw != null ? Number(shirtRaw) : null;

  const playingTime = raw.playing_time_min != null && raw.playing_time_min !== ""
    ? Number(raw.playing_time_min) : null;

  const numOrNull = (v: any) =>
    (v !== "" && v != null && !isNaN(Number(v))) ? Number(v) : null;

  return {
    spl_match_id:     matchId,
    lineup_idx:       idx,
    spl_team_id:      raw.team_id    || null,
    spl_player_id:    raw.player_id  || null,
    player_name:      raw.player_name || null,
    first_name:       raw.first_name  || null,
    last_name:        raw.last_name   || null,
    shirt_number:     shirt,
    squad,
    side,
    captain:          raw.captain === "C",
    position:         raw.position || null,
    playing_time_min: playingTime,
    goals:            numOrNull(raw.goals),
    assists:          numOrNull(raw.assists),
    warnings:         numOrNull(raw.warnings),
    suspensions:      numOrNull(raw.suspensions),
    birth_year:       raw.birthyear ? Number(raw.birthyear) : null,
  };
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== 4.scrape-match-lineups (SPL / Torneopal) ===`);
  console.log(`Seasons: ${fromYear} → ${toYear}  |  dry=${dry}  |  limit=${limit || "none"}  |  sleep=${sleepMs}ms\n`);

  // Fetch matches that have scores but haven't had lineups scraped yet
  const all: any[] = [];
  let from = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select("spl_match_id, season_year, home_team_spl_id, away_team_spl_id, scraped_lineups_at, status, kickoff_at")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .is("scraped_lineups_at", null)
      .eq("status", "Played")
      .order("kickoff_at", { ascending: false, nullsFirst: false })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  const target = limit > 0 ? all.slice(0, limit) : all;
  console.log(`Matches needing lineup scrape: ${all.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m = target[i];
    const mid = String(m.spl_match_id);
    const homeTeamId = m.home_team_spl_id ? String(m.home_team_spl_id) : null;
    const awayTeamId = m.away_team_spl_id ? String(m.away_team_spl_id) : null;

    const url = `${BASE_URL}/getMatch?match_id=${mid}`;
    console.log(`\n[${i + 1}/${target.length}] match ${mid}`);
    if (debug) console.log(`  GET ${url}`);

    try {
      const data = await fetchJSON(url);
      const match = data?.match;

      if (!match) throw new Error("No match object in response");
      if (match.status !== "Played") {
        console.log(`  ⚠ status=${match.status} — skipping`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      const rawLineups: any[] = Array.isArray(match.lineups) ? match.lineups : [];

      if (rawLineups.length === 0) {
        console.log(`  ⚠ lineups_filled=${match.lineups_filled} — no lineup data in response`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      // Determine side for each lineup entry by team_id
      const lineupRows: LineupRow[] = [];
      let idx = 0;

      for (const entry of rawLineups) {
        const entryTeamId = String(entry.team_id ?? "");
        let side: Side;
        if (homeTeamId && entryTeamId === homeTeamId) {
          side = "home";
        } else if (awayTeamId && entryTeamId === awayTeamId) {
          side = "away";
        } else {
          // Fallback: use match's team_A_id / team_B_id from the response
          const apiHomeId = String(match.team_A_id ?? "");
          const apiAwayId = String(match.team_B_id ?? "");
          side = entryTeamId === apiHomeId ? "home" : "away";
        }

        lineupRows.push(parseLineupEntry(entry, mid, side, idx));
        idx++;
      }

      const starters = lineupRows.filter((r) => r.squad === "xi").length;
      const bench    = lineupRows.filter((r) => r.squad === "bench").length;

      // If no starters at all, the club submitted a flat squad list without
      // marking starters — treat everyone as a starter
      if (starters === 0 && bench > 0) {
        for (const r of lineupRows) r.squad = "xi";
      }

      const finalStarters = lineupRows.filter((r) => r.squad === "xi").length;
      const finalBench    = lineupRows.filter((r) => r.squad === "bench").length;

      if (debug) {
        console.log(`  parsed: ${lineupRows.length} total (${finalStarters} starters, ${finalBench} bench)`);
        console.log(`  sample:`, lineupRows[0]);
      } else {
        console.log(`  ${lineupRows.length} players (${finalStarters} starters, ${finalBench} bench)`);
      }

      if (dry) {
        console.log(`  [dry] would upsert ${lineupRows.length} lineup rows`);
      } else {
        // Upsert lineups
        const CHUNK = 500;
        for (let c = 0; c < lineupRows.length; c += CHUNK) {
          const chunk = lineupRows.slice(c, c + CHUNK);
          const { error: lErr } = await supabase
            .from("match_lineups")
            .upsert(chunk, { onConflict: "spl_match_id,lineup_idx" });
          if (lErr) throw new Error(`match_lineups upsert failed: ${lErr.message}`);
        }

        // Mark match as scraped
        const { error: mErr } = await supabase
          .from("matches")
          .update({ scraped_lineups_at: new Date().toISOString() })
          .eq("spl_match_id", mid);
        if (mErr) throw new Error(`matches update failed: ${mErr.message}`);

        console.log(`  ✓ saved ${lineupRows.length} lineup rows`);
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