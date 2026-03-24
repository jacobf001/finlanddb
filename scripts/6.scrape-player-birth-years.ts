/**
 * 6.scrape-player-birth-years.ts
 *
 * Fills in missing player data (birth_year, full name, nationality) for all
 * players seen in match_lineups that don't yet have a players table row.
 *
 * Iceland fetched each player's HTML page and scraped a `span.eyebrow-2`
 * element to extract the birth year.
 *
 * Finland is much simpler — birth_year is already embedded in every lineup
 * entry from getMatch (script 4 captures it). So this script's primary job
 * is just to consolidate the `players` table from what's already in
 * match_lineups, plus optionally call getPlayer for any players still missing
 * a birth year after that.
 *
 * Two-phase approach:
 *   Phase 1 — fast: consolidate players from match_lineups rows (no API calls)
 *   Phase 2 — slow: for any still missing birth_year, call getPlayer API
 *
 * Usage:
 *   npx tsx 6.scrape-player-birth-years.ts [--limit 200] [--sleep 300] [--dry] [--debug] [--phase1-only] [--phase2-only]
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
const limit      = Number(arg("--limit") ?? "0");
const sleepMs    = Number(arg("--sleep") ?? "300");
const dry        = process.argv.includes("--dry");
const debug      = process.argv.includes("--debug");
const phase1Only = process.argv.includes("--phase1-only");
const phase2Only = process.argv.includes("--phase2-only");

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

// ── Player row shape ───────────────────────────────────────────────────────
interface PlayerRow {
  spl_player_id: string;
  first_name:    string | null;
  last_name:     string | null;
  player_name:   string | null;
  birth_year:    number | null;
  nationality:   string | null;
}

// ── Phase 1: consolidate from match_lineups ────────────────────────────────
// match_lineups already has first_name, last_name, player_name, birth_year
// from the getMatch response. We just aggregate the best known values per
// player_id and upsert into the players table — no API calls needed.
async function phase1ConsolidateFromLineups(): Promise<number> {
  console.log("\n── Phase 1: consolidate players from match_lineups ──");

  // Page through all lineup rows that have a player id
  const pageSize = 1000;
  let from = 0;
  const byPlayer = new Map<string, PlayerRow>();

  while (true) {
    const { data, error } = await supabase
      .from("match_lineups")
      .select("spl_player_id, first_name, last_name, player_name, birth_year")
      .not("spl_player_id", "is", null)
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];

    for (const r of batch) {
      const id = String(r.spl_player_id ?? "");
      if (!id) continue;

      const existing = byPlayer.get(id);
      const birthYear = r.birth_year ? Number(r.birth_year) : null;

      // Keep best known values — prefer entries that have a birth year
      if (!existing || (birthYear && !existing.birth_year)) {
        byPlayer.set(id, {
          spl_player_id: id,
          first_name:    r.first_name  || existing?.first_name  || null,
          last_name:     r.last_name   || existing?.last_name   || null,
          player_name:   r.player_name || existing?.player_name || null,
          birth_year:    birthYear     ?? existing?.birth_year  ?? null,
          nationality:   null, // filled in phase 2 if needed
        });
      }
    }

    if (batch.length < pageSize) break;
    from += pageSize;
  }

  console.log(`  Unique players found in lineups: ${byPlayer.size}`);
  const rows = Array.from(byPlayer.values());

  if (dry) {
    console.log(`  [dry] would upsert ${rows.length} player rows`);
    const withYear = rows.filter((r) => r.birth_year !== null).length;
    console.log(`  with birth_year: ${withYear} | without: ${rows.length - withYear}`);
    return rows.length;
  }

  // Upsert in batches of 500
  const BATCH = 500;
  let upserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    const { error } = await supabase
      .from("players")
      .upsert(chunk, { onConflict: "spl_player_id" });
    if (error) throw new Error(`players upsert failed: ${error.message}`);
    upserted += chunk.length;
    process.stdout.write(`\r  Upserted ${upserted} / ${rows.length}`);
  }
  console.log(`\n  ✓ Phase 1 complete: ${upserted} players upserted`);

  return upserted;
}

// ── Phase 2: API fallback for players still missing birth_year ─────────────
// getPlayer returns: player_id, first_name, last_name, birthday, birthyear,
//                    nationality, club_id, club_name, img_url, teams[], matches[]
async function phase2FetchMissingBirthYears(): Promise<{ ok: number; fail: number; found: number }> {
  console.log("\n── Phase 2: fetch missing birth_years via getPlayer API ──");

  // Find players in our DB that still have no birth_year
  const pageSize = 1000;
  let from = 0;
  const missing: string[] = [];

  while (true) {
    const { data, error } = await supabase
      .from("players")
      .select("spl_player_id")
      .is("birth_year", null)
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];
    for (const r of batch) {
      if (r.spl_player_id) missing.push(String(r.spl_player_id));
    }
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  const todo = limit > 0 ? missing.slice(0, limit) : missing;
  console.log(`  Players missing birth_year: ${missing.length} | processing: ${todo.length}`);

  let ok = 0;
  let fail = 0;
  let found = 0;

  for (let i = 0; i < todo.length; i++) {
    const id  = todo[i];
    const url = `${BASE_URL}/getPlayer?player_id=${id}`;

    if (debug) console.log(`\n[${i + 1}/${todo.length}] player ${id} GET ${url}`);
    else process.stdout.write(`\r  [${i + 1}/${todo.length}] player ${id}     `);

    try {
      const data   = await fetchJSON(url);
      const player = data?.player;

      if (!player) {
        if (debug) console.log(`  ⚠ no player object`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      const birthYear = player.birthyear ? Number(player.birthyear) : null;
      const now = new Date().getUTCFullYear();
      const validYear = birthYear && birthYear >= 1940 && birthYear <= now ? birthYear : null;

      if (!validYear) {
        if (debug) console.log(`  ⚠ no valid birth year (raw: ${player.birthyear})`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      found++;

      const patch: Partial<PlayerRow> = {
        birth_year:  validYear,
        first_name:  player.first_name  || null,
        last_name:   player.last_name   || null,
        player_name: `${player.first_name ?? ""} ${player.last_name ?? ""}`.trim() || null,
        nationality: player.nationality || null,
      };

      if (debug) console.log(`\n  player ${id} -> birth_year=${validYear} nationality=${patch.nationality}`);

      if (!dry) {
        const { error } = await supabase
          .from("players")
          .update(patch)
          .eq("spl_player_id", id);
        if (error) throw new Error(error.message);
      }

      ok++;
    } catch (e: any) {
      fail++;
      if (debug) console.error(`\n  ✗ player ${id}: ${e?.message ?? String(e)}`);
    }

    await sleep(sleepMs);
  }

  console.log(`\n  Phase 2 complete. OK=${ok} FAIL=${fail} | found birth_year=${found}`);
  return { ok, fail, found };
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== 6.scrape-player-birth-years (SPL / Torneopal) ===`);
  console.log(`dry=${dry}  |  limit=${limit || "none"}  |  sleep=${sleepMs}ms`);
  console.log(`phase1Only=${phase1Only}  |  phase2Only=${phase2Only}\n`);

  if (!phase2Only) {
    await phase1ConsolidateFromLineups();
  }

  if (!phase1Only) {
    await phase2FetchMissingBirthYears();
  }

  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});