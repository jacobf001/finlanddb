/**
 * 5.scrape-events-overview.ts
 *
 * Fetches match events (goals, cards, substitutions) for all played matches
 * that haven't had events scraped yet.
 *
 * Iceland parsed complex HTML grids — hunting for "Atburðir" headings,
 * detecting event types by SVG colour codes (#FAC83C yellow, #1A7941 green,
 * #DD3636 red), and resolving player→team mappings from lineup rows.
 *
 * Finland gets events directly from the Torneopal getMatch JSON response.
 * The `events` array contains structured entries with event_type, minute,
 * team_id, player_id, player_name already resolved — no HTML detection needed.
 *
 * Also back-fills minute_in / minute_out on match_lineups rows from
 * substitution events, exactly as Iceland did.
 *
 * Usage:
 *   npx tsx 5.scrape-events-overview.ts [--from 2020] [--to 2026] [--limit 200] [--sleep 300] [--dry] [--debug] [--replace]
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
const replace  = process.argv.includes("--replace");

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

// ── Event type normalisation ───────────────────────────────────────────────
// SPL uses Finnish "code" field:
//   "maali" = goal, "varoitus" = yellow, "ulosajo" = red,
//   "vaihto" = substitution, "rangaistusmaali" = penalty, "omamaali" = own goal
//   "ottelualkoi" / "otteluloppui" = match start/end (skip)
function normaliseEventType(code: string): string | null {
  switch (code?.toLowerCase()) {
    case "maali":              return "goal";
    case "rangaistusmaali":    return "penalty_goal";
    case "omamaali":           return "own_goal";
    case "varoitus":           return "yellow";
    case "ulosajo":            return "red";
    case "keltainen_punainen": return "yellow_red";
    case "vaihto":             return "substitution";
    // Skip all clock/period/system events
    case "ottelualkoi":        return null;
    case "otteluloppui":       return null;
    case "jaksoalkoi":         return null;
    case "jaksoloppui":        return null;
    case "jaksovaihtui":       return null;
    case "kellokay":           return null;
    case "kelloseis":          return null;
    default:
      // Skip any other system events (team=0, no player)
      return null;
  }
}

// ── Event row shape ────────────────────────────────────────────────────────
interface EventRow {
  spl_match_id:          string;
  event_idx:             number;
  minute:                number;
  stoppage:              number | null;
  event_type:            string;
  spl_team_id:           string | null;
  spl_player_id:         string | null;
  player_name:           string | null;
  sub_on_spl_player_id:  string | null;
  sub_off_spl_player_id: string | null;
  sub_on_name:           string | null;
  sub_off_name:          string | null;
}

// ── Parse events from getMatch response ───────────────────────────────────
// Raw API event entry shape:
// {
//   event_id, match_id, period, minute, stoppage_time,
//   event_type,           ← "goal" | "yellow_card" | "substitution" etc.
//   team_id,
//   player_id,  player_name,
//   player2_id, player2_name,   ← sub off / assist player
//   x, y                        ← shotmap coords (ignore)
// }
function parseEvents(rawEvents: any[], matchId: string): EventRow[] {
  const rows: EventRow[] = [];
  const seen = new Set<string>();

  for (const e of rawEvents) {
    const eventType = normaliseEventType(e.code ?? "");
    if (eventType === null) continue; // skip clock/system events

    // For substitutions, skip the duplicate "player off only" entry
    // The API logs each sub twice — keep only the entry that has player_2_id (coming on)
    if (eventType === "substitution" && !e.player_2_id) continue;

    const minuteRaw = e.time_min ?? "";
    const minute = parseInt(String(minuteRaw), 10);
    if (!Number.isFinite(minute) || isNaN(minute)) continue;

    const stoppage: number | null = null;
    const teamId    = e.team_id    || null;
    const playerId  = e.player_id  || null;
    const playerName = e.player_name || null;

    // For substitutions: player_id = coming ON, player_2_id = coming OFF
    const isSubstitution = eventType === "substitution";
    const subOnId    = isSubstitution ? (e.player_id   || null) : null;
    const subOffId   = isSubstitution ? (e.player_2_id || null) : null;
    const subOnName  = isSubstitution ? (e.player_name   || null) : null;
    const subOffName = isSubstitution ? (e.player_2_name || null) : null;

    const key = [minute, eventType, teamId ?? "", playerId ?? "", subOnId ?? "", subOffId ?? ""].join("|");
    if (seen.has(key)) continue;
    seen.add(key);

    rows.push({
      spl_match_id:          matchId,
      event_idx:             0,
      minute,
      stoppage,
      event_type:            eventType,
      spl_team_id:           teamId,
      spl_player_id:         isSubstitution ? null : playerId,
      player_name:           isSubstitution ? null : playerName,
      sub_on_spl_player_id:  subOnId,
      sub_off_spl_player_id: subOffId,
      sub_on_name:           subOnName,
      sub_off_name:          subOffName,
    });
  }

  // Sort chronologically then assign stable indices
  rows.sort((a, b) => {
    if (a.minute !== b.minute) return a.minute - b.minute;
    if ((a.stoppage ?? 0) !== (b.stoppage ?? 0)) return (a.stoppage ?? 0) - (b.stoppage ?? 0);
    return (a.event_type ?? "").localeCompare(b.event_type ?? "");
  });
  rows.forEach((r, i) => (r.event_idx = i));

  return rows;
}

// ── Back-fill sub minutes onto match_lineups ───────────────────────────────
// Identical logic to Iceland — substitution events tell us when players
// came on (minute_in) or went off (minute_out).
async function applySubMinutesToLineups(matchId: string, events: EventRow[]) {
  const { data: lineups, error } = await supabase
    .from("match_lineups")
    .select("id, spl_player_id, minute_in, minute_out")
    .eq("spl_match_id", matchId);

  if (error) throw new Error(`fetch match_lineups failed: ${error.message}`);

  const byPlayer = new Map<string, { id: number; minute_in: number | null; minute_out: number | null }>();
  for (const r of lineups ?? []) {
    if (r.spl_player_id) byPlayer.set(String(r.spl_player_id), { id: r.id, minute_in: r.minute_in, minute_out: r.minute_out });
  }

  const updates: Array<{ id: number; minute_in?: number; minute_out?: number }> = [];
  for (const e of events) {
    if (e.event_type !== "substitution") continue;

    if (e.sub_on_spl_player_id) {
      const r = byPlayer.get(e.sub_on_spl_player_id);
      if (r && r.minute_in == null) updates.push({ id: r.id, minute_in: e.minute });
    }
    if (e.sub_off_spl_player_id) {
      const r = byPlayer.get(e.sub_off_spl_player_id);
      if (r && r.minute_out == null) updates.push({ id: r.id, minute_out: e.minute });
    }
  }

  if (!updates.length) return { updated: 0 };

  if (dry) {
    if (debug) console.log("  [dry] would update match_lineups minutes:", updates.slice(0, 10));
    return { updated: updates.length };
  }

  let ok = 0;
  for (const u of updates) {
    const { error: upErr } = await supabase
      .from("match_lineups")
      .update({
        ...(u.minute_in  !== undefined ? { minute_in:  u.minute_in  } : {}),
        ...(u.minute_out !== undefined ? { minute_out: u.minute_out } : {}),
      })
      .eq("id", u.id);
    if (upErr) throw new Error(`match_lineups update failed (id=${u.id}): ${upErr.message}`);
    ok++;
  }
  return { updated: ok };
}

// ── Debug printer ──────────────────────────────────────────────────────────
function debugPrint(mid: string, events: EventRow[]) {
  console.log(`  ---- events for match ${mid} (${events.length}) ----`);
  for (const e of events) {
    const m = `${e.minute}${e.stoppage ? `+${e.stoppage}` : ""}`;
    const player = e.event_type === "substitution"
      ? `on=${e.sub_on_spl_player_id ?? "-"} off=${e.sub_off_spl_player_id ?? "-"}`
      : `p=${e.spl_player_id ?? "-"}`;
    console.log(
      `  idx=${String(e.event_idx).padStart(2)} min=${String(m).padStart(5)} ` +
      `type=${e.event_type.padEnd(14)} team=${e.spl_team_id ?? "-"} ${player}`
    );
  }
}

// ── Fetch matches that need events scraped ─────────────────────────────────
async function fetchMatchesInRange(): Promise<any[]> {
  const pageSize = 1000;
  let from = 0;
  const all: any[] = [];

  while (true) {
    let q = supabase
      .from("matches")
      .select("spl_match_id, season_year, home_team_spl_id, away_team_spl_id, status, kickoff_at")
      .gte("season_year", fromYear)
      .lte("season_year", toYear)
      .eq("status", "Played")
      .order("kickoff_at", { ascending: false, nullsFirst: false })
      .range(from, from + pageSize - 1);

    if (!replace) q = q.is("scraped_events_at", null);

    const { data, error } = await q;
    if (error) throw new Error(error.message);
    const batch = data ?? [];
    all.push(...batch);
    if (batch.length < pageSize) break;
    from += pageSize;
  }
  return all;
}

// ── Main ───────────────────────────────────────────────────────────────────
async function main() {
  console.log(`=== 5.scrape-events-overview (SPL / Torneopal) ===`);
  console.log(`Seasons: ${fromYear} → ${toYear}  |  dry=${dry}  |  limit=${limit || "none"}  |  replace=${replace}  |  sleep=${sleepMs}ms\n`);

  const matches = await fetchMatchesInRange();
  const target  = limit > 0 ? matches.slice(0, limit) : matches;
  console.log(`Matches in range: ${matches.length} | processing: ${target.length}`);

  let ok = 0;
  let fail = 0;

  for (let i = 0; i < target.length; i++) {
    const m   = target[i];
    const mid = String(m.spl_match_id);
    const url = `${BASE_URL}/getMatch?match_id=${mid}`;

    console.log(`\n[${i + 1}/${target.length}] match ${mid}`);
    if (debug) console.log(`  GET ${url}`);

    try {
      const data  = await fetchJSON(url);
      const match = data?.match;

      if (!match) throw new Error("No match object in response");
      if (match.status !== "Played") {
        console.log(`  ⚠ status=${match.status} — skipping`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      const rawEvents: any[] = Array.isArray(match.events) ? match.events : [];
      const events = parseEvents(rawEvents, mid);

      console.log(`  parsed events: ${events.length}`);
      if (debug) debugPrint(mid, events);

      if (dry) {
        console.log(`  [dry] would save events=${events.length}`);
      } else {
        // Optionally wipe existing events for this match before re-saving
        if (replace) {
          const { error: delErr } = await supabase
            .from("match_events")
            .delete()
            .eq("spl_match_id", mid);
          if (delErr) throw new Error(`match_events delete failed: ${delErr.message}`);

          const { error: resetErr } = await supabase
            .from("matches")
            .update({ scraped_events_at: null })
            .eq("spl_match_id", mid);
          if (resetErr) throw new Error(`matches reset scraped_events_at failed: ${resetErr.message}`);
        }

        if (events.length) {
          const { error: eErr } = await supabase
            .from("match_events")
            .upsert(events, { onConflict: "spl_match_id,event_idx" });
          if (eErr) throw new Error(`match_events upsert failed: ${eErr.message}`);
        }

        const { error: markErr } = await supabase
          .from("matches")
          .update({ scraped_events_at: new Date().toISOString() })
          .eq("spl_match_id", mid);
        if (markErr) throw new Error(`matches update failed: ${markErr.message}`);

        console.log(`  ✓ saved events=${events.length}`);
      }

      // Back-fill sub minutes on lineups
      const subRes = await applySubMinutesToLineups(mid, events);
      console.log(`  ${dry ? "[dry]" : "✓"} lineup minute updates from subs: ${subRes.updated}`);

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