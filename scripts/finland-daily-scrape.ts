/**
 * finland-daily-scrape.ts
 *
 * Scrapes all matches for a given date from:
 *   https://tulospalvelu.palloliitto.fi/livescore/today/all
 *
 * For each played match found:
 *   1. Upserts competition into competitions table
 *   2. Calls getMatch API for scores, teams, lineups all in one request
 *   3. Upserts match, teams, lineups into DB
 *
 * Usage:
 *   npx tsx finland-daily-scrape.ts [--date 2026-04-06] [--dry] [--limit 10]
 *
 * Defaults to yesterday if no date provided.
 */

import * as dotenv from "dotenv";
import path from "path";
import { createClient } from "@supabase/supabase-js";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!,
  { auth: { persistSession: false } }
);

const BASE_URL = "https://spl.torneopal.net/taso/rest";
const LIVESCORE_URL = "https://tulospalvelu.palloliitto.fi";

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : process.argv[i + 1] ?? null;
}

const dry    = process.argv.includes("--dry");
const limit  = Number(arg("--limit") ?? "0");
const sleepMs = 500;

function defaultDate(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split("T")[0];
}
const dateArg = arg("--date") ?? defaultDate();

function sleep(ms: number) { return new Promise((r) => setTimeout(r, ms)); }

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "text/html,application/xhtml+xml",
      "Referer": "https://tulospalvelu.palloliitto.fi/",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.text();
}

async function fetchJSON(url: string): Promise<any> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "json/n9tnjq45uuccbe8nbfy6q7ggmreqntvs",
      "Origin": "https://tulospalvelu.palloliitto.fi",
      "Referer": "https://tulospalvelu.palloliitto.fi/",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

// ── Competition classifier ─────────────────────────────────────────────────

function classifyCompetition(name: string): { tier: number; gender: string } {
  const n = name.toLowerCase();

  let gender = "Male";
  if (/naisten|naiset|tytöt|t\d{2}|npl/i.test(n)) gender = "Female";
  else if (/tytöt|t\d{2}/i.test(n)) gender = "Youth_Female";
  else if (/pojat|p\d{2}|juniorit/i.test(n)) gender = "Youth_Male";

  if (/cup|suomen cup/i.test(n)) return { tier: 99, gender };
  if (/miehet 35|masters/i.test(n)) return { tier: 99, gender };

// Veterans — skip
  if (/miehet \d+|masters|naiset \d+/i.test(n)) return { tier: 99, gender };

  // Youth — only allow P18/T18 and above, skip everything younger
  const youthMatch = n.match(/[pt](\d{2})/i);
  if (youthMatch) {
    const age = parseInt(youthMatch[1]);
    if (age < 18) return { tier: 99, gender };
  }
  // Skip generic youth labels
  if (/p11|p8|pojat|tytöt/i.test(n)) return { tier: 99, gender };

  if (gender === "Female") {
    if (/naisten liiga/i.test(n))    return { tier: 1, gender };
    if (/kakkonen/i.test(n))         return { tier: 2, gender };
    if (/kolmonen/i.test(n))         return { tier: 3, gender };
    return { tier: 4, gender };
  }

  if (/veikkausliiga/i.test(n))      return { tier: 1, gender };
  if (/ykkösliiga/i.test(n))         return { tier: 1, gender: "Female" }; // women's top
  if (/ykkönen/i.test(n))            return { tier: 2, gender };
  if (/kakkonen/i.test(n))           return { tier: 3, gender };
  if (/kolmonen/i.test(n))           return { tier: 4, gender };
  if (/nelonen/i.test(n))            return { tier: 5, gender };
  if (/vitonen/i.test(n))            return { tier: 6, gender };

  return { tier: 9, gender };
}

// ── Parse daily match list ─────────────────────────────────────────────────

interface DailyMatch {
  matchId:         string;
  competitionSlug: string;
  competitionName: string;
  status:          string; // "Played" | "Fixture" | "Live"
}

function extractDailyMatches(html: string): DailyMatch[] {
  const matches: DailyMatch[] = [];
  const outerRows = [...html.matchAll(/id="(\d+)"\s+matchid="\d+"\s+class="outerrow[^"]*"/g)];

  // Split on outerrow divs
  const blocks = html.split('class="outerrow ');
  for (const block of blocks.slice(1)) {
    // Match ID
    const matchIdM = block.match(/id="(\d+)"/);
    if (!matchIdM) continue;
    const matchId = matchIdM[1];

    // Status
    const statusM = block.match(/class="matchrow status-(\w+)"/);
    const status = statusM?.[1] ?? "Unknown";

    // Competition
    const compM = block.match(/href="\/category\/([^"]+)"[^>]*><span[^>]*>([^<]+)</);
    if (!compM) continue;
    const competitionSlug = compM[1];
    const competitionName = compM[2].trim();

    matches.push({ matchId, competitionSlug, competitionName, status });
  }

  return matches;
}

// ── Competition slug → ID mapping ─────────────────────────────────────────
// Extract the competition ID from the slug e.g. "VL!spljp26/group/1" → "VL!spljp26"

function slugToCompId(slug: string): string {
  return slug.split("/")[0]; // e.g. "VL!spljp26"
}

// ── Build kickoff ISO string ───────────────────────────────────────────────

function buildKickoffAt(date: string, time: string): string | null {
  if (!date) return null;
  return `${date}T${time || "00:00:00"}`;
}

// ── Lineup parsing (mirrors 4.scrape-match-lineups.ts) ────────────────────

type Squad = "xi" | "bench";
type Side  = "home" | "away";

interface LineupRow {
  spl_match_id:     string;
  lineup_idx:       number;
  spl_team_id:      string | null;
  spl_player_id:    string | null;
  player_name:      string | null;
  first_name:       string | null;
  last_name:        string | null;
  shirt_number:     number | null;
  squad:            Squad;
  side:             Side;
  captain:          boolean;
  position:         string | null;
  playing_time_min: number | null;
  goals:            number | null;
  assists:          number | null;
  warnings:         number | null;
  suspensions:      number | null;
  birth_year:       number | null;
}

function parseLineupEntry(raw: any, matchId: string, side: Side, idx: number): LineupRow {
  const squad: Squad = String(raw.start ?? "0") === "1" ? "xi" : "bench";
  const numOrNull = (v: any) => (v !== "" && v != null && !isNaN(Number(v))) ? Number(v) : null;

  return {
    spl_match_id:     matchId,
    lineup_idx:       idx,
    spl_team_id:      raw.team_id   || null,
    spl_player_id:    raw.player_id || null,
    player_name:      raw.player_name || null,
    first_name:       raw.first_name  || null,
    last_name:        raw.last_name   || null,
    shirt_number:     numOrNull(raw.shirt_number),
    squad,
    side,
    captain:          raw.captain === "C",
    position:         raw.position || null,
    playing_time_min: numOrNull(raw.playing_time_min),
    goals:            numOrNull(raw.goals),
    assists:          numOrNull(raw.assists),
    warnings:         numOrNull(raw.warnings),
    suspensions:      numOrNull(raw.suspensions),
    birth_year:       raw.birthyear ? Number(raw.birthyear) : null,
  };
}

// ── Main ───────────────────────────────────────────────────────────────────

async function main() {
  console.log(`=== finland-daily-scrape ===`);
  console.log(`date=${dateArg}  dry=${dry}  limit=${limit || "none"}\n`);

  // Fetch daily match list — use the date filter URL
  const [year, month, day] = dateArg.split("-");
  const livescoreUrl = `${LIVESCORE_URL}/livescore/${year}-${month}-${day}/all`;
  console.log(`Fetching: ${livescoreUrl}`);

  const html = await fetchHtml(livescoreUrl);
  let dailyMatches = extractDailyMatches(html);

  // Only process played matches
  dailyMatches = dailyMatches.filter(m => m.status === "Played");

  // Filter out youth/cup/veterans
  dailyMatches = dailyMatches.filter(m => {
    const { tier } = classifyCompetition(m.competitionName);
    return tier < 9 && tier !== 99;
  });

  console.log(`Found ${dailyMatches.length} played senior matches on ${dateArg}`);

  if (limit > 0) dailyMatches = dailyMatches.slice(0, limit);

  // Deduplicate and upsert competitions
  const competitionsSeen = new Map<string, string>();
  for (const m of dailyMatches) {
    const compId = slugToCompId(m.competitionSlug);
    if (!competitionsSeen.has(compId)) competitionsSeen.set(compId, m.competitionName);
  }

  const seasonYear = parseInt(dateArg.split("-")[0]);

  console.log(`\nUpserting ${competitionsSeen.size} competitions...`);
  for (const [compId, name] of competitionsSeen) {
    const { tier, gender } = classifyCompetition(name);
    if (dry) {
      console.log(`  DRY comp: ${compId} "${name}" tier=${tier} gender=${gender}`);
      continue;
    }
    const { error } = await supabase.from("competitions").upsert({
      spl_competition_id: compId,
      season_year:        seasonYear,
      name,
      tier,
      gender,
      updated_at:         new Date().toISOString(),
    }, { onConflict: "spl_competition_id" });
    if (error) console.error(`  [ERROR] comp ${compId}: ${error.message}`);
    else console.log(`  ✓ ${name} (tier=${tier} ${gender})`);
  }

  // Process each match
  console.log(`\nProcessing ${dailyMatches.length} matches...\n`);
  let ok = 0, fail = 0, noLineup = 0;

  for (const { matchId, competitionSlug, competitionName } of dailyMatches) {
    const compId = slugToCompId(competitionSlug);
    const url = `${BASE_URL}/getMatch?match_id=${matchId}`;

    try {
      const data = await fetchJSON(url);
      const match = data?.match;
      if (!match) throw new Error("No match object in response");
      if (match.status !== "Played") {
        console.log(`  [SKIP] ${matchId} status=${match.status}`);
        await sleep(sleepMs);
        continue;
      }

      const homeTeamId = match.team_A_id || null;
      const awayTeamId = match.team_B_id || null;
      const homeName   = match.team_A_name || null;
      const awayName   = match.team_B_name || null;
      const homeScore  = match.fs_A !== "" && match.fs_A != null ? Number(match.fs_A) : null;
      const awayScore  = match.fs_B !== "" && match.fs_B != null ? Number(match.fs_B) : null;
      const homeHalf   = match.hts_A !== "" && match.hts_A != null ? Number(match.hts_A) : null;
      const awayHalf   = match.hts_B !== "" && match.hts_B != null ? Number(match.hts_B) : null;
      const kickoffAt  = buildKickoffAt(match.date, match.time);
      const venue      = [match.venue_name, match.venue_city_name].filter(Boolean).join(", ") || null;
      const attendance = match.attendance ? Number(match.attendance) : null;

      if (dry) {
        console.log(`  DRY ${matchId}: ${homeName} ${homeScore ?? "?"}-${awayScore ?? "?"} ${awayName}`);
        ok++;
        await sleep(sleepMs);
        continue;
      }

      // Upsert teams
      const teamsToUpsert = [];
      if (homeTeamId) teamsToUpsert.push({
        spl_team_id: homeTeamId, spl_club_id: match.club_A_id || null,
        team_name: homeName, club_name: match.club_A_name || null,
        club_crest: match.club_A_crest || null,
      });
      if (awayTeamId) teamsToUpsert.push({
        spl_team_id: awayTeamId, spl_club_id: match.club_B_id || null,
        team_name: awayName, club_name: match.club_B_name || null,
        club_crest: match.club_B_crest || null,
      });
      if (teamsToUpsert.length) {
        await supabase.from("teams").upsert(teamsToUpsert, { onConflict: "spl_team_id" });
      }

      // Upsert match
      await supabase.from("matches").upsert({
        spl_match_id:        matchId,
        spl_competition_id:  compId,
        season_year:         seasonYear,
        status:              "Played",
        home_team_spl_id:    homeTeamId,
        away_team_spl_id:    awayTeamId,
        home_score:          homeScore,
        away_score:          awayScore,
        home_halftime:       homeHalf,
        away_halftime:       awayHalf,
        kickoff_at:          kickoffAt,
        venue,
        attendance,
        scraped_overview_at: new Date().toISOString(),
        updated_at:          new Date().toISOString(),
      }, { onConflict: "spl_match_id" });

      // Process lineups
      const rawLineups: any[] = Array.isArray(match.lineups) ? match.lineups : [];
      if (rawLineups.length === 0) {
        console.log(`  [NO LINEUP] ${matchId}: ${homeName} ${homeScore}-${awayScore} ${awayName}`);
        noLineup++;
        await sleep(sleepMs);
        continue;
      }

      const lineupRows: LineupRow[] = [];
      let idx = 0;
      for (const entry of rawLineups) {
        const entryTeamId = String(entry.team_id ?? "");
        const apiHomeId   = String(match.team_A_id ?? "");
        const side: Side  = entryTeamId === apiHomeId ? "home" : "away";
        lineupRows.push(parseLineupEntry(entry, matchId, side, idx));
        idx++;
      }

      // If no starters marked, treat all as starters
      const starters = lineupRows.filter(r => r.squad === "xi").length;
      if (starters === 0) lineupRows.forEach(r => r.squad = "xi");

      // Upsert lineups in chunks
      const CHUNK = 500;
      for (let c = 0; c < lineupRows.length; c += CHUNK) {
        await supabase.from("match_lineups")
          .upsert(lineupRows.slice(c, c + CHUNK), { onConflict: "spl_match_id,lineup_idx" });
      }

      await supabase.from("matches").update({
        scraped_lineups_at: new Date().toISOString(),
      }).eq("spl_match_id", matchId);

      console.log(`  ✓ ${matchId}: ${homeName} ${homeScore}-${awayScore} ${awayName} (${lineupRows.length} players)`);
      ok++;

    } catch (err: any) {
      console.warn(`  [FAIL] ${matchId}: ${err.message}`);
      fail++;
    }

    await sleep(sleepMs);
  }

  console.log(`\nDone. OK=${ok}  FAIL=${fail}  NO_LINEUP=${noLineup}`);
}

main().catch((e) => { console.error(e); process.exit(1); });