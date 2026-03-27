/**
 * populate-teams.ts
 *
 * One-off script to populate the teams table from match data.
 *
 * Strategy:
 * 1. Get all unique team IDs from the matches table
 * 2. For each team, call getTeam API to get name, club info, crest
 * 3. Upsert into teams table
 *
 * Usage:
 *   npx tsx populate-teams.ts [--limit 100] [--sleep 300] [--dry]
 */

import * as dotenv from "dotenv";
import path from "path";
import { createClient } from "@supabase/supabase-js";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing Supabase env vars");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const BASE_URL = "https://spl.torneopal.net/taso/rest";

function arg(name: string): string | null {
  const i = process.argv.indexOf(name);
  return i === -1 ? null : process.argv[i + 1] ?? null;
}
const limit   = Number(arg("--limit")  ?? "0");
const sleepMs = Number(arg("--sleep")  ?? "200");
const dry     = process.argv.includes("--dry");

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function fetchJSON(url: string): Promise<any> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      "Accept": "json/n9tnjq45uuccbe8nbfy6q7ggmreqntvs",
      "Origin": "https://tulospalvelu.palloliitto.fi",
      "Referer": "https://tulospalvelu.palloliitto.fi/",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function main() {
  console.log("=== populate-teams ===");

  // Get all unique team IDs from matches
  const teamIds = new Set<string>();
  let from = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .from("matches")
      .select("home_team_spl_id, away_team_spl_id")
      .range(from, from + pageSize - 1);

    if (error) throw new Error(error.message);
    const batch = data ?? [];
    for (const r of batch) {
      if (r.home_team_spl_id) teamIds.add(String(r.home_team_spl_id));
      if (r.away_team_spl_id) teamIds.add(String(r.away_team_spl_id));
    }
    if (batch.length < pageSize) break;
    from += pageSize;
  }

  console.log(`Unique teams in matches: ${teamIds.size}`);

  // Check which teams already have names
  const { data: existingTeams } = await supabase
    .from("teams")
    .select("spl_team_id, team_name")
    .not("team_name", "is", null);

  const alreadyNamed = new Set((existingTeams ?? []).map((t: any) => String(t.spl_team_id)));
  console.log(`Already have names for: ${alreadyNamed.size}`);

  const todo = Array.from(teamIds).filter(id => !alreadyNamed.has(id));
  const target = limit > 0 ? todo.slice(0, limit) : todo;
  console.log(`Teams to fetch: ${target.length}\n`);

  let ok = 0, fail = 0;

  for (let i = 0; i < target.length; i++) {
    const teamId = target[i];
    process.stdout.write(`\r[${i + 1}/${target.length}] team ${teamId}     `);

    try {
      // Use getTeam endpoint with a competition_id param
      // If that fails, fall back to extracting from match data
      const url = `${BASE_URL}/getTeam?team_id=${teamId}`;
      const data = await fetchJSON(url);
      const team = data?.team;

      if (!team) throw new Error("No team object");

      const row = {
        spl_team_id:  teamId,
        spl_club_id:  team.club_id   || null,
        team_name:    team.team_name  || team.name || null,
        club_name:    team.club_name  || null,
        club_crest:   team.club_crest || null,
      };

      if (dry) {
        if (i < 5) console.log(`\n  [dry] ${JSON.stringify(row)}`);
      } else {
        const { error } = await supabase
          .from("teams")
          .upsert(row, { onConflict: "spl_team_id" });
        if (error) throw new Error(error.message);
      }

      ok++;
    } catch (e: any) {
      fail++;
      if (fail <= 5) console.error(`\n  ✗ team ${teamId}: ${e?.message}`);
    }

    await sleep(sleepMs);
  }

  console.log(`\n\nDone. OK=${ok} FAIL=${fail}`);
}

main().catch(e => { console.error(e); process.exit(1); });