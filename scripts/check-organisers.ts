import * as dotenv from "dotenv";
import path from "path";
dotenv.config({ path: path.resolve(process.cwd(), "../.env"), override: true });

async function main() {
  const res = await fetch("https://spl.torneopal.fi/taso/rest/getCompetitions?api_key=&season_id=2026&sport=football");
  const data = await res.json();
  const orgs = [...new Set((data.competitions as any[]).map((c) => c.organiser))].sort();
  console.log("Organisers found:", orgs);
  console.log("\nSample competitions:");
  for (const c of (data.competitions as any[]).slice(0, 10)) {
    console.log(`  ${c.organiser.padEnd(20)} ${c.competition_id.padEnd(25)} ${c.competition_name}`);
  }
}

main();