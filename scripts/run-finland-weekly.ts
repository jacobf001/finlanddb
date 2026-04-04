// scripts/run-finland-weekly.ts
async function main() {
  console.log("1. Discover matches");
  await import("./2.discover-matches");

  console.log("2. Scrape match overview");
  await import("./3.scrape-match-overview");

  console.log("3. Scrape lineups");
  await import("./4.scrape-match-lineups");

  console.log("4. Scrape events");
  await import("./5.scrape-events-overview");

  console.log("5. Rebuild player season stats");
  await import("./7.rebuild-player-season-stats");

  console.log("6. Rebuild player season to date");
  await import("./8.rebuild-player-season-to-date");

  console.log("7. Rebuild league table");
  await import("./9.rebuild-computed-league-table");

  console.log("Done");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});