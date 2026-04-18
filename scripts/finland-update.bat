@echo off
echo === Finland Update %date% %time% ===
npx tsx 3.scrape-match-overview.ts --from 2026 --to 2026
npx tsx 4.scrape-match-lineups.ts --from 2026 --to 2026
echo === Done ===
pause