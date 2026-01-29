# MLB Scouting Pipeline

End-to-end pipeline for pulling, reconciling, and aggregating MLB data
for team-level scouting and game-day context.

## Scope (v1.0)
- Season: 2025
- Data access & aggregation only
- No rolling windows
- No bullpen workload
- No starter form logic

## Data Sources
- MLBAM / Statcast (pitch-level, roster, schedule)
- FanGraphs (season-level & aggregated context)
- Lahman database (historical reference)
- Baseball-Reference (daily box score context)

## Outputs
- Canonical player ID spine
- Team roster & schedule
- Statcast pitch-level data joined to schedule
- FanGraphs pitcher aggregates
- Lahman career summaries

## Status
- v1.0 locked
- Ready for v2 (player-game spine, rolling windows)

## Next Steps
- Build player-game spine
- Rolling 14 / 30 day windows
- Bullpen workload & rest modeling
- Scouting report generation
