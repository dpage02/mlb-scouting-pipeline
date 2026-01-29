# ============================================================
# Script: pipeline_build_v1.R
# Project: MLB Scouting & Game-Day Context System
#
# Purpose:
#   Establish connections to all core baseball data sources
#   and pull clean, canonical data for a single MLB team.
#
# Scope:
#   - Data access & normalization ONLY
#   - No rolling windows
#   - No bullpen logic
#   - No starter logic
#
# Outputs:
#   - Team roster (canonical IDs)
#   - Statcast pitch-level data (team-scoped)
#   - Schedule & opponent context
#   - Statcast + Schedule joined
#   - Lahman historical reference
#   - FanGraphs season context (player-level)
#   - Baseball-Reference daily box scores
#
# ------------------------------------------------------------
# Version History:
# v1.0 | 2026-01-21 | Initial multi-source pipeline skeleton
# v1.1 | 2026-01-27 | Updated script with commong date selection
# ============================================================

team_abbr <- "ATL"
season    <- 2025

test_start_date <- as.Date(paste0(season, "-04-01"))
test_end_date   <- as.Date(paste0(season, "-09-30"))

output_dir <- "Documents/Baseball Analytics/Scouting Report/outputs"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

options(stringsAsFactors = FALSE)
Sys.setenv(TZ = "UTC")
Sys.setenv("VROOM_CONNECTION_SIZE" = 131072 * 10)

# ------------------------------------------------------------
# Libraries
# ------------------------------------------------------------

pkgs <- c("tidyverse", "Lahman", "baseballr", "purrr", "stringi")
installed <- rownames(installed.packages())
to_install <- setdiff(pkgs, installed)
if (length(to_install) > 0) install.packages(to_install)

library(tidyverse)
library(Lahman)
library(baseballr)
library(purrr)
library(stringi)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

normalize_name <- function(x) {
  x %>%
    stringi::stri_trans_general("Latin-ASCII") %>%
    str_replace_all("\\.", "") %>%
    str_trim()
}

# ============================================================
# CONNECTION 1 — MLBAM TEAM
# ============================================================

mlbam_teams <- baseballr::mlb_teams()

team_row <- mlbam_teams %>%
  filter(sport_id == 1, team_abbreviation == team_abbr)

stopifnot(nrow(team_row) == 1)

team_id   <- team_row$team_id
team_name <- team_row$team_name

# ============================================================
# CONNECTION 2 — MLBAM ROSTER
# ============================================================

team_roster_raw <- baseballr::mlb_rosters(
  team_id     = team_id,
  season      = season,
  roster_type = "active"
)

stopifnot(nrow(team_roster_raw) > 0)

# ============================================================
# CONNECTION 3 — ULTIMATE PLAYER ID TABLE (SPINE)
# ============================================================

# Base: MLBAM humans
player_id_base <- team_roster_raw %>%
  transmute(
    mlbam_id    = person_id,
    player_name = person_full_name,
    position    = position_abbreviation
  ) %>%
  distinct() %>%
  mutate(name_norm = normalize_name(player_name))

# FanGraphs IDs (authoritative FG source)
fg_id_lookup <- baseballr::fg_batter_leaders(
  startseason = as.character(season),
  endseason   = as.character(season)
) %>%
  transmute(
    fangraphs_id = playerid,
    name_norm    = normalize_name(PlayerName)
  ) %>%
  distinct()

# Lahman + BBRef
lahman_ids <- Lahman::People %>%
  transmute(
    lahman_id = playerID,
    bbref_id  = bbrefID,
    name_norm = normalize_name(paste(nameFirst, nameLast))
  )

# Assemble spine
ultimate_player_ids <- player_id_base %>%
  left_join(fg_id_lookup, by = "name_norm") %>%
  left_join(lahman_ids,   by = "name_norm") %>%
  mutate(player_key = row_number()) %>%
  relocate(player_key)

stopifnot(nrow(ultimate_player_ids) > 0)

# ============================================================
# CONNECTION 4 — STATCAST (TEAM-SCOPED)
# ============================================================

safe_statcast <- purrr::possibly(
  baseballr::statcast_search,
  otherwise = tibble()
)

statcast_team <- safe_statcast(
  start_date = test_start_date,
  end_date   = test_end_date,
  team       = team_abbr
) %>%
  rename(game_id = game_pk)

# ============================================================
# CONNECTION 5 — SCHEDULE
# ============================================================

schedule_raw <- baseballr::mlb_schedule(season = season)

team_schedule <- schedule_raw %>%
  filter(
    teams_home_team_id == team_id |
      teams_away_team_id == team_id
  ) %>%
  transmute(
    game_id   = game_pk,
    game_date = as.Date(game_date),
    opponent_id = if_else(
      teams_home_team_id == team_id,
      teams_away_team_id,
      teams_home_team_id
    ),
    opponent_name = if_else(
      teams_home_team_id == team_id,
      teams_away_team_name,
      teams_home_team_name
    ),
    home_away = if_else(
      teams_home_team_id == team_id,
      "home", "away"
    ),
    venue_name
  ) %>%
  distinct(game_id, .keep_all = TRUE)

# Join Statcast ↔ Schedule
statcast_with_schedule <- statcast_team %>%
  left_join(team_schedule, by = "game_id")

# ============================================================
# CONNECTION 6 — LAHMAN CAREER CONTEXT
# ============================================================

lahman_batting <- Batting %>%
  group_by(playerID) %>%
  summarise(
    career_PA = sum(AB + BB + HBP + SF, na.rm = TRUE),
    career_HR = sum(HR, na.rm = TRUE),
    career_BB = sum(BB, na.rm = TRUE),
    career_SO = sum(SO, na.rm = TRUE),
    .groups = "drop"
  )

lahman_pitching <- Pitching %>%
  group_by(playerID) %>%
  summarise(
    career_IPouts = sum(IPouts, na.rm = TRUE),
    career_SO = sum(SO, na.rm = TRUE),
    career_BB = sum(BB, na.rm = TRUE),
    career_HR = sum(HR, na.rm = TRUE),
    .groups = "drop"
  )

lahman_career <- lahman_ids %>%
  left_join(lahman_batting,  by = c("lahman_id" = "playerID")) %>%
  left_join(lahman_pitching, by = c("lahman_id" = "playerID"))

# ============================================================
# CONNECTION 7 — FAN GRAPHS
# ============================================================

# Hitters (leaders)
fg_hitters <- baseballr::fg_batter_leaders(
  startseason = as.character(season),
  endseason   = as.character(season)
) %>%
  filter(team_name == team_abbr)


# ============================================================
# DERIVE PITCHER KEYS (AUTHORITATIVE)
# ============================================================

pitcher_keys <- ultimate_player_ids %>%
  filter(
    position == "P",
    !is.na(fangraphs_id)
  ) %>%
  transmute(
    player_key,
    mlbam_id,
    player_name,
    fg_id = as.integer(fangraphs_id)
  ) %>%
  distinct(fg_id, .keep_all = TRUE)

stopifnot(nrow(pitcher_keys) > 0)


# ============================================================
# FAN GRAPHS — PITCHER AGGREGATE (PHASE 3, FINAL)
# ============================================================

# Safe wrapper (FanGraphs endpoints can fail silently)
safe_fg_pgl <- purrr::possibly(
  baseballr::fg_pitcher_game_logs,
  otherwise = NULL
)

# ------------------------------------------------------------
# Pull FanGraphs pitcher game logs (one pitcher at a time)
# ------------------------------------------------------------

fg_pitcher_logs_list <- list()

for (i in seq_len(nrow(pitcher_keys))) {
  
  pid   <- as.character(pitcher_keys$fg_id[i])
  pname <- pitcher_keys$player_name[i]
  
  message(
    "Pulling FanGraphs pitcher logs for: ",
    pname, " (fg_id = ", pid, ", year = ", season, ")"
  )
  
  df <- safe_fg_pgl(
    playerid = pid,
    year     = season
  )
  
  if (is.null(df) || nrow(df) == 0) {
    message("  → No data returned")
    Sys.sleep(2)
    next
  }
  
  df$fg_id <- as.integer(pid)
  fg_pitcher_logs_list[[length(fg_pitcher_logs_list) + 1]] <- df
  
  # Slow down to avoid throttling
  Sys.sleep(2)
}

fg_pitcher_logs_raw <- dplyr::bind_rows(fg_pitcher_logs_list)

if (nrow(fg_pitcher_logs_raw) == 0) {
  warning("No FanGraphs pitcher game logs returned.")
}

# ------------------------------------------------------------
# Aggregate to one row per pitcher
# ------------------------------------------------------------

fg_pitcher_aggregate <- fg_pitcher_logs_raw %>%
  left_join(
    pitcher_keys,
    by = "fg_id"
  ) %>%
  group_by(
    player_key,
    player_name
  ) %>%
  summarise(
    games = n(),
    
    # Volume
    IP  = sum(IP, na.rm = TRUE),
    BF  = sum(TBF, na.rm = TRUE),
    
    # Outcomes
    H   = sum(H, na.rm = TRUE),
    HR  = sum(HR, na.rm = TRUE),
    BB  = sum(BB, na.rm = TRUE),
    SO  = sum(SO, na.rm = TRUE),
    
    # Run prevention / estimators
    ERA   = mean(ERA, na.rm = TRUE),
    FIP   = mean(FIP, na.rm = TRUE),
    xFIP  = mean(xFIP, na.rm = TRUE),
    SIERA = mean(SIERA, na.rm = TRUE),
    
    # Value
    #WAR = sum(WAR, na.rm = TRUE),
    
    .groups = "drop"
  ) %>%
  mutate(
    season      = season,
    team_abbr   = team_abbr,
    player_type = "P",
    source      = "FanGraphs"
  )

stopifnot(is.data.frame(fg_pitcher_aggregate))

message(
  "FanGraphs pitcher aggregate complete: ",
  nrow(fg_pitcher_aggregate),
  " pitchers."
)


# ============================================================
# CONNECTION 8 — BASEBALL REFERENCE (REFERENCE ONLY)
# ============================================================

bbref_hitters <- baseballr::bref_daily_batter(
  t1 = test_start_date,
  t2 = test_end_date
) %>%
  filter(Team == team_name)

bbref_pitchers <- baseballr::bref_daily_pitcher(
  t1 = test_start_date,
  t2 = test_end_date
) %>%
  filter(Team == team_name)

# ============================================================
# SAVE OUTPUTS
# ============================================================

saveRDS(ultimate_player_ids,        file.path(output_dir, "ultimate_player_ids.rds"))
saveRDS(statcast_with_schedule,     file.path(output_dir, "statcast_with_schedule.rds"))
saveRDS(lahman_career,              file.path(output_dir, "lahman_career.rds"))
saveRDS(fg_hitters,                 file.path(output_dir, "fg_hitters_raw.rds"))
saveRDS(bbref_hitters,              file.path(output_dir, "bbref_hitters.rds"))
saveRDS(bbref_pitchers,             file.path(output_dir, "bbref_pitchers.rds"))
saveRDS(fg_pitcher_aggregate,       file.path(output_dir, "fangraphs_pitcher_aggregate.rds"))
message("pipeline_build_v1 complete.")
