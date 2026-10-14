-- ============================================================
-- Athena exploration and data quality checks on the silver layer
-- Database: fpl_db (tables created by the Glue Crawler)
-- ============================================================

-- 1. Sense check: should return 20 Premier League teams
SELECT *
FROM teams
ORDER BY team_name;

-- 2. Data quality: one row per player per gameweek.
--    player_rows and distinct_players should be equal.
SELECT
    gameweek,
    COUNT(*) AS player_rows,
    COUNT(DISTINCT player_id) AS distinct_players
FROM player_gameweek_stats
GROUP BY gameweek
ORDER BY gameweek;

-- 3. Top 10 players by total points this season
--    Joins stats -> latest player snapshot -> teams
SELECT
    p.web_name,
    t.team_short_name,
    SUM(s.total_points) AS season_points,
    SUM(s.goals_scored) AS goals,
    SUM(s.assists) AS assists
FROM player_gameweek_stats s
JOIN player_snapshots p
    ON s.player_id = p.player_id
    AND p.ingest_date = (SELECT MAX(ingest_date) FROM player_snapshots)
JOIN teams t
    ON p.team_id = t.team_id
GROUP BY p.web_name, t.team_short_name
ORDER BY season_points DESC
LIMIT 10;