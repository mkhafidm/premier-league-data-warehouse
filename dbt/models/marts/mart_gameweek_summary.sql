{{ config(materialized='table') }}

-- Ambil statistik per pemain per GW
with ws as (
    select
        gameweek_id,
        goals,
        assists,
        clean_sheets,
        penalties_missed,
        penalties_saved,
        yellow_cards,
        red_cards
    from {{ ref('fact_weekly_stats') }}
),

-- Agregasi per gameweek
agg_stats as (
    select
        gameweek_id,

        -- Offensive
        sum(goals) as total_goals,
        sum(assists) as total_assists,

        -- Defensive
        sum(clean_sheets) as total_clean_sheets,

        -- Discipline
        sum(yellow_cards) as total_yellow_cards,
        sum(red_cards) as total_red_cards,

        -- Penalties
        sum(penalties_saved + penalties_missed) as total_penalties_taken,
        sum(penalties_saved) as penalties_saved,
        sum((penalties_saved + penalties_missed) - penalties_missed) as penalties_scored,

        count(*) as total_player_records
    from ws
    group by gameweek_id
),

-- Ambil hasil pertandingan per GW
fx as (
    select
        gameweek_id,
        home_team_id,
        away_team_id,
        home_score,
        away_score,
        abs(home_score - away_score) as goal_margin
    from {{ ref('fact_fixtures') }}
    where is_finished = true
),

-- cari biggest win
biggest_win as (
    select
        gameweek_id,
        home_team_id,
        away_team_id,
        home_score,
        away_score,
        goal_margin,
        row_number() over (partition by gameweek_id order by goal_margin desc) as rn
    from fx
)

select
    a.gameweek_id,
    a.total_goals,
    a.total_assists,
    a.total_clean_sheets,
    a.total_yellow_cards,
    a.total_red_cards,

    a.total_penalties_taken,
    a.penalties_scored,
    a.penalties_saved,

    -- biggest win
    bw.home_team_id,
    bw.away_team_id,
    bw.home_score,
    bw.away_score,
    bw.goal_margin

from agg_stats a
left join biggest_win bw
    on a.gameweek_id = bw.gameweek_id
   and bw.rn = 1
order by gameweek_id
