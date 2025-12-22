-- {{ config(materialized='table') }}

-- with ws as (
--     select
--         w.player_id,
--         w.gameweek_id,
--         w.goals,
--         w.assists,
--         w.saves,
--         w.clean_sheets,

--         -- from dim_player
--         p.team_id,
--         p.position_id
--     from {{ ref('fact_weekly_stats') }} w
--     join {{ ref('dim_player') }} p
--         on w.player_id = p.player_id
-- ),

-- agg as (
--     select
--         team_id,
--         player_id,
--         sum(goals) as total_goals,
--         sum(assists) as total_assists,
--         sum(saves) as total_saves,
--         sum(clean_sheets) as total_cleansheets
--     from ws
--     group by team_id, player_id
-- ),

-- ranked as (
--     select
--         a.team_id,
--         t.team_name,

--         a.player_id,
--         pl.player_name,
--         pos.singular_name as position,  -- ✅ FIXED

--         a.total_goals,
--         a.total_assists,
--         a.total_saves,
--         a.total_cleansheets,

--         row_number() over (partition by a.team_id order by total_goals desc)         as rank_goals,
--         row_number() over (partition by a.team_id order by total_assists desc)       as rank_assists,
--         row_number() over (partition by a.team_id order by total_saves desc)         as rank_saves,
--         row_number() over (partition by a.team_id order by total_cleansheets desc)   as rank_cs

--     from agg a

--     join {{ ref('dim_player') }} pl
--         on a.player_id = pl.player_id

--     join {{ ref('dim_position') }} pos
--         on pl.position_id = pos.position_id      -- 🔥 FIXED SOURCE FOR POSITION NAME

--     join {{ ref('dim_team') }} t
--         on a.team_id = t.team_id
-- )

-- select
--     team_id,
--     team_name,
--     player_id,
--     player_name,
--     position,
--     total_goals,
--     total_assists,
--     total_saves,
--     total_cleansheets,
--     rank_goals,
--     rank_assists,
--     rank_saves,
--     rank_cs
-- from ranked
-- where rank_goals = 1
--    or rank_assists = 1
--    or rank_saves = 1
--    or rank_cs = 1
-- order by team_name


{{ config(materialized='table') }}

with ws as (
    select
        w.player_id,
        w.team_id,
        w.goals,
        w.assists,
        w.saves,
        w.clean_sheets
    from {{ ref('fact_weekly_stats') }} w
),

agg as (
    select
        team_id,
        player_id,

        sum(goals)         as total_goals,
        sum(assists)       as total_assists,
        sum(saves)         as total_saves,
        sum(clean_sheets)  as total_cleansheets,

        -- 🔥 weighted contribution score
        (
            sum(goals) * 4.0 +
            sum(assists) * 3.0 +
            sum(clean_sheets) * 2.0 +
            sum(saves) * 0.1
        ) as contribution_score

    from ws
    group by team_id, player_id
),

ranked as (
    select
        a.team_id,
        t.team_name,

        a.player_id,
        p.player_name,
        pos.singular_name as position,

        a.total_goals,
        a.total_assists,
        a.total_saves,
        a.total_cleansheets,
        a.contribution_score,

        row_number() over (
            partition by a.team_id
            order by a.contribution_score desc
        ) as rank_key_player

    from agg a
    join {{ ref('dim_player') }} p
        on a.player_id = p.player_id
    join {{ ref('dim_position') }} pos
        on p.position_id = pos.position_id
    join {{ ref('dim_team') }} t
        on a.team_id = t.team_id
)

select
    team_id,
    team_name,
    player_id,
    player_name,
    position,
    total_goals,
    total_assists,
    total_saves,
    total_cleansheets,
    contribution_score
from ranked
where rank_key_player <= 5
order by team_name
