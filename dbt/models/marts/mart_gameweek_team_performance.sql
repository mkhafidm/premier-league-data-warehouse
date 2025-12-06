{{ config(materialized='table') }}

-- Ambil fixtures selesai
with fx as (
    select
        fixture_id,
        gameweek_id,
        home_team_id,
        away_team_id,
        home_score,
        away_score
    from {{ ref('fact_fixtures') }}
    where is_finished = true
),

-- HOME perspective
home_perf as (
    select
        gameweek_id,
        home_team_id as team_id,
        home_score as goals_for,
        away_score as goals_against,
        case
            when home_score > away_score then 3
            when home_score = away_score then 1
            else 0
        end as points
    from fx
),

-- AWAY perspective
away_perf as (
    select
        gameweek_id,
        away_team_id as team_id,
        away_score as goals_for,
        home_score as goals_against,
        case
            when away_score > home_score then 3
            when away_score = home_score then 1
            else 0
        end as points
    from fx
),

unioned as (
    select * from home_perf
    union all
    select * from away_perf
)

select
    u.team_id,
    t.team_name,
    u.gameweek_id,
    sum(u.goals_for) as goals_for,
    sum(u.goals_against) as goals_against,
    sum(points) as points
from unioned u
join {{ ref('dim_team') }} t on u.team_id = t.team_id
group by team_id, team_name, gameweek_id
order by team_id, gameweek_id
