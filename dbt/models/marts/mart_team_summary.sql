{{ config(materialized='table') }}

-- Ambil fixtures yang sudah selesai
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

-- Statistik HOME TEAM
home_stats as (
    select
        home_team_id as team_id,
        1 as played,
        case when home_score > away_score then 1 else 0 end as wins,
        case when home_score = away_score then 1 else 0 end as draws,
        case when home_score < away_score then 1 else 0 end as losses,
        home_score as goals_for,
        away_score as goals_against
    from fx
),

-- Statistik AWAY TEAM
away_stats as (
    select
        away_team_id as team_id,
        1 as played,
        case when away_score > home_score then 1 else 0 end as wins,
        case when away_score = home_score then 1 else 0 end as draws,
        case when away_score < home_score then 1 else 0 end as losses,
        away_score as goals_for,
        home_score as goals_against
    from fx
),

-- HOME + AWAY union
unioned as (
    select * from home_stats
    union all
    select * from away_stats
),

-- Agregasi per tim
aggregated as (
    select
        team_id,
        sum(played) as played,
        sum(wins)   as wins,
        sum(draws)  as draws,
        sum(losses) as losses,
        sum(goals_for)       as goals_for,
        sum(goals_against)   as goals_against,
        sum(goals_for) - sum(goals_against) as goal_difference,
        (sum(wins) * 3 + sum(draws)) as points
    from unioned
    group by team_id
)

select
    a.team_id,
    d.team_name,
    a.played,
    a.wins,
    a.draws,
    a.losses,
    a.goals_for,
    a.goals_against,
    a.goal_difference,
    a.points
from aggregated a
join {{ ref('dim_team') }} d
    on a.team_id = d.team_id
order by points desc, goal_difference desc, goals_for desc
