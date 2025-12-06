{{ config(materialized='table') }}

with fx as (
    select
        fixture_id,
        home_team_id,
        away_team_id,
        home_score,
        away_score,
        is_finished
    from {{ ref('fact_fixtures') }}
    where is_finished = true
),

-- HOME performance
home_side as (
    select
        home_team_id as team_id,
        1 as played_home,
        case when home_score > away_score then 1 else 0 end as wins_home,
        case when home_score = away_score then 1 else 0 end as draws_home,
        case when home_score < away_score then 1 else 0 end as losses_home,
        home_score as goals_for_home,
        away_score as goals_against_home
    from fx
),

-- AWAY performance
away_side as (
    select
        away_team_id as team_id,
        1 as played_away,
        case when away_score > home_score then 1 else 0 end as wins_away,
        case when away_score = home_score then 1 else 0 end as draws_away,
        case when away_score < home_score then 1 else 0 end as losses_away,
        away_score as goals_for_away,
        home_score as goals_against_away
    from fx
),

combined as (
    select
        coalesce(h.team_id, a.team_id) as team_id,

        -- HOME
        coalesce(h.played_home, 0) as played_home,
        coalesce(h.wins_home, 0) as wins_home,
        coalesce(h.draws_home, 0) as draws_home,
        coalesce(h.losses_home, 0) as losses_home,
        coalesce(h.goals_for_home, 0) as goals_for_home,
        coalesce(h.goals_against_home, 0) as goals_against_home,

        -- AWAY
        coalesce(a.played_away, 0) as played_away,
        coalesce(a.wins_away, 0) as wins_away,
        coalesce(a.draws_away, 0) as draws_away,
        coalesce(a.losses_away, 0) as losses_away,
        coalesce(a.goals_for_away, 0) as goals_for_away,
        coalesce(a.goals_against_away, 0) as goals_against_away
    from home_side h
    full outer join away_side a
        on h.team_id = a.team_id
)

select
    c.team_id,
    t.team_name,

    -- HOME
    c.played_home,
    c.wins_home,
    c.draws_home,
    c.losses_home,
    c.goals_for_home,
    c.goals_against_home,

    -- AWAY
    c.played_away,
    c.wins_away,
    c.draws_away,
    c.losses_away,
    c.goals_for_away,
    c.goals_against_away

from combined c
join {{ ref('dim_team') }} t
    on c.team_id = t.team_id
order by t.team_name