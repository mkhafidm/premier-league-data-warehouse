{{ config(materialized='table') }}

with fx as (
    select
        fixture_id,
        gameweek_id,
        home_team_id,
        away_team_id,
        kickoff_ts,
        is_finished
    from {{ ref('fact_fixtures') }}
    where is_finished = false  -- 🔥 hanya fixture yang belum dimainkan
),

-- Ubah menjadi perspektif per tim
expanded as (
    -- home perspective
    select
        home_team_id       as team_id,
        away_team_id       as opponent_team_id,
        gameweek_id,
        kickoff_ts,
        'HOME'             as venue
    from fx

    union all

    -- away perspective
    select
        away_team_id       as team_id,
        home_team_id       as opponent_team_id,
        gameweek_id,
        kickoff_ts,
        'AWAY'             as venue
    from fx
),

-- Rank 3 pertandingan terdekat
ranked as (
    select
        e.*,
        row_number() over (
            partition by team_id
            order by kickoff_ts asc
        ) as rn
    from expanded e
)

select
    r.team_id,
    t.team_name,

    r.opponent_team_id,
    opp.team_name as opponent_name,

    r.gameweek_id,
    r.kickoff_ts,
    r.venue
from ranked r
join {{ ref('dim_team') }} t
    on r.team_id = t.team_id
join {{ ref('dim_team') }} opp
    on r.opponent_team_id = opp.team_id
where rn <= 3
order by r.team_id, r.kickoff_ts
