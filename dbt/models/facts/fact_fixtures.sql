{{ config(
    materialized='table'
) }}

-- Ambil staging
with fixture as (
    select *
    from {{ ref('stg_fact_fixtures') }}
)

select
    -- Primary Key
    fixture_id,

    -- Foreign Keys
    gameweek_id,
    home_team_id,
    away_team_id,

    -- Match Info
    kickoff_ts,
    home_score,
    away_score,

    -- Difficulty Metrics
    home_difficulty,
    away_difficulty,

    -- Match Status
    is_finished,

    -- Metadata
    loaded_at

from fixture
