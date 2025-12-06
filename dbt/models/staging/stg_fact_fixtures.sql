with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_fixtures') }}
),

cleaned as (
    select
        -- Primary Key
        cast(id as int64) as fixture_id,

        -- Foreign Keys
        cast(event as int64) as gameweek_id,
        cast(team_h as int64) as home_team_id,
        cast(team_a as int64) as away_team_id,

        -- Match Information
        cast(kickoff_time as timestamp) as kickoff_ts,
        cast(team_h_score as int64) as home_score,
        cast(team_a_score as int64) as away_score,
        cast(team_h_difficulty as int64) as home_difficulty,
        cast(team_a_difficulty as int64) as away_difficulty,

        -- Match Status
        cast(finished as bool) as is_finished,

        -- Metadata
        cast(extraction_timestamp as timestamp) as loaded_at

    from raw
)

select * from cleaned
