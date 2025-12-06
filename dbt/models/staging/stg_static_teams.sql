with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_static_teams') }}
),

selected as (
    select
        id,
        name,
        short_name,
        strength,
        strength_overall_home,
        strength_overall_away,
        strength_attack_home,
        strength_attack_away,
        strength_defence_home,
        strength_defence_away,
        code,
        pulse_id,
        extraction_timestamp
    from raw
),

cleaned as (
    select
        -- Primary Key
        cast(id as int64) as team_id,

        -- Team Names
        name as team_name,
        short_name as team_short_name,

        -- Strength Metrics
        cast(strength as int64) as strength,
        cast(strength_overall_home as int64) as strength_overall_home,
        cast(strength_overall_away as int64) as strength_overall_away,
        cast(strength_attack_home as int64) as strength_attack_home,
        cast(strength_attack_away as int64) as strength_attack_away,
        cast(strength_defence_home as int64) as strength_defence_home,
        cast(strength_defence_away as int64) as strength_defence_away,

        -- Codes
        cast(code as int64) as team_code,
        cast(pulse_id as int64) as pulse_id,

        -- Metadata
        cast(extraction_timestamp as timestamp) as loaded_at
    from selected
)

select * from cleaned
