with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_static_positions') }}
),

selected as (
    select
        id,
        singular_name,
        singular_name_short,
        plural_name,
        plural_name_short,
        squad_min_play,
        squad_max_play,
        extraction_timestamp
    from raw
),

cleaned as (
    select
        cast(id as int64) as position_id,
        singular_name,
        singular_name_short,
        plural_name,
        plural_name_short,
        cast(squad_min_play as int64) as squad_min_play,
        cast(squad_max_play as int64) as squad_max_play,
        cast(extraction_timestamp as timestamp) as loaded_at
    from selected
)

select * from cleaned
