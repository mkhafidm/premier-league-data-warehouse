with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_static_gameweeks') }}
),

selected as (
    select
        id,
        name,
        deadline_time,
        deadline_time_epoch,
        extraction_timestamp
    from raw
),

cleaned as (
    select
        cast(id as int64) as gameweek_id,
        name as gameweek_name,
        cast(deadline_time as timestamp) as deadline_time,
        cast(deadline_time_epoch as int64) as deadline_time_epoch,
        cast(extraction_timestamp as timestamp) as loaded_at
    from selected
)

select * from cleaned
