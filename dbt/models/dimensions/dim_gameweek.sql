{{ config(
    materialized='table'
) }}

select
    gameweek_id,
    gameweek_name,
    deadline_time,
    deadline_time_epoch,
    loaded_at
from {{ ref('stg_static_gameweeks') }}