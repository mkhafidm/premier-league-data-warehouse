{{ config(
    materialized='table'
) }}

select
    position_id,
    singular_name,
    singular_name_short,
    plural_name,
    plural_name_short,
    squad_min_play,
    squad_max_play,
    loaded_at
from {{ ref('stg_static_positions') }}