{{ config(
    materialized='table'
) }}

select
    -- Keys
    player_id,
    player_code,
    team_id,
    position_id,

    -- Player Name Metadata
    player_name,
    first_name,
    second_name,

    -- Attributes
    player_region,
    birth_date,
    squad_number,
    photo,

    corners_indirect_fk_order,
    corners_indirect_fk_text,
    direct_fk_order,
    direct_fk_text,
    penalties_order,
    penalties_text,

    -- Metadata
    loaded_at

from {{ ref('stg_static_players') }}
