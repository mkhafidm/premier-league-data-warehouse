{{ config(materialized='table') }}

with ws as (
    select *
    from {{ ref('fact_weekly_stats') }}
    -- where season = '2024/2025'
),

p as (
    select player_id, player_name, position_id
    from {{ ref('dim_player') }}
),

pos as (
    select position_id, singular_name_short
    from {{ ref('dim_position') }}
)

select
    ws.player_id,
    p.player_name,
    sum(ws.clean_sheets) as total_cleansheets,
    pos.singular_name_short as position
from ws
join p on ws.player_id = p.player_id
join pos on p.position_id = pos.position_id
where p.position_id = 1   -- ⚠️ hanya GK
group by ws.player_id, p.player_name, pos.singular_name_short
order by total_cleansheets desc