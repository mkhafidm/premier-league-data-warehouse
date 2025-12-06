{{ config(materialized='table') }}

with ws as (
    select
        w.gameweek_id,
        w.player_id,
        w.minutes,
        w.goals,
        w.assists,
        w.clean_sheets,
        w.goals_conceded,
        w.saves,
        w.bonus,
        w.bps,
        w.total_points,
        w.xg,
        w.xa,
        w.xgi,
        w.xgc,
        w.penalties_missed,
        w.penalties_saved
    from {{ ref('fact_weekly_stats') }} w
),

joined as (
    select
        ws.*,
        p.player_name,
        p.position_id,
        pos.singular_name_short as position,
        t.team_name
    from ws
    join {{ ref('dim_player') }} p
        on ws.player_id = p.player_id
    join {{ ref('dim_team') }} t
        on p.team_id = t.team_id
    join {{ ref('dim_position') }} pos
        on p.position_id = pos.position_id
)

select *
from joined
order by gameweek_id, total_points desc