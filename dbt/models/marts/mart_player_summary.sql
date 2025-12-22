{{ config(materialized='table') }}

-- Ambil seluruh weekly stats
with ws as (
    select
        w.player_id,
        w.gameweek_id,
        w.minutes,
        w.goals,
        w.assists,
        w.clean_sheets,
        w.goals_conceded,
        w.saves,
        w.bonus,
        w.bps,
        w.total_points,

        -- expected metrics
        w.xg,
        w.xa,
        w.xgi,
        w.xgc

    from {{ ref('fact_weekly_stats') }} w
),

-- Agregasi per pemain
agg as (
    select
        player_id,

        -- basic totals
        sum(minutes)              as total_minutes,
        sum(goals)                as total_goals,
        sum(assists)              as total_assists,
        sum(clean_sheets)         as total_clean_sheets,
        sum(goals_conceded)       as total_goals_conceded,
        sum(saves)                as total_saves,
        sum(bonus)                as total_bonus,
        sum(bps)                  as total_bps,
        sum(total_points)         as total_points,

        -- expected stats
        sum(xg)                   as total_xg,
        sum(xa)                   as total_xa,
        sum(xgi)                  as total_xgi,
        sum(xgc)                  as total_xgc

    from ws
    group by player_id
),

-- Join metadata pemain + posisi + team + set pieces
joined as (
    select
        a.player_id,
        p.player_name,
        p.position_id,
        pos.singular_name_short as position,
        t.team_id,
        t.team_name,

        -- totals
        a.total_minutes,
        a.total_goals,
        a.total_assists,
        a.total_clean_sheets,
        a.total_goals_conceded,
        a.total_saves,
        a.total_bonus,
        a.total_bps,
        a.total_points,

        -- expected
        a.total_xg,
        a.total_xa,
        a.total_xgi,
        a.total_xgc,

        -- over/under-performance
        (a.total_goals - a.total_xg) as finishing_over_under,
        (a.total_assists - a.total_xa) as assist_over_under,

        -- set piece (from static dim_player)
        p.penalties_order,
        p.direct_fk_order,
        p.corners_indirect_fk_order,

        -- per 90
        safe_divide(a.total_goals, a.total_minutes) * 90 as goals_per_90,
        safe_divide(a.total_assists, a.total_minutes) * 90 as assists_per_90,
        safe_divide(a.total_xg, a.total_minutes) * 90 as xg_per_90,
        safe_divide(a.total_xa, a.total_minutes) * 90 as xa_per_90,
        safe_divide(a.total_xgi, a.total_minutes) * 90 as xgi_per_90,
        safe_divide(a.total_saves, a.total_minutes) * 90 as saves_per_90

    from agg a
    join {{ ref('dim_player') }} p
        on a.player_id = p.player_id
    join {{ ref('dim_team') }} t
        on p.team_id = t.team_id
    join {{ ref('dim_position') }} pos
        on p.position_id = pos.position_id
)

select *
from joined
order by total_points desc