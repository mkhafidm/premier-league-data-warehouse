{{ config(
    materialized = 'table'
) }}

with ws as (
    select *
    from {{ ref('stg_fact_weekly_stats') }}
),

sp as (
    -- static player metadata (set pieces, team id, position)
    select
        player_id,
        team_id,
        position_id,
        corners_indirect_fk_order,
        corners_indirect_fk_text,
        direct_fk_order,
        direct_fk_text,
        penalties_order,
        penalties_text
    from {{ ref('stg_static_players') }}
)

select
    -- Keys
    cast(ws.gameweek_id as int64) as gameweek_id,
    cast(ws.player_id as int64) as player_id,

    -- Added FK from static
    sp.team_id,
    sp.position_id,

    -- Core Stats
    cast(ws.minutes as int64) as minutes,
    cast(ws.goals as int64) as goals,
    cast(ws.assists as int64) as assists,
    cast(ws.clean_sheets as int64) as clean_sheets,
    cast(ws.goals_conceded as int64) as goals_conceded,
    cast(ws.own_goals as int64) as own_goals,
    cast(ws.penalties_saved as int64) as penalties_saved,
    cast(ws.penalties_missed as int64) as penalties_missed,
    cast(ws.yellow_cards as int64) as yellow_cards,
    cast(ws.red_cards as int64) as red_cards,
    cast(ws.saves as int64) as saves,

    -- Bonus & BPS
    cast(ws.bonus as int64) as bonus,
    cast(ws.bps as int64) as bps,

    -- xStats
    cast(ws.expected_goals as float64) as xg,
    cast(ws.expected_assists as float64) as xa,
    cast(ws.expected_goal_involvements as float64) as xgi,
    cast(ws.expected_goals_conceded as float64) as xgc,

    -- Advanced Player Metrics
    cast(ws.influence_score as float64) as influence_score,
    cast(ws.creativity_score as float64) as creativity_score,
    cast(ws.threat_score as float64) as threat_score,

    -- Defensive Metrics
    cast(ws.clearances_blocks_interceptions as int64) as clearances_blocks_interceptions,
    cast(ws.recoveries as int64) as recoveries,
    cast(ws.tackles as int64) as tackles,
    cast(ws.defensive_contribution as int64) as defensive_contribution,

    -- Other Metrics
    cast(ws.total_points as int64) as total_points,
    cast(ws.in_dreamteam as bool) as in_dreamteam,

    -- 🎯 Set Piece Metadata (from static)
    cast(sp.corners_indirect_fk_order as int64) as corners_indirect_fk_order,
    cast(sp.corners_indirect_fk_text as string) as corners_indirect_fk_text,
    cast(sp.direct_fk_order as int64) as direct_fk_order,
    cast(sp.direct_fk_text as string) as direct_fk_text,
    cast(sp.penalties_order as int64) as penalties_order,
    cast(sp.penalties_text as string) as penalties_text,

    -- Metadata
    ws.loaded_at

from ws
left join sp
    on ws.player_id = sp.player_id