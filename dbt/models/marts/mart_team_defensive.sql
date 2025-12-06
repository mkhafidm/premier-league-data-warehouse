{{ config(materialized='table') }}

with ws as (
    select
        w.player_id,
        p.team_id,
        w.goals_conceded,
        w.clean_sheets,
        w.xgc,
        w.tackles,
        w.clearances_blocks_interceptions,
        w.recoveries,
        w.defensive_contribution,
        w.saves
    from {{ ref('fact_weekly_stats') }} w
    join {{ ref('dim_player') }} p
        on w.player_id = p.player_id
),

agg as (
    select
        team_id,
        sum(goals_conceded) as total_goals_conceded,
        sum(clean_sheets)   as total_clean_sheets,
        sum(xgc)            as total_xgc,

        -- defensive actions
        sum(tackles + clearances_blocks_interceptions + recoveries)
            as defensive_actions,

        sum(defensive_contribution) as defensive_contribution_score,

        -- saves (GK only)
        sum(saves) as total_saves,

        safe_divide(sum(xgc), sum(goals_conceded)) as defensive_efficiency,
        (sum(xgc) - sum(goals_conceded)) as underperformance_def
    from ws
    group by team_id
)

select
    a.team_id,
    t.team_name,
    a.total_goals_conceded,
    a.total_clean_sheets,
    a.total_xgc,
    a.defensive_actions,
    a.total_saves,
    a.defensive_efficiency,
    a.underperformance_def,
    a.defensive_contribution_score
from agg a
join {{ ref('dim_team') }} t
    on a.team_id = t.team_id
order by total_goals_conceded asc
