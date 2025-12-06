{{ config(materialized='table') }}

with ws as (
    select
        w.player_id,
        p.team_id,
        w.goals,
        w.assists,
        w.xg,
        w.xa,
        w.xgi,
        w.influence_score,
        w.creativity_score,
        w.threat_score
    from {{ ref('fact_weekly_stats') }} w
    join {{ ref('dim_player') }} p
        on w.player_id = p.player_id
),

agg as (
    select
        team_id,
        sum(goals)  as total_goals,
        sum(assists) as total_assists,
        sum(xg)      as total_xg,
        sum(xa)      as total_xa,
        sum(xgi)     as total_xgi,

        -- offensive activity proxies
        sum(influence_score)  as total_influence,
        sum(creativity_score) as total_creativity,
        sum(threat_score)     as total_threat,

        -- finishing efficiency
        safe_divide(sum(goals), sum(xg)) as finishing_efficiency,
        (sum(goals) - sum(xg)) as goals_minus_xg
    from ws
    group by team_id
)

select
    a.team_id,
    t.team_name,

    -- core stats
    a.total_goals,
    a.total_assists,
    a.total_xg,
    a.total_xa,
    a.total_xgi,

    -- offensive activity
    a.total_influence,
    a.total_creativity,
    a.total_threat,

    -- finishing metrics
    a.finishing_efficiency,
    a.goals_minus_xg

from agg a
join {{ ref('dim_team') }} t
    on a.team_id = t.team_id
order by total_xg desc
