{{ config(materialized='table') }}

with ws as (
    select
        player_id,
        sum(goals) as total_goals
    from {{ ref('fact_weekly_stats') }}
    group by player_id
),

joined as (
    select
        w.player_id,
        p.player_name,
        t.team_name,
        w.total_goals
    from ws w
    join {{ ref('dim_player') }} p
        on w.player_id = p.player_id
    join {{ ref('dim_team') }} t
        on p.team_id = t.team_id
)

select *
from joined
order by total_goals desc
