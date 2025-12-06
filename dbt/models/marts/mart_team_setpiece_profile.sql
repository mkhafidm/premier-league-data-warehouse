{{ config(materialized='table') }}

with ws as (
    select
        w.player_id,
        p.team_id,

        -- penalties
        w.penalties_missed,
        w.penalties_saved,

        -- set-piece orders (correct column names)
        w.direct_fk_order,
        w.corners_indirect_fk_order

    from {{ ref('fact_weekly_stats') }} w
    join {{ ref('dim_player') }} p
        on w.player_id = p.player_id
),

agg as (
    select
        team_id,

        -- Penalty stats
        sum(penalties_missed + penalties_saved) as penalties_taken,
        sum(penalties_saved)                   as penalties_saved,
        sum((penalties_missed + penalties_saved) - penalties_missed) as penalties_scored,
        sum(penalties_missed)                  as penalties_missed,

        -- Set-piece involvement
        countif(direct_fk_order             > 0) as direct_fk_involvements,
        countif(corners_indirect_fk_order   > 0) as corner_fk_involvements

    from ws
    group by team_id
)

select
    a.team_id,
    t.team_name,

    -- Penalty stats
    a.penalties_taken,
    a.penalties_scored,
    a.penalties_missed,
    a.penalties_saved,
    safe_divide(a.penalties_scored, a.penalties_taken) as penalty_conversion,

    -- Set-piece involvement
    a.direct_fk_involvements,
    a.corner_fk_involvements

from agg a
join {{ ref('dim_team') }} t
    on a.team_id = t.team_id
order by a.penalties_taken desc
