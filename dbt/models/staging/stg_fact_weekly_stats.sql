with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_weekly_stats') }}
),

cleaned as (
    select
        -- Keys
        cast(gameweek_id as int64) as gameweek_id,
        cast(player_id as int64) as player_id,

        -- Core Stats
        cast(minutes as int64) as minutes,
        cast(goals_scored as int64) as goals,
        cast(assists as int64) as assists,
        cast(clean_sheets as int64) as clean_sheets,
        cast(goals_conceded as int64) as goals_conceded,
        cast(own_goals as int64) as own_goals,
        cast(penalties_saved as int64) as penalties_saved,
        cast(penalties_missed as int64) as penalties_missed,
        cast(yellow_cards as int64) as yellow_cards,
        cast(red_cards as int64) as red_cards,
        cast(saves as int64) as saves,
        cast(bonus as int64) as bonus,
        cast(bps as int64) as bps,
        cast(total_points as int64) as total_points,

        -- Advanced Metrics
        cast(influence as float64) as influence_score,
        cast(creativity as float64) as creativity_score,
        cast(threat as float64) as threat_score,
        cast(ict_index as float64) as ict_score,
        cast(clearances_blocks_interceptions as int64) as clearances_blocks_interceptions,
        cast(recoveries as int64) as recoveries,
        cast(tackles as int64) as tackles,
        cast(defensive_contribution as int64) as defensive_contribution,

        -- Expected Metrics
        cast(expected_goals as float64) as expected_goals,
        cast(expected_assists as float64) as expected_assists,
        cast(expected_goal_involvements as float64) as expected_goal_involvements,
        cast(expected_goals_conceded as float64) as expected_goals_conceded,

        -- Flags
        cast(in_dreamteam as bool) as in_dreamteam,

        -- Metadata
        cast(extraction_timestamp as timestamp) as loaded_at

    from raw
)

select * from cleaned
