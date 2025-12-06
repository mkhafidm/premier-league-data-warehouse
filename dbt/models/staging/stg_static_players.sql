with raw as (
    select *
    from {{ source('fpl_raw_data', 'raw_static_players') }}
),

selected as (
    select
        id,
        code,
        first_name,
        second_name,
        web_name,
        element_type,
        region,
        birth_date,
        team,
        team_code,
        team_join_date,
        squad_number,
        photo,
        corners_and_indirect_freekicks_order,
        corners_and_indirect_freekicks_text,
        direct_freekicks_order,
        direct_freekicks_text,
        penalties_order,
        penalties_text,
        extraction_timestamp
    from raw
),

cleaned as (
    select
        -- Primary Key
        cast(id as int64) as player_id,
        cast(code as int64) as player_code,

        -- Basic Info
        first_name,
        second_name,
        web_name as player_name,

        -- Foreign Keys
        cast(element_type as int64) as position_id,
        cast(team as int64) as team_id,
        cast(team_code as int64) as team_code,

        -- Player Metadata
        region as player_region,
        cast(birth_date as date) as birth_date,
        cast(team_join_date as timestamp) as team_join_date,
        squad_number,
        photo,

        -- Set Piece
        cast(corners_and_indirect_freekicks_order as int64) as corners_indirect_fk_order,
        cast(corners_and_indirect_freekicks_text as string) as corners_indirect_fk_text,
        cast(direct_freekicks_order as int64) as direct_fk_order,
        cast(direct_freekicks_text as string) as direct_fk_text,
        cast(penalties_order as int64) as penalties_order,
        cast(penalties_text as string) as penalties_text,


        -- Pipeline Metadata
        cast(extraction_timestamp as timestamp) as loaded_at
    from selected
)

select * from cleaned