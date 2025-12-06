{{ config(
    materialized='table'
) }}

select
    team_id,
    team_name,
    team_short_name,
    strength,
    strength_overall_home,
    strength_overall_away,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    team_code,
    pulse_id,
    loaded_at
from {{ ref('stg_static_teams') }}
