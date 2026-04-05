with stg_fights_v as (
    select * from {{ ref('stg_fights_v') }}
),

parsed_fights as (
    select 
        {{ generate_fight_id('fight_url') }} as fight_id,
        {{ generate_event_id('event_url') }} as event_id,
        {{ generate_fighter_id('fighter_1_url') }} as fighter_1_id,
        {{ generate_fighter_id('fighter_2_url') }} as fighter_2_id,
        {{ generate_fighter_id('winner_url') }} as winner_id,

        regexp_substr(kd, '([0-9]+)-([0-9]+)', 1, 1, 'e',1)::int as kd_1,
        regexp_substr(kd, '([0-9]+)-([0-9]+)', 1, 1, 'e',2)::int as kd_2,

        regexp_substr(strikes, '([0-9]+)-([0-9]+)', 1, 1, 'e',1)::int as str_1,
        regexp_substr(strikes, '([0-9]+)-([0-9]+)', 1, 1, 'e',2)::int as str_2,

        regexp_substr(td, '([0-9]+)-([0-9]+)', 1, 1, 'e',1)::int as td_1,
        regexp_substr(td, '([0-9]+)-([0-9]+)', 1, 1, 'e',2)::int as td_2,

        regexp_substr(sub, '([0-9]+)-([0-9]+)', 1, 1, 'e',1)::int as sub_1,
        regexp_substr(sub, '([0-9]+)-([0-9]+)', 1, 1, 'e',2)::int as sub_2,

        lower(match_outcome) as match_outcome,
        trim(weight_class) as weight_class,
        trim(method,'-') as won_by,
        round_num::int as round_num,
        fight_time,
        extracted_at
    from stg_fights_v
),

fighter_1_perspective as (
    select 
        fight_id,
        event_id,
        fighter_1_id as fighter_id,
        fighter_2_id as opponent_id,
        case
            when match_outcome ='win' and fighter_1_id = winner_id then 1
            when match_outcome != 'win' then 0
            else 0
        end as is_winner,

        kd_1 as knockdowns,
        str_1 as strikes_landed,
        td_1 as takedowns,
        sub_1 as submission_attempts,
        
        weight_class,
        won_by,
        round_num,
        fight_time,
        extracted_at
    from parsed_fights
),

fighter_2_perspective as (
    select 
        fight_id,
        event_id,
        fighter_2_id as fighter_id,
        fighter_1_id as opponent_id,
        case
            when match_outcome ='win' and fighter_2_id = winner_id then 1
            when match_outcome != 'win' then 0
            else 0
        end as is_winner,

        kd_2 as knockdowns,
        str_2 as strikes_landed,
        td_2 as takedowns,
        sub_2 as submission_attempts,
        
        weight_class,
        won_by,
        round_num,
        fight_time,
        extracted_at
    from parsed_fights
),

combined_perspective as (
select * from fighter_1_perspective
union all
select * from fighter_2_perspective
)

-- deduplicate logic
select * from combined_perspective
qualify row_number() over (
    partition by fight_id, fighter_id
    order by extracted_at desc
) = 1
