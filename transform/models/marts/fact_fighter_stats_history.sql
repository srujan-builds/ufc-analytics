with fct_fights as (
    select * from {{ ref('fact_fights') }}
),

dim_events as (
    select * from {{ ref('dim_events_master') }}
)

select 
    e.event_id,
    f.fight_id,
    f.championship_bout,
    e.event_date,
    f.fighter_id,
    f.opponent_id,
    f.match_outcome,
    f.is_winner,

    sum(case when (f.match_outcome='win' and f.is_winner=true) then 1 else 0 end) over(
        partition by f.fighter_id
        order by e.event_date, f.fight_id
        rows between unbounded preceding and current row
    ) as win,

    sum(case when (f.match_outcome='win' and f.is_winner=false) then 1 else 0 end) over(
        partition by f.fighter_id
        order by e.event_date, f.fight_id
        rows between unbounded preceding and current row
    ) as lose,

    sum(case when (f.match_outcome='draw' and f.is_winner=false) then 1 else 0 end) over(
        partition by f.fighter_id
        order by e.event_date, f.fight_id
        rows between unbounded preceding and current row
    ) as draw,
    sum(case when (f.match_outcome='nc' and f.is_winner=false) then 1 else 0 end) over(
        partition by f.fighter_id
        order by e.event_date, f.fight_id
        rows between unbounded preceding and current row
    ) as no_contest
from fct_fights as f
inner join dim_events as e 
on f.event_id = e.event_id
    