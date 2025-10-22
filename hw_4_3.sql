-- Созданы 2 таблицы и MV
drop table if exists user_events;
CREATE TABLE user_events (
    user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
;

drop table if exists user_events_agg;
CREATE TABLE user_events_agg (
    event_type String,
    event_date Date,
    points_spent_sum AggregateFunction(sum, UInt32),
	unique_users_count AggregateFunction(uniq, UInt32),
	event_count AggregateFunction(count, UInt32)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date)
TTL event_date + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;


drop view if exists user_events_mv;
create materialized view user_events_mv
to user_events_agg
as
select event_type,
event_time as event_date,
sumState(points_spent) as points_spent_sum,
uniqState(user_id) as unique_users_count,
countState(user_id) as event_count
from user_events
group by event_type, event_time;

-- Присутствуют 3 state функции, 3 merge функции и сделан расчет Retention

with events as (
	select user_id, toDate(event_time) as event_time from user_events
),
current_users as (
	select event_time, count(distinct user_id) current_amount
	from events
	group by event_time
)
, returned_users as (
select event_time, count(distinct user_id) returned_amount
from events e
where exists (
		select null
		from events r
		where r.user_id = e.user_id
		and r.event_time
		between e.event_time+interval 1 day and e.event_time+interval 7 day
	)
group by event_time
)
select c.event_time as event_date,
c.current_amount as total_users_day_0,
r.returned_amount as returned_in_7_days,
r.returned_amount * 100 / c.current_amount  as retention_7d_percent
from current_users c
left join returned_users r on c.event_time = r.event_time
order by c.event_time ;


--- Создан запрос на быструю аналитику по дням (через merge)
select  event_date, event_type,
uniqMerge(unique_users_count) as unique_users,
sumMerge(points_spent_sum) as total_spent,
countMerge(event_count) as total_actions
from user_events_agg
group by event_type, event_date;





