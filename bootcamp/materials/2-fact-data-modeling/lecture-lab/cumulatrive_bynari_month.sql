drop table users_cumulated

create  table users_cumulated (
user_id TEXT,
dates_active date[],
date date,
primary key (user_id, date))


select * from users_cumulated where date='2023-01-31'





INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT * 
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
),
    today AS (
          SELECT cast(user_id as text),
                 DATE(CAST( event_time as timestamp)) AS dates_active
                 FROM events
            WHERE DATE(CAST( event_time as timestamp)) = DATE('2023-01-31')
            AND user_id IS NOT NULL
         GROUP BY user_id,  DATE(CAST( event_time as timestamp)) 
    )
SELECT
      CAST(COALESCE(t.user_id, y.user_id)as TEXT) as user_id,
       COALESCE(y.dates_active,
           ARRAY[]::DATE[])
            || CASE WHEN
                t.user_id IS NOT NULL
                THEN ARRAY[t.dates_active]
                ELSE ARRAY[]::DATE[]
                END AS date_list,
       COALESCE(t.dates_active, y.date + Interval '1 day') as date
FROm yesterday y
    FULL OUTER JOIN
    today t ON t.user_id = y.user_id;





with users as (
select * from users_cumulated
where date = '2023-01-31'
), series as (
		select * 
		from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
), place_holder_int as (
select 
		case when dates_active @> array[date(series_date)] 
		then cast(POW(2, 32- (date - date(series_date))) as bigint)
		else 0
		end as place_holder_int_value
, *
from  users 
	cross join series
	--where users.user_id='137925124111668560'
)
select
	user_id,
	cast(cast( 	sum(place_holder_int_value)	as bigint) as bit(32)),
	bit_count(cast(cast( 	sum(place_holder_int_value)	as bigint) as bit(32))) > 0 as dim_is_monthly_active,
	bit_count(cast('111111100000000000000000000000' as bit(32)) &
	cast(cast( 	sum(place_holder_int_value)	as bigint) as bit(32))
	) >0  as dim_is_weekly_active,
	bit_count(cast('100000000000000000000000000000' as bit(32)) &
	cast(cast( 	sum(place_holder_int_value)	as bigint) as bit(32))
	) >0  as dim_is_daily_active
	from place_holder_int
	group by user_id













