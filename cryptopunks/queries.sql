-- DAILY
SELECT
          			DATE(event_timestamp) AS day,
          			COUNT(1) AS sales,
          			ROUND(AVG(amount_eth), 2) AS avg_price_eth,
          			ROUND(MIN(amount_eth), 2) AS min_price_eth
FROM 		        cryptopunks_events
WHERE           amount_eth > 0 and event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- WEEKLY
SELECT
          			DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY) AS week,
          			COUNT(1) AS sales,
          			ROUND(AVG(amount_eth), 2) AS avg_price_eth,
          			ROUND(MIN(amount_eth), 2) AS min_price_eth
FROM 		        cryptopunks_events
WHERE           amount_eth > 0 and event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- MONTHLY
SELECT
          			DATE_SUB(DATE(event_timestamp), INTERVAL DAY(event_timestamp)-1 DAY) AS month,
          			COUNT(1) AS sales,
          			ROUND(AVG(amount_eth), 2) AS avg_price_eth,
          			ROUND(MIN(amount_eth), 2) AS min_price_eth
FROM 		        cryptopunks_events
WHERE           amount_eth > 0 and event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- MEDIAN DAILY
select
    event_timestamp as day,
    ROUND(avg(amount_eth),2) as median_eth,
    ROUND(LOG(avg(amount_eth) + 1),2) as log_median_eth
from (
select
    DATE(event_timestamp) as event_timestamp,
    amount_eth,
    row_number() over(partition by DATE(event_timestamp) order by amount_eth) rn,
    count(*) over(partition by DATE(event_timestamp)) cnt
  from cryptopunks_events
  where event_type = 'successful'
) as dd
where rn in ( FLOOR((cnt + 1) / 2), FLOOR( (cnt + 2) / 2) )
group by event_timestamp
ORDER BY 1 DESC


-- MEDIAN WEEKLY
select
    event_timestamp as week,
    ROUND(avg(amount_eth),2) as median_eth,
    ROUND(LOG(avg(amount_eth) + 1),2) as log_median_eth
from (
select
    DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY) as event_timestamp,
    amount_eth,
    row_number() over(partition by DATE(event_timestamp) order by amount_eth) rn,
    count(*) over(partition by DATE(event_timestamp)) cnt
  from cryptopunks_events
  where event_type = 'successful'
) as dd
where rn in ( FLOOR((cnt + 1) / 2), FLOOR( (cnt + 2) / 2) )
group by event_timestamp
ORDER BY 1 DESC




-- MEDIAN MONTHLY
select
    event_timestamp as month,
    ROUND(avg(amount_eth),2) as median_eth,
    ROUND(LOG(avg(amount_eth) + 1),2) as log_median_eth
from (
select
    DATE_SUB(DATE(event_timestamp), INTERVAL DAY(event_timestamp)-1 DAY) as event_timestamp,
    amount_eth,
    row_number() over(partition by DATE(event_timestamp) order by amount_eth) rn,
    count(*) over(partition by DATE(event_timestamp)) cnt
  from cryptopunks_events
  where event_type = 'successful'
) as dd
where rn in ( FLOOR((cnt + 1) / 2), FLOOR( (cnt + 2) / 2) )
group by event_timestamp
ORDER BY 1 DESC
