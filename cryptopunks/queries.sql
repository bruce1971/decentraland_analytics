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


-- MEDIAN WEEKLY
select
    event_timestamp as week,
    ROUND(avg(amount_eth),2) as median_eth,
    ROUND(LOG(avg(amount_eth)),2) as log_median_eth
from (
select
    DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY) as event_timestamp,
    amount_eth,
    row_number() over(partition by DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY) order by amount_eth) rn,
    count(*) over(partition by DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY)) cnt
  from cryptopunks_events
  where 1=1
  	AND event_type = 'successful'
  	AND amount_eth > 0
) as dd
where rn in ( FLOOR((cnt + 1) / 2), FLOOR( (cnt + 2) / 2) )
group by event_timestamp
ORDER BY 1 DESC


-- non-floor punk
SELECT  count(1)
FROM 		punks
WHERE 	0=1
	OR 		type in ('Alien', 'Ape', 'Zombie')
	OR 		attribute_count in (0, 1, 5, 6, 7)
	OR		has_beanie
	OR 		has_choker
	OR		has_pilot_helmet
	OR		has_tiara
	OR		has_orange_side
	OR		has_buck_teeth
	OR		has_welding_goggles
	OR		has_pigtails
	OR		has_pink_with_hat
	OR		has_top_hat
	OR		has_blonde_short
	OR		has_wild_white_hair
	OR		has_cowboy_hat
	OR		has_wild_blonde
	OR		has_straight_hair_blonde
	OR		has_big_beard
	OR		has_blonde_bob
	OR		has_purple_hair
	OR		has_gold_chain
	OR		has_medical_mask
	OR		has_fedora
	OR		has_smile
	OR		has_hoodie
	OR		has_3d_glasses
	OR		has_luxurious_beard
	OR		has_pipe
	OR		has_vr
	OR		has_clown_nose
	OR		has_big_shades


-- rare punks
SELECT
			  count(1)
FROM 		punks
WHERE 	0=1
	OR 		type in ('Alien', 'Ape', 'Zombie')
	OR 		attribute_count in (0, 1, 5, 6, 7)
	OR		has_beanie
	OR		has_pilot_helmet
	OR		has_tiara
	OR		has_orange_side
	OR		has_top_hat
	OR		has_wild_blonde
	OR		has_hoodie
	OR		has_3d_glasses
