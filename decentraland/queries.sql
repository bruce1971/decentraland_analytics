-- DAILY
SELECT
          			DATE(event_timestamp) AS day,
          			COUNT(1) AS sales,
          			ROUND(AVG(land_size), 2) as avg_size,
          			ROUND(AVG(price_usd_parcel)) AS avg_price_usd_parcel,
          			ROUND(AVG(price_eth_parcel), 2) AS avg_price_eth_parcel,
          			ROUND(AVG(price_mana_parcel)) AS avg_price_mana_parcel
FROM 		        land_events
WHERE           price_usd_parcel > 50 AND event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- WEEKLY
SELECT
          			DATE_SUB(DATE(event_timestamp), INTERVAL DAYOFWEEK(event_timestamp)-2 DAY) AS week,
          			COUNT(1) AS sales,
          			ROUND(AVG(land_size), 2) as avg_size,
          			ROUND(AVG(price_usd_parcel)) AS avg_price_usd_parcel,
          			ROUND(AVG(price_eth_parcel), 2) AS avg_price_eth_parcel,
          			ROUND(AVG(price_mana_parcel)) AS avg_price_mana_parcel
FROM 		        land_events
WHERE           price_usd_parcel > 50 AND event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- MONTHLY
SELECT
          			DATE_SUB(DATE(event_timestamp), INTERVAL DAY(event_timestamp)-1 DAY) AS month,
          			COUNT(1) AS sales,
          			ROUND(AVG(land_size), 2) as avg_size,
          			ROUND(AVG(price_usd_parcel)) AS avg_price_usd_parcel,
          			ROUND(AVG(price_eth_parcel), 2) AS avg_price_eth_parcel,
          			ROUND(AVG(price_mana_parcel)) AS avg_price_mana_parcel
FROM 		        land_events
WHERE           price_usd_parcel > 50 AND event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


--------------------------------------------------------------------------

with y as (
	SELECT
	seller_address,
	land_id,
	max(event_timestamp) as listing_timestamp
	from land_events
	where event_type = 'created'
	GROUP by 1, 2
)
SELECT
  y.listing_timestamp,
  l.event_timestamp AS sale_timestamp,
  Round(UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(y.listing_timestamp)) as sale_time,
  l.land_size,
  l.price_usd,
  l.price_usd_parcel,
  l.price_eth,
  l.price_eth_parcel,
  l.opensea_url,
  l.external_url,
  l.distance_to_district,
  l.distance_to_plaza,
  l.distance_to_road
from land_events l
left join y on y.seller_address = l.seller_address and y.land_id = l.land_id
where event_type = 'successful' and listing_timestamp is not null
order by 3 asc


---------------------------


with y as (
	SELECT
	seller_address,
	land_id,
	max(event_timestamp) as listing_timestamp
	from land_events
	where event_type = 'created'
	GROUP by 1, 2
)


SELECT
DATE(y.listing_timestamp) AS listing_timestamp,
DATE(l.event_timestamp) AS sale_timestamp,
Round(UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(y.listing_timestamp)) as sale_time,
l.land_size,
l.price_usd_parcel,
l.opensea_url
from land_events l
left join y on y.seller_address = l.seller_address and y.land_id = l.land_id
where 1=1
	AND event_type = 'successful'
	and listing_timestamp is not null
	AND l.price_usd_parcel > 50
	AND UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(y.listing_timestamp) < 3600
	AND UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(y.listing_timestamp) > 0
order by 3 asc


SELECT count(1) from land_events
