-- DAILY
SELECT
          			DATE(event_timestamp) AS day,
          			COUNT(1) AS sales,
          			ROUND(AVG(price_eth), 2) AS avg_price_eth,
          			ROUND(MIN(price_eth), 2) AS min_price_eth
FROM 		        punks_events
WHERE           price_eth > 0
GROUP BY 	      1
ORDER BY  	    1 DESC


-- WEEKLY
SELECT
          			DATE_SUB(DATE(event_timestamp), INTERVAL DAYOFWEEK(event_timestamp)-2 DAY) AS week,
          			COUNT(1) AS sales,
          			ROUND(AVG(price_eth), 2) AS avg_price_eth,
          			ROUND(MIN(price_eth), 2) AS min_price_eth
FROM 		        punks_events
WHERE           price_eth > 0
GROUP BY 	      1
ORDER BY  	    1 DESC
