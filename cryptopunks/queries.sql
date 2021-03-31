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
