-- DAILY
SELECT
          			DATE(event_timestamp) AS day,
          			COUNT(1) AS sales,
          			ROUND(MIN(amount_eth), 2) AS min_eth,
          			ROUND(AVG(amount_eth), 2) AS avg_eth,
          			ROUND(MAX(amount_eth), 2) AS max_eth,
          			ROUND(LOG(AVG(amount_eth)),2) as log_avg_eth
FROM 		        sb_events
WHERE           event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- WEEKLY
SELECT
          			DATE_ADD(DATE(event_timestamp), INTERVAL - WEEKDAY(event_timestamp) DAY) AS week,
          			COUNT(1) AS sales,
          			ROUND(MIN(amount_eth), 2) AS min_eth,
          			ROUND(AVG(amount_eth), 2) AS avg_eth,
          			ROUND(MAX(amount_eth), 2) AS max_eth,
          			ROUND(LOG(AVG(amount_eth)),2) as log_avg_eth
FROM 		        sb_events
WHERE           event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC


-- MONTHLY
SELECT
          			DATE_SUB(DATE(event_timestamp), INTERVAL DAY(event_timestamp)-1 DAY) AS month,
          			COUNT(1) AS sales,
          			ROUND(MIN(amount_eth), 2) AS min_eth,
          			ROUND(AVG(amount_eth), 2) AS avg_eth,
          			ROUND(MAX(amount_eth), 2) AS max_eth,
          			ROUND(LOG(AVG(amount_eth)),2) as log_avg_eth
FROM 		        sb_events
WHERE           event_type = 'successful'
GROUP BY 	      1
ORDER BY  	    1 DESC
