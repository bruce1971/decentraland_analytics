-- DAILY
SELECT
          			DATE(sale_timestamp) AS date,
          			COUNT(1) AS sales,
                ROUND(AVG(size), 2) as avg_size,
          			ROUND(AVG(price_usd_parcel)) AS avg_price_usd_parcel,
          			ROUND(AVG(price_eth_parcel),3) AS avg_price_eth_parcel,
          			ROUND(AVG(price_mana_parcel)) AS avg_price_mana_parcel
FROM 		        sales
GROUP BY 	      1
ORDER BY  	    1 asc


-- WEEKLY
SELECT
          			DATE_SUB(DATE(sale_timestamp), INTERVAL DAYOFWEEK(sale_timestamp)-2 DAY) AS week,
          			COUNT(1) AS sales,
          			ROUND(AVG(size), 2) as avg_size,
          			ROUND(AVG(price_usd_parcel)) AS avg_price_usd_parcel,
          			ROUND(AVG(price_eth_parcel),3) AS avg_price_eth_parcel,
          			ROUND(AVG(price_mana_parcel)) AS avg_price_mana_parcel
FROM 		        sales
GROUP BY 	      1
ORDER BY  	    1 asc
