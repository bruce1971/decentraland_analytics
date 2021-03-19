-- Average sale price evolution
SELECT
  			Date(sale_timestamp) as date,
  			round(AVG(price_usd_parcel)) as avg_price_usd_parcel,
  			count(1) as sales
from 		sales
GROUP by 	1
order by 	1 desc
