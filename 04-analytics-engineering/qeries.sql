-- question 3
SELECT COUNT(*)
FROM prod.fct_monthly_zone_revenue;

-- question 4
SELECT pickup_zone, SUM(revenue_monthly_total_amount)
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND year(revenue_month) = '2020'
GROUP BY pickup_zone
ORDER BY SUM(revenue_monthly_total_amount) DESC
LIMIT 1;

-- question 5
SELECT SUM(total_monthly_trips)
FROM prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND revenue_month >= '2019-10-01' 
  AND revenue_month < '2019-11-01';

-- question 6
SELECT COUNT(*)
FROM prod.fhv_tripdata
WHERE dispatching_base_num IS NOT NULL;