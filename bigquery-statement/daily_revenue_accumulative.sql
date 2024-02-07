------ daily income cumulative
CREATE TABLE `data_product_taxi_analysis.daily_income_cumulative` AS
SELECT d1.pickup_date, ROUND(d1.total_amount_daily, 2) total_amount_daily, ROUND(SUM(d2.total_amount_daily), 2) cumulative_amount_daily FROM `data_product_taxi_analysis.daily_income` d1
INNER JOIN `data_product_taxi_analysis.daily_income` d2
ON d1.pickup_date>=d2.pickup_date
GROUP BY d1.pickup_date, d1.total_amount_daily
ORDER BY d1.pickup_date