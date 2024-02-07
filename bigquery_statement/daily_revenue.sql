------ daily revenue stream
CREATE OR REPLACE TABLE `data_product_taxi_analysis.daily_income` AS
SELECT DATE(pickup_datetime) pickup_date, SUM(total_amount) total_amount_daily FROM `dataflow_taxi_analysis.taxi_ride`
WHERE DATE(pickup_datetime) BETWEEN DATE('2023-07-01') AND DATE('2023-07-31')
GROUP BY pickup_date