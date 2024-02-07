----- tip behaviour
-- exclude 13 edge cases where trip is more than 24 hours
-- exclude edge cases where trip duration is negative

CREATE OR REPLACE TABLE `data_product_taxi_analysis.tip_behaviour_stg` AS
SELECT DATETIME_DIFF(dropoff_datetime, pickup_datetime, MINUTE) trip_duration_in_mins, trip_distance, tip_amount FROM `dataflow_taxi_analysis.taxi_ride`
WHERE DATE(pickup_datetime) BETWEEN DATE('2023-07-01') AND DATE('2023-07-31')
AND dropoff_datetime-pickup_datetime > INTERVAL 0 HOUR
AND dropoff_datetime-pickup_datetime < INTERVAL 24 HOUR