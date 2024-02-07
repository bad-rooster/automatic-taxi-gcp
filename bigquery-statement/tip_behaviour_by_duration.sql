----- tip behaviour by duration

CREATE TABLE `data_product_taxi_analysis.tip_behaviour_by_duration` AS
SELECT CAST(trip_duration_in_mins/10 AS INT) percentile, AVG(tip_amount) avg_tip FROM `data_product_taxi_analysis.tip_behaviour_stg`
WHERE trip_duration_in_mins BETWEEN 0 AND 120
AND trip_distance > 0
GROUP BY percentile
ORDER BY avg_tip DESC