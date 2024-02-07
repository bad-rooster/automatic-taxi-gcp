----- tip behaviour by distance

CREATE OR REPLACE TABLE `data_product_taxi_analysis.tip_behaviour_by_distance` AS
SELECT CAST(trip_distance/10 AS INT) miles_times_ten, AVG(tip_amount) avg_tip FROM `data_product_taxi_analysis.tip_behaviour_stg`
WHERE trip_duration_in_mins BETWEEN 0 AND 120
AND trip_distance > 0
GROUP BY miles_times_ten
HAVING COUNT(miles_times_ten) > 50
ORDER BY avg_tip DESC