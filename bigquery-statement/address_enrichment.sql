----- Enrich address, most traffic zones
CREATE TABLE `data_product_taxi_analysis.most_frequent_zone` AS
WITH traffic AS (SELECT COUNT(*) trip_count, CONCAT(z.zone, ', ',z.borough, ', ',z.state,', ', "US") address FROM `dataflow_taxi_analysis.taxi_ride` r
INNER JOIN `dataflow_taxi_analysis.taxi_zone` z
ON r.pu_location_id=z.location_id
GROUP BY z.zone, z.borough, z.state
UNION ALL
SELECT COUNT(*) trip_count, CONCAT(z.zone, ', ',z.borough, ', ',z.state,', ', "US") address FROM `dataflow_taxi_analysis.taxi_ride` r
INNER JOIN `dataflow_taxi_analysis.taxi_zone` z
ON r.do_location_id=z.location_id
GROUP BY z.zone, z.borough, z.state)

SELECT SUM(traffic.trip_count) trip_count, address FROM traffic
GROUP BY address