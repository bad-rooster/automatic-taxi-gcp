# trigger wordcount dataflow
python -m wordcount \
    --region eurpoe-west2 \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://wordcount-kinglear/results/outputs \
    --runner DataflowRunner \
    --project polar-storm-402611 \
    --temp_location gs://wordcount-kinglear/tmp/

# trigger nyc taxi dataflow
python -m nyc_taxi_df_job \
    --region europe-west2 \
    --input gs://dataflow-nyc-taxi-parquet-an/inputs/nyc_taxi_tripdata_2023-07.parquet \
    --output gs://dataflow-nyc-taxi-parquet-an/results/taxi_rides/rides_output \
    --runner DataflowRunner \
    --project polar-storm-402611 \
    --temp_location gs://dataflow-nyc-taxi-parquet-an/tmp/

python -m nyc_taxi_zones_df_job \
    --region europe-west2 \
    --input gs://dataflow-nyc-taxi-parquet-an/inputs/nyc_taxi_zones.csv \
    --output gs://dataflow-nyc-taxi-parquet-an/results/taxi_zones/zones_output \
    --runner DataflowRunner \
    --project polar-storm-402611 \
    --temp_location gs://dataflow-nyc-taxi-parquet-an/tmp/

# populate bq table

bq load \
    --source_format=PARQUET \
    dataflow_taxi_analysis.taxi_ride \
    "gs://dataflow-nyc-taxi-parquet-an/results/taxi_rides/rides_output*.parquet"

bq load \
    --source_format=AVRO \
    dataflow_taxi_analysis.taxi_zone \
    "gs://dataflow-nyc-taxi-parquet-an/results/taxi_rides/rides_output*.parquet"