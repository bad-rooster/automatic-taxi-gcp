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
    --input gs://dataflow-nyc-taxi-parquet-an/inputs/taxi_sample.parquet \
    --output gs://dataflow-nyc-taxi-parquet-an/results/taxi_rides \
    --runner DataflowRunner \
    --project polar-storm-402611 \
    --temp_location gs://dataflow-nyc-taxi-parquet-an/tmp/

# populate bq table
bq load \
    --source_format=PARQUET \
    dataflow_taxi_analysis.taxi_rides \
    "gs://dataflow-nyc-taxi-parquet-an/results/taxi_rides*.parquet"