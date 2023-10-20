import apache_beam as beam


def generate_nyc_taxi_data():
    with beam.Pipeline() as p:
        p | beam.io.ReadFromParquet(
            './taxi_sample.parquet-00000-of-00001') | beam.Map(print)


if __name__ == "__main__":
    generate_nyc_taxi_data()
