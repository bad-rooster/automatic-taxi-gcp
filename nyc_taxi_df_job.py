import apache_beam as beam


def rename_datetime_cols(element):
    element['pickup_datetime'] = element.pop('tpep_pickup_datetime')
    element['dropoff_datetime'] = element.pop('tpep_dropoff_datetime')
    return element


def formatting_col_name(element):
    element['vendor_id'] = element.pop('VendorID')
    element['rate_code_id'] = element.pop('RatecodeID')
    element['pu_location_id'] = element.pop('PULocationID')
    element['do_location_id'] = element.pop('DOLocationID')
    element['airport_fee'] = element.pop('Airport_fee')
    return element


def generate_nyc_taxi_data():
    with beam.Pipeline() as p:
        raw_input_pcoll = p | beam.io.ReadFromParquet(
            './taxi_sample.parquet-00000-of-00001') | beam.Map(rename_datetime_cols) | beam.Map(formatting_col_name) | beam.Map(print)


if __name__ == "__main__":
    generate_nyc_taxi_data()
