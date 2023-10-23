import apache_beam as beam
import pyarrow
import argparse
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


taxi_schema = pyarrow.schema([('vendor_id', pyarrow.int32()),
                             ('pickup_datetime', pyarrow.timestamp('us')),
                             ('dropoff_datetime', pyarrow.timestamp('us')),
                             ('passenger_count', pyarrow.int32()),
                             ('trip_distance', pyarrow.float32()),
                             ('rate_code_id', pyarrow.int32()),
                             ('store_and_fwd_flag', pyarrow.large_string()),
                             ('pu_location_id', pyarrow.int64()),
                             ('do_location_id', pyarrow.int64()),
                             ('payment_type', pyarrow.int32()),
                             ('fare_amount', pyarrow.float32()),
                             ('extra', pyarrow.float32()),
                             ('mta_tax', pyarrow.float32()),
                             ('tip_amount', pyarrow.float32()),
                             ('tolls_amount', pyarrow.float32()),
                             ('improvement_surcharge', pyarrow.float32()),
                             ('total_amount', pyarrow.float32()),
                             ('congestion_surcharge', pyarrow.float32()),
                             ('airport_fee', pyarrow.float32())])


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


def generate_nyc_taxi_data(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        required=True,
                        dest='input'
                        )
    parser.add_argument('--output',
                        required=True,
                        dest='output'
                        )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    datetime_stamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    output_name = f"{known_args.output}-{datetime_stamp}"
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | beam.io.ReadFromParquet(known_args.input)
         | beam.Map(rename_datetime_cols)
         | beam.Map(formatting_col_name)
         | beam.io.WriteToParquet(output_name, schema=taxi_schema, file_name_suffix='.parquet')
         )


if __name__ == "__main__":
    generate_nyc_taxi_data()
