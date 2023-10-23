import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse
import csv
from datetime import datetime

nyc_taxi_zone_avro_schema = {"namespace": "nyc_taxi_zones.avro",
                             "type": "record",
                             "name": "an_avro_nyc_taxi",
                             "fields": [
                                 {"name": "location_id", "type": 'int'},
                                 {"name": 'zone', "type": 'string'},
                                 {"name": 'borough', "type": 'string'}
                             ]
                             }


def parse_csv_dict(element, header_line):
    header_line = [col.strip() for col in header_line.split(',')]
    for line in csv.DictReader([element], fieldnames=header_line, skipinitialspace=True):
        if None in line.keys() or None in line.values():
            raise ValueError(f"Dimension mismatch: {line}")
        else:
            return line


def drop_cols(element, cols_to_be_dropped):
    cols_to_be_dropped = [col.strip() for col in cols_to_be_dropped.split(',')]
    for col in cols_to_be_dropped:
        element.pop(col)
    return element


def convert_loc_id_to_int(element):
    element["location_id"] = int(element["location_id"])
    return element


def generate_taxi_zones_data(argv=None):
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
        header_line = "object_id, shape_leng, shape_area, zone, location_id, borough"
        cols_to_be_dropped = "object_id, shape_leng, shape_area"
        (p
         | "ReadCsv" >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
         | "ParseCols" >> beam.Map(parse_csv_dict, header_line)
         | "DropUnwantedCols" >> beam.Map(drop_cols, cols_to_be_dropped)
         | "IntConversion" >> beam.Map(convert_loc_id_to_int)
         | "WriteAvro" >> beam.io.WriteToAvro(output_name, schema=nyc_taxi_zone_avro_schema, file_name_suffix='.avro')
         )


if __name__ == "__main__":
    generate_taxi_zones_data()
