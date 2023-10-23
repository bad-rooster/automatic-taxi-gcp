import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import argparse
import csv
from datetime import datetime


def parse_csv_dict(element, header_line):
    header_line = [col.strip() for col in header_line.split(',')]
    for line in csv.DictReader([element], fieldnames=header_line, skipinitialspace=True):
        if None in line.keys() or None in line.values():
            raise ValueError(f"Dimension mismatch: {line}")
        else:
            return line


def convert_loc_id_to_int(element):
    element["rate_code_id"] = int(element["rate_code_id"])
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
        header_line = "rate_code_id,rate_name"
        (p
         | "ReadCsv" >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
         | "ParseCols" >> beam.Map(parse_csv_dict, header_line)
         | "IntConversion" >> beam.Map(convert_loc_id_to_int)
         | "WriteCsv" >> beam.io.WriteToText(output_name, file_name_suffix='.json')
         )


if __name__ == "__main__":
    generate_taxi_zones_data()
