import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText, WriteToText
import argparse
import re
from datetime import datetime


class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return map(str.lower, re.findall(r'[\w\']+', element, re.UNICODE))


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        default="./text.txt",
                        dest='input'
                        )
    parser.add_argument('--output',
                        required=True,
                        dest='output'
                        )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    datetime_stamp = datetime.now().strftime('%y-%m-%d-%H%M%S')

    def format_result(word, count):
        return f"{word}: {count}"
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | ReadFromText(known_args.input)
         | beam.ParDo(WordExtractingDoFn().with_output_types(str))
         | beam.Map(lambda x: (x, 1))
         | beam.CombinePerKey(sum)
         | beam.MapTuple(format_result)
         | WriteToText(known_args.output + '-' + datetime_stamp, file_name_suffix='.txt')
         )


if __name__ == "__main__":
    run()
