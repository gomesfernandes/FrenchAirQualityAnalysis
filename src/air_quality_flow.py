"""
Module containing the Dataflow pipeline. The pipeline will read the CSV files in the given input bucket and
append each row to the given (and already existing) BigQuery table.
See the dataflow_runner.sh executable for an example on how to run the standalone pipleline.
"""
import argparse
import re

import apache_beam as beam


def e2_row_transform(row):
    values = re.split(',', re.sub(r'[\r\n"]', '', row))
    row = {
        'file_type': values[0],
        'network_code': values[1],
        'station_code': values[2],
        'pollutant_code': int(values[3]),
        'start_date': values[4].replace('+00:00', ''),
        'end_date': values[5].replace('+00:00', ''),
        'verification': int(values[6]),
        'validity': int(values[7]),
        'pollutant_quantity': float(values[8])
    }
    return row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True,
                        help='GCS bucket containing the CSV files.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output BQ table to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as pipeline:
        (pipeline
         | "Read CSV" >> beam.io.ReadFromText('{}/*.csv'.format(known_args.input))
         | "Convert To BQ Row" >> beam.Map(lambda r: e2_row_transform(r))
         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table=known_args.output,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
         )


if __name__ == '__main__':
    run()
