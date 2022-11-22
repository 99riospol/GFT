import argparse
import time
import logging
import json
import typing
from datetime import datetime
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AfterProcessingTime
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.runners import DataflowRunner, DirectRunner

# ### functions and classes

# https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/quests/dataflow_python/8a_Batch_Testing_Pipeline/solution/weather_statistics_pipeline.py
class Message(typing.Dict):
    quote: str
    character: str
    image: str
    characterDirection: str

beam.coders.registry.register_coder(Message, beam.coders.RowCoder)

class ConvertCsvToMessage(beam.DoFn):

    def process(self, line):
        fields = 'quote,character,image,characterDirection'.split(',')
        values = line.split(',')
        row = dict(zip(fields,values))

        yield Message(**row)

# ### main

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')

    # Google Cloud options
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location',default='gs://nabc-bucket/staging/', help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', default='gs://nabc-bucket/temp/', help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')

    # Pipeline-specific options
    parser.add_argument('--table_name', required=True, help='Output BQ table')
    parser.add_argument('--input_file', required=True, help='Input file')

    opts, pipeline_opts = parser.parse_known_args()

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_opts, save_main_session=True)
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = 'nabc-batch-job'
    options.view_as(StandardOptions).runner = opts.runner

    input_file = opts.input_file
    table_name = opts.table_name

    # Table schema for BigQuery
    table_schema = {
        "fields": [
            {
                "name": "quote",
                "type": "STRING"
            },
            {
                "name": "character",
                "type": "STRING"
            },
            {
                "name": "image",
                "type": "STRING"
            },
            {
                "name": "characterDirection",
                "type": "STRING"
            }
        ]
    }

    # Create the pipeline
    p = beam.Pipeline(options=options)



    rows = (p | 'ReadFromCSVFile' >> beam.io.ReadFromText(input_file,skip_header_lines=1)
              | "ParseCSV" >> beam.ParDo(ConvertCsvToMessage()))

    (rows   | 'WriteAggToBQ' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()

# LOCAL:
# py -m bucket_bigquery_pipeline --runner DirectRunner --table_name spa-datajuniorsprogram-sdb-001:data_juniors.nabc-simpsons --input_file gs://nabc-bucket/quotes.csv

# DATAFLOW:
# py -m pubsub_bigquery_pipeline --runner DataflowRunner --table_name spa-datajuniorsprogram-sdb-001:data_juniors.nabc-simpsons --input_file gs://nabc-bucket/quotes.csv --project spa-datajuniorsprogram-sdb-001 --region europe-southwest1 --service-account-email gft-data-sa@spa-datajuniorsprogram-sdb-001.iam.gserviceaccount.com