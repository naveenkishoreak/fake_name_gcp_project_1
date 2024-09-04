
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import BigQuerySink
import json
import os 


# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/naveenkishorek/Downloads/key.json"



class PreProcessData(beam.DoFn):
    def process(self, element, *args, **kwargs):
        data = json.loads(element.decode('utf-8'))
        
        yield data
        
def run(argv=None):
    pipeline_options = PipelineOptions([
    '--project=naveen-dbt-project',
    '--runner=DirectRunner',
    '--temp_location=gs://naveen_dataflow_temp/temp',
    '--staging_location=gs://naveen_dataflow_staging/staging',
    '--region=us-central1',
    '--job_name=naveen-test-job',
    '--autoscaling_algorithm=THROUGHPUT_BASED',
    '--max_num_workers=1',
    ])
    
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p
        | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/naveen-dbt-project/subscriptions/fake_name_subs')
        | 'Preprocess Data' >> beam.ParDo(PreProcessData())
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                    'naveen-dbt-project:fakename_api_data.fake_name_table',
                    schema='name:STRING,location:STRING,email:STRING,phone:STRING,cell:STRING,picture:STRING',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        )

if __name__ == '__main__':
    run()