import apache_beam as beam
import requests

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
google_cloud_options.project = 'quantum-toolbox-386102'
google_cloud_options.region = 'southamerica-west1'
google_cloud_options.staging_location = 'gs://data-lake-pf/staging'
google_cloud_options.temp_location = 'gs://data-lake-pf/temp'
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
    
url = 'https://drive.google.com/u/0/uc?id=1XHWRJsaPe_k0BddsFnH4Ql6xHL5IA_rb&export=download'
response = requests.get(url)
data = response.text.encode('utf-8').splitlines()

with beam.Pipeline(options=options) as p:
    pcoll = p | beam.Create(data)
    pcoll | 'Write to GCS' >> beam.io.WriteToText('gs://data-lake-pf/data-lake/pbi.csv')