import apache_beam as beam
import requests

options = beam.options.pipeline_options.PipelineOptions()
google_cloud_options = options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
google_cloud_options.project = 'quantum-toolbox-386102'
google_cloud_options.region = 'southamerica-west1'
google_cloud_options.staging_location = 'gs://data-lake-pf/staging'
google_cloud_options.temp_location = 'gs://data-lake-pf/temp'
options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
    
url = 'https://drive.google.com/u/0/uc?id=1KJrN-fKuwv3s1QQSeaR3jG832nrZuOFx&export=download'
response = requests.get(url)
data = response.text.splitlines()

countries_to_filter = ['Antigua y Barbuda','Argentina','Bahamas','Barbados','Belice','Bolivia','Brasil','Canadá','Cuba','Dominica',
            'Ecuador','El Salvador','Estados Unidos','Granada','Guatemala','Guyana','Haití','Honduras','Jamaica','México','Nicaragua',
            'Panamá','Perú','República Dominicana','San Cristóbal y Nieve','San Cristóbal y Nieves','San Vicente y las Granadinas', 
            'Santa Lucía','Surinam','Trinidad y Tobago','Antigua and Barbuda','Belize','Brazil','Canada','Chile','Colombia','Costa Rica',
            'Dominican Republic','Grenada','Haiti','Mexico','Panama','Paraguay','Peru','St. Kitts and Nevis','Saint Kitts and Nevis',
            'St. Vincent and the Grenadines','Saint Vincent and the Grenadines','St. Lucia','Saint Lucia','Suriname','Trinidad and Tobago',
            'United States','Uruguay','Venezuela','Venezuela, RB','Bahamas, The','Venezuela (Bolivarian Republic of)','Puerto Rico*']

with beam.Pipeline(options=options) as p:
    pcoll = p | beam.Create(data)

    def filter_countries(row, countries_to_filter):
        return row.split(',')[1] in countries_to_filter

    pcoll = pcoll | beam.Filter(filter_countries, countries_to_filter=countries_to_filter)
    pcoll | 'Write to GCS' >> beam.io.WriteToText('gs://data-lake-pf/data-lake/edad_sexo.csv')