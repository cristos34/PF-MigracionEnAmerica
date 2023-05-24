import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

# Se define función que permite aplicar la misma transformación a múltiples tablas
def transformar_tablas(tabla):
    
    # Se definen los parámetros de la canalización y se elige DirectRunner por su rapidez.
    options = PipelineOptions(
        runner='DirectRunner',
        project='divine-aegis-386920',
        job_name='trabajo1',
        temp_location='gs://dl-pf/temp',
        region='southamerica-west1')

    # Se crea una canalización para aplicar las sentencias SQL (BigQuery) a múltiples tablas
    with beam.Pipeline(options=options) as pipeline:
        
        sentencias_SQL = f"""
        DELETE FROM `{tabla}`
        WHERE `nombrepais` NOT IN ('Argentina','Bolivia','Brasil','Canadá','Chile','Colombia','Costa Rica',
            'Cuba','Ecuador','El Salvador','Estados Unidos','Guatemala','Guyana','Haití','Honduras','Jamaica',
            'México','Nicaragua','Panamá','Paraguay','Perú','República Dominicana','Uruguay','Venezuela');

            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `indicador`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `codigo`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1960`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1961`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1962`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1963`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1964`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1965`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1966`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1967`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1968`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1969`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1970`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1971`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1972`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1973`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1974`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1975`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1976`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1977`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1978`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1979`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1980`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1981`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1982`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1983`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1984`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1985`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1986`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1987`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1988`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1989`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1990`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1991`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1992`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1993`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1994`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1995`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1996`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1997`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1998`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `1999`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2000`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2001`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2002`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2003`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2004`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2005`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2006`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2007`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2008`;
            -- ALTER TABLE `{tabla}` DROP COLUMN IF EXISTS `2009`;
            """
        
        client = bigquery.Client()
        query_job = client.query(sentencias_SQL)
        query_job.result()

        # Se crea un paso en falso que luego será ignorado ya que es necesario para la canalización.
        _ = pipeline | 'PasoFalso' >> beam.Create([None])

# Tablas previamente creadas de forma automática con el archivo pipelines_ETL.py
tablas = ['divine-aegis-386920.dwh_automatizado.inflacion', 
          'divine-aegis-386920.dwh_automatizado.migracion_neta', 
          'divine-aegis-386920.dwh_automatizado.pbi',
          'divine-aegis-386920.dwh_automatizado.porcentaje_migrantes',
          'divine-aegis-386920.dwh_automatizado.remesas']

# Se llama la función para las 5 tablas
for tabla in tablas:
    transformar_tablas(tabla)

# Este llamado sirve para correr las canalizaciones. Debe ser ejecutado en la consola Cloud Shell.
""" gsutil cp gs://dl-pf/transformaciones_automaticas.py ~/transformaciones_automaticas.py
python ~/transformaciones_automaticas.py """