import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import bigquery

# Se definen los parámetros de la canalización.
# Dataflow Runner es empleado por su capacidad de realizar múltiples procesos en paralelo
options = PipelineOptions(
    runner='DataFlowRunner',
    project='divine-aegis-386920',
    job_name='trabajo1',
    temp_location='gs://dl-pf/temp',
    region='southamerica-west1')

# Se define función para leer y recorrer el archivo csv original con el fin de ajustar
# su estructura al la estructura de datos esperada (JSONL o JSON Lines) la cual se requiere 
# para cargar correctamente los datos a nuestro datawarehouse en BigQuery.
def csv_a_jsonl(fila):
    import csv
    reader = csv.reader([fila])
    columnas = next(reader)
    nombrepais = columnas[0]
    codpais = columnas[1]
    indicador = columnas[2]
    codigo = columnas[3]
    columnas_numericas = columnas[4:]
    datos_jsonl = {"nombrepais": nombrepais, "codpais": codpais, "indicador": indicador, "codigo": codigo}

    anios = range(1960, 2024)
    for i, anio in enumerate(anios):
        datos_jsonl[str(anio)] = columnas_numericas[i]

    return datos_jsonl

# Se establece la ubicación de los archivos crudos y los nombres de las respectivas tablas.    
archivos_crudos = ['gs://dl-pf/Datos en crudo/inf_raw.csv', 'gs://dl-pf/Datos en crudo/mgn_raw.csv', 
                   'gs://dl-pf/Datos en crudo/pbi_raw.csv', 'gs://dl-pf/Datos en crudo/por_raw.csv',
                   'gs://dl-pf/Datos en crudo/rem_raw.csv']
nombre_tablas = ['inflacion', 'migracion_neta', 'pbi', 'porcentaje_migrantes', 'remesas']

# Inician las canalizaciones
with beam.Pipeline(options=options) as pipeline:
    # Se itera sobre los diferentes archivos para aplicar las mismas transformaciones
    for i, archivo in enumerate(archivos_crudos):
        nombre_tabla = nombre_tablas[i]
         
        # Se omiten las 5 primeras lineas porque contienen datos sobrantes.
        datos_crudos = (pipeline | f"Leer {archivo}" >> beam.io.ReadFromText(archivo, skip_header_lines=5))
        
        # Se aplica la función csv_a_jsonl a la colección datos_crudos
        datos_jsonl = (datos_crudos | f'Convertir_a_JsonL_{i}' >> beam.Map(csv_a_jsonl))  
        
        table = f'divine-aegis-386920.dwh_automatizado.{nombre_tabla}' 
        
        # Se define el esquema para las tablas, indicando los nombres y tipos de las columnas.
        schema = {"fields": [
        {"name": "nombrepais", "type": "STRING"},{"name": "codpais", "type": "STRING"},
        {"name": "indicador", "type": "STRING"},{"name":"codigo", "type": "STRING"},
        {"name": "1960", "type": "STRING"},{"name": "1961", "type": "STRING"},{"name": "1962", "type": "STRING"},
        {"name": "1963", "type": "STRING"},{"name": "1964", "type": "STRING"},{"name": "1965", "type": "STRING"},
        {"name": "1966", "type": "STRING"},{"name": "1967", "type": "STRING"},{"name": "1968", "type": "STRING"},
        {"name": "1969", "type": "STRING"},{"name": "1970", "type": "STRING"},{"name": "1971", "type": "STRING"},
        {"name": "1972", "type": "STRING"},{"name": "1973", "type": "STRING"},{"name": "1974", "type": "STRING"},
        {"name": "1975", "type": "STRING"},{"name": "1976", "type": "STRING"},{"name": "1977", "type": "STRING"},
        {"name": "1978", "type": "STRING"},{"name": "1979", "type": "STRING"},{"name": "1980", "type": "STRING"},
        {"name": "1981", "type": "STRING"},{"name": "1982", "type": "STRING"},{"name": "1983", "type": "STRING"},
        {"name": "1984", "type": "STRING"},{"name": "1985", "type": "STRING"},{"name": "1986", "type": "STRING"},
        {"name": "1987", "type": "STRING"},{"name": "1988", "type": "STRING"},{"name": "1989", "type": "STRING"},        
        {"name": "1990", "type": "STRING"},{"name": "1991", "type": "STRING"},{"name": "1992", "type": "STRING"},
        {"name": "1993", "type": "STRING"},{"name": "1994", "type": "STRING"},{"name": "1995", "type": "STRING"},
        {"name": "1996", "type": "STRING"},{"name": "1997", "type": "STRING"},{"name": "1998", "type": "STRING"},
        {"name": "1999", "type": "STRING"},{"name": "2000", "type": "STRING"},{"name": "2001", "type": "STRING"},
        {"name": "2002", "type": "STRING"},{"name": "2003", "type": "STRING"},{"name": "2004", "type": "STRING"},
        {"name": "2005", "type": "STRING"},{"name": "2006", "type": "STRING"},{"name": "2007", "type": "STRING"},
        {"name": "2008", "type": "STRING"},{"name": "2009", "type": "STRING"},{"name": "2010", "type": "STRING"},
        {"name": "2011", "type": "STRING"},{"name": "2012", "type": "STRING"},{"name": "2013", "type": "STRING"},
        {"name": "2014", "type": "STRING"},{"name": "2015", "type": "STRING"},{"name": "2016", "type": "STRING"},
        {"name": "2017", "type": "STRING"},{"name": "2018", "type": "STRING"},{"name": "2019", "type": "STRING"},
        {"name": "2020", "type": "STRING"},{"name": "2021", "type": "STRING"},{"name": "2022", "type": "STRING"},{"name": "2023", "type": "STRING"}]} 
       
       # La colección de datos es cargada en BigQuery con los nombres y esquemas de las tablas
       # previamente definidos. Si la tabla no existe es creada mediante CREATE_IF_NEEDED y si la
       # tabla ya existe, entonces sobrescribe los datos anteriores mediante WRITE_TRUNCATE.
        datos_jsonl | f'WriteToBigQuery_{i}' >> beam.io.gcp.bigquery.WriteToBigQuery(
            table=table,
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

# Este llamado sirve para correr las canalizaciones. Debe ser ejecutado en la consola Cloud Shell.
""" gsutil cp gs://dl-pf/pipelines_ETL.py ~/pipelines_ETL.py
python ~/pipelines_ETL.py """
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
