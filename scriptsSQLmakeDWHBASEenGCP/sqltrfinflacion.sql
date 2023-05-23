#----------------------------------------------------------------------------
# Objetivo  : Crear un Datawarehouse base, con los archivos más relevantes
#             Los datos fueron depositados en el Cloud Storage
#             Fueron ingestados al Bigquery en el conjunto de datos: rawmigraciones
#             El sql crea la nueva tabla en el conjunto de datos: trfmigraciones
#             Transforma los nombres de columnas a las que van a ser usadas
#             en el dashboard y MLs 
# scriptsql : sqltrfinflacion
#----------------------------------------------------------------------------
#
create table `poetic-bison-386122.trfmigraciones.trfinflacion` AS (
SELECT
"INF" AS type_tabla,
string_field_0 AS nombrepais,
string_field_1 AS codpais, 
double_field_5 AS a2010,
double_field_6 AS a2011,
double_field_7 AS a2012,
double_field_8 AS a2013,
double_field_9 AS a2014,
double_field_10 AS a2015,
double_field_11 AS a2016,
double_field_12 AS a2017,
double_field_13 AS a2018,
double_field_14 AS a2019,
double_field_15 AS a2020,
double_field_16 AS a2021,
FROM `poetic-bison-386122.rawmigraciones.rawinflacion`
);