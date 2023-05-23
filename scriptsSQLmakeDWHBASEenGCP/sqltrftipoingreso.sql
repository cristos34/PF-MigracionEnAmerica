#----------------------------------------------------------------------------
# Objetivo  : Crear un Datawarehouse base, con los archivos más relevantes
#             Los datos fueron depositados en el Cloud Storage
#             Fueron ingestados al Bigquery en el conjunto de datos: rawmigraciones
#             El sql crea la nueva tabla en el conjunto de datos: trfmigraciones
#             Transforma los nombres de columnas a las que van a ser usadas
#             en el dashboard y MLs 
# scriptsql : sqltrftipoingreso
#----------------------------------------------------------------------------
#
create table `poetic-bison-386122.trfmigraciones.trftipoingreso` AS (
SELECT
"TPI" AS type_tabla,
string_field_0 AS nombrepais,
string_field_1 AS codpais, 
string_field_2 AS tipoingresopais,
FROM `poetic-bison-386122.rawmigraciones.rawtipoingreso`
);