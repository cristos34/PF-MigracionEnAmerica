#----------------------------------------------------------------------------
# CREAR DWH base
#----------------------------------------------------------------------------
# Se realiza el script tabla por tabla con la finalidad de que en cualquier 
# momento se puedan añadir a cada campo, alguna transformacion que sea
# necesario en el proceso a crear el datawarehouse
#----------------------------------------------------------------------------
# Objetivo  : Crear un Datawarehouse base, con los archivos más relevantes
#             Los datos fueron depositados en el Cloud Storage
#             Fueron ingestados al Bigquery en el conjunto de datos: rawmigraciones
#             El sql crea la nueva tabla en el conjunto de datos: trfmigraciones
#             Transforma los nombres de columnas a las que van a ser usadas
#             en el dashboard y MLs 
# scriptsql : sql07dwhtpi
#----------------------------------------------------------------------------
#
create table `poetic-bison-386122.DWHmigraciones.dwhtipoingreso` AS (
SELECT 
type_tabla,
nombrepais,
codpais, 
tipoingresopais
FROM `poetic-bison-386122.trfmigraciones.trftipoingreso`
);