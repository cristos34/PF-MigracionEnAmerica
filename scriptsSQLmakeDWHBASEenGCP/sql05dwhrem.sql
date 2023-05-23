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
# scriptsql : sql05dwhrem
#----------------------------------------------------------------------------
#
insert `poetic-bison-386122.DWHmigraciones.datawarehousemig` 
SELECT 
type_tabla,
codpais, 
nombrepais,
a2010,
a2011,
a2012,
a2013,
a2014,
a2015,
a2016,
a2017,
a2018,
a2019,
a2020,
a2021
FROM `poetic-bison-386122.trfmigraciones.trfremesa`