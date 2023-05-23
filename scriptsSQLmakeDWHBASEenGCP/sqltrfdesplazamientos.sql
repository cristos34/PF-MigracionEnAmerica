#----------------------------------------------------------------------------
# Objetivo  : Crear un Datawarehouse base, con los archivos más relevantes
#             Los datos fueron depositados en el Cloud Storage
#             Fueron ingestados al Bigquery en el conjunto de datos: rawmigraciones
#             El sql crea la nueva tabla en el conjunto de datos: trfmigraciones
#             Transforma los nombres de columnas a las que van a ser usadas
#             en el dashboard y MLs 
# scriptsql : sqltrfdesplazamientos
#----------------------------------------------------------------------------
#
create table `poetic-bison-386122.trfmigraciones.trfdesplazamientos` AS (
SELECT
"DES" AS type_tabla,
int64_field_0  AS coddesplazamiento,
ISO3           AS codpais, 
Name           AS nombrepais,
Year           AS yeardesplazamiento,
Conflict_Stock_Displacement            AS porconflicto,			
Conflict_Stock_Displacement__Raw_      AS porconflictoraw,
Conflict_Internal_Displacements			   AS porconflictointerno,
Conflict_Internal_Displacements__Raw_  AS porconflictointernoraw,
Disaster_Internal_Displacements			   AS pordesastreinterno,
Disaster_Internal_Displacements__Raw_  AS pordesastreinternoraw,
Disaster_Stock_Displacement			       AS pordesastre,
FROM `poetic-bison-386122.rawmigraciones.rawdesplazamientos`
);