#!/bin/bash

# Crear carpetas necesarias con permisos
mkdir -p ./data/processed/waze_limpio
chmod -R 777 ./data/processed/waze_limpio

# Ejecutar pig con ruta completa
/home/a/Tarea2-SD/pig ./scripts/clean_and_classify_csv.pig

# Crear archivo encabezado
echo "id,tipo_incidente,subtype,uuid,pubMillis,dateTime,country,state,comuna,street,confiabilidad,reportDescription,reportRating,confianza,nComments,latitude,longitude,x,y" > ./data/processed/waze_limpio/encabezado.csv

# Concatenar encabezado y datos procesados
cat ./data/processed/waze_limpio/waze_filtrado/part-* > ./data/processed/waze_limpio/waze_filtrado_sin_encabezado.csv

cat ./data/processed/waze_limpio/encabezado.csv ./data/processed/waze_limpio/waze_filtrado_sin_encabezado.csv > ./data/processed/waze_limpio/waze_completo.csv

echo "Proceso finalizado correctamente."
