-- Script de Apache Pig para procesamiento de datos de Waze
-- Tarea 2 - Sistemas Distribuidos
-- Limpieza, homogeneización y clasificación de incidentes

-- Configuración para modo local
SET pig.exec.type 'local';
SET pig.temp.dir '/tmp/pig';
SET pig.splitCombination 'false';

-- Registrar funciones si es necesario
-- REGISTER '/path/to/piggybank.jar';

-- 1. CARGA DE DATOS
-- Cargar datos desde el CSV exportado
raw_data = LOAD '/data/export/waze_incidents.csv' 
    USING PigStorage(',') 
    AS (
        id:chararray,
        type:chararray,
        subtype:chararray,
        uuid:chararray,
        pubMillis:long,
        dateTime:chararray,
        country:chararray,
        state:chararray,
        city:chararray,
        street:chararray,
        magvar:int,
        reliability:int,
        reportDescription:chararray,
        reportRating:int,
        confidence:int,
        nComments:int,
        latitude:double,
        longitude:double,
        x:double,
        y:double
    );

-- Saltar la primera línea (headers)
data_without_headers = FILTER raw_data BY id != '_id';

-- 2. LIMPIEZA DE DATOS
-- Eliminar registros con campos críticos nulos o vacíos
cleaned_data = FILTER data_without_headers BY 
    (type IS NOT NULL AND type != '') AND
    (latitude IS NOT NULL AND latitude != 0.0) AND
    (longitude IS NOT NULL AND longitude != 0.0) AND
    (city IS NOT NULL AND city != '');

-- 3. HOMOGENEIZACIÓN Y ESTANDARIZACIÓN
-- Normalizar tipos de incidentes
standardized_data = FOREACH cleaned_data GENERATE
    id,
    (CASE 
        WHEN UPPER(type) == 'ACCIDENT' OR UPPER(type) == 'CRASH' THEN 'ACCIDENTE'
        WHEN UPPER(type) == 'JAM' OR UPPER(type) == 'TRAFFIC_JAM' THEN 'ATASCO'
        WHEN UPPER(type) == 'ROAD_CLOSED' OR UPPER(type) == 'CLOSURE' THEN 'CORTE_VIA'
        WHEN UPPER(type) == 'HAZARD' OR UPPER(type) == 'WEATHER' THEN 'PELIGRO'
        WHEN UPPER(type) == 'POLICE' THEN 'POLICIA'
        ELSE 'OTRO'
    END) AS incident_type,
    subtype,
    uuid,
    pubMillis,
    dateTime,
    country,
    state,
    UPPER(TRIM(city)) AS comuna,
    street,
    magvar,
    reliability,
    reportDescription,
    reportRating,
    confidence,
    nComments,
    latitude,
    longitude,
    x,
    y;

-- 4. ELIMINACIÓN DE DUPLICADOS
-- Agrupar por ubicación similar y tipo para eliminar duplicados
grouped_for_dedup = GROUP standardized_data BY (
    incident_type, 
    comuna,
    ROUND(latitude * 1000),  -- Aproximar coordenadas para agrupar incidentes cercanos
    ROUND(longitude * 1000)
);

-- Mantener solo un registro por grupo (el más reciente)
deduped_data = FOREACH grouped_for_dedup {
    sorted = ORDER standardized_data BY pubMillis DESC;
    latest = LIMIT sorted 1;
    GENERATE FLATTEN(latest);
};

-- 5. CLASIFICACIÓN Y ANÁLISIS

-- A. Análisis por Comuna
incidents_by_comuna = GROUP deduped_data BY comuna;
comuna_stats = FOREACH incidents_by_comuna GENERATE
    group AS comuna,
    COUNT(deduped_data) AS total_incidents,
    AVG(deduped_data.reliability) AS avg_reliability,
    AVG(deduped_data.confidence) AS avg_confidence;

-- B. Análisis por Tipo de Incidente
incidents_by_type = GROUP deduped_data BY incident_type;
type_stats = FOREACH incidents_by_type GENERATE
    group AS incident_type,
    COUNT(deduped_data) AS total_count,
    AVG(deduped_data.reliability) AS avg_reliability;

-- C. Análisis Temporal (por hora del día)
-- Extraer hora del timestamp
temporal_data = FOREACH deduped_data GENERATE
    incident_type,
    comuna,
    (pubMillis / (1000 * 60 * 60)) % 24 AS hour_of_day,
    reliability,
    confidence;

temporal_analysis = GROUP temporal_data BY hour_of_day;
hourly_stats = FOREACH temporal_analysis GENERATE
    group AS hour,
    COUNT(temporal_data) AS incidents_count;

-- D. Análisis Combinado: Tipo + Comuna
type_comuna_analysis = GROUP deduped_data BY (incident_type, comuna);
type_comuna_stats = FOREACH type_comuna_analysis GENERATE
    FLATTEN(group) AS (incident_type, comuna),
    COUNT(deduped_data) AS incident_count,
    AVG(deduped_data.reliability) AS avg_reliability,
    MAX(deduped_data.pubMillis) AS latest_incident;

-- 6. IDENTIFICACIÓN DE ZONAS CRÍTICAS
-- Comunas con mayor número de incidentes
comuna_ranking = ORDER comuna_stats BY total_incidents DESC;
top_comunas = LIMIT comuna_ranking 10;

-- Tipos de incidentes más frecuentes
type_ranking = ORDER type_stats BY total_count DESC;

-- 7. ALMACENAMIENTO DE RESULTADOS
-- Crear directorio de salida si no existe
-- mkdir -p /data/processed

-- Guardar datos limpios y procesados
STORE deduped_data INTO '/data/processed/clean_incidents' 
    USING PigStorage(',');

-- Guardar estadísticas por comuna
STORE comuna_stats INTO '/data/processed/comuna_analysis' 
    USING PigStorage(',');

-- Guardar estadísticas por tipo
STORE type_stats INTO '/data/processed/type_analysis' 
    USING PigStorage(',');

-- Guardar análisis temporal
STORE hourly_stats INTO '/data/processed/temporal_analysis' 
    USING PigStorage(',');

-- Guardar análisis combinado
STORE type_comuna_stats INTO '/data/processed/type_comuna_analysis' 
    USING PigStorage(',');

-- Guardar ranking de comunas críticas
STORE top_comunas INTO '/data/processed/top_critical_comunas' 
    USING PigStorage(',');

-- Guardar ranking de tipos de incidentes
STORE type_ranking INTO '/data/processed/incident_type_ranking' 
    USING PigStorage(',');

-- 8. MÉTRICAS DE CALIDAD DE DATOS
-- Contar registros en cada etapa
raw_count = FOREACH (GROUP data_without_headers ALL) GENERATE COUNT(data_without_headers) AS raw_total;
clean_count = FOREACH (GROUP cleaned_data ALL) GENERATE COUNT(cleaned_data) AS clean_total;
final_count = FOREACH (GROUP deduped_data ALL) GENERATE COUNT(deduped_data) AS final_total;

-- Guardar métricas de procesamiento
STORE raw_count INTO '/data/processed/metrics_raw_count' USING PigStorage(',');
STORE clean_count INTO '/data/processed/metrics_clean_count' USING PigStorage(',');
STORE final_count INTO '/data/processed/metrics_final_count' USING PigStorage(',');

-- Mostrar progreso
DUMP raw_count;
DUMP clean_count;
DUMP final_count;