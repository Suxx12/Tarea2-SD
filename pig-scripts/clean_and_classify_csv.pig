-- Script de Pig para procesar incidentes de tráfico de Waze
-- Tarea 2 - Sistemas Distribuidos - MEJORADO

-- Cargar datos desde CSV con manejo de errores
raw_data = LOAD '/input/waze_incidents.csv' 
    USING PigStorage(',') 
    AS (id:chararray, type:chararray, subtype:chararray, uuid:chararray, 
        pubMillis:long, dateTime:chararray, country:chararray, state:chararray, 
        city:chararray, street:chararray, magvar:int, reliability:int,
        reportDescription:chararray, reportRating:int, confidence:int, 
        nComments:int, latitude:double, longitude:double, x:double, y:double);

-- Contar registros originales
raw_count = FOREACH (GROUP raw_data ALL) GENERATE COUNT(raw_data) as total;
STORE raw_count INTO '/output/metrics_raw_count' USING PigStorage(',');

-- Filtrar y limpiar datos
-- Eliminar la cabecera del CSV y registros inválidos
filtered_data = FILTER raw_data BY 
    id IS NOT NULL AND 
    id != '_id' AND 
    id != '';

-- Contar después de filtro inicial
filtered_count = FOREACH (GROUP filtered_data ALL) GENERATE COUNT(filtered_data) as total;
STORE filtered_count INTO '/output/metrics_filtered_count' USING PigStorage(',');

-- Filtrar registros válidos (que tengan coordenadas y tipo)
clean_data = FILTER filtered_data BY 
    latitude IS NOT NULL AND 
    longitude IS NOT NULL AND 
    latitude != 0.0 AND
    longitude != 0.0 AND
    type IS NOT NULL AND 
    type != '' AND
    city IS NOT NULL AND
    city != '';

-- Contar datos limpios
clean_count = FOREACH (GROUP clean_data ALL) GENERATE COUNT(clean_data) as total;
STORE clean_count INTO '/output/metrics_clean_count' USING PigStorage(',');

-- Estandarizar y enriquecer datos
standardized_data = FOREACH clean_data GENERATE
    id,
    (type == 'Alert'    ? 'ALERT' :
     (type == 'Jam'     ? 'JAM' :
     (type == 'Accident'? 'ACCIDENT' :
     (type == 'Hazard'  ? 'HAZARD' :
     UPPER(type))))) AS incident_type,
    subtype,
    uuid,
    pubMillis,
    dateTime,
    country,
    state,
    (city IS NULL ? 'UNKNOWN' : city) AS comuna,
    street,
    magvar,
    (reliability IS NULL ? 0 : reliability) AS reliability,
    reportDescription,
    reportRating,
    (confidence IS NULL ? 0 : confidence) AS confidence,
    nComments,
    latitude,
    longitude,
    x,
    y;

-- Eliminar duplicados basados en ID
unique_data = DISTINCT standardized_data;

-- Contar registros finales
final_count = FOREACH (GROUP unique_data ALL) GENERATE COUNT(unique_data) as total;
STORE final_count INTO '/output/metrics_final_count' USING PigStorage(',');

-- Almacenar datos limpios principales
STORE unique_data INTO '/output/clean_incidents' USING PigStorage(',');

-- === ANÁLISIS 1: Estadísticas por Comuna ===
incidents_by_comuna = GROUP unique_data BY comuna;
comuna_stats = FOREACH incidents_by_comuna GENERATE
    group AS comuna,
    COUNT(unique_data) AS total_incidents,
    AVG(unique_data.reliability) AS avg_reliability,
    AVG(unique_data.confidence) AS avg_confidence;

-- Ordenar por número de incidentes (descendente)
sorted_comuna_stats = ORDER comuna_stats BY total_incidents DESC;
STORE sorted_comuna_stats INTO '/output/comuna_analysis' USING PigStorage(',');

-- === ANÁLISIS 2: Análisis por Tipo de Incidente ===
incidents_by_type = GROUP unique_data BY incident_type;
type_stats = FOREACH incidents_by_type GENERATE
    group AS incident_type,
    COUNT(unique_data) AS total_count,
    AVG(unique_data.reliability) AS avg_reliability;

-- Ordenar por frecuencia (descendente)
sorted_type_stats = ORDER type_stats BY total_count DESC;
STORE sorted_type_stats INTO '/output/type_analysis' USING PigStorage(',');

-- === ANÁLISIS 3: Análisis Temporal ===
-- Extraer hora del campo dateTime (formato ISO: YYYY-MM-DDTHH:MM:SS)
temporal_data = FOREACH unique_data GENERATE
    id,
    incident_type,
    comuna,
    -- Extraer hora del día usando REGEX
    (int)REGEX_EXTRACT(dateTime, '.*T(\\d{2}):', 1) AS hour_of_day,
    dateTime,
    reliability;

-- Filtrar registros con hora válida
temporal_filtered = FILTER temporal_data BY 
    hour_of_day IS NOT NULL AND 
    hour_of_day >= 0 AND 
    hour_of_day <= 23;

-- Agrupar por hora del día
incidents_by_hour = GROUP temporal_filtered BY hour_of_day;
hourly_stats = FOREACH incidents_by_hour GENERATE
    group AS hour,
    COUNT(temporal_filtered) AS incidents_count;

-- Ordenar por hora
sorted_hourly_stats = ORDER hourly_stats BY hour;
STORE sorted_hourly_stats INTO '/output/temporal_analysis' USING PigStorage(',');

-- === ANÁLISIS 4: Top Comunas Críticas ===
-- Filtrar comunas con suficientes datos para análisis estadístico
significant_comunas = FILTER comuna_stats BY total_incidents >= 10;

-- Ordenar por múltiples criterios: cantidad de incidentes y baja confiabilidad
critical_comunas = ORDER significant_comunas BY total_incidents DESC, avg_confidence ASC;
STORE critical_comunas INTO '/output/top_critical_comunas' USING PigStorage(',');

-- === ANÁLISIS 5: Análisis Geográfico por Zona ===
-- Crear grid de coordenadas (redondear a 2 decimales para mayor precisión)
geo_data = FOREACH unique_data GENERATE
    incident_type,
    comuna,
    ROUND(latitude * 100) / 100.0 AS lat_zone,
    ROUND(longitude * 100) / 100.0 AS lng_zone,
    reliability,
    confidence;

-- Agrupar por zona geográfica
incidents_by_zone = GROUP geo_data BY (lat_zone, lng_zone);
zone_stats = FOREACH incidents_by_zone GENERATE
    FLATTEN(group) AS (latitude, longitude),
    COUNT(geo_data) AS incident_density,
    AVG(geo_data.reliability) AS avg_reliability,
    AVG(geo_data.confidence) AS avg_confidence;

-- Filtrar zonas con alta densidad (más de 3 incidentes)
high_density_zones = FILTER zone_stats BY incident_density > 3;

-- Ordenar por densidad
sorted_zones = ORDER high_density_zones BY incident_density DESC;
STORE sorted_zones INTO '/output/geographic_analysis' USING PigStorage(',');

-- === ANÁLISIS 6: Resumen Estadístico Completo ===
all_stats = FOREACH (GROUP unique_data ALL) GENERATE
    COUNT(unique_data) AS total_incidents,
    COUNT(DISTINCT unique_data.comuna) AS unique_comunas,
    COUNT(DISTINCT unique_data.incident_type) AS unique_types,
    AVG(unique_data.reliability) AS overall_avg_reliability,
    AVG(unique_data.confidence) AS overall_avg_confidence,
    MIN(unique_data.latitude) AS min_latitude,
    MAX(unique_data.latitude) AS max_latitude,
    MIN(unique_data.longitude) AS min_longitude,
    MAX(unique_data.longitude) AS max_longitude;

STORE all_stats INTO '/output/summary_statistics' USING PigStorage(',');