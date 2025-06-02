-- Script de Pig para procesar incidentes de tráfico de Waze
-- Tarea 2 - Sistemas Distribuidos

-- Registrar funciones UDF si las necesitamos
-- REGISTER /opt/pig/lib/piggybank.jar;

-- Cargar datos desde CSV
raw_data = LOAD '/input/waze_incidents.csv' 
    USING PigStorage(',') 
    AS (id:chararray, type:chararray, subtype:chararray, uuid:chararray, 
        pubMillis:long, dateTime:chararray, country:chararray, state:chararray, 
        city:chararray, street:chararray, magvar:int, reliability:int,
        reportDescription:chararray, reportRating:int, confidence:int, 
        nComments:int, latitude:double, longitude:double, x:double, y:double);

-- Filtrar y limpiar datos
-- Eliminar la cabecera del CSV
filtered_data = FILTER raw_data BY id != '_id';

-- Filtrar registros válidos (que tengan coordenadas y tipo)
clean_data = FILTER filtered_data BY 
    latitude IS NOT NULL AND 
    longitude IS NOT NULL AND 
    type IS NOT NULL AND 
    type != '';

-- Estandarizar tipos de incidentes
standardized_data = FOREACH clean_data GENERATE
    id,
    (type == 'Alert' ? 'ALERT' : 
     type == 'Jam' ? 'JAM' : 
     UPPER(type)) AS incident_type,
    subtype,
    city,
    state,
    latitude,
    longitude,
    dateTime,
    reliability,
    reportDescription;

-- === ANÁLISIS 1: Agrupar incidentes por comuna/ciudad ===
incidents_by_city = GROUP standardized_data BY city;
city_stats = FOREACH incidents_by_city GENERATE
    group AS city,
    COUNT(standardized_data) AS total_incidents,
    AVG(standardized_data.reliability) AS avg_reliability;

-- Ordenar por número de incidentes (descendente)
sorted_city_stats = ORDER city_stats BY total_incidents DESC;

-- Guardar resultados
STORE sorted_city_stats INTO '/output/incidents_by_city' USING PigStorage(',');

-- === ANÁLISIS 2: Contar frecuencia por tipo de incidente ===
incidents_by_type = GROUP standardized_data BY incident_type;
type_stats = FOREACH incidents_by_type GENERATE
    group AS incident_type,
    COUNT(standardized_data) AS frequency;

-- Ordenar por frecuencia (descendente)
sorted_type_stats = ORDER type_stats BY frequency DESC;

-- Guardar resultados
STORE sorted_type_stats INTO '/output/incidents_by_type' USING PigStorage(',');

-- === ANÁLISIS 3: Análisis temporal ===
-- Extraer información temporal del campo dateTime
temporal_data = FOREACH standardized_data GENERATE
    id,
    incident_type,
    city,
    -- Extraer hora del día (asumiendo formato ISO)
    REGEX_EXTRACT(dateTime, '.*T(\\d{2}):', 1) AS hour_of_day,
    -- Extraer día de la semana (simplificado)
    SUBSTRING(dateTime, 0, 10) AS date_part,
    latitude,
    longitude;

-- Filtrar registros con hora válida
temporal_filtered = FILTER temporal_data BY hour_of_day IS NOT NULL;

-- Agrupar por hora del día
incidents_by_hour = GROUP temporal_filtered BY hour_of_day;
hourly_stats = FOREACH incidents_by_hour GENERATE
    group AS hour,
    COUNT(temporal_filtered) AS incidents_count;

-- Ordenar por hora
sorted_hourly_stats = ORDER hourly_stats BY hour;

-- Guardar resultados
STORE sorted_hourly_stats INTO '/output/incidents_by_hour' USING PigStorage(',');

-- === ANÁLISIS 4: Incidentes por tipo y ciudad ===
city_type_data = FOREACH standardized_data GENERATE
    city,
    incident_type,
    reliability;

incidents_by_city_type = GROUP city_type_data BY (city, incident_type);
city_type_stats = FOREACH incidents_by_city_type GENERATE
    FLATTEN(group) AS (city, incident_type),
    COUNT(city_type_data) AS incident_count,
    AVG(city_type_data.reliability) AS avg_reliability;

-- Ordenar por ciudad y luego por conteo
sorted_city_type_stats = ORDER city_type_stats BY city, incident_count DESC;

-- Guardar resultados
STORE sorted_city_type_stats INTO '/output/incidents_by_city_and_type' USING PigStorage(',');

-- === ANÁLISIS 5: Zonas de alta concentración ===
-- Crear grid de coordenadas para identificar áreas de alta densidad
-- Redondear coordenadas a 3 decimales para crear "grid cells"
grid_data = FOREACH standardized_data GENERATE
    incident_type,
    city,
    ROUND(latitude * 1000) / 1000.0 AS lat_grid,
    ROUND(longitude * 1000) / 1000.0 AS lng_grid,
    reliability;

-- Agrupar por grid
incidents_by_grid = GROUP grid_data BY (lat_grid, lng_grid);
grid_stats = FOREACH incidents_by_grid GENERATE
    FLATTEN(group) AS (latitude, longitude),
    COUNT(grid_data) AS incident_density,
    AVG(grid_data.reliability) AS avg_reliability;

-- Filtrar solo áreas con alta densidad (más de 5 incidentes)
high_density_areas = FILTER grid_stats BY incident_density > 5;

-- Ordenar por densidad
sorted_density = ORDER high_density_areas BY incident_density DESC;

-- Guardar resultados
STORE sorted_density INTO '/output/high_density_areas' USING PigStorage(',');

-- === ANÁLISIS 6: Resumen estadístico general ===
all_stats = FOREACH (GROUP standardized_data ALL) GENERATE
    COUNT(standardized_data) AS total_incidents,
    COUNT(DISTINCT standardized_data.city) AS unique_cities,
    COUNT(DISTINCT standardized_data.incident_type) AS unique_types,
    AVG(standardized_data.reliability) AS overall_avg_reliability;

-- Guardar resumen
STORE all_stats INTO '/output/general_statistics' USING PigStorage(',');