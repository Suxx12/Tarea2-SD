REGISTER '/opt/pig/lib/piggybank.jar';

-- Carga datos desde ruta relativa
waze_raw = LOAD 'data/export/waze_incidents.csv' USING PigStorage(',') AS (
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

waze_sin_encabezado = FILTER waze_raw BY id != '_id';

waze_filtrado = FILTER waze_sin_encabezado BY 
    (id IS NOT NULL AND id != '') AND
    (type IS NOT NULL AND type != '' AND type != 'NONE') AND
    (latitude IS NOT NULL AND latitude != 0.0) AND
    (longitude IS NOT NULL AND longitude != 0.0);

waze_limpio = FOREACH waze_filtrado GENERATE
    id,
    (type == 'JAM' ? 'ATASCO' :
     (type == 'HAZARD' ? 'PELIGRO' :
     (type == 'ACCIDENT' ? 'ACCIDENTE' :
     (type == 'POLICE' ? 'POLICIA' : UPPER(type))))) AS tipo_incidente,
    subtype,
    uuid,
    pubMillis,
    dateTime,
    country,
    state,
    (city IS NULL OR city == '' ? 'DESCONOCIDA' : city) AS comuna,
    street,
    (reliability IS NULL ? 0 : reliability) AS confiabilidad,
    reportDescription,
    reportRating,
    (confidence IS NULL ? 0 : confidence) AS confianza,
    nComments,
    latitude,
    longitude,
    x,
    y;

STORE waze_limpio INTO 'data/processed/waze_limpio/waze_filtrado' USING PigStorage(',');
