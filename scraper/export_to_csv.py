import csv
import os
import json
from pymongo import MongoClient
from datetime import datetime

# Conexión a MongoDB
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
db_name = os.getenv("DB_NAME", "waze_data")
collection_name = os.getenv("COLLECTION_NAME", "waze_events")

# Ruta de salida
output_path = "/export/waze_incidents.csv"

print(f"[INFO] Conectando a MongoDB: {mongo_uri}")
print(f"[INFO] Base de datos: {db_name}")
print(f"[INFO] Colección: {collection_name}")

try:
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Verificar conexión
    client.admin.command('ping')
    print("[✓] Conexión a MongoDB exitosa")
    
    # Contar documentos
    total_docs = collection.count_documents({})
    print(f"[INFO] Total de documentos en la colección: {total_docs}")
    
    if total_docs == 0:
        print("[WARNING] No hay documentos para exportar")
        exit(0)
    
    # Campos que vamos a exportar (adaptados a la estructura de Waze)
    fields = [
        "_id", "type", "subtype", "uuid", "pubMillis", "dateTime",
        "country", "state", "city", "street", "magvar", "reliability",
        "reportDescription", "reportRating", "confidence", "nComments",
        "latitude", "longitude", "x", "y"
    ]
    
    print(f"[INFO] Exportando a: {output_path}")
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Extraer y escribir datos
    with open(output_path, mode="w", newline='', encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        
        documents = collection.find({})
        exported_count = 0
        
        for doc in documents:
            # Preparar el documento para CSV
            row = {}
            
            # Convertir ObjectId a string
            row["_id"] = str(doc.get("_id", ""))
            
            # Campos básicos
            row["type"] = doc.get("type", "")
            row["subtype"] = doc.get("subtype", "")
            row["uuid"] = doc.get("uuid", "")
            row["pubMillis"] = doc.get("pubMillis", "")
            row["dateTime"] = doc.get("dateTime", "")
            
            # Ubicación
            row["country"] = doc.get("country", "")
            row["state"] = doc.get("state", "")
            row["city"] = doc.get("city", "")
            row["street"] = doc.get("street", "")
            
            # Coordenadas - pueden estar en diferentes formatos
            location = doc.get("location", {})
            if isinstance(location, dict):
                row["latitude"] = location.get("y", "")
                row["longitude"] = location.get("x", "")
                row["x"] = location.get("x", "")
                row["y"] = location.get("y", "")
            else:
                row["latitude"] = doc.get("latitude", "") or doc.get("y", "")
                row["longitude"] = doc.get("longitude", "") or doc.get("x", "")
                row["x"] = doc.get("x", "")
                row["y"] = doc.get("y", "")
            
            # Otros campos
            row["magvar"] = doc.get("magvar", "")
            row["reliability"] = doc.get("reliability", "")
            row["reportDescription"] = doc.get("reportDescription", "")
            row["reportRating"] = doc.get("reportRating", "")
            row["confidence"] = doc.get("confidence", "")
            row["nComments"] = doc.get("nComments", "")
            
            writer.writerow(row)
            exported_count += 1
            
            if exported_count % 1000 == 0:
                print(f"[INFO] Exportados {exported_count} registros...")
    
    print(f"[✓] Exportación completada: {exported_count} registros exportados a {output_path}")
    
    # Verificar el archivo creado
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        print(f"[INFO] Tamaño del archivo: {file_size/1024:.2f} KB")
    
except Exception as e:
    print(f"[ERROR] Error durante la exportación: {e}")
    import traceback
    traceback.print_exc()
finally:
    try:
        client.close()
    except:
        pass