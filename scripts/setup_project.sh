version: '3.9'

services:
  mongo:
    image: mongo:5.0
    container_name: tarea2-sd-mongo
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db
      - ./data/export:/export
    restart: always

  exporter:
    build:
      context: ./scraper
    container_name: tarea2-sd-exporter
    environment:
      - MONGO_URI=mongodb://mongo:27017/
      - DB_NAME=waze_data
      - COLLECTION_NAME=waze_events
    depends_on:
      - mongo
    volumes:
      - ./scraper:/app
      - ./data/export:/export
    command: python export_to_csv.py

  pig-processor:
    build:
      context: ./pig
    container_name: tarea2-sd-pig-processor
    volumes:
      - ./data:/data
      - ./pig-scripts:/scripts
    working_dir: /scripts
    environment:
      - PIG_HOME=/opt/pig
      - HADOOP_HOME=/opt/hadoop
      - JAVA_HOME=/usr/local/openjdk-8
      - PIG_TEMP_DIR=/tmp/pig
      - HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
    depends_on:
      - exporter
    command: >
      bash -c "
        echo 'Esperando archivos de entrada...' &&
        while [ ! -f /data/export/waze_incidents.csv ]; do
          echo 'Esperando CSV...'
          sleep 5
        done &&
        echo 'Archivo CSV encontrado. Iniciando procesamiento con Apache Pig...' &&
        mkdir -p /data/processed &&
        echo 'Verificando archivos de entrada...' &&
        ls -la /data/export/ &&
        head -5 /data/export/waze_incidents.csv &&
        echo 'Ejecutando script de Pig...' &&
        pig -x local clean_and_classify_csv.pig &&
        echo 'Procesamiento completado. Listando resultados:' &&
        find /data/processed -type f -name 'part-*' | head -10
      "

  analyzer:
    image: python:3.9-slim
    container_name: tarea2-sd-analyzer
    volumes:
      - ./data:/data
      - ./analysis:/app
    working_dir: /app
    depends_on:
      - pig-processor
    command: >
      bash -c "
        echo 'Instalando dependencias para análisis...' &&
        pip install pandas matplotlib seaborn &&
        echo 'Esperando resultados del procesamiento...' &&
        while [ ! -d /data/processed/clean_incidents ]; do
          echo 'Esperando resultados de Pig...'
          sleep 10
        done &&
        echo 'Ejecutando análisis post-procesamiento...' &&
        python post_analysis.py
      "

volumes:
  mongo_data: