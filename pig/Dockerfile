FROM openjdk:8-jdk-slim

# Instalar dependencias básicas
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    bash \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Variables de entorno
ENV HADOOP_VERSION=3.2.4
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV JAVA_HOME=/usr/local/openjdk-8
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PIG_HOME/bin

# Descargar e instalar Hadoop
RUN cd /tmp && \
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Descargar e instalar Pig
RUN cd /tmp && \
    wget -q https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz && \
    tar -xzf pig-${PIG_VERSION}.tar.gz && \
    mv pig-${PIG_VERSION} ${PIG_HOME} && \
    rm pig-${PIG_VERSION}.tar.gz

# Configurar Hadoop para modo standalone/pseudo-distribuido
RUN echo '<?xml version="1.0" encoding="UTF-8"?>' > ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '<configuration>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml && \
    echo '</configuration>' >> ${HADOOP_HOME}/etc/hadoop/core-site.xml

RUN echo '<?xml version="1.0" encoding="UTF-8"?>' > ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '<configuration>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '<property><n>dfs.replication</n><value>1</value></property>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml && \
    echo '</configuration>' >> ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

# Configurar variables de entorno de Hadoop
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# Crear directorios necesarios
RUN mkdir -p /scripts /data/processed /tmp/pig && \
    chmod 755 /scripts /data/processed /tmp/pig

# Configurar directorio temporal para Pig
ENV PIG_TEMP_DIR=/tmp/pig

# Establecer directorio de trabajo
WORKDIR /scripts

# Verificar instalación
RUN echo "Verificando instalaciones..." && \
    java -version && \
    hadoop version && \
    pig -version

# Comando por defecto
CMD ["bash"]
