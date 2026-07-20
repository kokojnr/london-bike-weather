FROM apache/airflow:2.9.2

# Switch to root to perform installations
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jre-headless \
       wget \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Create directory and download GCS Connector
# This ensures Spark can always find the JAR file
RUN mkdir -p /opt/spark/jars && \
    wget -q https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.22.jar \
    -O /opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar && \
    chmod 644 /opt/spark/jars/gcs-connector-hadoop3-2.2.22.jar

# Set environment variable for Spark Classpath
ENV SPARK_CLASSPATH=/opt/spark/jars/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
