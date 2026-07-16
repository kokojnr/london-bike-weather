FROM apache/airflow:2.9.2

# Switch to root to install Java for Spark
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     default-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to airflow user to install Python packages
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt