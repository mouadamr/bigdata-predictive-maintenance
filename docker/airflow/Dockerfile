# Image Airflow + Java 17 + PySpark 3.5.3 (pour lancer spark-submit depuis les DAG)
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" pyspark==3.5.3

COPY download_jars.sh /tmp/download_jars.sh
RUN SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" \
 && sh /tmp/download_jars.sh "${SPARK_HOME}/jars"
