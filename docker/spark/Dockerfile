# Image Spark commune (master + worker) : Python 3.11 + Java 17 + Spark 3.5.3
# Meme version de Python que l'image Airflow -> Airflow peut soumettre les jobs.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java

RUN pip install --no-cache-dir pyspark==3.5.3 numpy==1.26.4 pandas==2.2.3

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY download_jars.sh /tmp/download_jars.sh
RUN sh /tmp/download_jars.sh "${SPARK_HOME}/jars"

WORKDIR /opt/project
