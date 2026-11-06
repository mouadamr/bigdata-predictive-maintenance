#!/bin/sh
# Telecharge les connecteurs Java necessaires a Spark dans le dossier donne :
#   Kafka (Structured Streaming), Hudi, MinIO/S3 (hadoop-aws), PostgreSQL (JDBC)
set -e
DEST="$1"
M=https://repo1.maven.org/maven2
for p in \
  org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar \
  org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
  org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
  org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
  org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
  org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
  org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
do
  echo "-> $(basename $p)"
  curl -fsSL -o "$DEST/$(basename $p)" "$M/$p"
done
