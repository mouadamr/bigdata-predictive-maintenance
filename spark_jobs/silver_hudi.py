"""
Partie 2 - Zone Silver : Bronze -> table Hudi `silver_mesures`.

- decode le JSON brut de la zone Bronze
- nettoie : lignes invalides retirees, colonnes constantes supprimees
- ecrit en UPSERT dans une table Hudi Copy-on-Write
    cle d'enregistrement  = (engine_id, cycle)   -> une ligne par moteur et cycle
    champ de precombine   = kafka_ts_ms          -> en cas de doublon, la plus recente gagne
    partition             = dataset              -> FD001_train / FD001_test

Relancer le job ne cree pas de doublons : Hudi met a jour les lignes existantes.

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/silver_hudi.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from common import (BRONZE_PATH, CONSTANT_COLUMNS, MESSAGE_SCHEMA,  # noqa: E402
                    SENSORS, SETTINGS, SILVER_PATH)

HUDI_OPTIONS = {
    "hoodie.table.name": "silver_mesures",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": "engine_id,cycle",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.precombine.field": "kafka_ts_ms",
    "hoodie.datasource.write.partitionpath.field": "dataset",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.upsert.shuffle.parallelism": "2",
    "hoodie.insert.shuffle.parallelism": "2",
}


def build_silver(spark: SparkSession):
    bronze = spark.read.parquet(BRONZE_PATH)

    parsed = (bronze
              .withColumn("m", F.from_json("value", MESSAGE_SCHEMA))
              .select("m.*", "partition", "offset", "kafka_timestamp"))

    measures = [c for c in SETTINGS + SENSORS if c not in CONSTANT_COLUMNS]
    clean = (parsed
             .filter(F.col("engine_id").isNotNull() & F.col("cycle").isNotNull() & (F.col("cycle") > 0))
             .dropna(subset=measures)                       # mesure manquante -> ligne rejetee
             .withColumn("event_ts", F.to_timestamp("event_time"))
             .withColumn("kafka_ts_ms", F.expr("unix_millis(kafka_timestamp)"))
             .withColumn("dataset", F.coalesce(F.col("dataset"), F.lit("inconnu")))
             .select("engine_id", "cycle", *measures, "dataset", "event_ts",
                     "kafka_ts_ms", F.col("partition").alias("kafka_partition"),
                     F.col("offset").alias("kafka_offset")))
    return parsed, clean


def main() -> None:
    spark = SparkSession.builder.appName("silver_hudi").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    parsed, clean = build_silver(spark)
    n_in, n_ok = parsed.count(), clean.count()
    print(f"Bronze : {n_in} messages | valides : {n_ok} | rejetes : {n_in - n_ok}")

    clean.write.format("hudi").options(**HUDI_OPTIONS).mode("append").save(SILVER_PATH)

    silver = spark.read.format("hudi").load(SILVER_PATH)
    print(f"Table Silver {SILVER_PATH} : {silver.count()} lignes (moteur, cycle) uniques")
    silver.groupBy("dataset").agg(F.countDistinct("engine_id").alias("moteurs"),
                                  F.count("*").alias("lignes")).show()


if __name__ == "__main__":
    main()
