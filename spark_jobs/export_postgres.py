"""
Partie 5 - Restitution : export de la zone Gold vers PostgreSQL (lu par Grafana).

Grafana ne sait pas lire des tables Hudi stockees dans HDFS. On copie donc la
zone Gold dans la base PostgreSQL `maintenance`, via JDBC :

  flotte_sante     <- gold_sante_moteurs : une ligne par moteur (etat actuel)
  predictions_rul  <- gold_predictions   : RUL predite a chaque cycle (historique)
  model_metrics    <- models/metrics     : scores de chaque entrainement

Chaque export remplace le contenu des tables (TRUNCATE + INSERT) : PostgreSQL
est toujours une copie a jour de la zone Gold, et Grafana garde ses requetes.

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/export_postgres.py
"""
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from ml_common import (GOLD_PREDICTIONS_PATH, GOLD_SANTE_PATH,  # noqa: E402
                       METRICS_PATH, drop_hudi_meta)

JDBC_URL = "jdbc:postgresql://postgres:5432/maintenance"
JDBC_PROPS = {"user": "maintenance", "password": "maintenance", "driver": "org.postgresql.Driver"}


def flotte_sante(gold: DataFrame) -> DataFrame:
    """Etat actuel de chaque moteur + priorite (1 = le plus urgent) pour trier dans Grafana."""
    priorite = (F.when(F.col("etat_predit") == "Critique", 1)
                .when(F.col("etat_predit") == "A surveiller", 2)
                .otherwise(3))
    return gold.select(
        "engine_id", "dernier_cycle", "rul_predite", "etat_predit", "proba_critique",
        "etat_selon_rul", "rul_reelle", "a_surveiller", priorite.alias("priorite"),
        "model_version", "date_calcul")


def predictions_rul(pred: DataFrame) -> DataFrame:
    return pred.select("engine_id", "cycle", "rul_predite", "rul_reelle",
                       "etat_predit", "proba_critique", "model_version")


def model_metrics(metrics: DataFrame) -> DataFrame:
    return metrics.withColumn("date_entrainement", F.to_timestamp("date_entrainement"))


def write_table(df: DataFrame, table: str) -> int:
    (df.write.mode("overwrite")
     .option("truncate", "true")          # garde la table (et les requetes Grafana)
     .jdbc(JDBC_URL, table, properties=JDBC_PROPS))
    return df.count()


def main() -> None:
    spark = SparkSession.builder.appName("export_postgres").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    gold = drop_hudi_meta(spark.read.format("hudi").load(GOLD_SANTE_PATH))
    pred = drop_hudi_meta(spark.read.format("hudi").load(GOLD_PREDICTIONS_PATH))
    metrics = spark.read.parquet(METRICS_PATH)

    print("Export Gold -> PostgreSQL (base maintenance)")
    for name, df in (("flotte_sante", flotte_sante(gold)),
                     ("predictions_rul", predictions_rul(pred)),
                     ("model_metrics", model_metrics(metrics))):
        print(f"  {name:<16} {write_table(df, name):>6} lignes")

    # Verification : relecture depuis PostgreSQL
    check = spark.read.jdbc(JDBC_URL, "flotte_sante", properties=JDBC_PROPS)
    print("\nRelu depuis PostgreSQL - etat de la flotte :")
    check.groupBy("etat_predit").count().orderBy("etat_predit").show()


if __name__ == "__main__":
    main()
