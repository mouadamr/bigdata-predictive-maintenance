"""
Partie 3 - Variables glissantes (Spark SQL) : Silver -> table Hudi `silver_features`.

Pour chaque moteur et chaque cycle, on calcule avec des fonctions de fenetre SQL
la moyenne et l'ecart-type de 14 capteurs sur les 5, 10 et 20 derniers cycles.
Ces moyennes lissent le bruit des capteurs et montrent la tendance de degradation.

On ajoute aussi les labels :
  rul        = cycles restants avant la panne
               train : dernier cycle du moteur - cycle courant
               test  : dernier cycle observe + RUL reelle (RUL_FD001.txt) - cycle courant
  rul_capped = min(rul, 125)                      -> cible du GBTRegressor
  etat_label = 0 Sain / 1 A surveiller / 2 Critique -> cible du classifieur

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/features.py
"""
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from common import SILVER_PATH  # noqa: E402
from ml_common import (FEATURE_SENSORS, FEATURES_TABLE_PATH, FLEET_DATASET,  # noqa: E402
                       RUL_CAP, RUL_FILE, SEUIL_CRITIQUE, SEUIL_SURVEILLANCE,
                       TRAIN_DATASET, WINDOWS, drop_hudi_meta, hudi_options)


def rolling_features_sql(view: str = "silver") -> str:
    """Construit la requete Spark SQL des variables glissantes."""
    aggregates = []
    for s in FEATURE_SENSORS:
        for w in WINDOWS:
            aggregates.append(f"AVG({s}) OVER w{w} AS {s}_mean_{w}")
            aggregates.append(f"COALESCE(STDDEV_SAMP({s}) OVER w{w}, 0.0) AS {s}_std_{w}")
    windows = [f"w{w} AS (PARTITION BY dataset, engine_id ORDER BY cycle "
               f"ROWS BETWEEN {w - 1} PRECEDING AND CURRENT ROW)" for w in WINDOWS]
    sep = ",\n       "
    return (
        "SELECT dataset, engine_id, cycle,\n       "
        + sep.join(FEATURE_SENSORS) + sep + sep.join(aggregates) + ",\n"
        "       MAX(cycle) OVER (PARTITION BY dataset, engine_id) AS max_cycle\n"
        f"FROM {view}\n"
        "WINDOW " + ",\n       ".join(windows)
    )


def load_true_rul(spark: SparkSession, path: str = RUL_FILE):
    """RUL reelle a la fin de chaque serie de test (1 ligne par moteur, dans l'ordre)."""
    if not os.path.exists(path):
        print(f"ATTENTION : {path} introuvable, les moteurs de test n'auront pas de RUL reelle")
        return None
    with open(path) as f:
        values = [int(line.split()[0]) for line in f if line.strip()]
    rows = [(i + 1, v) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "engine_id INT, rul_fin INT").withColumn("dataset", F.lit(FLEET_DATASET))


def add_labels(feat: DataFrame, true_rul) -> DataFrame:
    if true_rul is not None:
        feat = feat.join(true_rul, ["dataset", "engine_id"], "left")
    else:
        feat = feat.withColumn("rul_fin", F.lit(None).cast("int"))

    rul = (F.when(F.col("dataset") == TRAIN_DATASET, F.col("max_cycle") - F.col("cycle"))
           .otherwise(F.col("max_cycle") + F.col("rul_fin") - F.col("cycle")))
    etat = (F.when(F.col("rul") <= SEUIL_CRITIQUE, 2.0)
            .when(F.col("rul") <= SEUIL_SURVEILLANCE, 1.0)
            .otherwise(0.0))
    return (feat
            .withColumn("rul", rul.cast("double"))
            .withColumn("rul_capped", F.least(F.col("rul"), F.lit(float(RUL_CAP))))
            .withColumn("etat_label", F.when(F.col("rul").isNotNull(), etat))
            .withColumn("calc_ts_ms", F.lit(int(time.time() * 1000)).cast("long"))
            .drop("rul_fin"))


def build_features(spark: SparkSession, silver: DataFrame, true_rul) -> DataFrame:
    silver.createOrReplaceTempView("silver")
    feat = spark.sql(rolling_features_sql("silver"))
    return add_labels(feat, true_rul)


def main() -> None:
    spark = SparkSession.builder.appName("features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver = drop_hudi_meta(spark.read.format("hudi").load(SILVER_PATH))
    print("Requete Spark SQL des variables glissantes :\n" + rolling_features_sql()[:600] + "\n  ...")

    feat = build_features(spark, silver, load_true_rul(spark))
    (feat.write.format("hudi")
     .options(**hudi_options("silver_features", "engine_id,cycle", "calc_ts_ms"))
     .mode("append").save(FEATURES_TABLE_PATH))

    out = spark.read.format("hudi").load(FEATURES_TABLE_PATH)
    print(f"Table {FEATURES_TABLE_PATH} : {out.count()} lignes, {len(feat.columns)} colonnes")
    out.groupBy("dataset").agg(F.countDistinct("engine_id").alias("moteurs"),
                               F.count("*").alias("lignes")).show()
    (out.filter((F.col("dataset") == TRAIN_DATASET) & (F.col("engine_id") == 1))
     .orderBy(F.desc("cycle"))
     .select("engine_id", "cycle", "s11", "s11_mean_5", "s11_mean_20", "rul", "rul_capped", "etat_label")
     .show(5))


if __name__ == "__main__":
    main()
