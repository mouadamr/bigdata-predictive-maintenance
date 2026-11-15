"""
Partie 3 - Zone Gold : prediction de la RUL et de l'etat de sante de la flotte.

Charge les derniers modeles (models/*/latest), les applique aux moteurs en
service (FD001_test) et ecrit deux tables Hudi :

  gold_predictions    : une ligne par (moteur, cycle) -> evolution de la RUL predite
  gold_sante_moteurs  : une ligne par moteur, a son dernier cycle -> etat actuel de la flotte
                        (RUL predite, etat, probabilite d'etre critique, version du modele)

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/gold_scoring.py
"""
import sys
import time
from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from ml_common import (CLF_MODEL_PATH, ETATS, FEATURES_TABLE_PATH,  # noqa: E402
                       FLEET_DATASET, GOLD_PREDICTIONS_PATH, GOLD_SANTE_PATH,
                       METRICS_PATH, REG_MODEL_PATH, SEUIL_CRITIQUE,
                       SEUIL_SURVEILLANCE, drop_hudi_meta, hudi_options)


def score(fleet: DataFrame, reg: PipelineModel, clf: PipelineModel, version: str) -> DataFrame:
    names = F.create_map(*[x for i, n in enumerate(ETATS) for x in (F.lit(float(i)), F.lit(n))])
    pred = reg.transform(fleet).drop("features")
    pred = clf.transform(pred).drop("features")
    rul_pred = F.round(F.greatest(F.col("rul_pred"), F.lit(0.0)), 1)
    return pred.select(
        "dataset", "engine_id", "cycle",
        rul_pred.alias("rul_predite"),
        names[F.col("etat_pred")].alias("etat_predit"),
        F.round(vector_to_array("etat_proba")[2], 3).alias("proba_critique"),
        (F.when(rul_pred <= SEUIL_CRITIQUE, "Critique")
         .when(rul_pred <= SEUIL_SURVEILLANCE, "A surveiller")
         .otherwise("Sain")).alias("etat_selon_rul"),
        F.col("rul").alias("rul_reelle"),
        F.lit(version).alias("model_version"),
        F.current_timestamp().alias("date_calcul"),
        F.lit(int(time.time() * 1000)).cast("long").alias("calc_ts_ms"),
    )


def fleet_status(pred: DataFrame) -> DataFrame:
    """Etat actuel : la ligne du dernier cycle de chaque moteur."""
    w = Window.partitionBy("dataset", "engine_id").orderBy(F.desc("cycle"))
    return (pred.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
            .withColumnRenamed("cycle", "dernier_cycle")
            .withColumn("a_surveiller", F.col("etat_predit") != "Sain"))


def current_model_version(spark: SparkSession) -> str:
    try:
        return spark.read.parquet(METRICS_PATH).agg(F.max("version")).first()[0]
    except Exception:   # pas encore de metriques
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def main() -> None:
    spark = SparkSession.builder.appName("gold_scoring").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    reg = PipelineModel.load(f"{REG_MODEL_PATH}/latest")
    clf = PipelineModel.load(f"{CLF_MODEL_PATH}/latest")
    version = current_model_version(spark)

    fleet = drop_hudi_meta(spark.read.format("hudi").load(FEATURES_TABLE_PATH)) \
        .filter(F.col("dataset") == FLEET_DATASET)
    pred = score(fleet, reg, clf, version).cache()
    status = fleet_status(pred)

    (pred.write.format("hudi")
     .options(**hudi_options("gold_predictions", "engine_id,cycle", "calc_ts_ms"))
     .mode("append").save(GOLD_PREDICTIONS_PATH))
    (status.write.format("hudi")
     .options(**hudi_options("gold_sante_moteurs", "engine_id", "calc_ts_ms"))
     .mode("append").save(GOLD_SANTE_PATH))

    gold = spark.read.format("hudi").load(GOLD_SANTE_PATH)
    print(f"\nGold : {gold.count()} moteurs scores (modele {version})")
    print("Etat de la flotte :")
    gold.groupBy("etat_predit").count().orderBy(F.desc("count")).show()
    print("Moteurs les plus urgents :")
    (gold.orderBy("rul_predite")
     .select("engine_id", "dernier_cycle", "rul_predite", "etat_predit", "proba_critique", "rul_reelle")
     .show(10))


if __name__ == "__main__":
    main()
