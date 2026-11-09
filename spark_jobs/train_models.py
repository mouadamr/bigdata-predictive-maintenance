"""
Partie 3 - Entrainement des modeles Spark MLlib sur les moteurs FD001_train.

  1. GBTRegressor            -> predit la RUL (cycles restants, plafonnee a 125)
  2. RandomForestClassifier  -> predit l'etat : Sain / A surveiller / Critique
     (GBTClassifier de Spark ne gere que 2 classes ; nous en avons 3)

Evaluation honnete : validation sur 20 moteurs JAMAIS vus a l'entrainement
(decoupage par moteur, pas par ligne), puis test sur les 100 moteurs de
FD001_test compares a la RUL reelle fournie par la NASA (dernier cycle observe).

Les modeles finaux sont reentraines sur les 100 moteurs, puis sauvegardes dans
hdfs:///datalake/models/{gbt_rul,rf_etat}/<version> et .../latest.
Les metriques sont ajoutees a hdfs:///datalake/models/metrics (historique).

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/train_models.py
"""
import sys
from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (MulticlassClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from ml_common import (CLF_MODEL_PATH, ETATS, FEATURES_TABLE_PATH,  # noqa: E402
                       FLEET_DATASET, METRICS_PATH, REG_MODEL_PATH, TRAIN_DATASET,
                       drop_hudi_meta, feature_columns)


def regression_pipeline() -> Pipeline:
    return Pipeline(stages=[
        VectorAssembler(inputCols=feature_columns(), outputCol="features", handleInvalid="keep"),
        GBTRegressor(featuresCol="features", labelCol="rul_capped", predictionCol="rul_pred",
                     maxIter=60, maxDepth=5, stepSize=0.1, subsamplingRate=0.8, seed=42),
    ])


def classification_pipeline() -> Pipeline:
    return Pipeline(stages=[
        VectorAssembler(inputCols=feature_columns(), outputCol="features", handleInvalid="keep"),
        RandomForestClassifier(featuresCol="features", labelCol="etat_label", weightCol="poids",
                               predictionCol="etat_pred", probabilityCol="etat_proba",
                               rawPredictionCol="etat_raw", numTrees=60, maxDepth=8, seed=42),
    ])


def add_class_weights(df: DataFrame) -> DataFrame:
    """Poids inverse a la frequence de chaque classe (75 % Sain, 8 % Critique)."""
    counts = {r["etat_label"]: r["count"] for r in df.groupBy("etat_label").count().collect()}
    total = sum(counts.values())
    weights = {label: total / (len(counts) * n) for label, n in counts.items()}
    expr = F.create_map(*[x for label, w in weights.items() for x in (F.lit(label), F.lit(w))])
    return df.withColumn("poids", expr[F.col("etat_label")])


def split_by_engine(df: DataFrame):
    """20 % des moteurs (id multiple de 5) servent a la validation."""
    is_val = (F.col("engine_id") % 5) == 0
    return df.filter(~is_val), df.filter(is_val)


def last_cycle_per_engine(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("engine_id").orderBy(F.desc("cycle"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def regression_metrics(pred: DataFrame, label: str) -> dict:
    ev = RegressionEvaluator(labelCol=label, predictionCol="rul_pred")
    return {m: round(ev.evaluate(pred, {ev.metricName: m}), 2) for m in ("rmse", "mae", "r2")}


def classification_metrics(pred: DataFrame) -> dict:
    ev = MulticlassClassificationEvaluator(labelCol="etat_label", predictionCol="etat_pred")
    out = {m: round(ev.evaluate(pred, {ev.metricName: m}), 3) for m in ("f1", "accuracy")}
    out["recall_critique"] = round(ev.evaluate(pred, {ev.metricName: "recallByLabel",
                                                       ev.metricLabel: 2.0}), 3)
    return out


def show_confusion(pred: DataFrame) -> None:
    names = F.create_map(*[x for i, n in enumerate(ETATS) for x in (F.lit(float(i)), F.lit(n))])
    (pred.withColumn("reel", names[F.col("etat_label")])
     .withColumn("predit", names[F.col("etat_pred")])
     .groupBy("reel").pivot("predit", ETATS).count().na.fill(0)
     .orderBy(F.expr("CASE reel WHEN 'Sain' THEN 0 WHEN 'A surveiller' THEN 1 ELSE 2 END"))
     .show())


def evaluate(train: DataFrame, fleet: DataFrame) -> dict:
    """Entraine sur 80 moteurs, evalue sur 20 moteurs de validation et sur la flotte de test."""
    tr, val = split_by_engine(train)
    print(f"Entrainement : {tr.select('engine_id').distinct().count()} moteurs | "
          f"validation : {val.select('engine_id').distinct().count()} moteurs")

    reg = regression_pipeline().fit(tr)
    clf = classification_pipeline().fit(add_class_weights(tr))

    m = {}
    val_reg = reg.transform(val)
    for k, v in regression_metrics(val_reg, "rul_capped").items():
        m[f"val_{k}"] = v
    val_clf = clf.transform(val)
    for k, v in classification_metrics(val_clf).items():
        m[f"val_{k}"] = v
    print("Matrice de confusion (validation, toutes les lignes) :")
    show_confusion(val_clf)

    last = last_cycle_per_engine(fleet.filter(F.col("rul").isNotNull()))
    if last.count() > 0:
        test_reg = reg.transform(last)
        for k, v in regression_metrics(test_reg, "rul").items():
            m[f"test_{k}"] = v
        for k, v in classification_metrics(clf.transform(last)).items():
            m[f"test_{k}"] = v
    return m


def main() -> None:
    spark = SparkSession.builder.appName("train_models").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    version = datetime.now().strftime("%Y%m%d_%H%M%S")

    data = drop_hudi_meta(spark.read.format("hudi").load(FEATURES_TABLE_PATH))
    train = data.filter(F.col("dataset") == TRAIN_DATASET).cache()
    fleet = data.filter(F.col("dataset") == FLEET_DATASET).cache()
    print(f"Lignes d'entrainement : {train.count()} | lignes flotte : {fleet.count()} | "
          f"{len(feature_columns())} variables")

    metrics = evaluate(train, fleet)

    print("Entrainement final sur les 100 moteurs FD001_train...")
    reg = regression_pipeline().fit(train)
    clf = classification_pipeline().fit(add_class_weights(train))
    for model, path in ((reg, REG_MODEL_PATH), (clf, CLF_MODEL_PATH)):
        model.write().overwrite().save(f"{path}/{version}")
        model.write().overwrite().save(f"{path}/latest")

    importances = reg.stages[-1].featureImportances.toArray()
    top = sorted(zip(feature_columns(), importances), key=lambda x: -x[1])[:10]
    print("Variables les plus importantes (GBT) :")
    for name, imp in top:
        print(f"  {name:<14} {imp:.3f}")

    row = {"version": version, "date_entrainement": datetime.now().isoformat(timespec="seconds"),
           "n_lignes_train": train.count(), **metrics}
    spark.createDataFrame([row]).write.mode("append").parquet(METRICS_PATH)

    print("\n================ RESULTATS ================")
    for k, v in row.items():
        print(f"  {k:<22} {v}")
    print(f"Modeles sauvegardes : {REG_MODEL_PATH}/{version} et {CLF_MODEL_PATH}/{version}")


if __name__ == "__main__":
    main()
