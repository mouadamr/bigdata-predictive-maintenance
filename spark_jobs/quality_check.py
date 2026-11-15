"""
Partie 4 - Controle qualite de la table Silver, avant le reentrainement.

Controles BLOQUANTS (le job echoue -> la tache Airflow devient rouge et le DAG
s'arrete, on n'entraine pas un modele sur des donnees fausses) :
  - la table n'est pas vide et contient les jeux FD001_train et FD001_test
  - aucune cle (engine_id, cycle) manquante
  - aucun doublon (dataset, engine_id, cycle)
Controles d'AVERTISSEMENT (signales mais non bloquants) :
  - cycles manquants dans la serie d'un moteur (flux interrompu)
  - valeurs de capteurs negatives ou manquantes

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/quality_check.py
"""
import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from common import CONSTANT_COLUMNS, SENSORS, SILVER_PATH  # noqa: E402
from ml_common import FLEET_DATASET, TRAIN_DATASET, drop_hudi_meta  # noqa: E402


def run_checks(silver: DataFrame) -> list:
    """Retourne une liste de (controle, bloquant, ok, detail)."""
    results = []
    n = silver.count()
    results.append(("table non vide", True, n > 0, f"{n} lignes"))

    datasets = {r[0] for r in silver.select("dataset").distinct().collect()}
    missing = {TRAIN_DATASET, FLEET_DATASET} - datasets
    results.append(("jeux train et test presents", True, not missing,
                    f"presents : {sorted(datasets)}" + (f", manquants : {sorted(missing)}" if missing else "")))

    null_keys = silver.filter(F.col("engine_id").isNull() | F.col("cycle").isNull()).count()
    results.append(("cles (engine_id, cycle) renseignees", True, null_keys == 0, f"{null_keys} lignes sans cle"))

    distinct = silver.select("dataset", "engine_id", "cycle").distinct().count()
    results.append(("aucun doublon (moteur, cycle)", True, distinct == n, f"{n - distinct} doublons"))

    per_engine = silver.groupBy("dataset", "engine_id").agg(
        F.min("cycle").alias("cmin"), F.max("cycle").alias("cmax"), F.count("*").alias("n"))
    gaps = per_engine.filter((F.col("cmin") != 1) | (F.col("cmax") != F.col("n"))).count()
    results.append(("series de cycles completes", False, gaps == 0, f"{gaps} moteurs avec des cycles manquants"))

    sensors = [s for s in SENSORS if s not in CONSTANT_COLUMNS and s in silver.columns]
    suspect = reduce(lambda a, b: a | b, [F.col(s).isNull() | (F.col(s) < 0) for s in sensors])
    bad = silver.filter(suspect).count()
    results.append(("capteurs renseignes et positifs", False, bad == 0, f"{bad} lignes suspectes"))
    return results


def main() -> None:
    spark = SparkSession.builder.appName("quality_check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    silver = drop_hudi_meta(spark.read.format("hudi").load(SILVER_PATH)).cache()

    results = run_checks(silver)
    print("\n========== CONTROLE QUALITE - TABLE SILVER ==========")
    for name, blocking, ok, detail in results:
        status = "OK   " if ok else ("ECHEC" if blocking else "ALERTE")
        print(f"  [{status}] {name:<38} {detail}")
    failed = [r for r in results if r[1] and not r[2]]
    if failed:
        print(f"\n{len(failed)} controle(s) bloquant(s) en echec : arret du pipeline.")
        sys.exit(1)
    print("\nDonnees Silver valides : le reentrainement peut continuer.")


if __name__ == "__main__":
    main()
