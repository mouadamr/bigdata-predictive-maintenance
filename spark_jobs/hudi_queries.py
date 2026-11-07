"""
Partie 2 - Demonstration des fonctions Hudi sur la table Silver :
  1. requete snapshot   : etat actuel de la table
  2. historique         : liste des commits (timeline Hudi)
  3. requete incrementale : seulement les lignes modifiees depuis un commit
  4. time travel        : la table telle qu'elle etait au premier commit

Lancement :
    docker compose exec spark-master spark-submit /opt/project/spark_jobs/hudi_queries.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/project/spark_jobs")
from common import SILVER_PATH  # noqa: E402


def main() -> None:
    spark = SparkSession.builder.appName("hudi_queries").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== 1. Snapshot : nombre de lignes par jeu ===")
    snap = spark.read.format("hudi").load(SILVER_PATH)
    snap.groupBy("dataset").count().show()

    print("=== 2. Historique des commits ===")
    commits = [r[0] for r in snap.select("_hoodie_commit_time").distinct()
               .orderBy("_hoodie_commit_time").collect()]
    for c in commits:
        print("  commit", c)

    if commits:
        first = commits[0]
        print(f"\n=== 3. Incrementale : lignes ecrites apres le commit {first} ===")
        inc = (spark.read.format("hudi")
               .option("hoodie.datasource.query.type", "incremental")
               .option("hoodie.datasource.read.begin.instanttime", first)
               .load(SILVER_PATH))
        print(f"  {inc.count()} lignes")

        print(f"\n=== 4. Time travel : la table au commit {first} ===")
        old = spark.read.format("hudi").option("as.of.instant", first).load(SILVER_PATH)
        print(f"  {old.count()} lignes (contre {snap.count()} aujourd'hui)")

    print("\n=== Exemple : derniers cycles du moteur 1 ===")
    (snap.filter(F.col("engine_id") == 1).orderBy(F.desc("cycle"))
     .select("dataset", "engine_id", "cycle", "s2", "s3", "s4", "_hoodie_commit_time").show(5))


if __name__ == "__main__":
    main()
