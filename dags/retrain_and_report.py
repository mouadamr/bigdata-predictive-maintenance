"""
### DAG `retrain_and_report` - reentrainement et rapport de maintenance

Partie 4 - orchestration Airflow. Enchaine toute la chaine batch, chaque lundi
a 2 h (ou a la demande avec le bouton Trigger) :

1. **ingestion_bronze** : lit les nouveaux messages Kafka -> zone Bronze (puis s'arrete)
2. **silver_hudi** : Bronze -> table Hudi Silver (upserts)
3. **controle_qualite** : controles de la table Silver ; un echec arrete le DAG
4. **variables_glissantes** : Spark SQL -> table Hudi `silver_features`
5. **reentrainement** : GBTRegressor (RUL) + RandomForest (etat) -> nouveaux modeles
6. **scoring_gold** : RUL predite et etat de sante -> tables Hudi Gold
7. **export_postgres** : Gold -> PostgreSQL (lu par Grafana)
8. **rapport_maintenance** : rapport Markdown + CSV des moteurs a traiter dans `reports/`

Les jobs Spark sont soumis au cluster `spark://spark-master:7077` ; le driver
tourne dans le conteneur `airflow-scheduler`.
Arreter le job Bronze en continu avant de lancer ce DAG (meme checkpoint).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # module voisin du DAG
import maintenance_report  # noqa: E402

JOBS = "/opt/project/spark_jobs"
SPARK_SUBMIT = ("spark-submit --master spark://spark-master:7077 "
                "--conf spark.driver.host=airflow-scheduler")


def spark_job(task_id: str, script: str, args: str = "", doc: str = "") -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"{SPARK_SUBMIT} {JOBS}/{script} {args}".strip(),
                        doc_md=doc)


default_args = {
    "owner": "youssef",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="retrain_and_report",
    description="Reentrainement des modeles RUL / etat et rapport de maintenance",
    doc_md=__doc__,
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * 1",          # chaque lundi a 2 h
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["maintenance", "spark", "mllib", "hudi"],
) as dag:
    ingestion = spark_job("ingestion_bronze", "bronze_ingest.py", "--once",
                          "Kafka `capteurs_moteurs` -> Bronze (messages en attente seulement)")
    silver = spark_job("silver_hudi", "silver_hudi.py", doc="Bronze -> Hudi Silver (upsert)")
    qualite = spark_job("controle_qualite", "quality_check.py",
                        doc="Controles de la table Silver ; echec = arret du DAG")
    features = spark_job("variables_glissantes", "features.py", doc="Spark SQL : fenetres 5/10/20 cycles")
    entrainement = spark_job("reentrainement", "train_models.py", doc="GBTRegressor + RandomForest")
    scoring = spark_job("scoring_gold", "gold_scoring.py", doc="RUL predite et etat -> Gold")
    export = spark_job("export_postgres", "export_postgres.py", doc="Gold -> PostgreSQL pour Grafana")
    rapport = PythonOperator(
        task_id="rapport_maintenance",
        python_callable=maintenance_report.generer_rapport,
        op_kwargs={"date": "{{ ds }}", "run_id": "{{ run_id }}"},
        doc_md="Rapport Markdown + CSV des moteurs a traiter dans `reports/`",
    )

    ingestion >> silver >> qualite >> features >> entrainement >> scoring >> export >> rapport
