# Partie 4 - Orchestration Airflow
**Responsable : Youssef Abouelmahassine**

DAG `retrain_and_report` (chaque lundi a 2 h, ou bouton Trigger) :

ingestion_bronze -> silver_hudi -> controle_qualite -> variables_glissantes
-> reentrainement -> scoring_gold -> export_postgres -> rapport_maintenance

- `retrain_and_report.py` : definition du DAG (jobs Spark soumis avec spark-submit)
- `maintenance_report.py` : rapport Markdown + CSV des moteurs a traiter dans `reports/`
- `../spark_jobs/quality_check.py` : controles de la table Silver (echec bloquant = arret du DAG)

Interface : http://localhost:8088 (admin / admin)
