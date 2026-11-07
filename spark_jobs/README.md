# Jobs Spark

| Fichier | Role | Responsable |
| --- | --- | --- |
| `common.py`        | Chemins HDFS, schema des messages, colonnes constantes | partage |
| `bronze_ingest.py` | Kafka -> zone Bronze (HDFS), Structured Streaming | Bader Alouli (partie 2) |
| `silver_hudi.py`   | Bronze -> table Hudi Silver (par moteur et cycle, upserts) | Bader Alouli (partie 2) |
| `hudi_queries.py`  | Demo Hudi : snapshot, historique, incrementale, time travel | Bader Alouli (partie 2) |
| `features.py`      | Variables glissantes (Spark SQL, fenetres 5/10/20 cycles) | Mohamed Bouabidi (partie 3) |
| `train_models.py`  | MLlib : GBTRegressor (RUL) + classification de l'etat | Mohamed Bouabidi (partie 3) |
| `gold_scoring.py`  | Silver -> table Hudi Gold (RUL predite, etat de sante) | Mohamed Bouabidi (partie 3) |

## Lancer un job

```powershell
docker compose exec spark-master spark-submit /opt/project/spark_jobs/<fichier>.py
```
Suivi des jobs : http://localhost:8082 - fichiers dans HDFS : http://localhost:9870 (Utilities > Browse).
