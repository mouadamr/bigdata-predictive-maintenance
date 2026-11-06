# Maintenance predictive de moteurs industriels

Projet Big Data - sujet **ISSE-02** (Industrie 4.0).
Les mesures des capteurs de turboreacteurs (NASA C-MAPSS) sont ingerees en flux,
stockees en couches Bronze / Silver / Gold, et un modele Spark MLlib predit la
duree de vie restante (RUL) de chaque moteur pour planifier la maintenance.

## Architecture

```
C-MAPSS -> Producteur Python -> Kafka (capteurs_moteurs) -> Spark Structured Streaming
        -> Bronze (HDFS) -> Silver (Hudi) -> Gold (Hudi : RUL + etat de sante)
        -> PostgreSQL -> Grafana            orchestration : Airflow
```

## Equipe

| Membre | Partie | Dossiers |
| --- | --- | --- |
| Mouad AAMER | 1. Sources de donnees et ingestion Kafka | `producer/`, `notebooks/` |
| Bader Alouli | 2. Data Lake (HDFS) et Lakehouse Hudi | `spark_jobs/bronze_ingest.py`, `spark_jobs/silver_hudi.py` |
| Mohamed Bouabidi | 3. Traitements Spark (SQL, DataFrames, MLlib) | `spark_jobs/features.py`, `train_models.py`, `gold_scoring.py` |
| Youssef Abouelmahassine | 4. Orchestration Airflow et dashboard Grafana | `dags/`, `grafana/` |
| Tous | Docker Compose et integration | `docker-compose.yml`, `docker/` |

## Prerequis

- Docker Desktop (WSL2), 10 Go de RAM alloues a Docker
- Python 3.11 (pour le producteur), Git

## Donnees

Telecharger **NASA C-MAPSS** (Turbofan Engine Degradation Simulation) et placer dans `data/raw/` :
`train_FD001.txt`, `test_FD001.txt`, `RUL_FD001.txt`.

## Lancement

```bash
docker compose build        # premiere fois seulement (10-20 min)
docker compose up -d        # demarre toute la plateforme
docker compose ps           # etat des services
docker compose down         # arret (les donnees sont conservees)
```

| Service | URL | Identifiants |
| --- | --- | --- |
| Kafka UI | http://localhost:8080 | - |
| Spark Master | http://localhost:8082 | - |
| HDFS (NameNode) | http://localhost:9870 | - |
| Airflow | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

## Arborescence

```
docker-compose.yml     plateforme complete
docker/                images Spark et Airflow, config Spark, HDFS, PostgreSQL, Grafana
data/raw/              fichiers C-MAPSS (non versionnes)
producer/              producteur Kafka (partie 1)
spark_jobs/            jobs Spark Bronze / Silver / Gold / ML (parties 2 et 3)
dags/                  DAG Airflow (partie 4)
grafana/dashboards/    tableaux de bord (partie 4)
notebooks/             exploration des donnees
reports/               rapports de maintenance generes
docs/                  cahier des charges, rapport, presentation
```
