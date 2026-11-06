# Partie 1 - Sources de donnees et ingestion Kafka
**Responsable : Mouad AAMER**

- Exploration du jeu NASA C-MAPSS FD001 (`notebooks/`)
- `producer.py` : rejeu de `test_FD001.txt` en flux -> topic `capteurs_moteurs`
  (1 message JSON par moteur et par cycle, cle = `engine_id`)
- Topic : 3 partitions (cree automatiquement par le service `kafka-init`)

## Utilisation

```powershell
cd producer
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

python producer.py --dry-run --limit 5     # test sans Kafka
python producer.py                         # rejeu de test_FD001 vers Kafka
python producer.py --loop                  # rejeu en boucle (demo)
```

Options : `--file`, `--delay` (s entre 2 messages), `--mode fleet|engine`, `--limit`, `--topic`, `--bootstrap`.

Format d'un message (cle Kafka = `engine_id`) :
```json
{"engine_id": 12, "cycle": 87, "setting_1": -0.0007, "setting_2": 0.0004, "setting_3": 100.0,
 "s1": 518.67, "s2": 642.35, "...": "...", "s21": 23.42, "dataset": "FD001_test",
 "event_time": "2026-11-06T10:15:02.123+00:00"}
```
