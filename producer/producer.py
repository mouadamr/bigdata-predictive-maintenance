"""
Partie 1 - Producteur Kafka : rejeu en flux du jeu NASA C-MAPSS.

Lit un fichier C-MAPSS (ex. test_FD001.txt) et publie un message JSON par
(moteur, cycle) dans le topic Kafka `capteurs_moteurs`.

Mode "flotte" (par defaut) : on avance cycle par cycle et, a chaque cycle,
chaque moteur encore en service envoie sa mesure -> on simule une flotte de
moteurs qui fonctionnent en parallele.

Exemples :
    python producer.py --dry-run --limit 5            # affiche 5 messages, sans Kafka
    python producer.py                                # rejeu de test_FD001 (0.05 s / message)
    python producer.py --file ../data/raw/train_FD001.txt --delay 0 --dataset FD001_train
    python producer.py --loop                         # rejoue en boucle (demo continue)
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# 26 colonnes du format C-MAPSS : id moteur, cycle, 3 reglages, 21 capteurs
COLUMNS = (["engine_id", "cycle", "setting_1", "setting_2", "setting_3"]
           + [f"s{i}" for i in range(1, 22)])

DEFAULT_FILE = Path(__file__).resolve().parent.parent / "data" / "raw" / "test_FD001.txt"


def load_cmapss(path: Path) -> pd.DataFrame:
    """Charge un fichier C-MAPSS (separe par des espaces, sans en-tete)."""
    df = pd.read_csv(path, sep=r"\s+", header=None, engine="python")
    df = df.iloc[:, :len(COLUMNS)]            # colonnes vides en fin de ligne
    df.columns = COLUMNS
    df["engine_id"] = df["engine_id"].astype(int)
    df["cycle"] = df["cycle"].astype(int)
    return df


def order_rows(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "fleet":        # cycle 1 de tous les moteurs, puis cycle 2, ...
        return df.sort_values(["cycle", "engine_id"]).reset_index(drop=True)
    return df.sort_values(["engine_id", "cycle"]).reset_index(drop=True)   # moteur par moteur


def to_message(row: pd.Series, dataset: str) -> dict:
    msg = {"engine_id": int(row["engine_id"]), "cycle": int(row["cycle"])}
    for col in COLUMNS[2:]:
        msg[col] = float(row[col])
    msg["dataset"] = dataset
    msg["event_time"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    return msg


def build_producer(bootstrap: str):
    from kafka import KafkaProducer   # import ici : --dry-run marche sans Kafka
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",            # le broker confirme chaque message
        linger_ms=10,
        retries=5,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Rejeu C-MAPSS vers Kafka")
    p.add_argument("--file", type=Path, default=DEFAULT_FILE)
    p.add_argument("--dataset", default="FD001_test", help="etiquette ajoutee a chaque message")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="capteurs_moteurs")
    p.add_argument("--delay", type=float, default=0.05, help="secondes entre deux messages")
    p.add_argument("--mode", choices=["fleet", "engine"], default="fleet")
    p.add_argument("--limit", type=int, default=0, help="nombre max de messages (0 = tout)")
    p.add_argument("--loop", action="store_true", help="rejouer le fichier en boucle")
    p.add_argument("--dry-run", action="store_true", help="afficher sans envoyer a Kafka")
    args = p.parse_args()

    if not args.file.exists():
        print(f"Fichier introuvable : {args.file}\n"
              "Placez train_FD001.txt / test_FD001.txt dans data/raw/", file=sys.stderr)
        return 1

    df = order_rows(load_cmapss(args.file), args.mode)
    print(f"{len(df)} mesures, {df['engine_id'].nunique()} moteurs, "
          f"cycles 1 a {df['cycle'].max()} -> topic '{args.topic}'")

    producer = None if args.dry_run else build_producer(args.bootstrap)
    sent, passes = 0, 0
    try:
        while True:
            passes += 1
            for _, row in df.iterrows():
                msg = to_message(row, args.dataset)
                if producer is None:
                    print(json.dumps(msg))
                else:
                    producer.send(args.topic, key=msg["engine_id"], value=msg)
                sent += 1
                if sent % 500 == 0:
                    print(f"  {sent} messages envoyes (moteur {msg['engine_id']}, cycle {msg['cycle']})")
                if args.limit and sent >= args.limit:
                    raise StopIteration
                if args.delay:
                    time.sleep(args.delay)
            if not args.loop:
                break
            print(f"Fin du passage {passes}, on recommence...")
    except (StopIteration, KeyboardInterrupt):
        pass
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
    print(f"Termine : {sent} messages.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
