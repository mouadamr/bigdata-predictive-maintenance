"""Constantes partagees par tous les jobs Spark (chemins du Data Lake, schema)."""
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "capteurs_moteurs"

DATALAKE = "hdfs://namenode:8020/datalake"
BRONZE_PATH = f"{DATALAKE}/bronze/capteurs_moteurs"
SILVER_PATH = f"{DATALAKE}/silver/silver_mesures"
SILVER_FEATURES_PATH = f"{DATALAKE}/silver/silver_features"
GOLD_PATH = f"{DATALAKE}/gold/gold_sante_moteurs"
MODELS_PATH = f"{DATALAKE}/models"
CHECKPOINTS = f"{DATALAKE}/checkpoints"

SETTINGS = ["setting_1", "setting_2", "setting_3"]
SENSORS = [f"s{i}" for i in range(1, 22)]

# Schema d'un message JSON envoye par le producteur (partie 1)
MESSAGE_SCHEMA = StructType(
    [StructField("engine_id", IntegerType()), StructField("cycle", IntegerType())]
    + [StructField(c, DoubleType()) for c in SETTINGS + SENSORS]
    + [StructField("dataset", StringType()), StructField("event_time", StringType())]
)

# Colonnes constantes dans FD001 (voir notebooks/01_exploration_FD001.ipynb) :
# elles ne portent aucune information et sont retirees en zone Silver.
CONSTANT_COLUMNS = ["setting_3", "s1", "s5", "s10", "s16", "s18", "s19"]
