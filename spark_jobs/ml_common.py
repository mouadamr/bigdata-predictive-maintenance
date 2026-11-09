"""Partie 3 - Parametres communs aux jobs de features, d'entrainement et de scoring."""
from common import DATALAKE, MODELS_PATH

TRAIN_DATASET = "FD001_train"   # moteurs suivis jusqu'a la panne -> entrainement
FLEET_DATASET = "FD001_test"    # moteurs encore en service     -> flotte a surveiller
RUL_FILE = "/opt/project/data/raw/RUL_FD001.txt"   # RUL reelle des moteurs de test (evaluation)

# 14 capteurs informatifs (exploration : constants retires, s6 quasi sans lien avec la RUL)
FEATURE_SENSORS = ["s2", "s3", "s4", "s7", "s8", "s9", "s11",
                   "s12", "s13", "s14", "s15", "s17", "s20", "s21"]
WINDOWS = [5, 10, 20]           # tailles des fenetres glissantes (en cycles)

RUL_CAP = 125                   # RUL plafonnee : la degradation n'est visible qu'en fin de vie
SEUIL_CRITIQUE = 15             # RUL <= 15        -> Critique
SEUIL_SURVEILLANCE = 50         # 15 < RUL <= 50   -> A surveiller, sinon Sain
ETATS = ["Sain", "A surveiller", "Critique"]   # index = etat_label (0, 1, 2)

FEATURES_TABLE_PATH = f"{DATALAKE}/silver/silver_features"
GOLD_SANTE_PATH = f"{DATALAKE}/gold/gold_sante_moteurs"
GOLD_PREDICTIONS_PATH = f"{DATALAKE}/gold/gold_predictions"
REG_MODEL_PATH = f"{MODELS_PATH}/gbt_rul"
CLF_MODEL_PATH = f"{MODELS_PATH}/rf_etat"
METRICS_PATH = f"{MODELS_PATH}/metrics"


def feature_columns() -> list:
    """Colonnes donnees aux modeles : cycle, capteurs bruts, moyennes et ecarts-types glissants."""
    cols = ["cycle"] + FEATURE_SENSORS
    for s in FEATURE_SENSORS:
        for w in WINDOWS:
            cols += [f"{s}_mean_{w}", f"{s}_std_{w}"]
    return cols


def hudi_options(table: str, record_key: str, precombine: str, partition: str = "dataset") -> dict:
    keygen = ("org.apache.hudi.keygen.ComplexKeyGenerator" if "," in record_key
              else "org.apache.hudi.keygen.SimpleKeyGenerator")
    return {
        "hoodie.table.name": table,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.keygenerator.class": keygen,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partition,
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
    }


def drop_hudi_meta(df):
    """Retire les colonnes techniques ajoutees par Hudi (_hoodie_commit_time, ...)."""
    return df.select([c for c in df.columns if not c.startswith("_hoodie")])
