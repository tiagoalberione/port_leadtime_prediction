from src.paths import INTERIM_DIR, PROCESSED_DIR
# TODO: Incluir thresholds, lista de features, listas de colunas categóricas e numéricas

ESTADIA_CLEAN_FILE = INTERIM_DIR / "estadia_clean.parquet"
MASTER_CALLS_FILE = INTERIM_DIR / "master_calls.parquet"
MASTER_QC_FILE = INTERIM_DIR / "master_calls_qc.parquet"
TARGET_BASE_FILE = INTERIM_DIR / "target_base.parquet"
INTERNAL_FEATURES_FILE = INTERIM_DIR / "internal_features.parquet"
WEATHER_FEATURES_FILE = INTERIM_DIR / "weather_features.parquet"
ORIGIN_FEATURES_FILE = INTERIM_DIR / "origin_features.parquet"

EDA_BASE_FILE = PROCESSED_DIR / "eda_base.parquet"
TRAIN_BASE_FILE = PROCESSED_DIR / "train_base.parquet"


MAIN_EVENT_COLUMNS = [
    "arrival_port_ts",
    "berthing_ts",
    "unberthing_ts",
    "departure_port_ts",
]


TARGET_COLUMNS = [
    "t_wait_berthing_h",
    "t_operation_h",
    "t_post_operation_h",
    "t_total_port_h",
]