from src.paths import INTERIM_DIR, PROCESSED_DIR
# TODO: Incluir thresholds, lista de features, listas de colunas categóricas e numéricas

PORT_CALL_CLEAN_FILE = INTERIM_DIR / "port_call_clean.parquet"
MASTER_PORT_CALLS_FILE = INTERIM_DIR / "master_port_calls.parquet"
MASTER_PORT_CALLS_QC_FILE = INTERIM_DIR / "master_port_calls_qc.parquet"
TARGET_BASE_FILE = INTERIM_DIR / "target_base.parquet"

EDA_BASE_FILE = PROCESSED_DIR / "eda_base.parquet"


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