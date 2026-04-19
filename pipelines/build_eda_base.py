import pandas as pd

from src.paths import RAW_DIR, INTERIM_DIR, PROCESSED_DIR
from src.utils import ensure_directories
from src.config import (
    ESTADIA_CLEAN_FILE,
    MASTER_CALLS_FILE,
    MASTER_QC_FILE,
    TARGET_BASE_FILE,
    EDA_BASE_FILE,
)
from src.data_sources.port_call import load_port_call_files, process_port_call
from src.processing.master_table import build_master_calls
from src.processing.validation import (
    add_event_presence_flags,
    add_temporal_consistency_flags,
    add_duration_check_columns,
    define_eda_eligibility,
)
from src.processing.targets import create_duration_targets, create_log_targets
from src.features.calendar import create_calendar_features
from src.features.congestion import create_congestion_features


def main() -> None:
    ensure_directories([INTERIM_DIR, PROCESSED_DIR])

    df_estadia_raw = load_port_call_files(RAW_DIR / "estadia")
    df_estadia = process_port_call(df_estadia_raw)
    df_estadia.to_parquet(ESTADIA_CLEAN_FILE, index=False)

    df_master = build_master_calls(df_estadia)
    df_master.to_parquet(MASTER_CALLS_FILE, index=False)

    df_qc = add_event_presence_flags(df_master)
    df_qc = add_temporal_consistency_flags(df_qc)
    df_qc = add_duration_check_columns(df_qc)
    df_qc = define_eda_eligibility(df_qc)
    df_qc.to_parquet(MASTER_QC_FILE, index=False)

    df_eda = df_qc[df_qc["eligible_for_eda"]].copy()
    df_eda = create_duration_targets(df_eda)
    df_eda = create_log_targets(df_eda)
    df_eda.to_parquet(TARGET_BASE_FILE, index=False)

    df_eda = create_calendar_features(df_eda)
    df_eda = create_congestion_features(df_eda)
    df_eda.to_parquet(EDA_BASE_FILE, index=False)

    print("EDA base created successfully.")
    print(f"Rows: {len(df_eda):,}")
    print(f"Columns: {df_eda.shape[1]:,}")


if __name__ == "__main__":
    main()
