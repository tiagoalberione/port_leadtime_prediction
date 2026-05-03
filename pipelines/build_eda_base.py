from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config import (
    PORT_CALL_CLEAN_FILE,
    MASTER_PORT_CALLS_FILE,
    MASTER_PORT_CALLS_QC_FILE,
    TARGET_BASE_FILE,
    INTERNAL_FEATURES_FILE,
    PORTS_CLEAN_FILE,
    WEATHER_CLEAN_FILE,
    WEATHER_FEATURES_FILE,
    EDA_BASE_FILE,
)
from src.data_sources.port_call import load_port_call_files, process_port_call
from src.data_sources.ports import load_port_files, process_ports
from src.data_sources.weather import load_weather_files, process_weather
from src.features.calendar import create_calendar_features
from src.features.congestion import create_basic_congestion_features
from src.features.weather_features import (
    prepare_port_reference_for_weather,
    create_weather_history_features,
    merge_weather_features,
)
from src.paths import RAW_DIR, INTERIM_DIR, PROCESSED_DIR, TABLES_DIR
from src.processing.master_table import build_master_calls
from src.processing.targets import (
    filter_eligible_port_calls,
    create_duration_targets,
    create_duration_targets_in_days,
    create_log_targets,
    create_target_severity_flags,
    build_target_summary,
)
from src.processing.validation import (
    add_event_presence_flags,
    add_temporal_consistency_flags,
    add_duration_check_columns,
    add_extreme_duration_flags,
    define_eda_eligibility,
    build_quality_summary,
)
from src.utils import ensure_directories


def build_port_call_base() -> pd.DataFrame:
    df_port_call_raw = load_port_call_files(RAW_DIR / "estadia")
    df_port_call = process_port_call(df_port_call_raw)
    df_port_call.to_parquet(PORT_CALL_CLEAN_FILE, index=False)

    df_master = build_master_calls(df_port_call)
    df_master.to_parquet(MASTER_PORT_CALLS_FILE, index=False)

    df_qc = add_event_presence_flags(df_master)
    df_qc = add_temporal_consistency_flags(df_qc)
    df_qc = add_duration_check_columns(df_qc)
    df_qc = add_extreme_duration_flags(df_qc)
    df_qc = define_eda_eligibility(df_qc)
    df_qc.to_parquet(MASTER_PORT_CALLS_QC_FILE, index=False)

    quality_summary = build_quality_summary(df_qc)
    quality_summary.to_csv(TABLES_DIR / "port_call_quality_summary.csv", index=False)

    df_target = filter_eligible_port_calls(df_qc)
    df_target = create_duration_targets(df_target)
    df_target = create_duration_targets_in_days(df_target)
    df_target = create_log_targets(df_target)
    df_target = create_target_severity_flags(df_target)
    df_target.to_parquet(TARGET_BASE_FILE, index=False)

    target_summary = build_target_summary(df_target)
    target_summary.to_csv(TABLES_DIR / "target_summary.csv", index=False)

    df_internal = create_calendar_features(df_target)
    df_internal = create_basic_congestion_features(df_internal)
    df_internal.to_parquet(INTERNAL_FEATURES_FILE, index=False)

    return df_internal


def build_weather_reference() -> tuple[pd.DataFrame, pd.DataFrame]:
    df_ports_raw = load_port_files(RAW_DIR / "ports")
    df_ports = process_ports(df_ports_raw)
    df_ports = prepare_port_reference_for_weather(df_ports)
    df_ports.to_parquet(PORTS_CLEAN_FILE, index=False)

    df_weather_raw = load_weather_files(RAW_DIR / "weather")
    df_weather = process_weather(df_weather_raw)
    df_weather.to_parquet(WEATHER_CLEAN_FILE, index=False)

    df_weather_features = create_weather_history_features(df_weather)
    df_weather_features.to_parquet(WEATHER_FEATURES_FILE, index=False)

    return df_ports, df_weather_features


def main() -> None:
    ensure_directories([INTERIM_DIR, PROCESSED_DIR, TABLES_DIR])

    df_internal = build_port_call_base()
    df_ports, df_weather_features = build_weather_reference()

    df_eda = merge_weather_features(
        df=df_internal,
        weather_df=df_weather_features,
        ports_df=df_ports,
    )
    df_eda.to_parquet(EDA_BASE_FILE, index=False)

    print("EDA base created successfully.")
    print(f"Rows: {len(df_eda):,}")
    print(f"Columns: {df_eda.shape[1]:,}")
    print()
    print("Saved datasets:")
    print(f"- {PORT_CALL_CLEAN_FILE}")
    print(f"- {MASTER_PORT_CALLS_FILE}")
    print(f"- {MASTER_PORT_CALLS_QC_FILE}")
    print(f"- {TARGET_BASE_FILE}")
    print(f"- {INTERNAL_FEATURES_FILE}")
    print(f"- {PORTS_CLEAN_FILE}")
    print(f"- {WEATHER_CLEAN_FILE}")
    print(f"- {WEATHER_FEATURES_FILE}")
    print(f"- {EDA_BASE_FILE}")


if __name__ == "__main__":
    main()
