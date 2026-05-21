# src/config.py

from src.paths import INTERIM_DIR, PROCESSED_DIR

PORT_CALL_CLEAN_FILE = INTERIM_DIR / "port_call_clean.parquet"
MASTER_PORT_CALLS_FILE = INTERIM_DIR / "master_port_calls.parquet"
MASTER_PORT_CALLS_QC_FILE = INTERIM_DIR / "master_port_calls_qc.parquet"
TARGET_BASE_FILE = INTERIM_DIR / "target_base.parquet"
INTERNAL_FEATURES_FILE = INTERIM_DIR / "internal_features.parquet"
PORTS_CLEAN_FILE = INTERIM_DIR / "ports_clean.parquet"
WEATHER_CLEAN_FILE = INTERIM_DIR / "weather_clean.parquet"
WEATHER_FEATURES_FILE = INTERIM_DIR / "weather_features.parquet"

EDA_BASE_FILE = PROCESSED_DIR / "eda_base.parquet"

MODEL_TARGET = "t_total_port_stay_h"
MODEL_LOG_TARGET = "log_t_total_port_stay_h"

TRAIN_END_DATE = "2024-06-30"
VALIDATION_END_DATE = "2024-12-31"
TEST_START_DATE = "2025-01-01"

RISK_QUANTILES = [0.50, 0.90, 0.95]

CATEGORICAL_FEATURES = [
    "port",
    "region",
    "state",
    "arrival_shift",
    "arrival_season",
]

NUMERICAL_FEATURES = [
    "arrival_month",
    "arrival_quarter",
    "arrival_weekofyear",
    "arrival_dayofweek",
    "arrival_hour",
    "arrival_is_weekend",
    "avg_wait_prev_20_calls_port",
    "avg_operation_prev_20_calls_port",
    "std_wait_prev_20_calls_port",
    "arrivals_same_day_port",
    "arrivals_prev_day_port",
    "arrivals_prev_7d_avg_port",
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "rain_sum_prev_1d",
    "precipitation_sum_prev_1d",
    "wind_speed_10m_max_prev_1d",
    "wind_gusts_10m_max_prev_1d",
    "temperature_2m_mean_prev_1d",
    "rain_sum_prev_3d",
    "precipitation_hours_prev_3d",
    "temperature_2m_mean_prev_3d",
    "wind_speed_10m_max_prev_3d",
    "wind_gusts_10m_max_prev_3d",
    "rain_sum_prev_7d",
    "precipitation_hours_prev_7d",
    "temperature_2m_mean_prev_7d",
    "wind_speed_10m_max_prev_7d",
    "wind_gusts_10m_max_prev_7d",
]

OPERATION_FLAG_PREFIX = "op_"

MAIN_EVENT_COLUMNS = [
    "arrival_port_ts",
    "berthing_ts",
    "unberthing_ts",
    "departure_port_ts",
]

TARGET_COLUMNS = [
    "t_wait_for_berthing_h",
    "t_operation_h",
    "t_post_operation_h",
    "t_total_port_stay_h",
]