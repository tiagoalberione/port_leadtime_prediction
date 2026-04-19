import numpy as np
import pandas as pd


TARGET_COLUMNS = [
    "t_wait_for_berthing_h",
    "t_operation_h",
    "t_post_operation_h",
    "t_total_port_stay_h",
]


def filter_eligible_port_calls(df: pd.DataFrame) -> pd.DataFrame:
    """Filter only port calls eligible for EDA."""
    if "eligible_for_eda" not in df.columns:
        raise ValueError("Column 'eligible_for_eda' not found in DataFrame.")

    return df[df["eligible_for_eda"]].copy()


def create_duration_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Create final target duration columns in hours."""
    df = df.copy()

    df["t_wait_for_berthing_h"] = (
        df["berthing_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    df["t_operation_h"] = (
        df["unberthing_ts"] - df["berthing_ts"]
    ).dt.total_seconds() / 3600

    df["t_post_operation_h"] = (
        df["departure_port_ts"] - df["unberthing_ts"]
    ).dt.total_seconds() / 3600

    df["t_total_port_stay_h"] = (
        df["departure_port_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    return df


def create_duration_targets_in_days(df: pd.DataFrame) -> pd.DataFrame:
    """Create target duration columns in days."""
    df = df.copy()

    for col in TARGET_COLUMNS:
        df[col.replace("_h", "_d")] = df[col] / 24

    return df


def create_log_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Create log-transformed versions of the target columns."""
    df = df.copy()

    for col in TARGET_COLUMNS:
        df[f"log_{col}"] = np.log1p(df[col])

    return df


def create_target_severity_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Create high/extreme flags for selected targets using empirical quantiles."""
    df = df.copy()

    cols_for_flags = [
        "t_wait_for_berthing_h",
        "t_operation_h",
        "t_total_port_stay_h",
    ]

    for col in cols_for_flags:
        p75 = df[col].quantile(0.75)
        p90 = df[col].quantile(0.90)

        df[f"{col}_high"] = df[col] > p75
        df[f"{col}_extreme"] = df[col] > p90

    return df


def build_target_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build a summary table for the final target columns."""
    rows = []

    for col in TARGET_COLUMNS:
        rows.append(
            {
                "target": col,
                "count": df[col].count(),
                "mean": df[col].mean(),
                "median": df[col].median(),
                "std": df[col].std(),
                "min": df[col].min(),
                "p25": df[col].quantile(0.25),
                "p75": df[col].quantile(0.75),
                "p90": df[col].quantile(0.90),
                "p95": df[col].quantile(0.95),
                "max": df[col].max(),
            }
        )

    return pd.DataFrame(rows)