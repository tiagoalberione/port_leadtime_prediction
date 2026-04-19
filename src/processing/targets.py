import numpy as np
import pandas as pd


def create_duration_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Create target duration columns in hours."""
    df = df.copy()

    df["t_wait_berthing_h"] = (df["berthing_ts"] - df["arrival_port_ts"]).dt.total_seconds() / 3600
    df["t_operation_h"] = (df["unberthing_ts"] - df["berthing_ts"]).dt.total_seconds() / 3600
    df["t_post_operation_h"] = (df["departure_port_ts"] - df["unberthing_ts"]).dt.total_seconds() / 3600
    df["t_total_port_h"] = (df["departure_port_ts"] - df["arrival_port_ts"]).dt.total_seconds() / 3600

    return df


def create_log_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Create log-transformed target columns."""
    df = df.copy()

    target_cols = [
        "t_wait_berthing_h",
        "t_operation_h",
        "t_post_operation_h",
        "t_total_port_h",
    ]

    for col in target_cols:
        df[f"log_{col}"] = np.log1p(df[col])

    return df