import pandas as pd


def add_event_presence_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Create flags indicating availability of event timestamps."""
    df = df.copy()
    df["has_arrival_ts"] = df["arrival_port_ts"].notna()
    df["has_berthing_ts"] = df["berthing_ts"].notna()
    df["has_unberthing_ts"] = df["unberthing_ts"].notna()
    df["has_departure_ts"] = df["departure_port_ts"].notna()
    return df


def add_temporal_consistency_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Flag temporal sequence inconsistencies."""
    df = df.copy()
    df["flag_arrival_after_berthing"] = df["arrival_port_ts"] > df["berthing_ts"]
    df["flag_berthing_after_unberthing"] = df["berthing_ts"] > df["unberthing_ts"]
    df["flag_unberthing_after_departure"] = df["unberthing_ts"] > df["departure_port_ts"]
    return df


def add_duration_check_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create temporary duration columns used for QC."""
    df = df.copy()

    df["tmp_wait_h"] = (df["berthing_ts"] - df["arrival_port_ts"]).dt.total_seconds() / 3600
    df["tmp_operation_h"] = (df["unberthing_ts"] - df["berthing_ts"]).dt.total_seconds() / 3600
    df["tmp_post_h"] = (df["departure_port_ts"] - df["unberthing_ts"]).dt.total_seconds() / 3600
    df["tmp_total_h"] = (df["departure_port_ts"] - df["arrival_port_ts"]).dt.total_seconds() / 3600

    for col in ["tmp_wait_h", "tmp_operation_h", "tmp_post_h", "tmp_total_h"]:
        df[f"flag_negative_{col}"] = df[col] < 0

    return df


def define_eda_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """Define whether a row is valid for EDA."""
    df = df.copy()

    df["eligible_for_eda"] = (
        df["has_arrival_ts"]
        & df["has_berthing_ts"]
        & df["has_unberthing_ts"]
        & df["has_departure_ts"]
        & ~df["flag_arrival_after_berthing"].fillna(False)
        & ~df["flag_berthing_after_unberthing"].fillna(False)
        & ~df["flag_unberthing_after_departure"].fillna(False)
        & ~df["flag_negative_tmp_wait_h"].fillna(False)
        & ~df["flag_negative_tmp_operation_h"].fillna(False)
        & ~df["flag_negative_tmp_post_h"].fillna(False)
        & ~df["flag_negative_tmp_total_h"].fillna(False)
    )

    return df