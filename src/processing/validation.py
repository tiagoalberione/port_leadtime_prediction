import pandas as pd


def add_event_presence_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Create flags indicating whether each main event timestamp is available."""
    df = df.copy()

    df["has_arrival_port_ts"] = df["arrival_port_ts"].notna()
    df["has_berthing_ts"] = df["berthing_ts"].notna()
    df["has_unberthing_ts"] = df["unberthing_ts"].notna()
    df["has_departure_port_ts"] = df["departure_port_ts"].notna()

    return df


def add_temporal_consistency_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Flag invalid temporal ordering across the main port call events."""
    df = df.copy()

    df["flag_arrival_after_berthing"] = df["arrival_port_ts"] > df["berthing_ts"]
    df["flag_berthing_after_unberthing"] = df["berthing_ts"] > df["unberthing_ts"]
    df["flag_unberthing_after_departure"] = df["unberthing_ts"] > df["departure_port_ts"]

    return df


def add_duration_check_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Create temporary duration columns used for quality checks."""
    df = df.copy()

    df["tmp_wait_for_berthing_h"] = (
        df["berthing_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_operation_h"] = (
        df["unberthing_ts"] - df["berthing_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_post_operation_h"] = (
        df["departure_port_ts"] - df["unberthing_ts"]
    ).dt.total_seconds() / 3600

    df["tmp_total_port_stay_h"] = (
        df["departure_port_ts"] - df["arrival_port_ts"]
    ).dt.total_seconds() / 3600

    duration_cols = [
        "tmp_wait_for_berthing_h",
        "tmp_operation_h",
        "tmp_post_operation_h",
        "tmp_total_port_stay_h",
    ]

    for col in duration_cols:
        df[f"flag_negative_{col}"] = df[col] < 0

    return df


def add_extreme_duration_flags(
    df: pd.DataFrame,
    max_wait_days: int = 30,
    max_operation_days: int = 30,
    max_total_days: int = 60,
) -> pd.DataFrame:
    """Flag suspiciously long durations for manual review."""
    df = df.copy()

    df["flag_wait_too_long"] = df["tmp_wait_for_berthing_h"] > (24 * max_wait_days)
    df["flag_operation_too_long"] = df["tmp_operation_h"] > (24 * max_operation_days)
    df["flag_total_too_long"] = df["tmp_total_port_stay_h"] > (24 * max_total_days)

    return df


def define_eda_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """Define whether each port call is eligible for EDA."""
    df = df.copy()

    df["eligible_for_eda"] = (
        df["has_arrival_port_ts"]
        & df["has_berthing_ts"]
        & df["has_unberthing_ts"]
        & df["has_departure_port_ts"]
        & ~df["flag_arrival_after_berthing"].fillna(False)
        & ~df["flag_berthing_after_unberthing"].fillna(False)
        & ~df["flag_unberthing_after_departure"].fillna(False)
        & ~df["flag_negative_tmp_wait_for_berthing_h"].fillna(False)
        & ~df["flag_negative_tmp_operation_h"].fillna(False)
        & ~df["flag_negative_tmp_post_operation_h"].fillna(False)
        & ~df["flag_negative_tmp_total_port_stay_h"].fillna(False)
    )

    return df


def build_quality_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build a compact summary of the QC results."""
    summary = pd.DataFrame(
        {
            "metric": [
                "total_rows",
                "unique_port_call_id",
                "missing_arrival_port_ts",
                "missing_berthing_ts",
                "missing_unberthing_ts",
                "missing_departure_port_ts",
                "arrival_after_berthing",
                "berthing_after_unberthing",
                "unberthing_after_departure",
                "negative_wait_for_berthing",
                "negative_operation",
                "negative_post_operation",
                "negative_total_port_stay",
                "wait_too_long",
                "operation_too_long",
                "total_too_long",
                "eligible_for_eda",
            ],
            "value": [
                len(df),
                df["port_call_id"].nunique(),
                (~df["has_arrival_port_ts"]).sum(),
                (~df["has_berthing_ts"]).sum(),
                (~df["has_unberthing_ts"]).sum(),
                (~df["has_departure_port_ts"]).sum(),
                df["flag_arrival_after_berthing"].fillna(False).sum(),
                df["flag_berthing_after_unberthing"].fillna(False).sum(),
                df["flag_unberthing_after_departure"].fillna(False).sum(),
                df["flag_negative_tmp_wait_for_berthing_h"].fillna(False).sum(),
                df["flag_negative_tmp_operation_h"].fillna(False).sum(),
                df["flag_negative_tmp_post_operation_h"].fillna(False).sum(),
                df["flag_negative_tmp_total_port_stay_h"].fillna(False).sum(),
                df["flag_wait_too_long"].fillna(False).sum(),
                df["flag_operation_too_long"].fillna(False).sum(),
                df["flag_total_too_long"].fillna(False).sum(),
                df["eligible_for_eda"].fillna(False).sum(),
            ],
        }
    )

    return summary