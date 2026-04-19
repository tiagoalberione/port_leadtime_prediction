import pandas as pd


def create_basic_congestion_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create simple congestion proxies using historical port call information."""
    required_cols = [
        "port",
        "arrival_port_ts",
        "t_wait_for_berthing_h",
        "t_operation_h",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.copy()
    df = df.sort_values(["port", "arrival_port_ts"]).reset_index(drop=True)

    # Rolling historical performance by port, using only past calls
    df["avg_wait_prev_20_calls_port"] = (
        df.groupby("port")["t_wait_for_berthing_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    )

    df["avg_operation_prev_20_calls_port"] = (
        df.groupby("port")["t_operation_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    )

    df["std_wait_prev_20_calls_port"] = (
        df.groupby("port")["t_wait_for_berthing_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).std())
    )

    # Daily arrival counts
    df["arrival_date"] = df["arrival_port_ts"].dt.floor("D")

    daily_arrivals = (
        df.groupby(["port", "arrival_date"])
        .size()
        .reset_index(name="arrivals_same_day_port")
        .sort_values(["port", "arrival_date"])
    )

    # Previous day arrivals
    daily_arrivals["arrivals_prev_day_port"] = (
        daily_arrivals.groupby("port")["arrivals_same_day_port"]
        .shift(1)
    )

    # Average arrivals over previous 7 days
    daily_arrivals["arrivals_prev_7d_avg_port"] = (
        daily_arrivals.groupby("port")["arrivals_same_day_port"]
        .transform(lambda s: s.shift(1).rolling(7, min_periods=3).mean())
    )

    df = df.merge(
        daily_arrivals,
        on=["port", "arrival_date"],
        how="left",
    )

    return df