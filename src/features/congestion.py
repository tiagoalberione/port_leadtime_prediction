import pandas as pd


def create_congestion_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create simple congestion proxies using historical calls."""
    df = df.copy()
    df = df.sort_values(["port_name", "arrival_port_ts"]).reset_index(drop=True)

    df["avg_wait_prev_20_calls_port"] = (
        df.groupby("port_name")["t_wait_berthing_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    )

    df["avg_operation_prev_20_calls_port"] = (
        df.groupby("port_name")["t_operation_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    )

    df["std_wait_prev_20_calls_port"] = (
        df.groupby("port_name")["t_wait_berthing_h"]
        .transform(lambda s: s.shift(1).rolling(20, min_periods=5).std())
    )

    df["arrival_date"] = df["arrival_port_ts"].dt.date

    daily_arrivals = (
        df.groupby(["port_name", "arrival_date"])
        .size()
        .reset_index(name="arrivals_same_day_port")
    )

    df = df.merge(daily_arrivals, on=["port_name", "arrival_date"], how="left")

    return df