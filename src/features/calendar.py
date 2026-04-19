import pandas as pd


def map_shift(hour: int) -> str:
    """Map hour of day to a shift label."""
    if pd.isna(hour):
        return pd.NA
    if 0 <= hour < 6:
        return "night"
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    return "evening"


def map_season(month: int) -> str:
    """Map month to season for the Southern Hemisphere."""
    if pd.isna(month):
        return pd.NA
    if month in [12, 1, 2]:
        return "summer"
    if month in [3, 4, 5]:
        return "autumn"
    if month in [6, 7, 8]:
        return "winter"
    return "spring"


def create_calendar_features(
    df: pd.DataFrame,
    ref_col: str = "arrival_port_ts",
) -> pd.DataFrame:
    """Create calendar-derived features from a reference timestamp column."""
    if ref_col not in df.columns:
        raise ValueError(f"Column '{ref_col}' not found in DataFrame.")

    df = df.copy()
    ref = df[ref_col]

    df["arrival_year"] = ref.dt.year
    df["arrival_month"] = ref.dt.month
    df["arrival_quarter"] = ref.dt.quarter
    df["arrival_weekofyear"] = ref.dt.isocalendar().week.astype("Int64")
    df["arrival_day"] = ref.dt.day
    df["arrival_dayofweek"] = ref.dt.dayofweek
    df["arrival_hour"] = ref.dt.hour

    df["arrival_is_weekend"] = df["arrival_dayofweek"].isin([5, 6]).astype("Int64")
    df["arrival_shift"] = df["arrival_hour"].apply(map_shift)
    df["arrival_season"] = df["arrival_month"].apply(map_season)

    return df