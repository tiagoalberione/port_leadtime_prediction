import pandas as pd


WEATHER_BASE_COLUMNS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
]


def prepare_port_reference_for_weather(
    ports_df: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and prepare processed port reference data for weather merge."""
    df = ports_df.copy()

    required_cols = [
        "port",
        "port_name",
        "city",
        "state",
        "region",
        "latitude",
        "longitude",
        "latitude_r",
        "longitude_r",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in processed ports data: {missing_cols}")

    return df


def create_weather_history_features(
    weather_df: pd.DataFrame,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Create historical daily weather features from daily weather series."""
    weather_df = weather_df.copy()

    group_cols = group_cols or ["latitude_r", "longitude_r"]
    weather_df = weather_df.sort_values(group_cols + ["date"]).reset_index(drop=True)

    group_obj = weather_df.groupby(group_cols, dropna=False)

    # Previous day
    weather_df["rain_sum_prev_1d"] = group_obj["rain_sum"].shift(1)
    weather_df["precipitation_sum_prev_1d"] = group_obj["precipitation_sum"].shift(1)
    weather_df["wind_speed_10m_max_prev_1d"] = group_obj["wind_speed_10m_max"].shift(1)
    weather_df["wind_gusts_10m_max_prev_1d"] = group_obj["wind_gusts_10m_max"].shift(1)
    weather_df["temperature_2m_mean_prev_1d"] = group_obj["temperature_2m_mean"].shift(1)

    # Previous 3 days
    weather_df["rain_sum_prev_3d"] = (
        group_obj["rain_sum"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).sum())
    )
    weather_df["precipitation_hours_prev_3d"] = (
        group_obj["precipitation_hours"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).sum())
    )
    weather_df["temperature_2m_mean_prev_3d"] = (
        group_obj["temperature_2m_mean"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    )
    weather_df["wind_speed_10m_max_prev_3d"] = (
        group_obj["wind_speed_10m_max"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).max())
    )
    weather_df["wind_gusts_10m_max_prev_3d"] = (
        group_obj["wind_gusts_10m_max"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).max())
    )

    # Previous 7 days
    weather_df["rain_sum_prev_7d"] = (
        group_obj["rain_sum"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).sum())
    )
    weather_df["precipitation_hours_prev_7d"] = (
        group_obj["precipitation_hours"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).sum())
    )
    weather_df["temperature_2m_mean_prev_7d"] = (
        group_obj["temperature_2m_mean"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).mean())
    )
    weather_df["wind_speed_10m_max_prev_7d"] = (
        group_obj["wind_speed_10m_max"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).max())
    )
    weather_df["wind_gusts_10m_max_prev_7d"] = (
        group_obj["wind_gusts_10m_max"].transform(lambda s: s.shift(1).rolling(7, min_periods=3).max())
    )

    return weather_df


def merge_weather_features(
    df: pd.DataFrame,
    weather_df: pd.DataFrame,
    ports_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge processed ports and weather features into the analytical dataset."""
    if "port" not in df.columns:
        raise ValueError("Column 'port' not found in input DataFrame.")
    if "arrival_port_ts" not in df.columns:
        raise ValueError("Column 'arrival_port_ts' not found in input DataFrame.")

    df = df.copy()
    df["arrival_date"] = pd.to_datetime(df["arrival_port_ts"], errors="coerce").dt.floor("D")

    port_ref_cols = [
        "port",
        "port_name",
        "city",
        "state",
        "region",
        "latitude",
        "longitude",
        "latitude_r",
        "longitude_r",
    ]

    df = df.merge(
        ports_df[port_ref_cols].drop_duplicates(subset=["port"]),
        on="port",
        how="left",
    )

    weather_merge_cols = [
        "latitude_r",
        "longitude_r",
        "date",
        *WEATHER_BASE_COLUMNS,
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

    available_weather_cols = [col for col in weather_merge_cols if col in weather_df.columns]

    df = df.merge(
        weather_df[available_weather_cols],
        left_on=["latitude_r", "longitude_r", "arrival_date"],
        right_on=["latitude_r", "longitude_r", "date"],
        how="left",
    )

    df["has_port_reference"] = df["latitude_r"].notna().astype("Int64")
    df["has_weather_data"] = df["date"].notna().astype("Int64")

    return df