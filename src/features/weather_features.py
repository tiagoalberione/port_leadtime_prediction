"""Enriquecimento da base analítica com porto de referência e clima diário."""

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

WEATHER_HISTORY_COLUMNS = [
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


def prepare_port_reference_for_weather(
    ports_df: pd.DataFrame,
) -> pd.DataFrame:
    """Valida a referência de portos antes do merge climático.

    A etapa garante que cada porto tenha região e coordenadas arredondadas para
    buscar a série diária de clima correspondente.

    Parameters
    ----------
    ports_df:
        Referência de portos já processada por `process_ports`.

    Returns
    -------
    pandas.DataFrame
        Cópia validada da referência de portos.
    """
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

    if "port_display" not in df.columns:
        df["port_display"] = (
            df["port"].astype("string").fillna("")
            + " - "
            + df["port_name"].astype("string").fillna("")
        )

    return df


def create_weather_history_features(weather_df: pd.DataFrame) -> pd.DataFrame:
    """Cria clima histórico por coordenada, sempre excluindo o dia atual.

    As colunas `*_prev_1d`, `*_prev_3d` e `*_prev_7d` usam `shift(1)` antes da
    janela móvel. Assim, elas usam apenas datas anteriores ao dia de chegada e
    são conceitualmente `SAFE_FOR_PREDICTION`, desde que a série diária esteja
    disponível antes da chegada. A auditoria encontrou poucos saltos de datas na
    série; nesses casos, `prev_1d` significa o dia observado anterior, não
    necessariamente D-1 calendário.

    Parameters
    ----------
    weather_df:
        Série diária limpa de clima por coordenada arredondada.

    Returns
    -------
    pandas.DataFrame
        Série de clima com features históricas defasadas.
    """
    weather_df = weather_df.copy()
    group_cols = ["latitude_r", "longitude_r"]
    weather_df = weather_df.sort_values(group_cols + ["date"]).reset_index(drop=True)

    group_obj = weather_df.groupby(group_cols, dropna=False)

    weather_df["rain_sum_prev_1d"] = group_obj["rain_sum"].shift(1)
    weather_df["precipitation_sum_prev_1d"] = group_obj["precipitation_sum"].shift(1)
    weather_df["wind_speed_10m_max_prev_1d"] = group_obj["wind_speed_10m_max"].shift(1)
    weather_df["wind_gusts_10m_max_prev_1d"] = group_obj["wind_gusts_10m_max"].shift(1)
    weather_df["temperature_2m_mean_prev_1d"] = group_obj["temperature_2m_mean"].shift(1)

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
    """Adiciona referência de porto e clima diário à base de escalas.

    As colunas de clima sem sufixo `prev_*` são agregados realizados do próprio
    dia (`EDA_ONLY`), pois podem incluir horas posteriores à chegada. As colunas
    históricas `prev_1d`, `prev_3d` e `prev_7d` já chegam defasadas e são
    `SAFE_FOR_PREDICTION` em termos de disponibilidade temporal.

    Parameters
    ----------
    df:
        Base de escalas com `port` e `arrival_port_ts`.
    weather_df:
        Série climática com colunas realizadas e históricas.
    ports_df:
        Referência de portos com coordenadas arredondadas.

    Returns
    -------
    pandas.DataFrame
        Base analítica enriquecida com localização e clima.
    """
    if "port" not in df.columns:
        raise ValueError("Column 'port' not found in input DataFrame.")
    if "arrival_port_ts" not in df.columns:
        raise ValueError("Column 'arrival_port_ts' not found in input DataFrame.")

    df = df.copy()
    df["arrival_date"] = pd.to_datetime(df["arrival_port_ts"], errors="coerce").dt.floor("D")

    ports_ref = (
        ports_df[
            [
                "port",
                "port_name",
                "port_display",
                "city",
                "state",
                "region",
                "latitude",
                "longitude",
                "latitude_r",
                "longitude_r",
            ]
        ]
        .drop_duplicates(subset=["port"])
        .rename(
            columns={
                "port_name": "port_name_ref",
                "port_display": "port_display_ref",
            }
        )
    )

    df = df.merge(
        ports_ref,
        on="port",
        how="left",
    )
    df["port_display"] = df["port_display_ref"].fillna(df["port_display"])

    weather_merge_cols = [
        "latitude_r",
        "longitude_r",
        "date",
        *WEATHER_BASE_COLUMNS,
        *WEATHER_HISTORY_COLUMNS,
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