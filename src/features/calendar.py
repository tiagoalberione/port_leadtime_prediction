"""Features de calendário derivadas do instante de chegada ao porto."""

import pandas as pd


ARRIVAL_TIMESTAMP_COL = "arrival_port_ts"


def map_shift(hour: int) -> str:
    """Classifica a hora de chegada em turno do dia.

    O turno ajuda a EDA a descrever padrões operacionais por horário. Como a hora
    de chegada é conhecida no instante da previsão, a informação é
    `SAFE_FOR_PREDICTION`.
    """
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
    """Classifica o mês na estação do Hemisfério Sul.

    A estação é uma leitura simples de sazonalidade para o Capítulo 3. Ela é
    calculada apenas a partir da data de chegada e, portanto, é
    `SAFE_FOR_PREDICTION`.
    """
    if pd.isna(month):
        return pd.NA
    if month in [12, 1, 2]:
        return "summer"
    if month in [3, 4, 5]:
        return "autumn"
    if month in [6, 7, 8]:
        return "winter"
    return "spring"


def create_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Cria variáveis de calendário a partir de `arrival_port_ts`.

    Essas colunas sustentam a EDA temporal do Capítulo 3 e podem ser avaliadas no
    Capítulo 4 porque usam apenas o instante de chegada da embarcação. Não são
    criadas versões seno/cosseno aqui para manter o pipeline de dados simples; se
    forem necessárias, devem aparecer explicitamente no notebook de modelagem.

    Parameters
    ----------
    df:
        Base de escalas elegíveis com a coluna `arrival_port_ts`.

    Returns
    -------
    pandas.DataFrame
        Cópia com ano, mês, trimestre, semana, dia, dia da semana, hora, fim de
        semana, turno e estação da chegada.
    """
    if ARRIVAL_TIMESTAMP_COL not in df.columns:
        raise ValueError(f"Column '{ARRIVAL_TIMESTAMP_COL}' not found in DataFrame.")

    df = df.copy()
    ref = df[ARRIVAL_TIMESTAMP_COL]

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