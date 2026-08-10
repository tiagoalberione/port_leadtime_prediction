"""Proxies simples de congestionamento portuário para a base analítica."""

import pandas as pd


REQUIRED_CONGESTION_COLUMNS = [
    "port",
    "arrival_port_ts",
    "departure_port_ts",
    "t_wait_for_berthing_h",
    "t_operation_h",
]


def create_basic_congestion_features(df: pd.DataFrame) -> pd.DataFrame:
    """Cria proxies históricos e descritivos de congestionamento por porto.

    As colunas apoiam a EDA do Capítulo 3 ao mostrar volume de chegadas e
    desempenho recente no porto. Nem todas são válidas como preditoras no instante
    de chegada da embarcação:

    - `arrivals_same_day_port`: `EDA_ONLY`, pois conta todas as chegadas do dia,
      inclusive as que podem ocorrer depois da embarcação atual;
    - `arrivals_prev_day_port`: `SAFE_FOR_PREDICTION`, pois usa o total do dia
      calendário anterior;
    - `arrivals_prev_7d_avg_port`: `SAFE_FOR_PREDICTION`, pois usa apenas dias
      anteriores ao dia da chegada;
    - `avg_wait_prev_20_calls_port`, `avg_operation_prev_20_calls_port` e
      `std_wait_prev_20_calls_port`: `REQUIRES_REDESIGN`, pois são chamadas
      anteriores por ordem de chegada. A auditoria mostrou que a janela pode
      incluir escalas que ainda não tinham terminado no `arrival_port_ts` atual.

    Parameters
    ----------
    df:
        Base elegível com porto, timestamps e targets de duração.

    Returns
    -------
    pandas.DataFrame
        Cópia ordenada por porto e chegada, com proxies de congestionamento.

    Risco metodológico
    ------------------
    As médias de duração anteriores são úteis para descrição histórica, mas não
    devem entrar no modelo final até serem reconstruídas usando somente escalas
    encerradas antes da chegada atual.
    """
    missing_cols = [col for col in REQUIRED_CONGESTION_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.copy()
    df = df.sort_values(["port", "arrival_port_ts"]).reset_index(drop=True)

    # REQUIRES_REDESIGN para previsão: o shift exclui a linha atual, mas não
    # garante que as escalas anteriores já tinham `departure_port_ts` conhecido.
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

    df["arrival_date"] = df["arrival_port_ts"].dt.floor("D")

    daily_arrivals = (
        df.groupby(["port", "arrival_date"])
        .size()
        .reset_index(name="arrivals_same_day_port")
        .sort_values(["port", "arrival_date"])
    )

    # SAFE_FOR_PREDICTION: total de chegadas do dia calendário anterior.
    daily_arrivals["arrivals_prev_day_port"] = (
        daily_arrivals.groupby("port")["arrivals_same_day_port"]
        .shift(1)
    )

    # SAFE_FOR_PREDICTION: média dos totais diários anteriores, sem o dia atual.
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