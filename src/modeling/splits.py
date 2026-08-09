"""Divisões temporais usadas para evitar vazamento entre treino e avaliação."""

import pandas as pd


def temporal_four_way_split(
    df: pd.DataFrame,
    date_col: str,
    validation_start: str = "2024-07-01",
    calibration_start: str = "2025-01-01",
    final_test_start: str = "2025-07-01",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Divide a base em treino, validação, calibração e teste final.

    A divisão é cronológica porque o experimento tenta reproduzir uma situação
    real: aprender com o passado e prever períodos posteriores. Os intervalos
    são semiabertos (`>= início` e `< próximo início`) para que registros ao
    longo de todo o último dia de cada período não sejam perdidos por causa do
    horário do timestamp.

    O teste final deve permanecer intocado até que a escolha do modelo esteja
    congelada.

    Parameters
    ----------
    df:
        Base analítica completa.
    date_col:
        Coluna de referência temporal, normalmente `arrival_port_ts`.
    validation_start:
        Primeiro instante do conjunto de validação.
    calibration_start:
        Primeiro instante do conjunto de calibração/desenvolvimento.
    final_test_start:
        Primeiro instante do conjunto de teste final.

    Returns
    -------
    tuple[pandas.DataFrame, pandas.DataFrame, pandas.DataFrame, pandas.DataFrame]
        Treino, validação, calibração e teste final, nessa ordem.
    """
    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")

    validation_start_ts = pd.Timestamp(validation_start)
    calibration_start_ts = pd.Timestamp(calibration_start)
    final_test_start_ts = pd.Timestamp(final_test_start)

    if not (
        validation_start_ts < calibration_start_ts < final_test_start_ts
    ):
        raise ValueError("As datas de corte devem estar em ordem cronológica.")

    train_df = data[data[date_col] < validation_start_ts].copy()
    validation_df = data[
        (data[date_col] >= validation_start_ts)
        & (data[date_col] < calibration_start_ts)
    ].copy()
    calibration_df = data[
        (data[date_col] >= calibration_start_ts)
        & (data[date_col] < final_test_start_ts)
    ].copy()
    final_test_df = data[data[date_col] >= final_test_start_ts].copy()

    return train_df, validation_df, calibration_df, final_test_df
