"""Métricas de avaliação usadas no Capítulo 4."""

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_pinball_loss,
    median_absolute_error,
    root_mean_squared_error,
)


def root_mean_squared_log_error(y_true, y_pred) -> float:
    """Calcula RMSLE após impedir valores negativos nas previsões.

    O tempo de permanência não pode ser negativo. O clipping evita erro no
    `log1p` quando algum modelo produz uma previsão ligeiramente abaixo de zero.
    """
    y_true = np.clip(np.asarray(y_true), a_min=0, a_max=None)
    y_pred = np.clip(np.asarray(y_pred), a_min=0, a_max=None)

    return float(
        np.sqrt(np.mean((np.log1p(y_pred) - np.log1p(y_true)) ** 2))
    )


def regression_metrics(y_true, y_pred) -> dict[str, float]:
    """Calcula as quatro métricas usadas para previsões pontuais.

    MAE é a métrica principal do TCC por ser diretamente interpretável em horas.
    RMSE dá mais peso aos grandes erros, MedAE representa melhor o caso típico e
    RMSLE reduz a dominância dos valores extremos na escala original.
    """
    y_pred = np.clip(np.asarray(y_pred), a_min=0, a_max=None)

    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(root_mean_squared_error(y_true, y_pred)),
        "medae": float(median_absolute_error(y_true, y_pred)),
        "rmsle": root_mean_squared_log_error(y_true, y_pred),
    }


def quantile_metrics(y_true, y_pred, quantile: float) -> dict[str, float]:
    """Avalia uma previsão quantílica com perda e cobertura apropriadas.

    P90 e P95 não devem ser avaliados como se fossem previsões médias. A pinball
    loss penaliza erros de forma assimétrica de acordo com o quantil desejado e
    a cobertura indica a proporção observada de casos abaixo da previsão.
    """
    y_true = np.asarray(y_true)
    y_pred = np.clip(np.asarray(y_pred), a_min=0, a_max=None)

    return {
        "pinball_loss": float(mean_pinball_loss(y_true, y_pred, alpha=quantile)),
        "coverage": float(np.mean(y_true <= y_pred)),
    }


def evaluate_by_segment(
    df: pd.DataFrame,
    segment_col: str,
    actual_col: str,
    prediction_col: str,
    model_name: str,
    min_rows: int = 30,
) -> pd.DataFrame:
    """Calcula métricas pontuais para grupos com volume mínimo de observações.

    A análise segmentada ajuda a verificar se um bom resultado global esconde
    desempenho ruim em determinados portos, regiões ou faixas de lead time.
    Grupos muito pequenos são omitidos para evitar conclusões baseadas em poucos
    casos.
    """
    if segment_col not in df.columns:
        return pd.DataFrame()

    valid_df = df.dropna(subset=[actual_col, prediction_col]).copy()
    rows = []

    for segment_value, group in valid_df.groupby(segment_col, dropna=False):
        if len(group) < min_rows:
            continue

        rows.append(
            {
                "model": model_name,
                "segment_col": segment_col,
                "segment_value": segment_value,
                "n_rows": len(group),
                **regression_metrics(group[actual_col], group[prediction_col]),
            }
        )

    return pd.DataFrame(rows)
