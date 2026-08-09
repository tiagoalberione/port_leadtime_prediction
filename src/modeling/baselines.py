"""Baselines históricos usados como referência no Capítulo 4."""

import numpy as np
import pandas as pd


def predict_global_median(
    train_df: pd.DataFrame,
    target_col: str,
    scoring_df: pd.DataFrame,
) -> np.ndarray:
    """Prevê a mediana global histórica para todas as observações.

    A mediana global responde à pergunta mais simples possível: sem usar nenhum
    contexto da escala atual, qual foi o tempo típico observado no treino?
    Como a distribuição de permanência é fortemente assimétrica, a mediana é
    mais robusta a estadias extremas do que a média.
    """
    global_median = train_df[target_col].median()
    return np.repeat(global_median, len(scoring_df))


def predict_group_median(
    train_df: pd.DataFrame,
    scoring_df: pd.DataFrame,
    group_cols: list[str],
    target_col: str,
    min_group_size: int = 1,
) -> np.ndarray:
    """Prevê a mediana histórica de um grupo com fallback global.

    Esta função permite, por exemplo, calcular a mediana por porto. Grupos com
    menos observações que `min_group_size` são considerados insuficientes e
    recebem a mediana global do treino.
    """
    global_median = train_df[target_col].median()

    stats = (
        train_df.groupby(group_cols, dropna=False)[target_col]
        .agg(["median", "count"])
        .reset_index()
    )
    stats.loc[stats["count"] < min_group_size, "median"] = np.nan

    scored = scoring_df[group_cols].copy().merge(
        stats[group_cols + ["median"]],
        on=group_cols,
        how="left",
    )

    return scored["median"].fillna(global_median).to_numpy()


def predict_hierarchical_median(
    train_df: pd.DataFrame,
    scoring_df: pd.DataFrame,
    target_col: str,
    primary_group_cols: list[str],
    fallback_group_cols: list[str],
    min_group_size: int = 30,
) -> np.ndarray:
    """Prevê usando uma hierarquia de medianas históricas.

    Para o baseline principal do TCC, a hierarquia recomendada é:
    `porto + tipo de operação` -> `porto` -> mediana global.

    O primeiro nível só é aceito quando possui pelo menos `min_group_size`
    registros no treino. Isso evita tratar uma mediana calculada a partir de um
    ou dois casos como se fosse uma estimativa estável.
    """
    global_median = train_df[target_col].median()

    primary = (
        train_df.groupby(primary_group_cols, dropna=False)[target_col]
        .agg(["median", "count"])
        .reset_index()
    )
    primary = primary[primary["count"] >= min_group_size]
    primary = primary[primary_group_cols + ["median"]].rename(
        columns={"median": "primary_median"}
    )

    fallback = (
        train_df.groupby(fallback_group_cols, dropna=False)[target_col]
        .agg(["median", "count"])
        .reset_index()
    )
    fallback = fallback[fallback["count"] >= min_group_size]
    fallback = fallback[fallback_group_cols + ["median"]].rename(
        columns={"median": "fallback_median"}
    )

    scored = scoring_df[primary_group_cols].copy()
    scored = scored.merge(primary, on=primary_group_cols, how="left")
    scored = scored.merge(fallback, on=fallback_group_cols, how="left")

    return (
        scored["primary_median"]
        .fillna(scored["fallback_median"])
        .fillna(global_median)
        .to_numpy()
    )
