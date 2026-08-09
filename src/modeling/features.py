"""Seleção e preprocessamento das variáveis usadas no Capítulo 4."""

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def get_operation_flag_columns(df: pd.DataFrame, prefix: str = "op_") -> list[str]:
    """Retorna as colunas booleanas que representam tipos de operação.

    Os tipos de operação são multirrótulo: uma mesma escala pode conter mais de
    uma atividade. As colunas `op_*` permitem representar isso sem tratar a
    combinação completa como uma única categoria rara.

    Parameters
    ----------
    df:
        Base analítica.
    prefix:
        Prefixo usado para identificar os indicadores de operação.

    Returns
    -------
    list[str]
        Nomes das colunas encontradas.
    """
    return [col for col in df.columns if col.startswith(prefix)]


def select_model_features(
    df: pd.DataFrame,
    categorical_features: list[str],
    numerical_features: list[str],
    include_operation_flags: bool = True,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Seleciona apenas as features explicitamente aprovadas para modelagem.

    A lista de features deve ser definida no notebook do Capítulo 4 após a
    checagem de disponibilidade no instante da chegada. Esta função apenas
    seleciona as colunas existentes; ela não decide quais variáveis são válidas
    metodologicamente.

    Parameters
    ----------
    df:
        Base que será usada para treino ou avaliação.
    categorical_features:
        Colunas categóricas aprovadas no experimento.
    numerical_features:
        Colunas numéricas aprovadas no experimento.
    include_operation_flags:
        Se verdadeiro, inclui automaticamente as colunas que começam com
        `op_`.

    Returns
    -------
    tuple
        DataFrame de features, lista categórica efetivamente usada e lista
        numérica efetivamente usada.
    """
    selected_categorical = [col for col in categorical_features if col in df.columns]
    selected_numerical = [col for col in numerical_features if col in df.columns]

    if include_operation_flags:
        for col in get_operation_flag_columns(df):
            if col not in selected_numerical:
                selected_numerical.append(col)

    feature_cols = selected_categorical + selected_numerical
    return df[feature_cols].copy(), selected_categorical, selected_numerical


def build_preprocessor(
    categorical_features: list[str],
    numerical_features: list[str],
    scale_numeric: bool,
) -> ColumnTransformer:
    """Cria o preprocessamento comum aos modelos tabulares.

    Valores numéricos ausentes são substituídos pela mediana. Categorias
    ausentes usam a categoria mais frequente e depois passam por one-hot
    encoding. Para Ridge, `scale_numeric=True`; para modelos de árvore,
    `scale_numeric=False`.

    Parameters
    ----------
    categorical_features:
        Nomes das variáveis categóricas.
    numerical_features:
        Nomes das variáveis numéricas e flags de operação.
    scale_numeric:
        Define se as variáveis numéricas serão padronizadas com média zero e
        desvio padrão unitário.

    Returns
    -------
    sklearn.compose.ColumnTransformer
        Transformador ainda não ajustado aos dados.
    """
    numeric_steps = [("imputer", SimpleImputer(strategy="median"))]

    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))

    numeric_pipeline = Pipeline(steps=numeric_steps)
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numerical_features),
            ("cat", categorical_pipeline, categorical_features),
        ]
    )
