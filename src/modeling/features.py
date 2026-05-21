import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def get_operation_flag_columns(df: pd.DataFrame, prefix: str = "op_") -> list[str]:
    """Identify operation type flag columns."""
    return [col for col in df.columns if col.startswith(prefix)]


def build_feature_matrix(
    df: pd.DataFrame,
    categorical_features: list[str],
    numerical_features: list[str],
    operation_flag_prefix: str = "op_",
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """Select available features from the modeling dataset."""
    operation_features = get_operation_flag_columns(df, operation_flag_prefix)

    selected_categorical = [col for col in categorical_features if col in df.columns]
    selected_numerical = [
        col for col in [*numerical_features, *operation_features] if col in df.columns
    ]

    feature_cols = selected_categorical + selected_numerical
    return df[feature_cols].copy(), selected_categorical, selected_numerical


def build_preprocessor(
    categorical_features: list[str],
    numerical_features: list[str],
    scale_numeric: bool = False,
) -> ColumnTransformer:
    """Build a preprocessing pipeline for tabular machine learning models."""
    numeric_steps = [("imputer", SimpleImputer(strategy="median"))]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))

    numeric_pipeline = Pipeline(numeric_steps)

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