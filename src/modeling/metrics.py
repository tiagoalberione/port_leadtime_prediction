import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, median_absolute_error, root_mean_squared_error


def root_mean_squared_log_error(y_true, y_pred) -> float:
    """Calculate RMSLE with clipping to avoid invalid log values."""
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)

    y_true = np.clip(y_true, a_min=0, a_max=None)
    y_pred = np.clip(y_pred, a_min=0, a_max=None)

    return float(np.sqrt(np.mean((np.log1p(y_pred) - np.log1p(y_true)) ** 2)))


def regression_metrics(y_true, y_pred) -> dict:
    """Calculate the main regression metrics for lead time prediction."""
    return {
        "mae": mean_absolute_error(y_true, y_pred),
        "rmse": root_mean_squared_error(y_true, y_pred),
        "rmsle": root_mean_squared_log_error(y_true, y_pred),
        "medae": median_absolute_error(y_true, y_pred),
    }


def evaluate_predictions(
    df: pd.DataFrame,
    actual_col: str,
    prediction_col: str,
    model_name: str,
    dataset_name: str,
) -> pd.DataFrame:
    """Return a one-row DataFrame with regression metrics."""
    metrics = regression_metrics(df[actual_col], df[prediction_col])
    return pd.DataFrame(
        [
            {
                "model": model_name,
                "dataset": dataset_name,
                **metrics,
            }
        ]
    )


def evaluate_by_segment(
    df: pd.DataFrame,
    segment_cols: list[str],
    actual_col: str,
    prediction_col: str,
    model_name: str,
    min_rows: int = 30,
) -> pd.DataFrame:
    """Calculate metrics by relevant business segments."""
    rows = []

    for segment_col in segment_cols:
        for segment_value, group in df.groupby(segment_col, dropna=False):
            if len(group) < min_rows:
                continue

            metrics = regression_metrics(group[actual_col], group[prediction_col])
            rows.append(
                {
                    "model": model_name,
                    "segment_col": segment_col,
                    "segment_value": segment_value,
                    "n_rows": len(group),
                    **metrics,
                }
            )

    return pd.DataFrame(rows)