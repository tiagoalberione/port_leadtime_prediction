import pandas as pd

from src.modeling.metrics import regression_metrics


def add_target_range(
    df: pd.DataFrame,
    target_col: str,
    output_col: str = "target_range",
) -> pd.DataFrame:
    """Create business-friendly lead time ranges for segmented error analysis."""
    result = df.copy()

    result[output_col] = pd.cut(
        result[target_col],
        bins=[0, 24, 48, 72, 168, float("inf")],
        labels=[
            "até 1 dia",
            "1 a 2 dias",
            "2 a 3 dias",
            "3 a 7 dias",
            "acima de 7 dias",
        ],
        include_lowest=True,
    )

    return result

def evaluate_model_by_flag_segments(
    df: pd.DataFrame,
    actual_col: str,
    prediction_col: str,
    model_name: str,
    flag_cols: list[str],
    min_rows: int = 30,
) -> pd.DataFrame:
    """Calculate metrics for multi-label boolean flag segments."""
    rows = []

    valid_df = df.dropna(subset=[actual_col, prediction_col]).copy()

    for flag_col in flag_cols:
        if flag_col not in valid_df.columns:
            continue

        flag_group = valid_df[valid_df[flag_col] == True].copy()

        if len(flag_group) < min_rows:
            continue

        metrics = regression_metrics(
            y_true=flag_group[actual_col],
            y_pred=flag_group[prediction_col],
        )

        rows.append(
            {
                "model": model_name,
                "segment_col": "operation_flag",
                "segment_value": flag_col,
                "n_rows": len(flag_group),
                **metrics,
            }
        )

    return pd.DataFrame(rows)


def evaluate_model_by_segment(
    df: pd.DataFrame,
    actual_col: str,
    prediction_col: str,
    model_name: str,
    segment_col: str,
    min_rows: int = 30,
) -> pd.DataFrame:
    """Calculate regression metrics for one model across one segment column."""
    rows = []

    if segment_col not in df.columns:
        return pd.DataFrame()

    valid_df = df.dropna(subset=[actual_col, prediction_col]).copy()

    for segment_value, group in valid_df.groupby(segment_col, dropna=False):
        if len(group) < min_rows:
            continue

        metrics = regression_metrics(
            y_true=group[actual_col],
            y_pred=group[prediction_col],
        )

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


def evaluate_all_models_by_segments(
    df: pd.DataFrame,
    actual_col: str,
    prediction_cols: list[str],
    segment_cols: list[str],
    flag_cols: list[str] | None = None,
    min_rows: int = 30,
) -> pd.DataFrame:
    """Calculate segmented metrics for regular segments and multi-label flag segments."""
    all_results = []
    flag_cols = flag_cols or []

    for prediction_col in prediction_cols:
        model_name = prediction_col.replace("pred_", "")

        for segment_col in segment_cols:
            segment_result = evaluate_model_by_segment(
                df=df,
                actual_col=actual_col,
                prediction_col=prediction_col,
                model_name=model_name,
                segment_col=segment_col,
                min_rows=min_rows,
            )

            if not segment_result.empty:
                all_results.append(segment_result)

        flag_result = evaluate_model_by_flag_segments(
            df=df,
            actual_col=actual_col,
            prediction_col=prediction_col,
            model_name=model_name,
            flag_cols=flag_cols,
            min_rows=min_rows,
        )

        if not flag_result.empty:
            all_results.append(flag_result)

    if not all_results:
        return pd.DataFrame()

    return pd.concat(all_results, ignore_index=True)


def get_top_segment_errors(
    segmented_metrics: pd.DataFrame,
    metric_col: str = "mae",
    top_n: int = 20,
) -> pd.DataFrame:
    """Return the worst segment/model combinations according to a selected metric."""
    if segmented_metrics.empty:
        return segmented_metrics

    return (
        segmented_metrics
        .sort_values(metric_col, ascending=False)
        .head(top_n)
        .reset_index(drop=True)
    )