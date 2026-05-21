import pandas as pd


def add_global_mean_baseline(
    train_df: pd.DataFrame,
    target_col: str,
    scoring_df: pd.DataFrame,
    output_col: str = "pred_global_mean",
) -> pd.DataFrame:
    """Predict the global historical mean from the training set."""
    result = scoring_df.copy()
    result[output_col] = train_df[target_col].mean()
    return result


def add_global_median_baseline(
    train_df: pd.DataFrame,
    target_col: str,
    scoring_df: pd.DataFrame,
    output_col: str = "pred_global_median",
) -> pd.DataFrame:
    """Predict the global historical median from the training set."""
    result = scoring_df.copy()
    result[output_col] = train_df[target_col].median()
    return result


def add_group_median_baseline(
    train_df: pd.DataFrame,
    scoring_df: pd.DataFrame,
    group_cols: list[str],
    target_col: str,
    output_col: str,
) -> pd.DataFrame:
    """Predict historical median by group, falling back to global median."""
    result = scoring_df.copy()
    global_median = train_df[target_col].median()

    group_median = (
        train_df.groupby(group_cols, dropna=False)[target_col]
        .median()
        .reset_index()
        .rename(columns={target_col: output_col})
    )

    result = result.merge(group_median, on=group_cols, how="left")
    result[output_col] = result[output_col].fillna(global_median)

    return result