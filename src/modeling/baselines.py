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

def add_hierarchical_median_baseline(
    train_df: pd.DataFrame,
    scoring_df: pd.DataFrame,
    target_col: str,
    primary_group_cols: list[str],
    fallback_group_cols: list[str],
    min_group_size: int = 30,
    output_col: str = "pred_hierarchical_median",
) -> pd.DataFrame:
    """Predict using a hierarchical historical median with fallback levels."""
    result = scoring_df.copy()
    global_median = train_df[target_col].median()

    primary_stats = (
        train_df.groupby(primary_group_cols, dropna=False)[target_col]
        .agg(["median", "count"])
        .reset_index()
    )

    primary_stats = primary_stats[primary_stats["count"] >= min_group_size]
    primary_stats = primary_stats.rename(columns={"median": output_col})
    primary_stats = primary_stats[primary_group_cols + [output_col]]

    fallback_col = f"{output_col}_fallback"

    fallback_stats = (
        train_df.groupby(fallback_group_cols, dropna=False)[target_col]
        .agg(["median", "count"])
        .reset_index()
    )

    fallback_stats = fallback_stats[fallback_stats["count"] >= min_group_size]
    fallback_stats = fallback_stats.rename(columns={"median": fallback_col})
    fallback_stats = fallback_stats[fallback_group_cols + [fallback_col]]

    result = result.merge(
        primary_stats,
        on=primary_group_cols,
        how="left",
    )

    result = result.merge(
        fallback_stats,
        on=fallback_group_cols,
        how="left",
    )

    result[output_col] = (
        result[output_col]
        .fillna(result[fallback_col])
        .fillna(global_median)
    )

    result = result.drop(columns=[fallback_col])

    return result