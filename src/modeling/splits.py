import pandas as pd


def temporal_train_validation_test_split(
    df: pd.DataFrame,
    date_col: str,
    train_end_date: str,
    validation_end_date: str,
    test_start_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split data into train, validation, and test sets using chronological order."""
    data = df.copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")

    train_end = pd.Timestamp(train_end_date)
    validation_end = pd.Timestamp(validation_end_date)
    test_start = pd.Timestamp(test_start_date)

    train_df = data[data[date_col] <= train_end].copy()
    validation_df = data[
        (data[date_col] > train_end) & (data[date_col] <= validation_end)
    ].copy()
    test_df = data[data[date_col] >= test_start].copy()

    return train_df, validation_df, test_df