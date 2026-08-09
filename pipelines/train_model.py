from pathlib import Path
import sys
import joblib
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.config import (
    EDA_BASE_FILE,
    MODEL_TARGET,
    MODEL_LOG_TARGET,
    TRAIN_END_DATE,
    VALIDATION_END_DATE,
    TEST_START_DATE,
    CATEGORICAL_FEATURES,
    NUMERICAL_FEATURES,
    OPERATION_FLAG_PREFIX,
    RISK_QUANTILES,
)
from src.paths import MODELS_DIR, TABLES_DIR
from src.utils import ensure_directories
from src.modeling.baselines import (
    add_global_mean_baseline,
    add_global_median_baseline,
    add_group_median_baseline,
    add_hierarchical_median_baseline,
)
from src.modeling.features import build_feature_matrix, build_preprocessor
from src.modeling.metrics import evaluate_predictions, evaluate_by_segment
from src.modeling.models import build_regression_models, build_quantile_model
from src.modeling.segment_evaluation import (
    add_target_range,
    evaluate_all_models_by_segments,
    get_top_segment_errors,
)
from src.modeling.splits import temporal_train_validation_test_split


def main() -> None:
    ensure_directories([MODELS_DIR, TABLES_DIR])

    df = pd.read_parquet(EDA_BASE_FILE)
    df = df.dropna(subset=["arrival_port_ts", MODEL_TARGET]).copy()

    if MODEL_LOG_TARGET not in df.columns:
        df[MODEL_LOG_TARGET] = np.log1p(df[MODEL_TARGET])

    train_df, val_df, test_df = temporal_train_validation_test_split(
        df=df,
        date_col="arrival_port_ts",
        train_end_date=TRAIN_END_DATE,
        validation_end_date=VALIDATION_END_DATE,
        test_start_date=TEST_START_DATE,
    )

    print("Temporal split:")
    print(f"Train: {len(train_df):,}")
    print(f"Validation: {len(val_df):,}")
    print(f"Test: {len(test_df):,}")

    metrics_rows = []

    # Baselines
    baseline_test = test_df.copy()
    baseline_test = add_global_mean_baseline(train_df, MODEL_TARGET, baseline_test)
    baseline_test = add_global_median_baseline(train_df, MODEL_TARGET, baseline_test)
    baseline_test = add_group_median_baseline(
        train_df=train_df,
        scoring_df=baseline_test,
        group_cols=["port"],
        target_col=MODEL_TARGET,
        output_col="pred_median_by_port",
    )

    if "operation_type" in train_df.columns:
        baseline_test = add_group_median_baseline(
            train_df=train_df,
            scoring_df=baseline_test,
            group_cols=["port", "operation_type"],
            target_col=MODEL_TARGET,
            output_col="pred_median_by_port_operation",
        )

    for pred_col in [
        "pred_global_mean",
        "pred_global_median",
        "pred_median_by_port",
        "pred_median_by_port_operation",
    ]:
        if pred_col in baseline_test.columns:
            metrics_rows.append(
                evaluate_predictions(
                    baseline_test,
                    actual_col=MODEL_TARGET,
                    prediction_col=pred_col,
                    model_name=pred_col.replace("pred_", "baseline_"),
                    dataset_name="test",
                )
            )

    # ML models
    X_train, cat_cols, num_cols = build_feature_matrix(
        train_df,
        categorical_features=CATEGORICAL_FEATURES,
        numerical_features=NUMERICAL_FEATURES,
        operation_flag_prefix=OPERATION_FLAG_PREFIX,
    )
    X_val, _, _ = build_feature_matrix(
        val_df,
        categorical_features=CATEGORICAL_FEATURES,
        numerical_features=NUMERICAL_FEATURES,
        operation_flag_prefix=OPERATION_FLAG_PREFIX,
    )
    X_test, _, _ = build_feature_matrix(
        test_df,
        categorical_features=CATEGORICAL_FEATURES,
        numerical_features=NUMERICAL_FEATURES,
        operation_flag_prefix=OPERATION_FLAG_PREFIX,
    )

    preprocessor = build_preprocessor(cat_cols, num_cols, scale_numeric=False)
    models = build_regression_models(preprocessor)

    operation_flag_cols = [
        col for col in test_df.columns
        if col.startswith("op_")
    ]

    prediction_context_cols = [
        "port",
        "port_name",
        "port_display",
        "region",
        "state",
        "operation_type",
        "arrival_port_ts",
        "arrival_month",
        "arrival_dayofweek",
        "arrival_is_weekend",
        MODEL_TARGET,
    ]

    available_prediction_context_cols = [
        col for col in prediction_context_cols if col in test_df.columns
    ]

    test_predictions = test_df[
        available_prediction_context_cols + operation_flag_cols
    ].copy()

    baseline_prediction_cols = [
        col for col in baseline_test.columns if col.startswith("pred_")
    ]

    for col in baseline_prediction_cols:
        test_predictions[col] = baseline_test[col].values

    for model_name, model in models.items():
        print(f"Training {model_name}...")

        if model_name in {"ridge_log_target", "elasticnet_log_target"}:
            y_train = train_df[MODEL_LOG_TARGET]
            model.fit(X_train, y_train)
            y_pred = np.expm1(model.predict(X_test))
        else:
            y_train = train_df[MODEL_TARGET]
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

        y_pred = np.clip(y_pred, a_min=0, a_max=None)
        pred_col = f"pred_{model_name}"
        test_predictions[pred_col] = y_pred

        metrics_rows.append(
            evaluate_predictions(
                test_predictions,
                actual_col=MODEL_TARGET,
                prediction_col=pred_col,
                model_name=model_name,
                dataset_name="test",
            )
        )

        joblib.dump(model, MODELS_DIR / f"{model_name}.joblib")

    # Quantile models
    for quantile in RISK_QUANTILES:
        model_name = f"gradient_boosting_q{int(quantile * 100)}"
        print(f"Training {model_name}...")

        q_model = build_quantile_model(preprocessor, quantile=quantile)
        q_model.fit(X_train, train_df[MODEL_TARGET])

        pred_col = f"pred_q{int(quantile * 100)}"
        test_predictions[pred_col] = np.clip(q_model.predict(X_test), a_min=0, a_max=None)

        joblib.dump(q_model, MODELS_DIR / f"{model_name}.joblib")

    overall_metrics = pd.concat(metrics_rows, ignore_index=True)
    overall_metrics.to_csv(TABLES_DIR / "model_metrics_overall.csv", index=False)

    segment_metrics = []
    for pred_col in [col for col in test_predictions.columns if col.startswith("pred_")]:
        segment_metrics.append(
            evaluate_by_segment(
                test_predictions,
                segment_cols=["port", "region"],
                actual_col=MODEL_TARGET,
                prediction_col=pred_col,
                model_name=pred_col.replace("pred_", ""),
                min_rows=30,
            )
        )

    pd.concat(segment_metrics, ignore_index=True).to_csv(
        TABLES_DIR / "model_metrics_by_segment.csv",
        index=False,
    )
    

    test_predictions.to_parquet(TABLES_DIR / "model_predictions_test.parquet", index=False)

    test_predictions = add_target_range(
        df=test_predictions,
        target_col=MODEL_TARGET,
        output_col="target_range",
    )

    prediction_cols = [
        col for col in test_predictions.columns
        if col.startswith("pred_")
    ]

    segment_cols = [
        "port",
        "region",
        "state",
        "target_range",
    ]

    available_segment_cols = [
        col for col in segment_cols
        if col in test_predictions.columns
    ]

    operation_flag_cols = [
        col for col in test_predictions.columns
        if col.startswith("op_")
    ]

    segmented_metrics = evaluate_all_models_by_segments(
        df=test_predictions,
        actual_col=MODEL_TARGET,
        prediction_cols=prediction_cols,
        segment_cols=available_segment_cols,
        flag_cols=operation_flag_cols,
        min_rows=30,
    )

    segmented_metrics.to_csv(
        TABLES_DIR / "model_metrics_by_segment.csv",
        index=False,
    )

    top_segment_errors = get_top_segment_errors(
        segmented_metrics=segmented_metrics,
        metric_col="mae",
        top_n=30,
    )

    top_segment_errors.to_csv(
        TABLES_DIR / "model_top_segment_errors.csv",
        index=False,
    )

    test_predictions.to_parquet(
        TABLES_DIR / "model_predictions_test.parquet",
        index=False,
    )

    print("Modeling pipeline finished successfully.")
    print(f"Saved: {TABLES_DIR / 'model_metrics_overall.csv'}")
    print(f"Saved: {TABLES_DIR / 'model_metrics_by_segment.csv'}")
    print(f"Saved: {TABLES_DIR / 'model_predictions_test.parquet'}")


if __name__ == "__main__":
    main()
