"""Reconstrucao canonica da modelagem do Capitulo 4.

Este script transforma o experimento historico validado em uma pipeline oficial
e reproduzivel no repositorio principal. Ele escreve somente em
`results/cap4_rebuild/` e cria o notebook `notebooks/modeling_step_by_step_v2.ipynb`.

O objetivo e academico: manter as decisoes explicitas, rastreaveis e alinhadas
com a regra de anti-leakage do TCC.
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    GradientBoostingRegressor,
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.linear_model import Ridge
from sklearn.metrics import (
    mean_absolute_error,
    mean_pinball_loss,
    mean_squared_error,
    median_absolute_error,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT_BOOTSTRAP = Path(__file__).resolve().parents[1]
if str(ROOT_BOOTSTRAP) not in sys.path:
    sys.path.insert(0, str(ROOT_BOOTSTRAP))

from src.features.historical_context import (
    add_cyclical_features,
    build_historical_features,
    run_leakage_tests,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "processed" / "eda_base.parquet"
RESULTS_DIR = ROOT / "results" / "cap4_rebuild"
FIGURES_DIR = RESULTS_DIR / "figures"
NOTEBOOK_PATH = ROOT / "notebooks" / "modeling_step_by_step_v2.ipynb"

TARGET = "t_total_port_stay_h"
DATE_COL = "arrival_port_ts"
PORT_COL = "port"
OPERATION_COL = "operation_type"
RANDOM_STATE = 42
MIN_GROUP_SIZE = 30

SPLIT_BOUNDS = {
    "train_start": "2023-01-01",
    "validation_start": "2024-07-01",
    "calibration_start": "2025-01-01",
    "final_test_start": "2025-07-01",
    "final_test_end": "2026-01-01",
}

NOTEBOOK_EXPECTED = {
    "baseline_global_median": {
        "mae": 56.36327882002423,
        "rmse": 112.0850157358452,
        "medae": 23.933333333333337,
        "rmsle": 1.1003884559748442,
    },
    "baseline_median_by_port": {
        "mae": 52.10191369544577,
        "rmse": 108.12844578521434,
        "medae": 20.666666666666668,
        "rmsle": 0.9822516369310347,
    },
    "baseline_hierarchical_port_operation": {
        "mae": 47.864135,
        "rmse": 100.996561,
        "medae": 19.283333,
        "rmsle": 0.895052,
    },
    "random_forest_original": {
        "mae": 47.588449854600995,
        "rmse": 99.29463304180452,
        "medae": 19.324820003674404,
        "rmsle": 0.8761107153998976,
    },
    "gradient_boosting_original": {
        "mae": 47.607366,
        "rmse": 99.512784,
        "medae": 18.992126,
        "rmsle": 0.877131,
    },
    "hist_gradient_boosting_original": {
        "mae": 47.303739,
        "rmse": 98.982542,
        "medae": 19.034311,
        "rmsle": 0.868721,
    },
}


@dataclass(frozen=True)
class FeatureRegistry:
    """Registro das features oficiais e de suas familias metodologicas."""

    original_features: list[str]
    enriched_features: list[str]
    categorical_features: list[str]
    numerical_features: list[str]
    operation_features: list[str]
    historical_families: dict[str, list[str]]
    feature_metadata: dict[str, str]


@dataclass(frozen=True)
class ModelSpec:
    """Especificacao pequena dos modelos pontuais comparados."""

    name: str
    family: str
    feature_config: str


def log(message: str) -> None:
    """Registra progresso da execucao canonica."""

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line, flush=True)
    with (RESULTS_DIR / "run_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def git_value(args: list[str]) -> str:
    """Executa consulta Git de leitura para documentar a reproducibilidade."""

    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def assert_official_context() -> dict[str, str]:
    """Bloqueia execucao fora do repositorio/branch oficiais."""

    branch = git_value(["branch", "--show-current"])
    if branch == "main":
        raise RuntimeError("Recusando executar modelagem oficial diretamente em main.")
    if branch != "cap4/rebuild-safe-historical-features":
        raise RuntimeError(f"Branch inesperada: {branch}")
    status = git_value(["status", "--short", "--branch"])
    if not str(ROOT).lower().endswith("port_leadtime_prediction"):
        raise RuntimeError(f"Diretorio raiz inesperado: {ROOT}")
    return {
        "root": str(ROOT),
        "branch": branch,
        "head": git_value(["rev-parse", "HEAD"]),
        "git_status": status,
        "python": sys.executable,
    }


def rmsle(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Calcula RMSLE com previsoes truncadas em zero."""

    y_true = np.clip(np.asarray(y_true, dtype=float), 0, None)
    y_pred = np.clip(np.asarray(y_pred, dtype=float), 0, None)
    return float(np.sqrt(np.mean((np.log1p(y_pred) - np.log1p(y_true)) ** 2)))


def regression_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    """Metricas pontuais principais do Capitulo 4."""

    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.clip(np.asarray(y_pred, dtype=float), 0, None)
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "medae": float(median_absolute_error(y_true, y_pred)),
        "rmsle": rmsle(y_true, y_pred),
    }


def tail_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    q90_threshold: float,
    q95_threshold: float,
) -> dict[str, float | int]:
    """Metricas de cauda para observacoes acima dos limiares historicos."""

    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.clip(np.asarray(y_pred, dtype=float), 0, None)
    out: dict[str, float | int] = {
        "q90_threshold_h": float(q90_threshold),
        "q95_threshold_h": float(q95_threshold),
    }
    for label, threshold in [("q90", q90_threshold), ("q95", q95_threshold)]:
        mask = y_true >= threshold
        errors = y_pred[mask] - y_true[mask]
        out[f"n_{label}"] = int(mask.sum())
        out[f"mae_{label}"] = float(np.abs(errors).mean()) if mask.any() else np.nan
        out[f"bias_{label}"] = float(errors.mean()) if mask.any() else np.nan
        out[f"underprediction_rate_{label}"] = (
            float((errors < 0).mean()) if mask.any() else np.nan
        )
    return out


def score_row(
    model: str,
    feature_config: str,
    dataset: str,
    y_true: np.ndarray,
    y_pred: np.ndarray,
    q90: float,
    q95: float,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    """Une metricas globais e de cauda em uma linha tabular."""

    row: dict[str, object] = {
        "model": model,
        "feature_config": feature_config,
        "dataset": dataset,
        **regression_metrics(y_true, y_pred),
        **tail_metrics(y_true, y_pred, q90, q95),
    }
    if extra:
        row.update(extra)
    return row


def load_modeling_data() -> pd.DataFrame:
    """Carrega a base oficial da EDA e prepara colunas temporais basicas."""

    df = pd.read_parquet(DATA_FILE)
    for col in [DATE_COL, "berthing_ts", "unberthing_ts", "departure_port_ts"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df.dropna(subset=[DATE_COL, TARGET]).copy()
    df = df[
        (df[DATE_COL] >= SPLIT_BOUNDS["train_start"])
        & (df[DATE_COL] < SPLIT_BOUNDS["final_test_end"])
    ].copy()
    return add_cyclical_features(df)


def chapter4_feature_lists(df: pd.DataFrame) -> tuple[list[str], list[str], list[str]]:
    """Retorna o conjunto ORIGINAL usado no Capitulo 4 atual."""

    categorical = [
        "port",
        "region",
        "state",
        "arrival_shift",
        "arrival_season",
    ]
    numerical = [
        "arrival_quarter",
        "arrival_weekofyear",
        "arrival_is_weekend",
        "arrival_hour_sin",
        "arrival_hour_cos",
        "arrival_dow_sin",
        "arrival_dow_cos",
        "arrival_month_sin",
        "arrival_month_cos",
        "rain_sum_prev_1d",
        "precipitation_sum_prev_1d",
        "wind_speed_10m_max_prev_1d",
        "wind_gusts_10m_max_prev_1d",
        "temperature_2m_mean_prev_1d",
        "rain_sum_prev_3d",
        "precipitation_hours_prev_3d",
        "temperature_2m_mean_prev_3d",
        "wind_speed_10m_max_prev_3d",
        "wind_gusts_10m_max_prev_3d",
        "rain_sum_prev_7d",
        "precipitation_hours_prev_7d",
        "temperature_2m_mean_prev_7d",
        "wind_speed_10m_max_prev_7d",
        "wind_gusts_10m_max_prev_7d",
    ]
    operation = sorted(col for col in df.columns if col.startswith("op_"))
    categorical = [col for col in categorical if col in df.columns]
    numerical = [col for col in numerical if col in df.columns]
    operation = [col for col in operation if col in df.columns]
    return categorical, numerical, operation


def split_data(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Aplica os splits temporais semiabertos definidos para o Capitulo 4."""

    return {
        "train": df[
            (df[DATE_COL] >= SPLIT_BOUNDS["train_start"])
            & (df[DATE_COL] < SPLIT_BOUNDS["validation_start"])
        ].copy(),
        "validation": df[
            (df[DATE_COL] >= SPLIT_BOUNDS["validation_start"])
            & (df[DATE_COL] < SPLIT_BOUNDS["calibration_start"])
        ].copy(),
        "calibration": df[
            (df[DATE_COL] >= SPLIT_BOUNDS["calibration_start"])
            & (df[DATE_COL] < SPLIT_BOUNDS["final_test_start"])
        ].copy(),
        "final_test": df[
            (df[DATE_COL] >= SPLIT_BOUNDS["final_test_start"])
            & (df[DATE_COL] < SPLIT_BOUNDS["final_test_end"])
        ].copy(),
    }


def group_quantile_prediction(
    train: pd.DataFrame,
    scoring: pd.DataFrame,
    group_cols: list[str],
    quantile: float,
    min_group_size: int = 1,
) -> np.ndarray:
    """Predicao por quantil historico com fallback global."""

    global_q = train[TARGET].quantile(quantile)
    stats = (
        train.groupby(group_cols, dropna=False)[TARGET]
        .agg(q=lambda x: x.quantile(quantile), count="count")
        .reset_index()
    )
    stats.loc[stats["count"] < min_group_size, "q"] = np.nan
    scored = scoring[group_cols].copy().merge(stats[group_cols + ["q"]], on=group_cols, how="left")
    return scored["q"].fillna(global_q).to_numpy(dtype=float)


def hierarchical_quantile_prediction(
    train: pd.DataFrame,
    scoring: pd.DataFrame,
    quantile: float,
    primary_cols: list[str] | None = None,
    fallback_cols: list[str] | None = None,
    min_group_size: int = MIN_GROUP_SIZE,
) -> np.ndarray:
    """Baseline hierarquico por porto e operacao para mediana/quantis."""

    primary_cols = primary_cols or [PORT_COL, OPERATION_COL]
    fallback_cols = fallback_cols or [PORT_COL]
    global_q = train[TARGET].quantile(quantile)
    primary = (
        train.groupby(primary_cols, dropna=False)[TARGET]
        .agg(q=lambda x: x.quantile(quantile), count="count")
        .reset_index()
    )
    primary = primary[primary["count"] >= min_group_size]
    primary = primary[primary_cols + ["q"]].rename(columns={"q": "primary_q"})
    fallback = (
        train.groupby(fallback_cols, dropna=False)[TARGET]
        .agg(q=lambda x: x.quantile(quantile), count="count")
        .reset_index()
    )
    fallback = fallback[fallback["count"] >= min_group_size]
    fallback = fallback[fallback_cols + ["q"]].rename(columns={"q": "fallback_q"})
    out = scoring[primary_cols].copy()
    out = out.merge(primary, on=primary_cols, how="left")
    out = out.merge(fallback, on=fallback_cols, how="left")
    return out["primary_q"].fillna(out["fallback_q"]).fillna(global_q).to_numpy(dtype=float)


def build_registry(df: pd.DataFrame) -> tuple[pd.DataFrame, FeatureRegistry]:
    """Reconstrui features seguras e devolve o registro canonico de colunas."""

    categorical, numerical, operation = chapter4_feature_lists(df)
    original = categorical + numerical + operation
    historical = build_historical_features(df, operation)
    family_order = [
        "flow",
        "port_performance",
        "state",
        "vessel_history",
        "vessel_port_history",
        "source_history",
        "destination_history",
        "route_history",
        "operation_mix",
    ]
    historical_cols: list[str] = []
    for family in family_order:
        historical_cols.extend(historical.families.get(family, []))
    enriched = list(dict.fromkeys(original + historical_cols))
    return historical.data, FeatureRegistry(
        original_features=original,
        enriched_features=enriched,
        categorical_features=categorical,
        numerical_features=numerical,
        operation_features=operation,
        historical_families=historical.families,
        feature_metadata=historical.metadata,
    )


def build_preprocessor(
    numeric_features: list[str],
    categorical_features: list[str],
    scale_numeric: bool = False,
) -> ColumnTransformer:
    """Cria preprocessamento compativel com Ridge, RF e GB."""

    numeric_steps: list[tuple[str, object]] = [("imputer", SimpleImputer(strategy="median"))]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))
    categorical_pipeline = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    return ColumnTransformer(
        [
            ("num", Pipeline(numeric_steps), numeric_features),
            ("cat", categorical_pipeline, categorical_features),
        ]
    )


def prepare_hgb_frames(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    features: list[str],
    categorical_features: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepara categoricas para o HGB sem aprender categorias no conjunto avaliado."""

    x_train = train_df[features].copy()
    x_eval = eval_df[features].copy()
    for col in categorical_features:
        if col not in features:
            continue
        categories = pd.Index(x_train[col].astype("string").dropna().astype(str).unique())
        x_train[col] = pd.Categorical(x_train[col].astype("string"), categories=categories)
        x_eval[col] = pd.Categorical(x_eval[col].astype("string"), categories=categories)
    return x_train, x_eval


def fit_predict_model(
    family: str,
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    features: list[str],
    categorical_features: list[str],
    ridge_alpha: float | None = None,
) -> tuple[object, np.ndarray, pd.DataFrame]:
    """Treina modelo pontual e devolve predicoes em horas."""

    numeric_features = [col for col in features if col not in categorical_features]
    y_train = train_df[TARGET].to_numpy(dtype=float)
    if family == "ridge":
        model = Pipeline(
            [
                ("preprocessor", build_preprocessor(numeric_features, categorical_features, True)),
                ("model", Ridge(alpha=float(ridge_alpha if ridge_alpha is not None else 1.0))),
            ]
        )
        x_train = train_df[features].copy()
        x_eval = eval_df[features].copy()
    elif family == "rf":
        model = Pipeline(
            [
                ("preprocessor", build_preprocessor(numeric_features, categorical_features, False)),
                (
                    "model",
                    RandomForestRegressor(
                        n_estimators=250,
                        min_samples_leaf=50,
                        max_features=1.0,
                        random_state=RANDOM_STATE,
                        n_jobs=-1,
                    ),
                ),
            ]
        )
        x_train = train_df[features].copy()
        x_eval = eval_df[features].copy()
    elif family == "gb":
        model = Pipeline(
            [
                ("preprocessor", build_preprocessor(numeric_features, categorical_features, False)),
                (
                    "model",
                    GradientBoostingRegressor(
                        loss="huber",
                        n_estimators=300,
                        learning_rate=0.10,
                        max_depth=7,
                        min_samples_leaf=20,
                        subsample=1.0,
                        random_state=RANDOM_STATE,
                    ),
                ),
            ]
        )
        x_train = train_df[features].copy()
        x_eval = eval_df[features].copy()
    elif family == "hgb":
        model = HistGradientBoostingRegressor(
            loss="squared_error",
            learning_rate=0.10,
            max_iter=300,
            max_leaf_nodes=15,
            min_samples_leaf=20,
            l2_regularization=1.0,
            categorical_features="from_dtype",
            early_stopping=False,
            random_state=RANDOM_STATE,
        )
        x_train, x_eval = prepare_hgb_frames(train_df, eval_df, features, categorical_features)
    else:
        raise ValueError(f"Familia desconhecida: {family}")

    model.fit(x_train, np.log1p(y_train))
    pred = np.clip(np.expm1(model.predict(x_eval)), 0, None)
    return model, pred, x_eval


def tune_ridge_alpha(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    features: list[str],
    categorical_features: list[str],
) -> tuple[float, pd.DataFrame]:
    """Repete a comparacao simples de alphas do Capitulo 4 para Ridge."""

    rows = []
    for alpha in [0.01, 0.1, 1.0, 10.0, 100.0]:
        _, pred, _ = fit_predict_model(
            "ridge",
            train_df,
            val_df,
            features,
            categorical_features,
            ridge_alpha=alpha,
        )
        rows.append({"alpha": alpha, **regression_metrics(val_df[TARGET], pred)})
    table = pd.DataFrame(rows).sort_values(["mae", "rmse"]).reset_index(drop=True)
    return float(table.iloc[0]["alpha"]), table


def reproduce_chapter4(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Reproduz os resultados atuais do Capitulo 4 antes das novas features."""

    log("Reproduzindo o Capitulo 4 atual em validation.")
    train_df = splits["train"]
    val_df = splits["validation"]
    q90 = float(train_df[TARGET].quantile(0.90))
    q95 = float(train_df[TARGET].quantile(0.95))
    rows: list[dict[str, object]] = []

    predictions = {
        "baseline_global_median": np.repeat(train_df[TARGET].median(), len(val_df)),
        "baseline_median_by_port": group_quantile_prediction(train_df, val_df, [PORT_COL], 0.50),
        "baseline_hierarchical_port_operation": hierarchical_quantile_prediction(train_df, val_df, 0.50),
    }
    ridge_alpha, ridge_table = tune_ridge_alpha(
        train_df,
        val_df,
        registry.original_features,
        registry.categorical_features,
    )
    ridge_table.to_csv(RESULTS_DIR / "ridge_alpha_validation.csv", index=False)
    _, predictions["ridge_original"], _ = fit_predict_model(
        "ridge",
        train_df,
        val_df,
        registry.original_features,
        registry.categorical_features,
        ridge_alpha=ridge_alpha,
    )
    for name, family in [
        ("random_forest_original", "rf"),
        ("gradient_boosting_original", "gb"),
        ("hist_gradient_boosting_original", "hgb"),
    ]:
        log(f"Reproduzindo {name}.")
        _, pred, _ = fit_predict_model(
            family,
            train_df,
            val_df,
            registry.original_features,
            registry.categorical_features,
        )
        predictions[name] = pred

    for name, pred in predictions.items():
        row = score_row(name, "ORIGINAL", "validation", val_df[TARGET], pred, q90, q95)
        expected = NOTEBOOK_EXPECTED.get(name, {})
        for metric, expected_value in expected.items():
            row[f"notebook_{metric}"] = expected_value
            row[f"delta_{metric}"] = row[metric] - expected_value
        row["material_divergence"] = bool(
            expected
            and any(abs(row.get(f"delta_{m}", 0.0)) > 0.05 for m in ["mae", "rmse", "medae", "rmsle"])
        )
        rows.append(row)

    table = pd.DataFrame(rows)
    table.to_csv(RESULTS_DIR / "baseline_reproduction.csv", index=False)
    if table["material_divergence"].any():
        raise RuntimeError("Divergencia material na reproducao do Capitulo 4.")
    return table, {"ORIGINAL": ridge_alpha}


def run_point_model_comparison(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
    ridge_alphas: dict[str, float],
) -> tuple[pd.DataFrame, dict[tuple[str, str], np.ndarray], dict[str, float]]:
    """Compara Ridge, RF, GB e HGB nas configuracoes ORIGINAL e ENRICHED."""

    log("Executando comparacao pontual validation/calibration.")
    train_df = splits["train"]
    eval_df = pd.concat([splits["validation"], splits["calibration"]], ignore_index=True)
    n_val = len(splits["validation"])
    q90 = float(train_df[TARGET].quantile(0.90))
    q95 = float(train_df[TARGET].quantile(0.95))
    feature_sets = {
        "ORIGINAL": registry.original_features,
        "ENRICHED_SAFE_HISTORY": registry.enriched_features,
    }

    if "ENRICHED_SAFE_HISTORY" not in ridge_alphas:
        alpha, table = tune_ridge_alpha(
            train_df,
            splits["validation"],
            registry.enriched_features,
            registry.categorical_features,
        )
        ridge_alphas["ENRICHED_SAFE_HISTORY"] = alpha
        table.to_csv(RESULTS_DIR / "ridge_alpha_enriched_validation.csv", index=False)

    rows: list[dict[str, object]] = []
    predictions: dict[tuple[str, str], np.ndarray] = {}
    baseline_pred = hierarchical_quantile_prediction(train_df, eval_df, 0.50)
    for dataset, y_true, pred in [
        ("validation", splits["validation"][TARGET], baseline_pred[:n_val]),
        ("calibration", splits["calibration"][TARGET], baseline_pred[n_val:]),
    ]:
        rows.append(
            score_row(
                "baseline_hierarchical",
                "ORIGINAL",
                dataset,
                y_true,
                pred,
                q90,
                q95,
            )
        )

    for family, label in [
        ("ridge", "Ridge"),
        ("rf", "Random Forest"),
        ("gb", "Gradient Boosting"),
        ("hgb", "HistGradientBoosting"),
    ]:
        for config_name, features in feature_sets.items():
            log(f"Treinando {label} {config_name}.")
            alpha = ridge_alphas.get(config_name) if family == "ridge" else None
            _, pred_eval, _ = fit_predict_model(
                family,
                train_df,
                eval_df,
                features,
                registry.categorical_features,
                ridge_alpha=alpha,
            )
            predictions[(label, config_name)] = pred_eval
            for dataset, y_true, pred in [
                ("validation", splits["validation"][TARGET], pred_eval[:n_val]),
                ("calibration", splits["calibration"][TARGET], pred_eval[n_val:]),
            ]:
                rows.append(
                    score_row(
                        label,
                        config_name,
                        dataset,
                        y_true,
                        pred,
                        q90,
                        q95,
                        extra={"ridge_alpha": alpha if family == "ridge" else np.nan},
                    )
                )

    table = pd.DataFrame(rows)
    table.to_csv(RESULTS_DIR / "point_model_comparison_validation.csv", index=False)
    return table, predictions, ridge_alphas


def historical_feature_sets(registry: FeatureRegistry) -> dict[str, list[str]]:
    """Define E0-E6 da ablacao historica."""

    base = registry.original_features
    fam = registry.historical_families
    sets = {
        "E0_original": base,
        "E1_fluxo_d1": base
        + ["arrivals_prev_1d", "departures_prev_1d", "flow_balance_prev_1d"],
        "E2_fluxo_1_3d": base
        + [
            "arrivals_prev_1d",
            "arrivals_prev_3d",
            "arrivals_avg_prev_3d",
            "arrivals_max_prev_3d",
            "departures_prev_1d",
            "departures_prev_3d",
            "departures_avg_prev_3d",
            "flow_balance_prev_1d",
            "flow_balance_prev_3d",
        ],
        "E3_fluxo_1_3_7d": base
        + [
            "arrivals_prev_1d",
            "arrivals_prev_3d",
            "arrivals_prev_7d",
            "arrivals_avg_prev_3d",
            "arrivals_avg_prev_7d",
            "arrivals_max_prev_3d",
            "arrivals_max_prev_7d",
            "departures_prev_1d",
            "departures_prev_3d",
            "departures_prev_7d",
            "departures_avg_prev_3d",
            "departures_avg_prev_7d",
            "flow_balance_prev_1d",
            "flow_balance_prev_3d",
            "flow_balance_prev_7d",
        ],
    }
    sets["E4_desempenho_porto"] = sets["E3_fluxo_1_3_7d"] + fam.get("port_performance", [])
    sets["E5_estado_operacional"] = sets["E4_desempenho_porto"] + fam.get("state", [])
    sets["E6_historico_adicional"] = registry.enriched_features
    return {name: list(dict.fromkeys([col for col in cols if col])) for name, cols in sets.items()}


def choose_tree_family(point_table: pd.DataFrame) -> str:
    """Escolhe a familia de arvores para ablaÃ§Ãµes sem usar final_test."""

    val = point_table[
        (point_table["dataset"] == "validation")
        & (point_table["model"].isin(["Random Forest", "Gradient Boosting", "HistGradientBoosting"]))
        & (point_table["feature_config"] == "ENRICHED_SAFE_HISTORY")
    ].copy()
    val = val.sort_values(["mae", "rmse", "mae_q90"]).reset_index(drop=True)
    winner = val.iloc[0]["model"]
    return {
        "Random Forest": "rf",
        "Gradient Boosting": "gb",
        "HistGradientBoosting": "hgb",
    }[winner]


def run_historical_ablation(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
    tree_family: str,
) -> pd.DataFrame:
    """Executa ablacao incremental das features historicas."""

    log("Executando ablacao historica E0-E6.")
    feature_sets = historical_feature_sets(registry)
    train_df = splits["train"]
    eval_df = pd.concat([splits["validation"], splits["calibration"]], ignore_index=True)
    n_val = len(splits["validation"])
    q90 = float(train_df[TARGET].quantile(0.90))
    q95 = float(train_df[TARGET].quantile(0.95))
    rows = []
    for name, features in feature_sets.items():
        log(f"Ablacao {name} com {tree_family}.")
        _, pred_eval, _ = fit_predict_model(
            tree_family,
            train_df,
            eval_df,
            features,
            registry.categorical_features,
        )
        for dataset, y_true, pred in [
            ("validation", splits["validation"][TARGET], pred_eval[:n_val]),
            ("calibration", splits["calibration"][TARGET], pred_eval[n_val:]),
        ]:
            rows.append(
                score_row(name, "ABLAÃ‡ÃƒO_HISTORICA", dataset, y_true, pred, q90, q95)
            )
    table = pd.DataFrame(rows)
    base_by_dataset = table[table["model"] == "E0_original"][["dataset", "mae"]].rename(
        columns={"mae": "e0_mae"}
    )
    table = table.merge(base_by_dataset, on="dataset", how="left")
    table["mae_gain_vs_e0_h"] = table["e0_mae"] - table["mae"]
    table["mae_gain_vs_e0_pct"] = table["mae_gain_vs_e0_h"] / table["e0_mae"] * 100
    table.to_csv(RESULTS_DIR / "historical_feature_ablation.csv", index=False)
    return table


def select_point_winner(point_table: pd.DataFrame) -> dict[str, object]:
    """Congela modelo/features antes do final_test."""

    candidates = point_table[
        (point_table["dataset"] == "validation")
        & (point_table["model"] != "baseline_hierarchical")
    ].copy()
    candidates = candidates.sort_values(["mae", "rmse", "mae_q90"]).reset_index(drop=True)
    winner = candidates.iloc[0].to_dict()
    winner["selection_criterion"] = (
        "Menor MAE em validation; desempate por RMSE e MAE na cauda Q90. "
        "Calibration foi reportado como verificacao secundaria, sem consulta ao final_test."
    )
    return winner


def family_to_code(model: str) -> str:
    """Converte rotulo tabular em codigo interno de familia."""

    return {
        "Ridge": "ridge",
        "Random Forest": "rf",
        "Gradient Boosting": "gb",
        "HistGradientBoosting": "hgb",
    }[model]


def feature_config_to_cols(registry: FeatureRegistry, feature_config: str) -> list[str]:
    """Seleciona colunas conforme configuracao nomeada."""

    if feature_config == "ORIGINAL":
        return registry.original_features
    if feature_config == "ENRICHED_SAFE_HISTORY":
        return registry.enriched_features
    raise ValueError(f"Configuracao desconhecida: {feature_config}")


def block_bootstrap(
    df: pd.DataFrame,
    block_cols: list[str],
    label: str,
    n_boot: int = 800,
) -> dict[str, object]:
    """Bootstrap em blocos para ganho pareado de erro absoluto."""

    temp = df.copy()
    temp["_block"] = temp[block_cols].astype("string").fillna("__MISSING__").agg("|".join, axis=1)
    blocks = temp["_block"].drop_duplicates().to_numpy()
    if len(blocks) < 10:
        return {
            "method": label,
            "n_blocks": int(len(blocks)),
            "mean_abs_error_gain_h": np.nan,
            "ci95_low_h": np.nan,
            "ci95_high_h": np.nan,
            "status": "not_enough_blocks",
        }
    rng = np.random.default_rng(RANDOM_STATE)
    reps = np.empty(n_boot)
    for i in range(n_boot):
        sample_blocks = rng.choice(blocks, size=len(blocks), replace=True)
        sample = pd.concat([temp[temp["_block"] == block] for block in sample_blocks], ignore_index=True)
        reps[i] = (
            sample["abs_error_original_h"].mean()
            - sample["abs_error_enriched_h"].mean()
        )
    return {
        "method": label,
        "n_blocks": int(len(blocks)),
        "mean_abs_error_gain_h": float(
            temp["abs_error_original_h"].mean() - temp["abs_error_enriched_h"].mean()
        ),
        "ci95_low_h": float(np.quantile(reps, 0.025)),
        "ci95_high_h": float(np.quantile(reps, 0.975)),
        "status": "ok",
    }


def paired_bootstrap(df: pd.DataFrame, label: str, n_boot: int = 1000) -> dict[str, object]:
    """Bootstrap pareado simples para ganho de MAE."""

    if len(df) < 50:
        return {
            "method": label,
            "n": int(len(df)),
            "mean_abs_error_gain_h": np.nan,
            "ci95_low_h": np.nan,
            "ci95_high_h": np.nan,
            "status": "not_enough_rows",
        }
    diffs = df["abs_error_original_h"].to_numpy() - df["abs_error_enriched_h"].to_numpy()
    rng = np.random.default_rng(RANDOM_STATE)
    reps = np.empty(n_boot)
    for i in range(n_boot):
        reps[i] = rng.choice(diffs, size=len(diffs), replace=True).mean()
    return {
        "method": label,
        "n": int(len(df)),
        "mean_abs_error_gain_h": float(diffs.mean()),
        "ci95_low_h": float(np.quantile(reps, 0.025)),
        "ci95_high_h": float(np.quantile(reps, 0.975)),
        "status": "ok",
    }


def final_point_evaluation(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
    winner: dict[str, object],
    ridge_alphas: dict[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Avalia final_test apos congelar modelo, features e hiperparametros."""

    log("Executando avaliacao final pareada.")
    final_train = pd.concat(
        [splits["train"], splits["validation"], splits["calibration"]],
        ignore_index=True,
    )
    test_df = splits["final_test"].copy()
    family = family_to_code(str(winner["model"]))
    q90 = float(final_train[TARGET].quantile(0.90))
    q95 = float(final_train[TARGET].quantile(0.95))

    alpha_original = ridge_alphas.get("ORIGINAL") if family == "ridge" else None
    alpha_enriched = ridge_alphas.get("ENRICHED_SAFE_HISTORY") if family == "ridge" else None
    _, pred_original, _ = fit_predict_model(
        family,
        final_train,
        test_df,
        registry.original_features,
        registry.categorical_features,
        ridge_alpha=alpha_original,
    )
    model_enriched, pred_enriched, x_test_prepared = fit_predict_model(
        family,
        final_train,
        test_df,
        registry.enriched_features,
        registry.categorical_features,
        ridge_alpha=alpha_enriched,
    )

    point_final = pd.DataFrame(
        [
            score_row(
                str(winner["model"]),
                "ORIGINAL",
                "final_test",
                test_df[TARGET],
                pred_original,
                q90,
                q95,
            ),
            score_row(
                str(winner["model"]),
                "ENRICHED_SAFE_HISTORY",
                "final_test",
                test_df[TARGET],
                pred_enriched,
                q90,
                q95,
            ),
        ]
    )
    point_final.to_csv(RESULTS_DIR / "point_model_comparison_final.csv", index=False)

    predictions = test_df[
        [
            "port_call_id",
            PORT_COL,
            "port_display",
            OPERATION_COL,
            DATE_COL,
            TARGET,
        ]
    ].copy()
    predictions["selected_model"] = winner["model"]
    predictions["prediction_original_h"] = pred_original
    predictions["prediction_enriched_h"] = pred_enriched
    predictions["abs_error_original_h"] = np.abs(pred_original - predictions[TARGET])
    predictions["abs_error_enriched_h"] = np.abs(pred_enriched - predictions[TARGET])
    predictions["error_original_h"] = pred_original - predictions[TARGET]
    predictions["error_enriched_h"] = pred_enriched - predictions[TARGET]
    predictions["arrival_week"] = predictions[DATE_COL].dt.to_period("W").astype(str)
    predictions.to_parquet(RESULTS_DIR / "point_final_predictions.parquet", index=False)

    port_rows = []
    for port, group in predictions.groupby("port_display", dropna=False):
        if len(group) < 50:
            continue
        mae_o = group["abs_error_original_h"].mean()
        mae_e = group["abs_error_enriched_h"].mean()
        port_rows.append(
            {
                "port_display": port,
                "n": int(len(group)),
                "mae_original": float(mae_o),
                "mae_enriched": float(mae_e),
                "absolute_gain_h": float(mae_o - mae_e),
                "percent_gain": float((mae_o - mae_e) / mae_o * 100) if mae_o else np.nan,
                "actual_q90_h": float(group[TARGET].quantile(0.90)),
                "actual_q95_h": float(group[TARGET].quantile(0.95)),
            }
        )
    port_comparison = pd.DataFrame(port_rows).sort_values("n", ascending=False)
    port_comparison.to_csv(RESULTS_DIR / "port_comparison.csv", index=False)

    bootstrap_rows = [
        paired_bootstrap(predictions, "paired_global"),
        block_bootstrap(predictions, ["arrival_week"], "block_week"),
        block_bootstrap(predictions, [PORT_COL, "arrival_week"], "block_port_week"),
    ]
    bootstrap = pd.DataFrame(bootstrap_rows)
    bootstrap.to_csv(RESULTS_DIR / "bootstrap_results.csv", index=False)

    importance = permutation_importance_manual(
        model_enriched,
        x_test_prepared,
        test_df[TARGET].to_numpy(dtype=float),
        registry.enriched_features,
        set(registry.original_features),
        family,
        registry.feature_metadata,
    )
    importance.to_csv(RESULTS_DIR / "feature_importance.csv", index=False)
    feature_family_summary = (
        importance.groupby(["family", "feature_origin"], dropna=False)
        .agg(
            n_features=("feature", "count"),
            total_importance_h=("importance_mae_increase_h", "sum"),
            mean_importance_h=("importance_mae_increase_h", "mean"),
            max_importance_h=("importance_mae_increase_h", "max"),
        )
        .reset_index()
        .sort_values("total_importance_h", ascending=False)
    )
    feature_family_summary.to_csv(RESULTS_DIR / "feature_family_summary.csv", index=False)

    tail_comparison = point_final[
        [
            "model",
            "feature_config",
            "dataset",
            "mae_q90",
            "mae_q95",
            "bias_q90",
            "bias_q95",
            "underprediction_rate_q90",
            "underprediction_rate_q95",
        ]
    ].copy()
    tail_comparison.to_csv(RESULTS_DIR / "tail_comparison.csv", index=False)
    return point_final, port_comparison, bootstrap, predictions


def predict_original_scale(model: object, x: pd.DataFrame) -> np.ndarray:
    """Prediz em horas a partir de modelo treinado em log1p."""

    return np.clip(np.expm1(model.predict(x)), 0, None)


def permutation_importance_manual(
    model: object,
    x_test: pd.DataFrame,
    y_true: np.ndarray,
    features: list[str],
    original_feature_set: set[str],
    family: str,
    feature_metadata: dict[str, str],
    sample_size: int = 2500,
    n_repeats: int = 3,
) -> pd.DataFrame:
    """Calcula permutation importance manual por aumento de MAE."""

    log("Calculando permutation importance.")
    rng = np.random.default_rng(RANDOM_STATE)
    if len(x_test) > sample_size:
        positions = rng.choice(len(x_test), size=sample_size, replace=False)
        x_sample = x_test.iloc[positions].copy()
        y_sample = y_true[positions]
    else:
        x_sample = x_test.copy()
        y_sample = y_true
    base_pred = predict_original_scale(model, x_sample)
    base_mae = mean_absolute_error(y_sample, base_pred)
    rows = []
    for feature in features:
        deltas = []
        for _ in range(n_repeats):
            shuffled = x_sample.copy()
            shuffled[feature] = rng.permutation(shuffled[feature].to_numpy())
            pred = predict_original_scale(model, shuffled)
            deltas.append(mean_absolute_error(y_sample, pred) - base_mae)
        rows.append(
            {
                "feature": feature,
                "feature_origin": "original" if feature in original_feature_set else "new",
                "family": feature_metadata.get(feature, infer_feature_family(feature)),
                "importance_mae_increase_h": float(np.mean(deltas)),
                "importance_std_h": float(np.std(deltas)),
                "sample_size": int(len(x_sample)),
                "model_family": family,
            }
        )
    return pd.DataFrame(rows).sort_values("importance_mae_increase_h", ascending=False)


def infer_feature_family(feature: str) -> str:
    """Classificacao simples de familias para catalogo e importancias."""

    if feature in {PORT_COL, "region", "state"}:
        return "ESTRUTURAL_GEOGRAFICA"
    if feature == OPERATION_COL or feature.startswith("op_"):
        return "OPERACIONAL_DA_ESCALA"
    if feature.startswith("arrival_"):
        return "TEMPORAL"
    if feature.endswith("_prev_1d") or feature.endswith("_prev_3d") or feature.endswith("_prev_7d"):
        if feature.startswith(("rain_", "precipitation_", "wind_", "temperature_")):
            return "CLIMATICA_HISTORICA"
        return "FLUXO_PORTUARIO_HISTORICO"
    return "OUTRA"


def feature_catalog(registry: FeatureRegistry, source_columns: set[str]) -> pd.DataFrame:
    """Cria catalogo canonico das features utilizadas na nova modelagem."""

    rows = []
    all_features = list(dict.fromkeys(registry.enriched_features))
    for feature in all_features:
        family = registry.feature_metadata.get(feature, infer_feature_family(feature))
        reconstructed = feature not in registry.original_features
        originally_eda = feature in source_columns
        if reconstructed:
            availability = "Conhecida ate 23:59:59 de D-1 por reconstrucao historica."
            rule = "Construida com calendario porto-dia completo e/ou eventos conhecidos antes do cutoff."
        elif family == "CLIMATICA_HISTORICA":
            availability = "Historico climatico defasado, excluindo o dia da chegada."
            rule = "Usar colunas prev_1d/prev_3d/prev_7d ja defasadas na base."
        elif family == "TEMPORAL":
            availability = "Disponivel no instante de chegada."
            rule = "Derivada diretamente de arrival_port_ts."
        else:
            availability = "Disponivel ou tratada como declarada no instante de chegada."
            rule = "Usar diretamente no modelo com imputacao ajustada no treino."
        rows.append(
            {
                "feature": feature,
                "familia": family,
                "descricao_linguagem_negocio": business_description(feature, family),
                "disponibilidade_temporal": availability,
                "regra_de_construcao": rule,
                "existia_originalmente_na_eda": originally_eda,
                "foi_reconstruida_devido_a_leakage": reconstructed,
                "entra_na_modelagem_final": True,
            }
        )
    table = pd.DataFrame(rows)
    table.to_csv(RESULTS_DIR / "feature_catalog.csv", index=False)
    return table


def business_description(feature: str, family: str) -> str:
    """Gera descricao curta em linguagem de negocio para o catalogo."""

    descriptions = {
        "ESTRUTURAL_GEOGRAFICA": "Identifica porto e recortes geograficos associados ao contexto operacional.",
        "OPERACIONAL_DA_ESCALA": "Representa o tipo declarado de operacao da escala.",
        "TEMPORAL": "Representa sazonalidade e horario de chegada da escala.",
        "CLIMATICA_HISTORICA": "Resume condicoes climaticas historicas antes da chegada.",
        "FLUXO_PORTUARIO_HISTORICO": "Mede volume recente de entradas e saidas no porto.",
        "DESEMPENHO_HISTORICO_CONHECIDO_DO_PORTO": "Resume desempenho portuario de escalas ja conhecidas.",
        "ESTADO_OPERACIONAL_RECONSTRUIDO_D_1": "Estima fila, atracacao e presenca acumulada no porto ate D-1.",
        "HISTORICO_DA_EMBARCACAO": "Resume historico conhecido da embarcacao.",
        "HISTORICO_EMBARCACAO_PORTO": "Resume memoria operacional da embarcacao naquele porto.",
        "HISTORICO_DE_ORIGEM": "Resume historico conhecido associado ao porto de origem.",
        "HISTORICO_DE_DESTINO": "Resume historico conhecido associado ao destino.",
        "HISTORICO_DE_ROTA": "Resume historico conhecido da rota origem-destino.",
        "MIX_OPERACIONAL_RECENTE": "Resume composicao recente das operacoes no porto.",
    }
    return descriptions.get(family, f"Feature {feature} usada como informacao auxiliar segura.")


def fit_quantile_models(
    train_df: pd.DataFrame,
    eval_df: pd.DataFrame,
    features: list[str],
    categorical_features: list[str],
) -> tuple[dict[str, object], dict[str, np.ndarray], pd.DataFrame]:
    """Treina HGB quantilico P50/P90/P95 com target log1p."""

    x_train, x_eval = prepare_hgb_frames(train_df, eval_df, features, categorical_features)
    models = {}
    predictions = {}
    for name, quantile in {"p50": 0.50, "p90": 0.90, "p95": 0.95}.items():
        model = HistGradientBoostingRegressor(
            loss="quantile",
            quantile=quantile,
            learning_rate=0.10,
            max_iter=300,
            max_leaf_nodes=15,
            min_samples_leaf=20,
            l2_regularization=1.0,
            categorical_features="from_dtype",
            early_stopping=False,
            random_state=RANDOM_STATE,
        )
        model.fit(x_train, np.log1p(train_df[TARGET].to_numpy(dtype=float)))
        predictions[name] = np.clip(np.expm1(model.predict(x_eval)), 0, None)
        models[name] = model
    return models, predictions, x_eval


def rearrange_quantiles(preds: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """Aplica rearranjo monotonico simples entre P50, P90 e P95."""

    stacked = np.vstack([preds["p50"], preds["p90"], preds["p95"]])
    ordered = np.maximum.accumulate(stacked, axis=0)
    return {"p50": ordered[0], "p90": ordered[1], "p95": ordered[2]}


def quantile_evaluation(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Reavalia regressao quantilica atual e enriquecida."""

    log("Executando regressao quantilica.")
    train_df = pd.concat([splits["train"], splits["validation"]], ignore_index=True)
    cal_df = splits["calibration"]
    eval_df = pd.concat([splits["calibration"], splits["final_test"]], ignore_index=True)
    n_cal = len(cal_df)
    rows = []
    coverage_rows = []
    prediction_tables = []

    for feature_config, features in [
        ("ORIGINAL", registry.original_features),
        ("ENRICHED_SAFE_HISTORY", registry.enriched_features),
    ]:
        log(f"Quantis HGB {feature_config}.")
        _, preds_raw, _ = fit_quantile_models(
            train_df,
            eval_df,
            features,
            registry.categorical_features,
        )
        crossing_raw = np.mean(
            (preds_raw["p50"] > preds_raw["p90"]) | (preds_raw["p90"] > preds_raw["p95"])
        )
        preds = rearrange_quantiles(preds_raw)
        for dataset, part, start, stop in [
            ("calibration", splits["calibration"], 0, n_cal),
            ("final_test", splits["final_test"], n_cal, len(eval_df)),
        ]:
            y = part[TARGET].to_numpy(dtype=float)
            row = {
                "model": "hgb_quantile_log",
                "feature_config": feature_config,
                "dataset": dataset,
                "quantile_crossing_rate_raw": float(crossing_raw),
                "mean_width_p90_p50_h": float(np.mean(preds["p90"][start:stop] - preds["p50"][start:stop])),
                "mean_width_p95_p50_h": float(np.mean(preds["p95"][start:stop] - preds["p50"][start:stop])),
            }
            for name, quantile in {"p50": 0.50, "p90": 0.90, "p95": 0.95}.items():
                pred = preds[name][start:stop]
                row[f"pinball_{name}"] = float(mean_pinball_loss(y, pred, alpha=quantile))
                coverage_rows.append(
                    {
                        "model": "hgb_quantile_log",
                        "feature_config": feature_config,
                        "dataset": dataset,
                        "quantile": name,
                        "target_coverage": quantile,
                        "empirical_coverage": float(np.mean(y <= pred)),
                        "pinball_loss": row[f"pinball_{name}"],
                    }
                )
            rows.append(row)
            pred_table = part[["port_call_id", PORT_COL, OPERATION_COL, DATE_COL, TARGET]].copy()
            pred_table["feature_config"] = feature_config
            pred_table["dataset"] = dataset
            for name in ["p50", "p90", "p95"]:
                pred_table[f"pred_{name}_h"] = preds[name][start:stop]
            pred_table["width_p90_p50_h"] = pred_table["pred_p90_h"] - pred_table["pred_p50_h"]
            pred_table["width_p95_p50_h"] = pred_table["pred_p95_h"] - pred_table["pred_p50_h"]
            prediction_tables.append(pred_table)

    for dataset, part in [("calibration", splits["calibration"]), ("final_test", splits["final_test"])]:
        base_row = {
            "model": "baseline_quantile_hierarchical",
            "feature_config": "ORIGINAL",
            "dataset": dataset,
            "quantile_crossing_rate_raw": 0.0,
        }
        preds_base = {}
        for name, quantile in {"p50": 0.50, "p90": 0.90, "p95": 0.95}.items():
            preds_base[name] = hierarchical_quantile_prediction(train_df, part, quantile)
        preds_base = rearrange_quantiles(preds_base)
        y = part[TARGET].to_numpy(dtype=float)
        base_row["mean_width_p90_p50_h"] = float(np.mean(preds_base["p90"] - preds_base["p50"]))
        base_row["mean_width_p95_p50_h"] = float(np.mean(preds_base["p95"] - preds_base["p50"]))
        for name, quantile in {"p50": 0.50, "p90": 0.90, "p95": 0.95}.items():
            base_row[f"pinball_{name}"] = float(mean_pinball_loss(y, preds_base[name], alpha=quantile))
            coverage_rows.append(
                {
                    "model": "baseline_quantile_hierarchical",
                    "feature_config": "ORIGINAL",
                    "dataset": dataset,
                    "quantile": name,
                    "target_coverage": quantile,
                    "empirical_coverage": float(np.mean(y <= preds_base[name])),
                    "pinball_loss": base_row[f"pinball_{name}"],
                }
            )
        rows.append(base_row)

    metrics = pd.DataFrame(rows)
    coverage = pd.DataFrame(coverage_rows)
    predictions = pd.concat(prediction_tables, ignore_index=True)
    metrics.to_csv(RESULTS_DIR / "quantile_model_comparison.csv", index=False)
    coverage.to_csv(RESULTS_DIR / "quantile_coverage.csv", index=False)
    predictions.to_parquet(RESULTS_DIR / "quantile_predictions.parquet", index=False)
    return metrics, coverage, predictions


def uncertainty_analysis(quantile_predictions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reavalia P95-P50 globalmente e dentro de porto x operacao."""

    log("Analisando P95-P50 e incerteza.")
    df = quantile_predictions[
        (quantile_predictions["dataset"] == "final_test")
        & (quantile_predictions["feature_config"] == "ENRICHED_SAFE_HISTORY")
    ].copy()
    df["abs_error_p50_h"] = np.abs(df["pred_p50_h"] - df[TARGET])
    df["extreme_q90"] = df[TARGET] >= df[TARGET].quantile(0.90)
    global_rows = []
    for width_col in ["width_p90_p50_h", "width_p95_p50_h"]:
        global_rows.append(
            {
                "measure": width_col,
                "spearman_abs_error": safe_spearman(df[width_col], df["abs_error_p50_h"]),
                "spearman_actual_stay": safe_spearman(df[width_col], df[TARGET]),
                "mean_width_h": float(df[width_col].mean()),
                "median_width_h": float(df[width_col].median()),
            }
        )
    df["risk_quintile"] = pd.qcut(
        df["width_p95_p50_h"].rank(method="first"),
        5,
        labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
    )
    quintiles = (
        df.groupby("risk_quintile", observed=True)
        .agg(
            n=("port_call_id", "count"),
            mean_abs_error_h=("abs_error_p50_h", "mean"),
            mean_actual_h=(TARGET, "mean"),
            extreme_rate=("extreme_q90", "mean"),
            mean_width_p95_p50_h=("width_p95_p50_h", "mean"),
        )
        .reset_index()
    )
    global_table = pd.concat([pd.DataFrame(global_rows), add_prefix_rows(quintiles, "risk_quintile")], ignore_index=True)
    global_table.to_csv(RESULTS_DIR / "uncertainty_global.csv", index=False)

    group_stats = (
        df.groupby([PORT_COL, OPERATION_COL], dropna=False)
        .agg(
            n=("port_call_id", "count"),
            hist_actual_std_h=(TARGET, "std"),
            mean_width_p95_p50_h=("width_p95_p50_h", "mean"),
            corr_width_abs_error=("width_p95_p50_h", lambda s: np.nan),
        )
        .reset_index()
    )
    within_rows = []
    for keys, group in df.groupby([PORT_COL, OPERATION_COL], dropna=False):
        if len(group) < 30:
            continue
        within_rows.append(
            {
                PORT_COL: keys[0],
                OPERATION_COL: keys[1],
                "n": int(len(group)),
                "spearman_width_abs_error": safe_spearman(
                    group["width_p95_p50_h"],
                    group["abs_error_p50_h"],
                ),
                "spearman_width_actual": safe_spearman(group["width_p95_p50_h"], group[TARGET]),
                "hist_actual_std_h": float(group[TARGET].std()),
                "mean_width_p95_p50_h": float(group["width_p95_p50_h"].mean()),
            }
        )
    within = pd.DataFrame(within_rows)
    if not within.empty:
        within["can_individualize_risk"] = within["spearman_width_abs_error"] > 0.20
    within.to_csv(RESULTS_DIR / "uncertainty_within_group.csv", index=False)
    return global_table, within


def add_prefix_rows(df: pd.DataFrame, measure: str) -> pd.DataFrame:
    """Converte tabela auxiliar de quintis para formato empilhado simples."""

    out = df.copy()
    out.insert(0, "measure", measure)
    return out


def safe_spearman(a: pd.Series, b: pd.Series) -> float:
    """Calcula Spearman tratando series constantes ou curtas como NaN."""

    valid = pd.DataFrame({"a": a, "b": b}).dropna()
    if len(valid) < 5 or valid["a"].nunique() < 2 or valid["b"].nunique() < 2:
        return np.nan
    return float(spearmanr(valid["a"], valid["b"]).correlation)


def contextual_feature_sets(registry: FeatureRegistry) -> dict[str, list[str]]:
    """Define a analise incremental de contexto solicitada."""

    structure = ["port"] + registry.operation_features
    time_features = [
        "arrival_shift",
        "arrival_season",
        "arrival_quarter",
        "arrival_weekofyear",
        "arrival_is_weekend",
        "arrival_hour_sin",
        "arrival_hour_cos",
        "arrival_dow_sin",
        "arrival_dow_cos",
        "arrival_month_sin",
        "arrival_month_cos",
    ]
    weather = [col for col in registry.numerical_features if infer_feature_family(col) == "CLIMATICA_HISTORICA"]
    port_hist = registry.historical_families.get("port_performance", [])
    state = registry.historical_families.get("state", [])
    route_vessel = (
        registry.historical_families.get("route_history", [])
        + registry.historical_families.get("vessel_history", [])
        + registry.historical_families.get("vessel_port_history", [])
        + registry.historical_families.get("source_history", [])
        + registry.historical_families.get("destination_history", [])
    )
    sets = {
        "1_estrutura": structure,
        "2_estrutura_tempo": structure + time_features,
        "3_estrutura_clima": structure + weather,
        "4_estrutura_tempo_clima": structure + time_features + weather,
        "5_historico_porto": structure + time_features + weather + port_hist,
        "6_estado_operacional_d1": structure + time_features + weather + port_hist + state,
        "7_historico_rota_navio": structure + time_features + weather + port_hist + state + route_vessel,
        "8_full_model": registry.enriched_features,
    }
    return {name: list(dict.fromkeys([col for col in cols if col])) for name, cols in sets.items()}


def run_contextual_ablation(
    splits: dict[str, pd.DataFrame],
    registry: FeatureRegistry,
    tree_family: str,
) -> pd.DataFrame:
    """Refaz a analise incremental de estrutura, tempo, clima e historico."""

    log("Executando ablacao contextual.")
    rows = []
    train_df = splits["train"]
    eval_df = pd.concat([splits["validation"], splits["calibration"]], ignore_index=True)
    n_val = len(splits["validation"])
    q90 = float(train_df[TARGET].quantile(0.90))
    q95 = float(train_df[TARGET].quantile(0.95))
    for name, features in contextual_feature_sets(registry).items():
        _, pred_eval, _ = fit_predict_model(
            tree_family,
            train_df,
            eval_df,
            features,
            [col for col in registry.categorical_features if col in features],
        )
        for dataset, y_true, pred in [
            ("validation", splits["validation"][TARGET], pred_eval[:n_val]),
            ("calibration", splits["calibration"][TARGET], pred_eval[n_val:]),
        ]:
            rows.append(score_row(name, "CONTEXTUAL", dataset, y_true, pred, q90, q95))
    table = pd.DataFrame(rows)
    table.to_csv(RESULTS_DIR / "contextual_ablation.csv", index=False)
    return table


def safety_stock_simulation(
    final_predictions: pd.DataFrame,
    quantile_predictions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Executa simulacao de estoque de seguranca como analise de cenario.

    Nao ha demanda, custo unitario ou estoque reais no dataset. Por isso, a
    simulacao usa cenarios explicitos de demanda media, variabilidade e nivel de
    servico para comparar politicas de buffer, nao para afirmar economia real.
    """

    log("Executando simulacao de estoque de seguranca.")
    df = final_predictions.copy()
    q = quantile_predictions[
        (quantile_predictions["dataset"] == "final_test")
        & (quantile_predictions["feature_config"] == "ENRICHED_SAFE_HISTORY")
    ][["port_call_id", "pred_p50_h", "pred_p90_h", "pred_p95_h"]]
    df = df.merge(q, on="port_call_id", how="left")
    df["actual_lead_time_d"] = df[TARGET] / 24
    df["original_lead_time_d"] = df["prediction_original_h"] / 24
    df["enriched_lead_time_d"] = df["prediction_enriched_h"] / 24
    df["quantile_p90_lead_time_d"] = df["pred_p90_h"] / 24
    df["quantile_p95_lead_time_d"] = df["pred_p95_h"] / 24

    group_q = (
        df.groupby([PORT_COL, OPERATION_COL], dropna=False)["actual_lead_time_d"]
        .quantile(0.90)
        .rename("static_segment_p90_d")
        .reset_index()
    )
    df = df.merge(group_q, on=[PORT_COL, OPERATION_COL], how="left")
    df["static_segment_p90_d"] = df["static_segment_p90_d"].fillna(
        df["actual_lead_time_d"].quantile(0.90)
    )

    scenarios = [
        {"scenario": "base", "demand_mean_units_d": 100.0, "demand_std_units_d": 25.0, "service_z": 1.65, "unit_value_brl": 50.0},
        {"scenario": "low_demand", "demand_mean_units_d": 50.0, "demand_std_units_d": 15.0, "service_z": 1.65, "unit_value_brl": 50.0},
        {"scenario": "high_demand", "demand_mean_units_d": 200.0, "demand_std_units_d": 50.0, "service_z": 1.65, "unit_value_brl": 50.0},
        {"scenario": "higher_service", "demand_mean_units_d": 100.0, "demand_std_units_d": 25.0, "service_z": 2.05, "unit_value_brl": 50.0},
    ]
    policies = {
        "A_static_segment_port_operation": "static_segment_p90_d",
        "B_dynamic_original_point": "original_lead_time_d",
        "C_dynamic_enriched_point": "enriched_lead_time_d",
        "D_dynamic_quantile_p90": "quantile_p90_lead_time_d",
        "D_dynamic_quantile_p95": "quantile_p95_lead_time_d",
    }
    rows = []
    sens_rows = []
    for scenario in scenarios:
        mu = scenario["demand_mean_units_d"]
        sigma = scenario["demand_std_units_d"]
        z = scenario["service_z"]
        value = scenario["unit_value_brl"]
        for policy, col in policies.items():
            lead = df[col].clip(lower=0).fillna(df["static_segment_p90_d"])
            variance = sigma**2 * lead + mu**2 * lead.var()
            safety_stock = z * np.sqrt(np.maximum(variance, 0))
            buffer_excess_d = lead - df["actual_lead_time_d"]
            protection_rate = (lead >= df["actual_lead_time_d"]).mean()
            row = {
                **scenario,
                "policy": policy,
                "mean_policy_lead_time_d": float(lead.mean()),
                "mean_safety_stock_units": float(np.mean(safety_stock)),
                "proxy_working_capital_brl": float(np.mean(safety_stock) * value),
                "protection_rate": float(protection_rate),
                "mean_buffer_excess_d": float(buffer_excess_d.mean()),
                "underbuffer_rate": float((buffer_excess_d < 0).mean()),
            }
            rows.append(row)
            sens_rows.append(row.copy())
    simulation = pd.DataFrame(rows)
    sensitivity = pd.DataFrame(sens_rows)
    simulation.to_csv(RESULTS_DIR / "safety_stock_simulation.csv", index=False)
    sensitivity.to_csv(RESULTS_DIR / "safety_stock_sensitivity.csv", index=False)
    return simulation, sensitivity


def make_figures(
    point_table: pd.DataFrame,
    hist_ablation: pd.DataFrame,
    quantile_metrics: pd.DataFrame,
    safety_stock: pd.DataFrame,
) -> None:
    """Gera poucas figuras diretamente reutilizaveis no Capitulo 4."""

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    val = point_table[point_table["dataset"] == "validation"].copy()
    plt.figure(figsize=(10, 5))
    labels = val["model"] + "\n" + val["feature_config"].str.replace("_", " ")
    plt.bar(labels, val["mae"])
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("MAE (h)")
    plt.title("Comparacao pontual em validation")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "point_model_evolution.png", dpi=160)
    plt.close()

    abl = hist_ablation[hist_ablation["dataset"] == "validation"].copy()
    plt.figure(figsize=(9, 4.8))
    plt.plot(abl["model"], abl["mae_gain_vs_e0_pct"], marker="o")
    plt.xticks(rotation=35, ha="right")
    plt.ylabel("Ganho de MAE vs E0 (%)")
    plt.title("Efeito incremental das familias historicas")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "historical_feature_ablation.png", dpi=160)
    plt.close()

    qt = quantile_metrics[quantile_metrics["dataset"] == "final_test"].copy()
    plt.figure(figsize=(8, 4.8))
    labels = qt["model"] + "\n" + qt["feature_config"].str.replace("_", " ")
    plt.bar(labels, qt["mean_width_p95_p50_h"])
    plt.xticks(rotation=35, ha="right")
    plt.ylabel("Largura media P95-P50 (h)")
    plt.title("Largura dos intervalos quantilicos")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "quantile_width_p95_p50.png", dpi=160)
    plt.close()

    ss = safety_stock[safety_stock["scenario"] == "base"].copy()
    plt.figure(figsize=(9, 4.8))
    plt.bar(ss["policy"], ss["proxy_working_capital_brl"])
    plt.xticks(rotation=35, ha="right")
    plt.ylabel("Capital de giro proxy (R$)")
    plt.title("Simulacao de estoque de seguranca - cenario base")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "safety_stock_working_capital.png", dpi=160)
    plt.close()


def write_notebook() -> None:
    """Cria notebook didatico v2 apontando para artefatos canonicos."""

    sections = [
        "preparacao e temporal leakage",
        "catalogo das features",
        "divisao temporal",
        "baselines",
        "Ridge",
        "Random Forest",
        "Gradient Boosting",
        "HGB",
        "comparacao pontual",
        "analise da cauda",
        "regressao quantilica",
        "comparacao com incerteza historica",
        "analise intragrupo",
        "valor incremental das familias de features",
        "simulacao de estoque de seguranca",
        "sintese",
    ]
    cells: list[dict[str, object]] = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Modelagem do Capitulo 4 - versao v2\n",
                "\n",
                "Este notebook e a trilha didatica da reconstruÃ§Ã£o canonica com features historicas temporalmente seguras. O codigo pesado vive em `scripts/run_cap4_rebuild.py`; os resultados reproduziveis ficam em `results/cap4_rebuild/`.\n",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "from pathlib import Path\n",
                "import pandas as pd\n",
                "RESULTS = Path('..') / 'results' / 'cap4_rebuild'\n",
                "FIGURES = RESULTS / 'figures'\n",
            ],
        },
    ]
    table_by_section = {
        "catalogo das features": "feature_catalog.csv",
        "baselines": "baseline_reproduction.csv",
        "comparacao pontual": "point_model_comparison_validation.csv",
        "analise da cauda": "tail_comparison.csv",
        "regressao quantilica": "quantile_model_comparison.csv",
        "comparacao com incerteza historica": "uncertainty_global.csv",
        "analise intragrupo": "uncertainty_within_group.csv",
        "valor incremental das familias de features": "contextual_ablation.csv",
        "simulacao de estoque de seguranca": "safety_stock_simulation.csv",
        "sintese": "bootstrap_results.csv",
    }
    for idx, section in enumerate(sections, start=1):
        cells.append(
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [f"## {idx}. {section.capitalize()}\n"],
            }
        )
        table = table_by_section.get(section)
        if table:
            cells.append(
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [f"pd.read_csv(RESULTS / '{table}').head(20)\n"],
                }
            )
        else:
            cells.append(
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["Texto interpretativo preenchido a partir de `results/cap4_rebuild/report.md`.\n"],
                }
            )
    cells.append(
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": ["print((RESULTS / 'report.md').read_text(encoding='utf-8'))\n"],
        }
    )
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    NOTEBOOK_PATH.write_text(json.dumps(nb, indent=2, ensure_ascii=False), encoding="utf-8")


def write_report(
    context: dict[str, str],
    point_table: pd.DataFrame,
    final_table: pd.DataFrame,
    quantile_metrics: pd.DataFrame,
    uncertainty_global: pd.DataFrame,
    uncertainty_within: pd.DataFrame,
    hist_ablation: pd.DataFrame,
    contextual_ablation: pd.DataFrame,
    importance: pd.DataFrame,
    safety_stock: pd.DataFrame,
    leakage_tests: pd.DataFrame,
    winner: dict[str, object],
) -> None:
    """Escreve relatorio-fonte para reescrita da monografia."""

    val = point_table[point_table["dataset"] == "validation"]
    final_o = final_table[final_table["feature_config"] == "ORIGINAL"].iloc[0]
    final_e = final_table[final_table["feature_config"] == "ENRICHED_SAFE_HISTORY"].iloc[0]
    gain = final_o["mae"] - final_e["mae"]
    gain_pct = gain / final_o["mae"] * 100
    top_new = importance[importance["feature_origin"] == "new"].head(10)
    ridge = val[val["model"] == "Ridge"][["feature_config", "mae", "rmse", "medae", "rmsle"]]
    rf = val[val["model"] == "Random Forest"][["feature_config", "mae", "rmse", "medae", "rmsle"]]
    gb = val[val["model"] == "Gradient Boosting"][["feature_config", "mae", "rmse", "medae", "rmsle"]]
    hgb = val[val["model"] == "HistGradientBoosting"][["feature_config", "mae", "rmse", "medae", "rmsle"]]
    quant_final = quantile_metrics[quantile_metrics["dataset"] == "final_test"].copy()
    time_rows = contextual_ablation[
        contextual_ablation["model"].isin(["1_estrutura", "2_estrutura_tempo", "3_estrutura_clima", "4_estrutura_tempo_clima", "8_full_model"])
    ]
    safety_base = safety_stock[safety_stock["scenario"] == "base"].copy()
    def md_table(frame: pd.DataFrame) -> str:
        return frame.to_string(index=False)

    text = f"""# Rebuild canonico do Capitulo 4

## Contexto de execucao

- Diretorio: `{context['root']}`
- Branch: `{context['branch']}`
- HEAD inicial: `{context['head']}`
- Python: `{context['python']}`

## O que mudou

A modelagem deixa de tratar as proxies antigas de congestionamento da EDA como candidatas diretas e passa a usar `ENRICHED_SAFE_HISTORY`: features historicas reconstruidas com cutoff D-1, calendario diario completo e disponibilidade por evento conhecido. O final_test permaneceu bloqueado ate a escolha do modelo e do feature set.

## Resultados pontuais

- Modelo congelado sem final_test: `{winner['model']}` com criterio `{winner['selection_criterion']}`.
- Final_test ORIGINAL MAE: {final_o['mae']:.3f} h.
- Final_test ENRICHED MAE: {final_e['mae']:.3f} h.
- Ganho final pareado: {gain:.3f} h ({gain_pct:.2f}%).
- Q90/Q95: ver `tail_comparison.csv`; a cauda foi avaliada por MAE, bias e underprediction.

## Ridge

{md_table(ridge)}

## Random Forest

{md_table(rf)}

## Gradient Boosting

{md_table(gb)}

## HGB

{md_table(hgb)}

## Quantis e P95-P50

{md_table(quant_final)}

A largura P95-P50 foi reavaliada globalmente e dentro de porto x operacao. A tabela `uncertainty_global.csv` resume associacao com erro absoluto, permanencia realizada e quintis de risco. A tabela `uncertainty_within_group.csv` indica se a largura passou a individualizar risco dentro dos grupos.

Resumo intragrupo: {len(uncertainty_within)} grupos com n>=30; mediana da correlacao largura-erro = {uncertainty_within['spearman_width_abs_error'].median() if len(uncertainty_within) else np.nan:.3f}.

## Valor incremental das familias

A ablacao historica E0-E6 esta em `historical_feature_ablation.csv`. A analise contextual esta em `contextual_ablation.csv`.

{md_table(time_rows[['dataset', 'model', 'mae', 'mae_q90']].head(16))}

## Importancia

As importancias sao permutation importance por aumento de MAE, portanto nao devem ser somadas mecanicamente quando features sao correlacionadas. Top novas features:

{md_table(top_new[['feature', 'family', 'importance_mae_increase_h']])}

## Estoque de seguranca

A simulacao e um exercicio de cenario, sem afirmar economia real observada. Ela explicita a cadeia: melhor informacao de lead time -> melhor representacao da variabilidade -> politica de buffer -> estoque de seguranca -> capital de giro proxy.

{md_table(safety_base[['policy', 'mean_safety_stock_units', 'proxy_working_capital_brl', 'protection_rate', 'underbuffer_rate']])}

## Testes de leakage

Todos passaram? {bool(leakage_tests['passed'].all())}.

## Narrativa recomendada

Reescrever o Capitulo 4 para separar claramente features exploratorias da EDA e features historicas seguras da modelagem. A nova narrativa deve enfatizar que o ganho vem de memoria operacional conhecida e contexto historico de porto/rota/embarcacao, nao de contagens simples de fluxo. As conclusoes sobre Ridge/RF/GB/HGB devem ser atualizadas conforme `point_model_comparison_validation.csv` e `point_model_comparison_final.csv`; as conclusoes sobre quantis e P95-P50 devem usar a nova avaliacao de individualizacao de risco.
"""
    (RESULTS_DIR / "report.md").write_text(text, encoding="utf-8")


def save_split_summary(splits: dict[str, pd.DataFrame]) -> None:
    """Salva resumo dos splits temporais."""

    rows = []
    for name, part in splits.items():
        rows.append(
            {
                "split": name,
                "n_rows": int(len(part)),
                "period_start": part[DATE_COL].min(),
                "period_end": part[DATE_COL].max(),
                "target_mean_h": float(part[TARGET].mean()),
                "target_median_h": float(part[TARGET].median()),
                "target_q90_h": float(part[TARGET].quantile(0.90)),
                "target_q95_h": float(part[TARGET].quantile(0.95)),
            }
        )
    pd.DataFrame(rows).to_csv(RESULTS_DIR / "split_summary.csv", index=False)


def save_config(context: dict[str, str], registry: FeatureRegistry, winner: dict[str, object]) -> None:
    """Persiste configuracao canonica em JSON."""

    payload = {
        "context": context,
        "target": TARGET,
        "date_col": DATE_COL,
        "splits": SPLIT_BOUNDS,
        "random_state": RANDOM_STATE,
        "min_group_size": MIN_GROUP_SIZE,
        "original_features": registry.original_features,
        "enriched_features": registry.enriched_features,
        "historical_families": registry.historical_families,
        "winner": winner,
        "walk_forward_note": (
            "Features historicas sao atualizadas em regime walk-forward: previsoes "
            "posteriores podem usar escalas anteriores ja concluidas e conhecidas "
            "antes de 23:59:59 de D-1."
        ),
    }
    (RESULTS_DIR / "run_config.json").write_text(
        json.dumps(payload, indent=2, default=str, ensure_ascii=False),
        encoding="utf-8",
    )


def main() -> int:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "run_log.txt").write_text("", encoding="utf-8")

    context = assert_official_context()
    log(f"Contexto oficial confirmado: {context['root']} | {context['branch']}")
    log(f"Python: {context['python']}")

    df = load_modeling_data()
    df_features, registry = build_registry(df)
    splits = split_data(df_features)
    save_split_summary(splits)
    feature_catalog(registry, set(df.columns))

    leakage_tests = run_leakage_tests()
    leakage_tests.to_csv(RESULTS_DIR / "leakage_tests.csv", index=False)
    if not leakage_tests["passed"].all():
        raise RuntimeError("Testes de leakage falharam; execucao oficial abortada.")

    baseline_reproduction, ridge_alphas = reproduce_chapter4(splits, registry)
    point_table, _, ridge_alphas = run_point_model_comparison(splits, registry, ridge_alphas)
    tree_family = choose_tree_family(point_table)
    hist_ablation = run_historical_ablation(splits, registry, tree_family)
    contextual_ablation = run_contextual_ablation(splits, registry, tree_family)
    winner = select_point_winner(point_table)
    final_table, port_comparison, bootstrap, final_predictions = final_point_evaluation(
        splits,
        registry,
        winner,
        ridge_alphas,
    )
    quantile_metrics, quantile_coverage, quantile_predictions = quantile_evaluation(splits, registry)
    uncertainty_global, uncertainty_within = uncertainty_analysis(quantile_predictions)
    safety_stock, sensitivity = safety_stock_simulation(final_predictions, quantile_predictions)

    importance = pd.read_csv(RESULTS_DIR / "feature_importance.csv")
    make_figures(point_table, hist_ablation, quantile_metrics, safety_stock)
    save_config(context, registry, winner)
    write_notebook()
    write_report(
        context,
        point_table,
        final_table,
        quantile_metrics,
        uncertainty_global,
        uncertainty_within,
        hist_ablation,
        contextual_ablation,
        importance,
        safety_stock,
        leakage_tests,
        winner,
    )
    log("Reconstrucao canonica concluida.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
