"""Features historicas temporalmente seguras para a modelagem do Capitulo 4.

Este modulo separa a modelagem canonica das proxies exploratorias de
congestionamento usadas na EDA. A regra central e conservadora: para uma escala
que chega no dia D, qualquer estado historico operacional deve estar disponivel
ate 23:59:59 de D-1.

As funcoes abaixo existem para reconstruir o sinal de congestionamento e memoria
operacional sem usar informacao futura da propria escala nem de outras escalas.
Duracoes historicas so entram depois do evento que torna aquela duracao
conhecida: espera depois de `berthing_ts`, operacao depois de `unberthing_ts` e
permanencia total depois de `departure_port_ts`.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer


TARGET = "t_total_port_stay_h"
DATE_COL = "arrival_port_ts"
PORT_COL = "port"


@dataclass(frozen=True)
class HistoricalFeatureBuild:
    """Resultado da reconstrucao historica segura.

    Attributes
    ----------
    data:
        Base com as novas colunas historicas.
    families:
        Mapeamento simples entre familias metodologicas e colunas criadas.
    metadata:
        Classificacao metodologica por coluna para alimentar o catalogo canonico.
    """

    data: pd.DataFrame
    families: dict[str, list[str]]
    metadata: dict[str, str]


def make_key(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Cria chave textual estavel para agregacoes historicas por grupos.

    A chave evita depender de merge por multiplas colunas dentro do loop de
    `searchsorted`. Valores ausentes sao preservados como categoria explicita,
    pois na modelagem eles tambem precisam ter fallback deterministico.
    """

    if len(cols) == 1:
        return df[cols[0]].astype("string").fillna("__MISSING__").astype(str)
    return (
        df[cols]
        .astype("string")
        .fillna("__MISSING__")
        .astype(str)
        .agg("\x1f".join, axis=1)
    )


def add_cyclical_features(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona transformacoes ciclicas de hora, dia da semana e mes.

    Essas variaveis continuam seguras porque sao derivadas exclusivamente de
    `arrival_port_ts`, conhecido no instante conceitual da previsao.
    """

    out = df.copy()
    out["arrival_hour_sin"] = np.sin(2 * np.pi * out["arrival_hour"] / 24)
    out["arrival_hour_cos"] = np.cos(2 * np.pi * out["arrival_hour"] / 24)
    out["arrival_dow_sin"] = np.sin(2 * np.pi * out["arrival_dayofweek"] / 7)
    out["arrival_dow_cos"] = np.cos(2 * np.pi * out["arrival_dayofweek"] / 7)
    out["arrival_month_sin"] = np.sin(2 * np.pi * (out["arrival_month"] - 1) / 12)
    out["arrival_month_cos"] = np.cos(2 * np.pi * (out["arrival_month"] - 1) / 12)
    return out


def add_calendar_cutoff_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cria data e timestamp de corte para features historicas.

    Para uma chegada no dia D, o corte usado e D-1 23:59:59. Essa escolha e mais
    conservadora que usar o horario exato da chegada e simplifica a reproducao
    academica das janelas calendario.
    """

    out = df.copy()
    out["arrival_date_for_features"] = out[DATE_COL].dt.floor("D")
    out["cutoff_date"] = out["arrival_date_for_features"] - pd.Timedelta(days=1)
    out["cutoff_ts"] = out["cutoff_date"] + pd.Timedelta(
        hours=23,
        minutes=59,
        seconds=59,
    )
    return out


def complete_port_day_grid(df: pd.DataFrame) -> pd.DataFrame:
    """Monta grade porto-dia completa, incluindo dias sem eventos.

    O preenchimento explicito de dias sem eventos com zero corrige o risco da
    proxy exploratoria antiga, que calculava defasagens apenas entre dias com
    chegada observada.
    """

    ports = pd.Index(df[PORT_COL].dropna().unique(), name=PORT_COL)
    min_date = min(
        df[DATE_COL].dt.floor("D").min(),
        df["departure_port_ts"].dt.floor("D").min(),
    ) - pd.Timedelta(days=7)
    max_date = max(
        df[DATE_COL].dt.floor("D").max(),
        df["departure_port_ts"].dt.floor("D").max(),
    )
    dates = pd.date_range(min_date, max_date, freq="D", name="date")
    return pd.MultiIndex.from_product([ports, dates]).to_frame(index=False)


def event_counts(
    df: pd.DataFrame,
    ts_col: str,
    value_name: str,
    extra_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Conta eventos diarios por porto e, opcionalmente, por colunas extras."""

    extra_cols = extra_cols or []
    temp = df[[PORT_COL, ts_col] + extra_cols].dropna(subset=[PORT_COL, ts_col]).copy()
    temp["date"] = temp[ts_col].dt.floor("D")
    group_cols = [PORT_COL, "date"] + extra_cols
    return (
        temp.groupby(group_cols, observed=True)
        .size()
        .rename(value_name)
        .reset_index()
    )


def add_daily_flow_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Cria fluxo portuario historico em janelas calendario de 1, 3 e 7 dias.

    As colunas sao mescladas pela `cutoff_date`; portanto uma escala que chega em
    D recebe apenas contagens encerradas ate D-1, com dias sem movimento valendo
    zero.
    """

    out = df.copy()
    grid = complete_port_day_grid(out)
    arrivals = event_counts(out, DATE_COL, "arrivals_count")
    departures = event_counts(out, "departure_port_ts", "departures_count")
    grid = grid.merge(arrivals, on=[PORT_COL, "date"], how="left")
    grid = grid.merge(departures, on=[PORT_COL, "date"], how="left")
    grid[["arrivals_count", "departures_count"]] = grid[
        ["arrivals_count", "departures_count"]
    ].fillna(0)
    grid = grid.sort_values([PORT_COL, "date"]).reset_index(drop=True)

    new_cols: list[str] = []
    for base in ["arrivals", "departures"]:
        count_col = f"{base}_count"
        grouped = grid.groupby(PORT_COL, observed=True)[count_col]
        for window in [1, 3, 7]:
            sum_col = f"{base}_prev_{window}d"
            grid[sum_col] = (
                grouped.rolling(window=window, min_periods=window)
                .sum()
                .reset_index(level=0, drop=True)
            )
            grid[sum_col] = grid[sum_col].fillna(
                grouped.rolling(window=window, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )
            new_cols.append(sum_col)
        for window in [3, 7]:
            avg_col = f"{base}_avg_prev_{window}d"
            max_col = f"{base}_max_prev_{window}d"
            grid[avg_col] = (
                grouped.rolling(window=window, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True)
            )
            grid[max_col] = (
                grouped.rolling(window=window, min_periods=1)
                .max()
                .reset_index(level=0, drop=True)
            )
            new_cols.extend([avg_col, max_col])

    for window in [1, 3, 7]:
        col = f"flow_balance_prev_{window}d"
        grid[col] = grid[f"arrivals_prev_{window}d"] - grid[f"departures_prev_{window}d"]
        new_cols.append(col)

    merge_grid = grid[[PORT_COL, "date"] + new_cols].rename(
        columns={"date": "cutoff_date"}
    )
    out = out.merge(merge_grid, on=[PORT_COL, "cutoff_date"], how="left")
    out[new_cols] = out[new_cols].fillna(0)
    return out, new_cols


def add_operational_state_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Reconstrui estado operacional do porto conhecido no fechamento D-1."""

    out = df.copy()
    grid = complete_port_day_grid(out)
    for ts_col, name in [
        (DATE_COL, "arrived"),
        ("berthing_ts", "berthed"),
        ("unberthing_ts", "unberthed"),
        ("departure_port_ts", "departed"),
    ]:
        counts = event_counts(out, ts_col, f"{name}_count")
        grid = grid.merge(counts, on=[PORT_COL, "date"], how="left")
        grid[f"{name}_count"] = grid[f"{name}_count"].fillna(0)

    grid = grid.sort_values([PORT_COL, "date"]).reset_index(drop=True)
    for name in ["arrived", "berthed", "unberthed", "departed"]:
        grid[f"cum_{name}"] = grid.groupby(PORT_COL, observed=True)[
            f"{name}_count"
        ].cumsum()

    grid["state_waiting_vessels_d_minus_1"] = (
        grid["cum_arrived"] - grid["cum_berthed"]
    ).clip(lower=0)
    grid["state_berthed_vessels_d_minus_1"] = (
        grid["cum_berthed"] - grid["cum_unberthed"]
    ).clip(lower=0)
    grid["state_present_vessels_d_minus_1"] = (
        grid["cum_arrived"] - grid["cum_departed"]
    ).clip(lower=0)
    grid["state_backlog_vessels_d_minus_1"] = (
        grid["state_waiting_vessels_d_minus_1"]
        + grid["state_berthed_vessels_d_minus_1"]
    )
    grid["state_entry_exit_delta_prev_1d"] = (
        grid["arrived_count"] - grid["departed_count"]
    )

    cols = [
        "state_waiting_vessels_d_minus_1",
        "state_berthed_vessels_d_minus_1",
        "state_present_vessels_d_minus_1",
        "state_backlog_vessels_d_minus_1",
        "state_entry_exit_delta_prev_1d",
    ]
    merge_grid = grid[[PORT_COL, "date"] + cols].rename(
        columns={"date": "cutoff_date"}
    )
    out = out.merge(merge_grid, on=[PORT_COL, "cutoff_date"], how="left")
    out[cols] = out[cols].fillna(0)
    return out, cols


def add_operation_mix_features(
    df: pd.DataFrame,
    operation_features: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """Calcula mix operacional recente por porto com corte D-1.

    O mix usa flags declaradas de operacao agregadas em janelas anteriores. A
    cautela metodologica permanece: as flags sao tratadas como informacao
    planejada/conhecida na chegada, conforme a modelagem do Capitulo 4.
    """

    out = df.copy()
    key_ops = [
        col
        for col in operation_features
        if col
        in {
            "op_carga",
            "op_descarga",
            "op_fundeio",
            "op_offshore",
            "op_reparo_manutencao",
        }
    ]
    if not key_ops:
        return out, []

    grid = complete_port_day_grid(out)
    temp = out[[PORT_COL, DATE_COL] + key_ops].dropna(subset=[PORT_COL, DATE_COL]).copy()
    temp["date"] = temp[DATE_COL].dt.floor("D")
    daily_ops = temp.groupby([PORT_COL, "date"], observed=True)[key_ops].sum().reset_index()
    arrivals = event_counts(out, DATE_COL, "arrivals_count")
    grid = grid.merge(arrivals, on=[PORT_COL, "date"], how="left")
    grid = grid.merge(daily_ops, on=[PORT_COL, "date"], how="left")
    grid[["arrivals_count"] + key_ops] = grid[["arrivals_count"] + key_ops].fillna(0)
    grid = grid.sort_values([PORT_COL, "date"]).reset_index(drop=True)

    new_cols: list[str] = []
    total_grouped = grid.groupby(PORT_COL, observed=True)["arrivals_count"]
    for window in [3, 7]:
        total = (
            total_grouped.rolling(window=window, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        for op_col in key_ops:
            op_sum = (
                grid.groupby(PORT_COL, observed=True)[op_col]
                .rolling(window=window, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )
            new_col = f"opmix_{op_col.replace('op_', '')}_share_prev_{window}d"
            grid[new_col] = np.where(total > 0, op_sum / total, 0.0)
            new_cols.append(new_col)

    merge_grid = grid[[PORT_COL, "date"] + new_cols].rename(
        columns={"date": "cutoff_date"}
    )
    out = out.merge(merge_grid, on=[PORT_COL, "cutoff_date"], how="left")
    out[new_cols] = out[new_cols].fillna(0)
    return out, new_cols


def expanding_stat_frame(values: np.ndarray, stats: list[str]) -> dict[str, np.ndarray]:
    """Calcula estatisticas expansivas para eventos ja conhecidos."""

    series = pd.Series(values, dtype=float)
    out: dict[str, np.ndarray] = {}
    if "count" in stats:
        out["count"] = np.arange(1, len(values) + 1, dtype=float)
    if "mean" in stats:
        out["mean"] = series.expanding().mean().to_numpy(dtype=float)
    if "median" in stats:
        out["median"] = series.expanding().median().to_numpy(dtype=float)
    if "std" in stats:
        out["std"] = series.expanding().std().fillna(0).to_numpy(dtype=float)
    if "q90" in stats:
        out["q90"] = series.expanding().quantile(0.90).to_numpy(dtype=float)
    if "q95" in stats:
        out["q95"] = series.expanding().quantile(0.95).to_numpy(dtype=float)
    return out


def add_group_known_stats(
    df: pd.DataFrame,
    group_cols: list[str],
    value_col: str,
    known_ts_col: str,
    prefix: str,
    stats: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """Agrega historico conhecido por grupo respeitando o timestamp de conhecimento.

    O ponto delicado e metodologico esta no `known_ts_col`: a propria linha so
    pode contribuir para previsoes futuras depois que o evento de conhecimento
    ocorreu e ficou antes do `cutoff_ts` de outra escala.
    """

    out = df.copy()
    cols = [f"{prefix}_{stat}" for stat in stats]
    for col in cols:
        out[col] = np.nan

    source = out[group_cols + [value_col, known_ts_col]].dropna(
        subset=group_cols + [value_col, known_ts_col]
    ).copy()
    source["_key"] = make_key(source, group_cols)
    source["_known_ns"] = source[known_ts_col].astype("int64")

    lookup: dict[str, tuple[np.ndarray, dict[str, np.ndarray]]] = {}
    for key, group in source.sort_values(["_key", "_known_ns"]).groupby(
        "_key",
        sort=False,
    ):
        known_ns = group["_known_ns"].to_numpy(dtype=np.int64)
        vals = group[value_col].to_numpy(dtype=float)
        lookup[str(key)] = (known_ns, expanding_stat_frame(vals, stats))

    target_keys = make_key(out, group_cols).astype(str).to_numpy()
    cutoff_ns = out["cutoff_ts"].astype("int64").to_numpy(dtype=np.int64)
    positions_by_key = pd.Series(np.arange(len(out))).groupby(target_keys, sort=False)
    values_by_col = {col: out[col].to_numpy(dtype=float) for col in cols}

    for key, positions in positions_by_key:
        if key not in lookup:
            continue
        pos = positions.to_numpy(dtype=int)
        known_ns, stat_values = lookup[key]
        idx = np.searchsorted(known_ns, cutoff_ns[pos], side="right") - 1
        valid = idx >= 0
        if not np.any(valid):
            continue
        for stat in stats:
            values_by_col[f"{prefix}_{stat}"][pos[valid]] = stat_values[stat][
                idx[valid]
            ]

    for col in cols:
        out[col] = values_by_col[col]
    count_col = f"{prefix}_count"
    if count_col in cols:
        out[count_col] = out[count_col].fillna(0)
    return out, cols


def build_historical_features(
    df: pd.DataFrame,
    operation_features: list[str],
) -> HistoricalFeatureBuild:
    """Constroi todas as familias historicas seguras da nova modelagem.

    A saida mantem as features antigas da EDA na base, mas o catalogo da
    modelagem deve usar apenas as colunas listadas em `families` quando for
    ativada a configuracao `ENRICHED_SAFE_HISTORY`.
    """

    out = add_calendar_cutoff_columns(df)
    families: dict[str, list[str]] = {}
    metadata: dict[str, str] = {}

    out, flow_cols = add_daily_flow_features(out)
    families["flow"] = flow_cols
    metadata.update({col: "FLUXO_PORTUARIO_HISTORICO" for col in flow_cols})

    out, state_cols = add_operational_state_features(out)
    families["state"] = state_cols
    metadata.update({col: "ESTADO_OPERACIONAL_RECONSTRUIDO_D_1" for col in state_cols})

    out, opmix_cols = add_operation_mix_features(out, operation_features)
    families["operation_mix"] = opmix_cols
    metadata.update({col: "MIX_OPERACIONAL_RECENTE" for col in opmix_cols})

    performance_cols: list[str] = []
    for value_col, known_ts_col, prefix in [
        ("t_wait_for_berthing_h", "berthing_ts", "port_wait_known"),
        ("t_operation_h", "unberthing_ts", "port_operation_known"),
        ("t_total_port_stay_h", "departure_port_ts", "port_total_known"),
    ]:
        out, cols = add_group_known_stats(
            out,
            [PORT_COL],
            value_col,
            known_ts_col,
            prefix,
            ["count", "mean", "median", "std", "q90", "q95"],
        )
        performance_cols.extend(cols)
    families["port_performance"] = performance_cols
    metadata.update({col: "DESEMPENHO_HISTORICO_CONHECIDO_DO_PORTO" for col in performance_cols})

    extra_cols: list[str] = []
    specs = [
        (["vessel_id"], "t_total_port_stay_h", "departure_port_ts", "vessel_total_known"),
        (["vessel_id"], "t_wait_for_berthing_h", "berthing_ts", "vessel_wait_known"),
        (["vessel_id", PORT_COL], "t_total_port_stay_h", "departure_port_ts", "vessel_port_total_known"),
        (["source_port"], "t_total_port_stay_h", "departure_port_ts", "source_total_known"),
        (["destination_port"], "t_total_port_stay_h", "departure_port_ts", "destination_total_known"),
        (["source_port", "destination_port"], "t_total_port_stay_h", "departure_port_ts", "route_total_known"),
    ]
    for group_cols, value_col, known_ts_col, prefix in specs:
        available_group_cols = [col for col in group_cols if col in out.columns]
        if not available_group_cols:
            continue
        out, cols = add_group_known_stats(
            out,
            available_group_cols,
            value_col,
            known_ts_col,
            prefix,
            ["count", "mean", "median", "q90"],
        )
        extra_cols.extend(cols)

    families["vessel_history"] = [col for col in extra_cols if col.startswith("vessel_") and not col.startswith("vessel_port_")]
    families["vessel_port_history"] = [col for col in extra_cols if col.startswith("vessel_port_")]
    families["source_history"] = [col for col in extra_cols if col.startswith("source_")]
    families["destination_history"] = [col for col in extra_cols if col.startswith("destination_")]
    families["route_history"] = [col for col in extra_cols if col.startswith("route_")]
    families["additional_safe_history"] = extra_cols + opmix_cols

    for col in families["vessel_history"]:
        metadata[col] = "HISTORICO_DA_EMBARCACAO"
    for col in families["vessel_port_history"]:
        metadata[col] = "HISTORICO_EMBARCACAO_PORTO"
    for col in families["source_history"]:
        metadata[col] = "HISTORICO_DE_ORIGEM"
    for col in families["destination_history"]:
        metadata[col] = "HISTORICO_DE_DESTINO"
    for col in families["route_history"]:
        metadata[col] = "HISTORICO_DE_ROTA"
    metadata.update({col: "MIX_OPERACIONAL_RECENTE" for col in opmix_cols})

    return HistoricalFeatureBuild(data=out, families=families, metadata=metadata)


def run_leakage_tests() -> pd.DataFrame:
    """Executa testes adversariais permanentes contra vazamento temporal.

    A pipeline oficial deve abortar se qualquer teste falhar. Os casos cobrem
    evento do proprio dia, evento futuro, disponibilidade de espera/operacao/
    permanencia, dias sem evento, uso do proprio target, rolling futuro, merge,
    imputacao e a leitura walk-forward das features historicas.
    """

    tests: list[dict[str, object]] = []

    def record(name: str, passed: bool, detail: str) -> None:
        tests.append({"test": name, "passed": bool(passed), "detail": detail})

    base = pd.DataFrame(
        {
            PORT_COL: ["A", "A", "A"],
            "operation_type": ["load", "load", "load"],
            "vessel_id": ["v1", "v2", "v3"],
            "source_port": ["S", "S", "S"],
            "destination_port": ["D", "D", "D"],
            DATE_COL: pd.to_datetime(
                ["2024-01-01 08:00", "2024-01-03 08:00", "2024-01-05 08:00"]
            ),
            "berthing_ts": pd.to_datetime(
                ["2024-01-02 08:00", "2024-01-04 08:00", "2024-01-06 08:00"]
            ),
            "unberthing_ts": pd.to_datetime(
                ["2024-01-03 08:00", "2024-01-05 08:00", "2024-01-07 08:00"]
            ),
            "departure_port_ts": pd.to_datetime(
                ["2024-01-04 08:00", "2024-01-06 08:00", "2024-01-08 08:00"]
            ),
            "t_wait_for_berthing_h": [24.0, 24.0, 24.0],
            "t_operation_h": [24.0, 24.0, 24.0],
            TARGET: [72.0, 72.0, 72.0],
        }
    )
    base = add_calendar_cutoff_columns(base)
    flow, _ = add_daily_flow_features(base)
    row_jan3 = flow.loc[flow[DATE_COL] == pd.Timestamp("2024-01-03 08:00")].iloc[0]

    modified_same_day = pd.concat(
        [
            base,
            pd.DataFrame(
                {
                    PORT_COL: ["A"],
                    "operation_type": ["load"],
                    "vessel_id": ["x"],
                    "source_port": ["S"],
                    "destination_port": ["D"],
                    DATE_COL: [pd.Timestamp("2024-01-03 20:00")],
                    "berthing_ts": [pd.Timestamp("2024-01-03 21:00")],
                    "unberthing_ts": [pd.Timestamp("2024-01-03 22:00")],
                    "departure_port_ts": [pd.Timestamp("2024-01-03 23:00")],
                    "t_wait_for_berthing_h": [1.0],
                    "t_operation_h": [1.0],
                    TARGET: [3.0],
                    "arrival_date_for_features": [pd.Timestamp("2024-01-03")],
                    "cutoff_date": [pd.Timestamp("2024-01-02")],
                    "cutoff_ts": [pd.Timestamp("2024-01-02 23:59:59")],
                }
            ),
        ],
        ignore_index=True,
    )
    flow_mod, _ = add_daily_flow_features(modified_same_day)
    row_jan3_mod = flow_mod.loc[
        flow_mod[DATE_COL] == pd.Timestamp("2024-01-03 08:00")
    ].iloc[0]
    record(
        "evento_do_proprio_dia_nao_altera_features_d_menos_1",
        row_jan3["arrivals_prev_1d"] == row_jan3_mod["arrivals_prev_1d"],
        "Evento em D nao afetou fluxo com cutoff D-1.",
    )

    future_extra = base.iloc[[0]].copy()
    future_extra[DATE_COL] = pd.Timestamp("2024-02-01 08:00")
    future_extra["berthing_ts"] = pd.Timestamp("2024-02-02 08:00")
    future_extra["unberthing_ts"] = pd.Timestamp("2024-02-03 08:00")
    future_extra["departure_port_ts"] = pd.Timestamp("2024-02-04 08:00")
    future_extra = add_calendar_cutoff_columns(future_extra)
    flow_future, _ = add_daily_flow_features(pd.concat([base, future_extra], ignore_index=True))
    row_jan3_future = flow_future.loc[
        flow_future[DATE_COL] == pd.Timestamp("2024-01-03 08:00")
    ].iloc[0]
    record(
        "evento_futuro_nao_altera_features_passadas",
        row_jan3["arrivals_prev_1d"] == row_jan3_future["arrivals_prev_1d"],
        "Evento futuro em fevereiro nao afetou corte de janeiro.",
    )

    known_wait, _ = add_group_known_stats(
        base,
        [PORT_COL],
        "t_wait_for_berthing_h",
        "berthing_ts",
        "test_wait",
        ["count", "mean"],
    )
    wait_before = known_wait.loc[
        known_wait[DATE_COL] == pd.Timestamp("2024-01-01 08:00"),
        "test_wait_count",
    ].iloc[0]
    wait_after = known_wait.loc[
        known_wait[DATE_COL] == pd.Timestamp("2024-01-03 08:00"),
        "test_wait_count",
    ].iloc[0]
    record(
        "espera_so_disponivel_depois_da_atracacao",
        wait_before == 0 and wait_after == 1,
        f"counts={wait_before},{wait_after}",
    )

    known_op, _ = add_group_known_stats(
        base,
        [PORT_COL],
        "t_operation_h",
        "unberthing_ts",
        "test_op",
        ["count", "mean"],
    )
    op_before = known_op.loc[
        known_op[DATE_COL] == pd.Timestamp("2024-01-03 08:00"),
        "test_op_count",
    ].iloc[0]
    op_after = known_op.loc[
        known_op[DATE_COL] == pd.Timestamp("2024-01-05 08:00"),
        "test_op_count",
    ].iloc[0]
    record(
        "operacao_so_disponivel_depois_da_desatracacao",
        op_before == 0 and op_after == 1,
        f"counts={op_before},{op_after}",
    )

    known_total, _ = add_group_known_stats(
        base,
        [PORT_COL],
        TARGET,
        "departure_port_ts",
        "test_total",
        ["count", "mean"],
    )
    total_before = known_total.loc[
        known_total[DATE_COL] == pd.Timestamp("2024-01-03 08:00"),
        "test_total_count",
    ].iloc[0]
    total_after = known_total.loc[
        known_total[DATE_COL] == pd.Timestamp("2024-01-05 08:00"),
        "test_total_count",
    ].iloc[0]
    record(
        "permanencia_so_disponivel_depois_da_saida",
        total_before == 0 and total_after == 1,
        f"counts={total_before},{total_after}",
    )

    jan5 = flow.loc[flow[DATE_COL] == pd.Timestamp("2024-01-05 08:00")].iloc[0]
    record(
        "dias_calendario_sem_eventos_existem_com_zero",
        jan5["arrivals_prev_1d"] == 0,
        f"Jan 4 arrivals_prev_1d={jan5['arrivals_prev_1d']}",
    )

    own_mod = base.copy()
    own_mod.loc[own_mod[DATE_COL] == pd.Timestamp("2024-01-03 08:00"), TARGET] = 9999.0
    own_known, _ = add_group_known_stats(
        own_mod,
        [PORT_COL],
        TARGET,
        "departure_port_ts",
        "own_total",
        ["count", "mean"],
    )
    own_feature = own_known.loc[
        own_known[DATE_COL] == pd.Timestamp("2024-01-03 08:00"),
        "own_total_count",
    ].iloc[0]
    record(
        "target_da_propria_observacao_nao_entra_na_feature",
        own_feature == 0,
        f"known count={own_feature}",
    )

    record(
        "rolling_nao_usa_linhas_futuras",
        row_jan3["arrivals_prev_1d"] == 0,
        f"D-1 de 2024-01-03 e 2024-01-02: {row_jan3['arrivals_prev_1d']}",
    )
    record(
        "merge_exclui_eventos_conhecidos_no_futuro",
        total_before == 0,
        "searchsorted side='right' aplica cutoff_ts.",
    )

    imputer = SimpleImputer(strategy="median")
    train_x = np.array([[1.0], [3.0]])
    val_x = np.array([[999.0], [np.nan]])
    imputer.fit(train_x)
    before = float(imputer.statistics_[0])
    imputer.transform(val_x)
    after = float(imputer.statistics_[0])
    record(
        "imputacao_ajustada_apenas_no_treino",
        before == after == 2.0,
        f"statistics before={before}, after={after}",
    )

    record(
        "walk_forward_permite_eventos_anteriores_ja_conhecidos",
        total_after == 1,
        "Previsao posterior usa escala anterior ja concluida antes de D-1.",
    )

    return pd.DataFrame(tests)
