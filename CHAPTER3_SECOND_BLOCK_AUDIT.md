# Chapter 3 Second Refactor Block Audit

This document records the audit for the second Chapter 3 refactoring block. The block covered only:

- `src/data_sources/ports.py`
- `src/data_sources/weather.py`
- `src/features/calendar.py`
- `src/features/operation_types.py`
- `src/features/congestion.py`
- `src/features/weather_features.py`

No Chapter 4 notebook and no file under `src/modeling/` was modified.

## Files Changed

| File | What changed |
|---|---|
| `src/data_sources/ports.py` | Added Portuguese docstrings, explicit required columns, explicit coordinate rounding constant, and methodological note about keeping the first row per `port`. |
| `src/data_sources/weather.py` | Added Portuguese docstrings, explicit coordinate rounding constant, and methodological note that same-day daily weather aggregates are descriptive only at arrival time. |
| `src/features/calendar.py` | Removed unused generic `ref_col` configurability from the official function, anchored features to `arrival_port_ts`, and documented calendar features as known at arrival. |
| `src/features/operation_types.py` | Removed unused `patterns` and `add_unmapped_col` configurability, kept the same operation flags, documented `_compact_text`, false-positive guards, and the operation-type conflict audit. |
| `src/features/congestion.py` | Added required-column constant, Portuguese docstring, and explicit availability classification for each congestion feature. No congestion calculation was redesigned. |
| `src/features/weather_features.py` | Removed unused `group_cols` configurability, added explicit weather history column list, and documented same-day vs lagged weather availability. |
| `REFACTOR_CHECKLIST.md` | Marked only the six completed second-block review items as done. |

## Code Removed or Simplified

- Removed unused optional parameters from official pipeline functions where they were not used anywhere else in the repository:
  - `create_calendar_features(ref_col=...)` now uses `arrival_port_ts` directly.
  - `create_operation_type_flags(patterns=..., add_unmapped_col=...)` now always uses `OPERATION_TYPE_PATTERNS` and always creates the unmapped flag.
  - `create_weather_history_features(group_cols=...)` now uses `latitude_r` and `longitude_r` directly.
  - `process_ports(coord_decimals=...)` and `process_weather(coord_decimals=...)` now use the module constant `COORD_DECIMALS = 5`.
- No output column was removed.
- No Chapter 3 transformation was intentionally changed.

## Regression Comparison of `eda_base`

Baseline: `data/processed/eda_base.parquet` copied before the second block to `C:\Users\tiago\AppData\Local\Temp\eda_base_approved_before_second_block.parquet`.

After refactoring, the complete pipeline was rerun with `conda run -n mbausp python pipelines\build_eda_base.py`.

| Check | Result |
|---|---|
| Rows before | 129,625 |
| Columns before | 126 |
| Rows after | 129,625 |
| Columns after | 126 |
| Column set identical | Yes |
| Column order identical | Yes |
| DataFrame equality | Yes |
| Different cells | 0 |

`t_total_port_stay_h` after the refactor:

| Statistic | Value |
|---|---:|
| count | 129,625 |
| mean | 71.103868723883 |
| median | 38.416666666667 |
| std | 97.116161781872 |
| P90 | 163.083333333333 |
| P95 | 253.733333333332 |
| max | 1280.083333333333 |

Conclusion: the second block is behavior-preserving for the final `eda_base`.

## Feature Availability Classification

Prediction moment for Chapter 4: at `arrival_port_ts`, predict `t_total_port_stay_h`.

### Calendar Features

| Feature | Classification | Reason |
|---|---|---|
| `arrival_year`, `arrival_month`, `arrival_quarter`, `arrival_weekofyear`, `arrival_day`, `arrival_dayofweek`, `arrival_hour`, `arrival_is_weekend`, `arrival_shift`, `arrival_season` | `SAFE_FOR_PREDICTION` | Derived only from `arrival_port_ts`, known at the prediction moment. |

### Operation-Type Features

| Feature group | Classification | Reason |
|---|---|---|
| `op_*`, including `op_tipo_operacao_nao_mapeado` | `SAFE_FOR_PREDICTION` | Derived from declared `operation_type`, treated as operational information available in the port-call record at arrival. The master aggregation caveat remains documented. |

### Congestion Features

| Feature | Classification | What the calculation uses |
|---|---|---|
| `arrivals_same_day_port` | `EDA_ONLY` | Counts all arrivals for the same `port` and calendar `arrival_date`, including arrivals that may occur later than the current vessel on the same day. Audit: 85,594 rows, or 66.0320% of `eda_base`, include future same-day arrivals in this count. |
| `arrivals_prev_day_port` | `REQUIRES_REDESIGN` | Uses the prior observed arrival-date row in the daily arrival-count series by port. Because dates with zero arrivals are absent, this is not necessarily the immediately previous calendar day. It does not leak future information, but its semantics do not match the current name/intended interpretation. |
| `arrivals_prev_7d_avg_port` | `REQUIRES_REDESIGN` | Uses `shift(1).rolling(7, ...)` over previous observed arrival-date rows. Because zero-arrival dates are absent, this is not necessarily the previous seven calendar days. It does not leak future information, but needs redesign before Chapter 4 prediction use. |
| `avg_wait_prev_20_calls_port` | `REQUIRES_REDESIGN` | Uses `t_wait_for_berthing_h` from the previous 20 port calls by arrival order. The shift excludes the current row but does not ensure those previous calls had already departed before current `arrival_port_ts`. |
| `avg_operation_prev_20_calls_port` | `REQUIRES_REDESIGN` | Same issue, using `t_operation_h` from previous calls by arrival order. |
| `std_wait_prev_20_calls_port` | `REQUIRES_REDESIGN` | Same issue, using `t_wait_for_berthing_h` from previous calls by arrival order. |

Prior-call duration audit:

| Check | Rows |
|---|---:|
| Rows whose previous-20 arrival-order window includes at least one call not yet completed at current arrival | 117,302 |
| Rows with non-null `avg_wait_prev_20_calls_port` and at least one unfinished call in the previous-20 window | 117,117 |
| Rows with non-null `avg_operation_prev_20_calls_port` and at least one unfinished call in the previous-20 window | 117,117 |
| Rows with non-null `std_wait_prev_20_calls_port` and at least one unfinished call in the previous-20 window | 117,117 |

### Weather Features

| Feature group | Classification | Reason |
|---|---|---|
| `temperature_2m_mean`, `temperature_2m_max`, `temperature_2m_min`, `precipitation_sum`, `rain_sum`, `precipitation_hours`, `wind_speed_10m_max`, `wind_gusts_10m_max`, `wind_direction_10m_dominant` | `EDA_ONLY` | These are realized daily aggregates for the arrival date and may include observations after the vessel arrived. |
| `rain_sum_prev_1d`, `precipitation_sum_prev_1d`, `wind_speed_10m_max_prev_1d`, `wind_gusts_10m_max_prev_1d`, `temperature_2m_mean_prev_1d` | `SAFE_FOR_PREDICTION` | Created with `shift(1)` by coordinate, so they use only weather rows before the arrival date. |
| `rain_sum_prev_3d`, `precipitation_hours_prev_3d`, `temperature_2m_mean_prev_3d`, `wind_speed_10m_max_prev_3d`, `wind_gusts_10m_max_prev_3d` | `SAFE_FOR_PREDICTION` | Created with `shift(1).rolling(3, ...)`, excluding the current arrival date. |
| `rain_sum_prev_7d`, `precipitation_hours_prev_7d`, `temperature_2m_mean_prev_7d`, `wind_speed_10m_max_prev_7d`, `wind_gusts_10m_max_prev_7d` | `SAFE_FOR_PREDICTION` | Created with `shift(1).rolling(7, ...)`, excluding the current arrival date. |
| `has_weather_data` | `EDA_ONLY` | Indicates whether same-day weather was merged; useful for coverage checks, not a substantive predictor. |

Weather-series audit: historical features use preceding observed dates only. The weather series had 168,785 one-day gaps and 8 non-daily gaps after sorting by coordinate/date. Therefore, `prev_1d` is temporally prior, but in rare gaps it means the previous observed date rather than exactly D-1 calendar.

## Operation-Type Conflict Analysis

Previous audit found 79 `port_call_id` values with multiple distinct non-null raw `operation_type` strings.

After applying `_compact_text`:

| Check | Result |
|---|---:|
| Raw conflicting `port_call_id` values | 79 |
| Still different after `_compact_text` | 1 |
| Would generate different final operation flags depending on which raw value was selected by `first` | 1 |

The one analytically meaningful case found:

| `port_call_id` | Raw values | Flag implication |
|---|---|---|
| `260102025` | `Carga`; `0` | `Carga` sets `op_carga=True`; `0` sets `op_tipo_operacao_nao_mapeado=True`. |

Conclusion: the current `first` aggregation rule was not changed. Only 1 of 79 raw conflicts changes operation flags after normalization, so changing master aggregation would be a methodological change rather than a safe refactor.

## Commands and Tests Executed

| Command / action | Result |
|---|---|
| `git status --short` | Completed successfully; worktree was clean before this block. |
| Copied `data/processed/eda_base.parquet` to `C:\Users\tiago\AppData\Local\Temp\eda_base_approved_before_second_block.parquet` | Completed successfully. |
| `rg "create_operation_type_flags|OPERATION_TYPE_PATTERNS|_compact_text|create_weather_history_features|create_basic_congestion_features|create_calendar_features|process_ports|process_weather" -n .` | Completed successfully; uses were limited to `pipelines/build_eda_base.py` and definitions in the target modules. |
| Temporary Python audit for operation-type conflicts, congestion leakage and weather date gaps | Completed successfully. |
| `conda run -n mbausp python -m compileall src\data_sources\ports.py src\data_sources\weather.py src\features\calendar.py src\features\operation_types.py src\features\congestion.py src\features\weather_features.py` | Completed successfully. |
| `conda run -n mbausp python pipelines\build_eda_base.py` | Completed successfully; produced `129,625` rows and `126` columns. |
| Temporary Python comparison of approved vs rebuilt `eda_base.parquet` | Completed successfully; DataFrames were equal with 0 differing cells. |

## Decisions Still Required Before Chapter 4

- Decide whether to redesign prior-call duration features so they use only calls whose `departure_port_ts` is before the current `arrival_port_ts`. Until then, keep `avg_wait_prev_20_calls_port`, `avg_operation_prev_20_calls_port` and `std_wait_prev_20_calls_port` out of the predictive feature set.
- Keep `arrivals_prev_day_port` and `arrivals_prev_7d_avg_port` out of the Chapter 4 predictive feature set until they are redesigned with a complete daily calendar per port, filling zero-arrival dates before applying lag/rolling calculations.
- Decide whether `arrivals_same_day_port` should remain EDA-only or be replaced by an arrival-so-far feature calculated within the calendar day.
- Decide whether to keep realized same-day weather only for EDA and use only `prev_*` weather features for modeling.
- Decide whether the single operation-type conflict that changes flags (`port_call_id=260102025`) needs manual data correction or simply documentation.

## Conclusion

The second Chapter 3 refactoring block is behavior-preserving for the final analytical base. The code is more explicit about what is descriptive EDA, what is safe at the prediction moment, and what requires redesign before Chapter 4 modeling. No issue in this block requires stopping the structural refactor, but the `REQUIRES_REDESIGN` features, including the current lagged arrival-count features, must not be used silently in the final predictive model.