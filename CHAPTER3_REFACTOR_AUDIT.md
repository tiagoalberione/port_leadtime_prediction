# Chapter 3 Refactor Audit

This document records the regression and methodological audit performed after the first Chapter 3 refactoring block. The intent of the block was behavior preservation: improve readability, comments and docstrings without changing the data transformations used to reproduce the thesis.

Comparison baseline: `main` exported to a temporary clean directory with `git archive`.
Refactor branch: `refactor/tcc-simplification`.

## Regression Comparison: `main` vs `refactor/tcc-simplification`

| Metric | `main` | `refactor/tcc-simplification` |
|---|---:|---:|
| Raw port-call rows | 162,036 | 162,036 |
| Unique raw `port_call_id` | 146,037 | 146,037 |
| Rows after master aggregation | 146,037 | 146,037 |
| Eligible rows | 129,625 | 129,625 |
| Final `eda_base` rows | 129,625 | 129,625 |
| Final `eda_base` columns | 126 | 126 |

Final column sets: identical.
Final column order: identical.

### `t_total_port_stay_h`

| Statistic | `main` | `refactor/tcc-simplification` |
|---|---:|---:|
| count | 129,625 | 129,625 |
| mean | 71.103868723883 | 71.103868723883 |
| median | 38.416666666667 | 38.416666666667 |
| std | 97.116161781872 | 97.116161781872 |
| P90 | 163.083333333333 | 163.083333333333 |
| P95 | 253.733333333332 | 253.733333333332 |
| max | 1280.083333333333 | 1280.083333333333 |

### Exported Summary Differences

`outputs/tables/port_call_quality_summary.csv`: no differences.

`outputs/tables/target_summary.csv`: no differences.

## `port_call_id` Audit Before `groupby`

| Check | Result |
|---|---:|
| Raw rows before `groupby` | 162,036 |
| Rows with null/missing `port_call_id` | 0 |
| Unique non-null `port_call_id` before `groupby` | 146,037 |
| Rows that would be discarded by `groupby` due to null ID | 0 |

Conclusion: no explicit missing-ID treatment is required before the current `groupby`, because there are no raw rows with null/missing `port_call_id` in the audited input. Keeping this audit visible is still useful because `groupby` would silently omit null keys if they appeared in future input files.

## Multiple-Values-Per-Port-Call Audit

Denominator for percentages: 146,037 unique non-null raw `port_call_id` values.
Counts below use more than one distinct **non-null** value within the same `port_call_id`.

### Summary

| Field | Port calls with >1 distinct non-null value | % of port calls affected | Is `first` methodologically acceptable for now? |
|---|---:|---:|---|
| `port` | 0 | 0.0000% | Yes. No conflicts found. |
| `port_name` | 8 | 0.0055% | Yes for now. Conflicts are very rare and mostly naming/encoding variants. |
| `imo` | 0 | 0.0000% | Yes. No conflicts found. |
| `vessel_id` | 3 | 0.0021% | Yes for now. Very rare identifier-format conflicts; not enough evidence to change aggregation in a behavior-preserving refactor. |
| `vessel_name` | 21 | 0.0144% | Yes for now. Very rare naming variants; should be documented if vessel-level analysis becomes central. |
| `operation_type` | 79 | 0.0541% | Acceptable only for preserving current behavior. Many examples are delimiter variants, but this field is analytically important and should be revisited before methodological changes. |
| `source_port` | 44 | 0.0301% | Acceptable for now. Rare conflicts; changing the rule would require a documented methodological decision. |
| `destination_port` | 48 | 0.0329% | Acceptable for now. Rare conflicts; changing the rule would require a documented methodological decision. |

### Representative Examples

#### `port`

No conflicts found.

#### `port_name`

| `port_call_id` | Distinct non-null values |
|---|---|
| `197142025` | `TERMINAL FLUVIAL DISTRIBUIDORA ATEM´S - MANAUS - AM`; `TERMINAL FLUVIAL DISTRIBUIDORA ATEMŽS - MANAUS - AM` |
| `208602025` | `TERMINAL FLUVIAL DISTRIBUIDORA ATEM´S - MANAUS - AM`; `TERMINAL FLUVIAL DISTRIBUIDORA ATEMŽS - MANAUS - AM` |
| `247552025` | `TERMINAL FLUVIAL DISTRIBUIDORA ATEM´S - MANAUS - AM`; `TERMINAL FLUVIAL DISTRIBUIDORA ATEMŽS - MANAUS - AM` |
| `291372025` | `TERMINAL FLUVIAL DISTRIBUIDORA ATEM´S - MANAUS - AM`; `TERMINAL FLUVIAL DISTRIBUIDORA ATEMŽS - MANAUS - AM` |
| `318742025` | `ITAGUAÍ`; `ITAGUAI ( EX SEPETIBA)` |

#### `imo`

No conflicts found.

#### `vessel_id`

| `port_call_id` | Distinct non-null values |
|---|---|
| `32252024` | `4430005450`; `4430123896` |
| `96342024` | `183.005891-6`; `5432112345` |
| `512392024` | `381-388614-0`; `3813886140` |

#### `vessel_name`

| `port_call_id` | Distinct non-null values |
|---|---|
| `193662025` | `SEVEN SUN`; `SEVEN SUN (REB)` |
| `195692025` | `MSC PEGASUS VII`; `MSC PEGASUS` |
| `197292025` | `MSC PEGASUS VII`; `MSC PEGASUS` |
| `199582025` | `MSC PEGASUS VII`; `MSC PEGASUS` |
| `202242025` | `MSC PEGASUS VII`; `MSC PEGASUS` |

#### `operation_type`

| `port_call_id` | Distinct non-null values |
|---|---|
| `148522025` | `Abastecimento (Bunker)Off-shoreRetirada de Resíduos - sem operação comercial`; `Abastecimento (Bunker),Off-shore,Retirada de Resíduos - sem operação comercial` |
| `150532025` | `Off-shoreSolicitação de certificado`; `Off-shore,Solicitação de certificado` |
| `152992025` | `Abastecimento (Bunker)Carga e Descarga`; `Abastecimento (Bunker),Carga e Descarga` |
| `164632025` | `CargaFundeio`; `Carga,Fundeio` |
| `165542025` | `Off-shoreReparo/Manutenção`; `Off-shore,Reparo/Manutenção` |

#### `source_port`

| `port_call_id` | Distinct non-null values |
|---|---|
| `2312025` | `USPDX`; `CLPUQ` |
| `20932022` | `BRBAR`; `BRSUA` |
| `179572025` | `GBIMM`; `DEHAM` |
| `211472025` | `BRIGI`; `BRADR` |
| `211592025` | `BRPNG`; `BRPNG002` |

#### `destination_port`

| `port_call_id` | Distinct non-null values |
|---|---|
| `167492025` | `CNQIN`; `CNHFE` |
| `171462025` | `CNQIN`; `CNDJK` |
| `175152025` | `CNDJK`; `CNLSN` |
| `180992025` | `BRSLZ`; `BRIQI` |
| `184972025` | `CNQIN`; `CNHFE` |

## Modeling Dependency Map

No current file or notebook imports modules from `src/modeling` according to:

```powershell
rg "src\.modeling|from src\.modeling|import src\.modeling" -n .
```

The command completed with exit code 1, which is `rg`'s normal result for no matches.

## Commands and Tests Executed

| Command / action | Result |
|---|---|
| `git status --short` | Completed successfully; worktree was clean before creating this document. |
| `rg "src\.modeling|from src\.modeling|import src\.modeling" -n .` | Completed with no matches. |
| `conda run -n mbausp python pipelines\build_eda_base.py` on `refactor/tcc-simplification` | Completed successfully; produced `129,625` rows and `126` columns. |
| `git archive --format=zip --output=<temp_zip> main` and `Expand-Archive` to a temporary directory | Completed successfully; created a clean comparison copy of `main`. |
| `conda run -n mbausp python pipelines\build_eda_base.py` inside the temporary `main` copy | Completed successfully; produced `129,625` rows and `126` columns. |
| Temporary Python audit script comparing current outputs with `main` outputs | Completed successfully after forcing ASCII JSON output for Windows console compatibility. |
| Temporary Python audit script for distinct non-null values per `port_call_id` | Completed successfully. |
| `git diff --check` before the previous correction commit | Completed successfully; only normal LF/CRLF warnings were reported by Git on Windows. |
| `conda run -n mbausp python -m compileall src\io_utils.py src\data_sources\port_call.py src\processing\cleaning.py src\processing\master_table.py src\processing\validation.py src\processing\targets.py pipelines\build_eda_base.py` | Completed successfully. |

Notes from execution:

- Direct `python` was not usable in this Windows shell because the Windows alias did not point to the project environment.
- Direct execution of the environment Python previously failed on `numpy` DLL initialization; `conda run -n mbausp` loaded the environment correctly.
- A first JSON-printing attempt failed because `conda run` tried to print Unicode to a CP1252 console. The final audit output used ASCII JSON escaping and completed successfully.

## Conclusion

The Chapter 3 refactor is behavior-preserving relative to `main` for the audited outputs. There were no differences in row counts, final column set/order, `port_call_quality_summary.csv`, `target_summary.csv`, or the reported statistics for `t_total_port_stay_h`.

No issue must be resolved before proceeding to the next refactoring block. The multiple-values-per-port-call findings are methodological notes, not regression failures. The current `first` aggregation rule should remain unchanged until a separate, explicit methodological decision is made, especially for `operation_type`, `source_port`, and `destination_port`.