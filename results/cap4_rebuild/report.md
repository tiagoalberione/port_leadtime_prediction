# Rebuild canonico do Capitulo 4

## Contexto de execucao

- Diretorio: `G:\My Drive\Graduação e Pós\USP\MBA USP IA e Big Data\TCC\port_leadtime_prediction`
- Branch: `cap4/rebuild-safe-historical-features`
- HEAD inicial: `db283e8b4014551bc30552e5de0997160249d05c`
- Python: `C:\Users\tiago\miniconda3\envs\mbausp\python.exe`

## O que mudou

A modelagem deixa de tratar as proxies antigas de congestionamento da EDA como candidatas diretas e passa a usar `ENRICHED_SAFE_HISTORY`: features historicas reconstruidas com cutoff D-1, calendario diario completo e disponibilidade por evento conhecido. O final_test permaneceu bloqueado ate a escolha do modelo e do feature set.

## Resultados pontuais

- Modelo congelado sem final_test: `HistGradientBoosting` com criterio `Menor MAE em validation; desempate por RMSE e MAE na cauda Q90. Calibration foi reportado como verificacao secundaria, sem consulta ao final_test.`.
- Final_test ORIGINAL MAE: 41.670 h.
- Final_test ENRICHED MAE: 38.351 h.
- Ganho final pareado: 3.319 h (7.97%).
- Q90/Q95: ver `tail_comparison.csv`; a cauda foi avaliada por MAE, bias e underprediction.

## Ridge

       feature_config       mae       rmse     medae    rmsle
             ORIGINAL 53.120483 130.497274 20.332378 0.939143
ENRICHED_SAFE_HISTORY 48.079541 100.725252 19.303621 0.869569

## Random Forest

       feature_config       mae      rmse     medae    rmsle
             ORIGINAL 47.588450 99.294633 19.324820 0.876111
ENRICHED_SAFE_HISTORY 45.271442 95.931937 18.551318 0.820050

## Gradient Boosting

       feature_config       mae      rmse     medae    rmsle
             ORIGINAL 47.607366 99.512784 18.992126 0.877131
ENRICHED_SAFE_HISTORY 45.185802 94.418152 18.453612 0.818535

## HGB

       feature_config       mae      rmse     medae    rmsle
             ORIGINAL 47.303739 98.982542 19.034311 0.868721
ENRICHED_SAFE_HISTORY 44.534831 94.559515 17.820677 0.803052

## Quantis e P95-P50

                         model        feature_config    dataset  quantile_crossing_rate_raw  mean_width_p90_p50_h  mean_width_p95_p50_h  pinball_p50  pinball_p90  pinball_p95
              hgb_quantile_log              ORIGINAL final_test                    0.023091            103.610561            164.371399    20.970902    16.696246    11.746850
              hgb_quantile_log ENRICHED_SAFE_HISTORY final_test                    0.034944             87.390337            134.785173    19.345338    15.169474    10.702127
baseline_quantile_hierarchical              ORIGINAL final_test                    0.000000            103.537229            164.288076    20.961879    16.601336    11.599105

A largura P95-P50 foi reavaliada globalmente e dentro de porto x operacao. A tabela `uncertainty_global.csv` resume associacao com erro absoluto, permanencia realizada e quintis de risco. A tabela `uncertainty_within_group.csv` indica se a largura passou a individualizar risco dentro dos grupos.

Resumo intragrupo: 103 grupos com n>=30; mediana da correlacao largura-erro = 0.178.

## Valor incremental das familias

A ablacao historica E0-E6 esta em `historical_feature_ablation.csv`. A analise contextual esta em `contextual_ablation.csv`.

    dataset                   model       mae    mae_q90
 validation             1_estrutura 47.570065 227.791514
calibration             1_estrutura 41.949831 218.847339
 validation       2_estrutura_tempo 47.299830 226.866013
calibration       2_estrutura_tempo 41.789235 221.017168
 validation       3_estrutura_clima 47.494887 228.750462
calibration       3_estrutura_clima 41.935747 220.971418
 validation 4_estrutura_tempo_clima 47.343760 227.048446
calibration 4_estrutura_tempo_clima 41.805758 221.847855
 validation            8_full_model 44.534831 213.864434
calibration            8_full_model 39.449479 209.764392

## Importancia

As importancias sao permutation importance por aumento de MAE, portanto nao devem ser somadas mecanicamente quando features sao correlacionadas. Top novas features:

                        feature                              family  importance_mae_increase_h
       route_total_known_median                   HISTORICO_DE_ROTA                   2.365470
      source_total_known_median                 HISTORICO_DE_ORIGEM                   0.826184
state_waiting_vessels_d_minus_1 ESTADO_OPERACIONAL_RECONSTRUIDO_D_1                   0.779132
 vessel_port_total_known_median          HISTORICO_EMBARCACAO_PORTO                   0.418882
        source_total_known_mean                 HISTORICO_DE_ORIGEM                   0.359961
   vessel_port_total_known_mean          HISTORICO_EMBARCACAO_PORTO                   0.341847
         route_total_known_mean                   HISTORICO_DE_ROTA                   0.305819
      vessel_total_known_median             HISTORICO_DA_EMBARCACAO                   0.241812
        route_total_known_count                   HISTORICO_DE_ROTA                   0.225776
 destination_total_known_median                HISTORICO_DE_DESTINO                   0.221811

## Estoque de seguranca

A simulacao e um exercicio de cenario, sem afirmar economia real observada. Ela explicita a cadeia: melhor informacao de lead time -> melhor representacao da variabilidade -> politica de buffer -> estoque de seguranca -> capital de giro proxy.

                         policy  mean_safety_stock_units  proxy_working_capital_brl  protection_rate  underbuffer_rate
A_static_segment_port_operation               725.316661               36265.833066         0.895724          0.104276
       B_dynamic_original_point               239.021463               11951.073173         0.528712          0.471288
       C_dynamic_enriched_point               262.324899               13116.244962         0.522665          0.477335
         D_dynamic_quantile_p90               727.291858               36364.592876         0.894772          0.105228
         D_dynamic_quantile_p95               887.056521               44352.826046         0.944053          0.055947

## Testes de leakage

Todos passaram? True.

## Narrativa recomendada

Reescrever o Capitulo 4 para separar claramente features exploratorias da EDA e features historicas seguras da modelagem. A nova narrativa deve enfatizar que o ganho vem de memoria operacional conhecida e contexto historico de porto/rota/embarcacao, nao de contagens simples de fluxo. As conclusoes sobre Ridge/RF/GB/HGB devem ser atualizadas conforme `point_model_comparison_validation.csv` e `point_model_comparison_final.csv`; as conclusoes sobre quantis e P95-P50 devem usar a nova avaliacao de individualizacao de risco.
