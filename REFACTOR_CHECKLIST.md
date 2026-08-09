# Checklist de Refatoração — TCC

Este arquivo acompanha a simplificação do repositório antes da revisão do notebook oficial do Capítulo 4.

## Concluído
- [x] Criar branch isolada `refactor/tcc-simplification`.
- [x] Adicionar `AGENTS.md` com contexto metodológico para o Codex.
- [x] Simplificar `src/paths.py`.
- [x] Simplificar `src/config.py` para conter apenas configuração de preparação dos dados.
- [x] Remover dependências de modelagem não usadas (`lightgbm`, `xgboost`, `shap`, `joblib`).
- [x] Remover import morto de `src/processing/master_table.py` e documentar a agregação.
- [x] Remover `pipelines/train_model.py` do fluxo oficial.
- [x] Remover `src/modeling/segment_evaluation.py`, que duplicava lógica de métricas.
- [x] Remover `notebooks/02_modeling_segment_analysis.ipynb`.
- [x] Reduzir os modelos pontuais para Ridge, Random Forest e Gradient Boosting.
- [x] Consolidar métricas pontuais, quantílicas e segmentadas em `src/modeling/metrics.py`.
- [x] Corrigir a divisão temporal usando intervalos semiabertos e criar treino/validação/calibração/teste final.
- [x] Reduzir os baselines para mediana global, mediana por grupo e mediana hierárquica.
- [x] Separar claramente o preprocessamento com e sem padronização numérica.

## Ainda a revisar antes do notebook do Capítulo 4
- [ ] Revisar e comentar integralmente `src/io_utils.py`.
- [ ] Revisar e comentar integralmente `src/data_sources/port_call.py`.
- [ ] Revisar e comentar integralmente `src/data_sources/ports.py`.
- [ ] Revisar e comentar integralmente `src/data_sources/weather.py`.
- [ ] Revisar e comentar integralmente `src/processing/cleaning.py`.
- [ ] Revisar e comentar integralmente `src/processing/validation.py`.
- [ ] Revisar e comentar integralmente `src/processing/targets.py`.
- [ ] Revisar e comentar integralmente `src/features/calendar.py`.
- [ ] Revisar `src/features/congestion.py` e separar features válidas para EDA das features válidas para previsão.
- [ ] Revisar e comentar integralmente `src/features/operation_types.py`.
- [ ] Revisar `src/features/weather_features.py` e deixar explícita a distinção entre clima do próprio dia e clima histórico.
- [ ] Revisar e comentar `pipelines/build_eda_base.py`.
- [ ] Verificar referências a arquivos/diretórios removidos.
- [ ] Atualizar `README.md` ao final da estrutura.
- [ ] Revisar `.gitignore` conforme a estrutura final.
- [ ] Substituir `notebooks/modeling_step_by_step.ipynb` pela versão oficial simplificada somente após a refatoração do código de suporte.
- [ ] Executar uma revisão final da árvore do repositório e verificar se há código morto.
- [ ] Somente então iniciar a leitura célula a célula do notebook do Capítulo 4.

## Regra de trabalho
Não avançar para interpretação dos resultados do Capítulo 4 enquanto os itens estruturais acima não estiverem concluídos. A refatoração deve preservar a rastreabilidade dos dados e as análises que sustentam os Capítulos 3 e 4.