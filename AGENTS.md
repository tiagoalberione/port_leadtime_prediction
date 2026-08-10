# AGENTS.md — TCC Port Lead Time Prediction

## Objetivo do projeto
Este repositório suporta o TCC sobre predição do tempo total de permanência de embarcações em porto e sua conexão conceitual com estoque de segurança e capital de giro.

O objetivo não é construir software de produção. O objetivo é manter uma implementação acadêmica, reproduzível, simples e didática, que o autor consiga abrir, executar e explicar integralmente.

## Princípios de alteração
1. Prefira código simples e explícito a abstrações genéricas.
2. Não crie classes, factories, helpers ou camadas extras sem necessidade acadêmica clara.
3. Funções relevantes devem ter docstring em português explicando o que fazem, por que existem no TCC, entradas/saídas principais e riscos metodológicos quando houver.
4. Use comentários em português apenas onde houver raciocínio metodológico ou detalhe não óbvio.
5. Preserve a rastreabilidade: leitura bruta, limpeza, consolidação por escala, qualidade, targets, enriquecimento e EDA devem continuar reconstruíveis.
6. Não altere silenciosamente definições metodológicas. Se uma decisão mudar, documente no README e no notebook adequado.
7. Antes de apagar arquivo ou diretório, confirme que não é necessário para reconstruir a base, a EDA ou os resultados finais.

## Problema preditivo
- Target principal: `t_total_port_stay_h`.
- Instante da previsão: chegada da embarcação ao porto, representada por `arrival_port_ts`.
- Interpretação: no momento da chegada, estimar quantas horas a embarcação permanecerá no porto até sua saída.

Regra central de anti-leakage:
> Uma feature só pode entrar no modelo se estiver disponível, ou puder ser calculada apenas com informação já conhecida, no instante da chegada da embarcação.

## Disponibilidade das features
### EDA_ONLY
Podem permanecer em `eda_base.parquet` para análise histórica/descritiva, mas não devem ser usadas como preditoras substantivas no modelo final:
- `arrivals_same_day_port`, pois conta todas as chegadas do mesmo dia e pode incluir chegadas posteriores ao horário da embarcação atual;
- clima realizado do próprio dia da chegada, como `temperature_2m_*`, `precipitation_sum`, `rain_sum`, `precipitation_hours`, `wind_speed_10m_max`, `wind_gusts_10m_max` e `wind_direction_10m_dominant` sem defasagem;
- `has_weather_data` como preditor substantivo, pois é indicador de cobertura/merge do clima do próprio dia.

### REQUIRES_REDESIGN
Não usar no modelo final até reconstruir com lógica temporal rigorosa:
- `arrivals_prev_day_port`;
- `arrivals_prev_7d_avg_port`;
- `avg_wait_prev_20_calls_port`;
- `avg_operation_prev_20_calls_port`;
- `std_wait_prev_20_calls_port`.

Motivo: as contagens defasadas atuais usam datas observadas com chegada, não um calendário diário completo com dias sem chegada preenchidos com zero. As durações de chamadas anteriores são ordenadas por chegada e podem incluir escalas que ainda não tinham terminado no instante da chegada atual.

### SAFE temporal information
São informações conceitualmente seguras no instante de chegada:
- variáveis de calendário derivadas de `arrival_port_ts`;
- flags declaradas de tipo de operação (`op_*`), respeitando a cautela documentada sobre agregação por `first`;
- clima histórico/defasado (`prev_1d`, `prev_3d`, `prev_7d`) porque exclui o dia de chegada, com a ressalva de que raros saltos na série climática fazem `prev_1d` significar data observada anterior, não sempre D-1 calendário.

## Divisão temporal recomendada para o Capítulo 4
Como 2025 já foi consultado em análises preliminares, usar separação conservadora:
- treino: `2023-01-01` até antes de `2024-07-01`;
- validação: `2024-07-01` até antes de `2025-01-01`;
- calibração/desenvolvimento: `2025-01-01` até antes de `2025-07-01`;
- teste final: `2025-07-01` em diante.

Implementar fronteiras como intervalos semiabertos. O teste final não deve ser usado para escolher modelo ou hiperparâmetros.

## Modelos e métricas do Capítulo 4
O notebook oficial deve ser didático e autocontido, sem depender de um pacote paralelo de modelagem.

Manter o conjunto mínimo:
- baselines: mediana global, mediana por porto, mediana hierárquica porto + tipo de operação com fallback;
- modelos pontuais: Ridge com target log-transformado, Random Forest e Gradient Boosting;
- quantis: Gradient Boosting quantílico para P50, P90 e P95 após congelar o fluxo pontual.

Métricas pontuais: MAE, RMSE, MedAE e RMSLE. Métricas quantílicas: pinball loss, cobertura empírica e checagem de cruzamento de quantis.

Não adicionar XGBoost, LightGBM, CatBoost ou SHAP sem necessidade clara e decisão acadêmica documentada.

## Simulação de estoque de segurança
O dataset não contém demanda real por item, custo unitário real, estoque real nem lead time logístico completo. Portanto:
- não afirmar economia real observada em R$;
- tratar a conexão com capital de giro como simulação/análise de sensibilidade;
- declarar que o tempo de permanência portuária é proxy ou componente do lead time total.

Referência teórica:
- `E[D(L)] = mu * E[L]`;
- `Var(D(L)) = sigma^2 * E[L] + mu^2 * Var(L)`;
- `SS = k * sqrt(Var(D(L)))`.

## Forma de trabalho
Ao receber uma tarefa neste repositório:
1. leia este arquivo primeiro;
2. verifique os arquivos envolvidos antes de editar;
3. faça alterações pequenas e revisáveis;
4. não misture refatoração estrutural e mudança metodológica quando puder evitar;
5. explique no final quais arquivos foram alterados e por quê;
6. aponte decisões que ainda dependem do autor do TCC.