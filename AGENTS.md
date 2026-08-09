# AGENTS.md — TCC Port Lead Time Prediction

## 1. Objetivo do projeto
Este repositório suporta o TCC sobre predição do tempo total de permanência de embarcações em porto e sua conexão com estoque de segurança e capital de giro.

O objetivo principal agora NÃO é transformar o repositório em software de produção. O objetivo é manter uma implementação acadêmica, reproduzível, simples e didática, que o autor do TCC consiga entender e explicar integralmente.

## 2. Princípios obrigatórios para qualquer alteração
1. Prefira código simples e explícito a abstrações genéricas.
2. Não crie classes, factories, helpers ou camadas extras sem necessidade acadêmica clara.
3. Cada função deve ter docstring em português explicando:
   - o que a função faz;
   - por que ela existe no contexto do TCC;
   - parâmetros principais;
   - retorno;
   - risco metodológico relevante, quando aplicável.
4. Adicione comentários em português nos trechos que exigem raciocínio metodológico. Não comente operações triviais apenas para aumentar a quantidade de comentários.
5. Preserve a reprodutibilidade dos dados e resultados descritos no TCC, mas remova código duplicado, não utilizado ou experimental que não faça parte do fluxo final.
6. Não altere silenciosamente definições metodológicas. Quando uma decisão metodológica mudar, documente-a no notebook e no README.
7. Antes de apagar um arquivo, confirme que ele não é necessário para reconstruir a base, a EDA ou os resultados finais.

## 3. Definição do problema preditivo
- Target principal: `t_total_port_stay_h`.
- Instante da previsão: chegada da embarcação ao porto, representada por `arrival_port_ts`.
- Interpretação: no momento da chegada, estimar quantas horas a embarcação permanecerá no porto até sua saída.

Regra central de anti-leakage:
> Uma feature só pode entrar no modelo se estiver disponível, ou puder ser calculada apenas com informação já conhecida, no instante da chegada da embarcação.

## 4. Features inicialmente proibidas no modelo final
Até que sejam reconstruídas com lógica temporal rigorosa, não usar como features preditivas:
- `arrivals_same_day_port`;
- clima realizado do próprio dia da chegada (`temperature_2m_*`, `precipitation_sum`, `rain_sum`, `precipitation_hours`, `wind_speed_10m_max`, `wind_gusts_10m_max` sem lag);
- `avg_wait_prev_20_calls_port`;
- `avg_operation_prev_20_calls_port`;
- `std_wait_prev_20_calls_port`.

Motivo:
- a contagem do mesmo dia pode incluir chegadas futuras em relação ao horário da embarcação atual;
- o clima diário realizado pode usar horas posteriores à chegada;
- as durações das chamadas anteriores ordenadas por chegada podem ainda não ser conhecidas caso essas chamadas não tenham terminado.

Features históricas com lag, como chegadas do dia anterior, média móvel de chegadas anteriores e clima dos dias anteriores, podem ser usadas desde que o cálculo realmente exclua o dia atual.

## 5. Divisão temporal recomendada para o Capítulo 4
Como 2025 já foi consultado durante análises preliminares de baseline, utilizar uma separação mais conservadora:
- treino: 2023-01-01 até 2024-06-30;
- validação: 2024-07-01 até 2024-12-31;
- calibração/desenvolvimento: 2025-01-01 até 2025-06-30;
- teste final: 2025-07-01 em diante.

Implementar fronteiras como intervalos semiabertos, por exemplo `data < "2024-07-01"`, para evitar o bug de corte em meia-noite.

O teste final não deve ser utilizado para escolher modelo ou hiperparâmetros.

## 6. Modelos estritamente necessários
Manter o conjunto mínimo abaixo:

### Baselines
1. mediana global;
2. mediana por porto;
3. mediana hierárquica:
   - porto + tipo de operação, quando houver pelo menos 30 observações;
   - fallback para porto;
   - fallback final para mediana global.

### Modelos pontuais
1. Ridge com target log-transformado;
2. Random Forest;
3. Gradient Boosting.

Não usar ElasticNet no fluxo final, salvo decisão acadêmica posterior explicitamente documentada.

Não adicionar XGBoost, LightGBM ou CatBoost sem necessidade clara comprovada pelos resultados.

### Modelos quantílicos
Usar Gradient Boosting quantílico apenas para P50, P90 e P95 após selecionar/congelar o fluxo de modelagem principal.

## 7. Métricas
### Regressão pontual
- MAE: métrica principal;
- RMSE;
- MedAE;
- RMSLE.

### Quantis
Não avaliar P90/P95 como se fossem previsões pontuais comuns. Usar:
- pinball loss;
- cobertura empírica (`mean(y_true <= q_pred)`);
- checagem de cruzamento de quantis.

## 8. Preprocessamento
- Variáveis categóricas: imputação simples + OneHotEncoder com `handle_unknown="ignore"`.
- Variáveis numéricas: imputação pela mediana.
- Ridge deve utilizar padronização das variáveis numéricas.
- Árvores não precisam de padronização.
- Prefira pipelines explícitos dentro do notebook a factories genéricas, se isso facilitar a compreensão.

## 9. Notebook oficial do Capítulo 4
O fluxo final deve existir em UM notebook didático e linear, preferencialmente `notebooks/modeling_step_by_step.ipynb`.

Ordem esperada:
1. definição do problema e instante da previsão;
2. seleção de features e prevenção de leakage;
3. divisão temporal;
4. baselines;
5. Ridge;
6. Random Forest;
7. Gradient Boosting;
8. comparação na validação;
9. análise por segmentos;
10. congelamento do modelo pontual;
11. avaliação no teste final;
12. quantis P50/P90/P95;
13. interpretabilidade;
14. simulação de estoque de segurança e capital de giro;
15. exportação de tabelas necessárias para o TCC.

O notebook deve explicar em Markdown o motivo de cada etapa antes de executar o código.

## 10. Simulação de estoque de segurança
O dataset não contém demanda real por item, custo unitário real, estoque real nem o lead time logístico completo.

Portanto:
- não afirmar economia real observada em R$;
- tratar a ligação com capital de giro como simulação ou análise de sensibilidade;
- declarar explicitamente que o tempo de permanência portuária é um proxy/componente do lead time total.

Referência teórica usada no TCC:
- `E[D(L)] = mu * E[L]`;
- `Var(D(L)) = sigma^2 * E[L] + mu^2 * Var(L)`;
- `SS = k * sqrt(Var(D(L)))`.

## 11. Estrutura de código a preservar
Preservar o código necessário para reconstruir e auditar:
- leitura dos dados brutos;
- limpeza;
- consolidação por port call;
- controle de qualidade;
- criação dos targets;
- criação da base analítica;
- enriquecimento de porto e clima histórico;
- EDA consolidada.

Essas etapas sustentam os Capítulos 3 e 4 e não devem ser substituídas por uma base pronta sem rastreabilidade.

## 12. Estrutura a simplificar
- `pipelines/train_model.py`: retirar do fluxo oficial; preferir o notebook didático.
- `src/modeling/segment_evaluation.py`: consolidar apenas o necessário em uma única implementação simples.
- funções duplicadas de métricas ou baselines: consolidar.
- dependências não usadas: remover do `requirements.txt`.
- imports mortos, TODOs antigos e diretórios configurados sem uso: remover quando comprovadamente desnecessários.
- notebooks antigos de modelagem: arquivar ou excluir somente depois que o notebook oficial reproduzir o que é necessário.

## 13. README
O README final deve explicar apenas:
1. objetivo acadêmico;
2. estrutura mínima do repositório;
3. como preparar o ambiente;
4. como reconstruir `eda_base.parquet`;
5. como executar o notebook oficial do Capítulo 4;
6. quais outputs são usados no texto do TCC.

Não manter instruções de scripts que já não existem.

## 14. Forma de trabalho com Codex
Ao receber uma tarefa neste repositório:
1. leia este arquivo primeiro;
2. verifique os arquivos envolvidos antes de editar;
3. faça alterações pequenas e revisáveis;
4. não misture refatoração estrutural e mudança metodológica no mesmo passo quando puder evitar;
5. explique no final quais arquivos foram alterados e por quê;
6. aponte qualquer decisão que ainda dependa do autor do TCC.

## 15. Critério de sucesso
Uma pessoa deve conseguir abrir o projeto, seguir o README e o notebook oficial, reconstruir o experimento e entender por que cada decisão foi tomada sem precisar interpretar uma arquitetura de software de produção.