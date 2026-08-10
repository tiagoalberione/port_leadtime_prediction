# Predição de Permanência Portuária — TCC

Este repositório contém o fluxo reprodutível do TCC sobre predição do tempo total de permanência de embarcações em porto e sua relação conceitual com estoque de segurança e capital de giro.

O projeto é acadêmico e didático. Ele não é uma aplicação de produção, API ou serviço de modelos.

## Objetivo da pesquisa

Estimar, no momento em que a embarcação chega ao porto, quanto tempo ela permanecerá até a saída.

- Target principal: `t_total_port_stay_h`.
- Instante da previsão: `arrival_port_ts`.
- Regra metodológica central: somente informações disponíveis no instante da chegada podem entrar no modelo preditivo.

## Fluxo dos dados

1. Ler os dados brutos de estadia, portos e clima em `data/raw/`.
2. Limpar e padronizar os campos.
3. Consolidar os registros de estadia em uma linha por `port_call_id`.
4. Aplicar controles de qualidade e definir a base elegível para EDA.
5. Criar os targets de duração.
6. Adicionar calendário, tipo de operação, congestionamento descritivo, referência de porto e clima.
7. Salvar a base analítica final em `data/processed/eda_base.parquet`.

Com o snapshot atual de dados, a referência de reprodutibilidade é:

- 129.625 escalas portuárias elegíveis;
- 126 colunas na base analítica final.

Esses valores são referências do estado atual dos dados. Eles podem mudar se os arquivos brutos forem atualizados.

## Estrutura do repositório

```text
data/raw/                 Dados brutos esperados pelo pipeline
data/interim/             Bases intermediárias geradas pelo pipeline
data/processed/           Base analítica final gerada pelo pipeline
notebooks/01_eda_ptbr.ipynb              Notebook da EDA do Capítulo 3
notebooks/modeling_step_by_step.ipynb    Notebook oficial de modelagem do Capítulo 4
pipelines/build_eda_base.py              Reconstrução da base analítica
src/                       Código de apoio para preparação dos dados
outputs/                   Tabelas e figuras geradas por notebooks/pipeline
```

## Ambiente Python

O ambiente usado neste trabalho se chama `mbausp`. Uma forma simples de criá-lo é:

```bash
conda create -n mbausp python=3.11
conda activate mbausp
pip install -r requirements.txt
```

Para executar comandos sem ativar manualmente o ambiente:

```bash
conda run -n mbausp python --version
```

## Dados brutos esperados

O pipeline espera arquivos nas seguintes pastas:

```text
data/raw/estadia/
data/raw/ports/
data/raw/weather/
```

Os arquivos brutos não são substituídos por uma base pronta: eles sustentam a rastreabilidade dos Capítulos 3 e 4.

## Reconstrução da base analítica

Execute:

```bash
conda run -n mbausp python pipelines/build_eda_base.py
```

Saída principal esperada:

```text
data/processed/eda_base.parquet
```

O pipeline também gera bases intermediárias em `data/interim/` e tabelas auxiliares em `outputs/tables/`.

## Notebooks oficiais

- Capítulo 3 — EDA: `notebooks/01_eda_ptbr.ipynb`.
- Capítulo 4 — Modelagem: `notebooks/modeling_step_by_step.ipynb`.

O notebook do Capítulo 4 deve permanecer didático e autocontido. Ele é o fluxo oficial de modelagem.

## Regra de prevenção de vazamento

No Capítulo 4, uma variável só pode ser usada como feature se estiver disponível em `arrival_port_ts`.

Exemplos que não devem entrar diretamente no modelo final sem redesign:

- `arrivals_same_day_port`;
- `arrivals_prev_day_port` e `arrivals_prev_7d_avg_port` na forma atual;
- médias/desvios de duração de chamadas anteriores ordenadas por chegada;
- clima realizado do próprio dia da chegada;
- `has_weather_data` como preditor substantivo.

Clima histórico defasado, calendário da chegada e flags declaradas de operação são as informações seguras atualmente documentadas, respeitadas as ressalvas metodológicas em `AGENTS.md`.