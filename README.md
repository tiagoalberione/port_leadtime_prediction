# TCC Lead Time Project

Projeto de análise de dados para o TCC sobre predição de lead time e redução de capital de giro.

## Estrutura
- `data/raw`: arquivos originais
- `data/interim`: dados consolidados intermediários
- `data/processed`: dados tratados
- `src/ingestion`: leitura e consolidação
- `src/processing`: tratamento e criação de colunas
- `scripts`: scripts de execução

## Como rodar

### Ingestion - estadia_embarcacao
```bash
python scripts/run_ingestion_estadia.py
```

### Procesing - estadia_embarcacao
```bash
python scripts/run_processing_estadia.py
```