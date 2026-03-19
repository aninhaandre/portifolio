# Databricks + PySpark (Delta Lake) – Demo de ETL

Este projeto é uma demo para portfólio mostrando um ETL com **PySpark no Databricks**, gerando dados sintéticos de saúde, aplicando transformações e gravando em **Delta Lake** particionado por data.

## O que o projeto faz

1. Gera um dataset sintético (grande o suficiente para demonstrar leitura/transformação).
2. Padroniza tipos e cria campos auxiliares (ex.: `date`, `month`).
3. Calcula métricas de negócio agregando por dia e por setor.
4. Persiste o resultado em formato Delta particionado (`/delta/healthcare_daily`).

## Como executar (Databricks)

### Opção A: usar como notebook
- Crie um notebook no Databricks
- Cole o conteúdo de `scripts/pyspark_etl_delta.py`
- Ajuste apenas `OUTPUT_BASE_PATH` se quiser salvar em outra pasta

### Opção B: rodar como job
- Faça upload do arquivo para seu workspace e rode via Databricks Job

## Ajustes para deixar “com cara de grande volume”

No arquivo `scripts/pyspark_etl_delta.py`, procure:
- `rows_per_day`
- `days`

Aumente esses valores para aumentar o volume.

> Observação: como é uma demo, os dados são sintéticos.

