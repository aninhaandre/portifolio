# Airflow Large Volume ETL (Demo)

Projeto de exemplo para **Engenharia de Dados** mostrando um pipeline em **Apache Airflow** que:

1. Gera **grande volume de dados sintéticos** (shards CSV) para simular ingestao/ETL.
2. Faz transformação/agregação **por dia** (contagem, soma e média de valores).
3. Salva um dataset final pequeno (agregado) para facilitar consumo em BI.

> Importante: este projeto é uma demo para portfólio. Ajuste `rows_per_shard` e `shards` para o volume desejado no seu ambiente.

## Estrutura

```text
airflow-large-volume-demo/
  dags/
    large_volume_etl.py
  scripts/
    generate_transactions.py
    aggregate_metrics.py
  requirements.txt
  output/               (criado automaticamente em runtime)
  raw/                  (criado automaticamente em runtime)
```

## Requisitos

- Python 3.10+
- Apache Airflow instalado e configurado no seu ambiente
- Pacotes Python (instalar com `pip install -r requirements.txt`)

## Como usar

1. Copie a pasta `airflow-large-volume-demo/` para dentro do seu repositório Git (ou mantenha nela).
2. No Airflow, aponte o diretório `dags/` para onde está `large_volume_etl.py`.
3. Abra o DAG no UI do Airflow e clique em **Trigger DAG**.

O DAG vai gerar:
- `output/raw/transactions_shard_*.csv`
- `output/metrics_daily.csv`

## Onde configurar o volume

No arquivo `dags/large_volume_etl.py`, ajuste:

- `shards` (quantidade de arquivos)
- `rows_per_shard` (linhas por arquivo)

Exemplo:
- para rodar rápido: `shards=5` e `rows_per_shard=200_000`
- para “grande volume”: `shards=10` e `rows_per_shard=1_000_000` (ajuste conforme sua máquina)

## Observações para evoluir (opcional)

- Trocar a agregação em Python por **PySpark** (ou job em cluster) para mostrar processamento distribuido.
- Persistir em **Parquet particionado** e/ou carregar em um banco (SQL Server/PostgreSQL).
- Adicionar validações (contagem total, checagem de nulidade e faixas de valores) e logs/metrics.

