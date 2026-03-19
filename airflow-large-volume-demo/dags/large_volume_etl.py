from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="large_volume_etl_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # execute via Trigger DAG
    catchup=False,
    tags=["data-engineering", "demo", "large-volume"],
)
def large_volume_etl_demo():
    """
    DAG de demonstração:
    - Gera dados sintéticos em múltiplos shards (CSV)
    - Agrega métricas por dia a partir dos shards

    Para simular grande volume, aumente:
    - shards
    - rows_per_shard
    """

    base_dir = Path(__file__).resolve().parents[1]
    raw_dir = str(base_dir / "output" / "raw")
    output_metrics_path = str(base_dir / "output" / "metrics_daily.csv")

    shards = 10
    rows_per_shard = 200_000  # aumente para 1_000_000+ conforme seu ambiente
    days = 30

    @task
    def generate_raw():
        # Import aqui para evitar dependencias em tempo de parse do Airflow
        from subprocess import check_call

        script_path = str(base_dir / "scripts" / "generate_transactions.py")
        out_dir = raw_dir

        check_call(
            [
                "python",
                script_path,
                "--out-dir",
                out_dir,
                "--shards",
                str(shards),
                "--rows-per-shard",
                str(rows_per_shard),
                "--start-date",
                "2024-01-01",
                "--days",
                str(days),
            ]
        )

        return {"raw_dir": out_dir, "shards": shards, "rows_per_shard": rows_per_shard}

    @task
    def aggregate_metrics(_generate_result: dict):
        from subprocess import check_call

        script_path = str(base_dir / "scripts" / "aggregate_metrics.py")

        check_call(
            [
                "python",
                script_path,
                "--raw-dir",
                raw_dir,
                "--output-path",
                output_metrics_path,
                "--shards",
                str(shards),
            ]
        )

        return {"metrics_path": output_metrics_path}

    aggregate_metrics(generate_raw())


dag = large_volume_etl_demo()

