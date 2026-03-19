"""
Databricks PySpark ETL Demo
----------------------------

Pipeline de exemplo para portfólio:
- Gera dados sintéticos de saúde
- Transforma e padroniza schema
- Agrega métricas por dia e setor
- Grava resultado em Delta Lake particionado por data

Como usar no Databricks:
1) Cole este conteúdo em um notebook (.ipynb) ou rode como script via Job.
2) Ajuste OUTPUT_BASE_PATH se quiser outra pasta.

Requer:
- Runtime com Spark + Delta Lake
"""

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Ajuste conforme sua demo
OUTPUT_BASE_PATH = "/delta/healthcare_daily"

# Volume da demo (sintético)
days = 30
rows_per_day = 250_000  # aumente para simular mais volume
seed = 42

departments = ["UTI", "Clínica Médica", "Pronto Atendimento", "Internação"]


def build_synthetic_healthcare_df(spark) -> DataFrame:
    """
    Gera um DataFrame sintético com colunas:
    event_time, department, visits, admissions, discharges, avg_wait_minutes, avg_length_stay_days, readmissions
    """
    start = date(2024, 1, 1)
    # Cria uma base por dia com número de linhas por dia
    # Para simular grande volume, multiplicamos um grid com explode de range.
    day_rows = (
        spark.range(0, days)
        .select(
            (F.lit(start).cast("date") + F.expr("interval 1 day * id")).alias("event_date"),
            F.lit(rows_per_day).alias("rows_per_day"),
        )
        .withColumn("row_idx", F.explode(F.sequence(F.lit(0), F.col("rows_per_day") - 1)))
    )

    rng = F.rand(seed)
    dept_idx = (rng * F.lit(len(departments))).cast("int")

    dept_array = F.array([F.lit(x) for x in departments])
    department = dept_array[dept_idx]

    # Monta event_time dentro do dia (segundos)
    seconds_in_day = 24 * 60 * 60
    seconds_offset = (rng * F.lit(seconds_in_day)).cast("int")
    event_time = F.to_timestamp(F.concat_ws(" ", F.col("event_date"), F.lit("00:00:00"))) + F.expr(
        "interval 1 second * offset"
    )

    df = (
        day_rows.drop("rows_per_day")
        .withColumn("department", department)
        .withColumn("offset", seconds_offset)
        .withColumn("event_time", event_time)
    )

    # Métricas sintéticas com distribuições simples
    # visits/admissions/discharges/readmissions inteiros
    visits = (F.abs(F.sin(rng * 1000)) * F.lit(220)).cast("int").alias("visits")
    admissions = (F.abs(F.cos(rng * 500)) * F.lit(120)).cast("int").alias("admissions")
    discharges = (F.abs(F.sin(rng * 250)) * F.lit(120)).cast("int").alias("discharges")
    readmissions = (F.abs(F.cos(rng * 900)) * F.lit(20)).cast("int").alias("readmissions")

    avg_wait = (rng * F.lit(35) + F.lit(5)).alias("avg_wait_minutes")
    avg_los = (rng * F.lit(8) + F.lit(1)).alias("avg_length_stay_days")

    df = (
        df.select(
            "event_time",
            "department",
            visits,
            admissions,
            discharges,
            avg_wait,
            avg_los,
            readmissions,
        )
        # Padroniza campos
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("date", F.to_date("event_time"))
        .drop("event_time")
        .withColumnRenamed("date", "date")
    )

    return df


def transform_and_aggregate(df: DataFrame) -> DataFrame:
    """
    Agrega por data e departamento gerando métricas:
    - event_count (só para demonstrar volume/contagem)
    - sum_visits, sum_admissions, sum_discharges, sum_readmissions
    - avg_wait_minutes, avg_length_stay_days (médias)
    """
    return (
        df.groupBy("date", "department")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.sum("visits").alias("visits"),
            F.sum("admissions").alias("admissions"),
            F.sum("discharges").alias("discharges"),
            F.sum("readmissions").alias("readmissions"),
            F.avg("avg_wait_minutes").alias("avg_wait_minutes"),
            F.avg("avg_length_stay_days").alias("avg_length_stay_days"),
        )
        .select(
            "date",
            "department",
            "visits",
            "admissions",
            "discharges",
            "avg_wait_minutes",
            "avg_length_stay_days",
            "readmissions",
        )
    )


def write_delta_partitioned(agg_df: DataFrame, output_path: str) -> None:
    """
    Escreve em Delta particionado por data (date).
    """
    (
        agg_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("date")
        .save(output_path)
    )


def main():
    # Obtém o Spark Session do Databricks
    spark = None  # o Databricks injeta "spark"; aqui deixamos para apontar o uso correto

    # Quando rodar no notebook, remova esta linha e use o "spark" já disponível.
    # Exemplo (no notebook):
    #   agg_df = transform_and_aggregate(build_synthetic_healthcare_df(spark))
    #   write_delta_partitioned(agg_df, OUTPUT_BASE_PATH)
    #
    # Para manter o arquivo como demo/copiar, deixamos apenas as funções.
    pass


if __name__ == "__main__":
    # Mesma lógica, mas aqui o script deve ser chamado em um ambiente com spark disponível.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = build_synthetic_healthcare_df(spark)
    agg_df = transform_and_aggregate(df)
    write_delta_partitioned(agg_df, OUTPUT_BASE_PATH)
    print(f"OK: Delta escrito em {OUTPUT_BASE_PATH}")

