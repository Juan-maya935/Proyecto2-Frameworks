"""DAG principal - Pipeline Lakehouse Medallion (Bronze -> Silver -> Gold).

Proyecto 2 - Frameworks y Herramientas para Big Data.

Un solo "Play" ejecuta en orden:
  1. Bronze: ingesta con dlt de users 2021 -> s3://bronze/users/2021/users_2021.parquet (override)
  2. Silver: limpieza + MERGE INTO en Iceberg  -> nessie.silver.users_hist (bucket silver)
  3. Gold:   cruce post_hist + users_hist, KPIs + MERGE INTO -> nessie.gold.cant_post_x_user_hist

Los scripts viven en dags/scripts (ignorados por el scheduler via .airflowignore) y son
los mismos que reutilizan los notebooks de respaldo.

Si el equipo tiene poca RAM, se pueden definir las variables de entorno SILVER_MAX_ROWS /
GOLD_MAX_ROWS en el servicio airflow-scheduler (docker-compose.yml) para limitar
registros, como permite el enunciado.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPTS = "/opt/airflow/dags/scripts"
DLT_PYTHON = "/home/airflow/dlt-venv/bin/python"  # venv aislado con dlt (Dockerfile.airflow)

default_args = {
    "owner": "grupo_lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_medallion_pipeline",
    description="Bronze (dlt) -> Silver (Spark + Iceberg MERGE) -> Gold (Spark + Iceberg MERGE)",
    default_args=default_args,
    schedule=None,          # ejecucion manual con el boton Play
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "dlt", "spark", "iceberg", "nessie"],
) as dag:

    bronze = BashOperator(
        task_id="1_bronze_ingesta_dlt",
        bash_command=f"{DLT_PYTHON} {SCRIPTS}/bronze_ingest.py users 2021",
        execution_timeout=timedelta(hours=1),
    )

    silver = BashOperator(
        task_id="2_silver_users_hist_merge",
        bash_command=f"python {SCRIPTS}/silver_transform.py users --max-rows 100000",
        execution_timeout=timedelta(hours=1),
    )

    gold = BashOperator(
        task_id="3_gold_cant_post_x_user_hist_merge",
        bash_command=f"python {SCRIPTS}/gold_agg.py --max-rows 100000",
        execution_timeout=timedelta(hours=1),
    )

    bronze >> silver >> gold
