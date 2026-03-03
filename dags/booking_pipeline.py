"""
Booking Medallion Pipeline DAG
Orchestrates: CSV → Bronze → Silver → Gold → Tests → Summary
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

# === Default configuration for all tasks ===
default_args = {
    "owner": "helena",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
}

# === DAG definition ===
dag = DAG(
    "booking_medallion_pipeline",
    default_args=default_args,
    description="End-to-end booking analytics pipeline with Medallion Architecture",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["booking", "medallion", "dbt"],
)


# === Task 1: Bronze Ingestion ===
def run_ingestion(**context):
    """Loads CSV into Bronze layer."""
    import sys

    sys.path.insert(0, "/opt/airflow/scripts")
    from ingestion import ingest_csv_to_bronze

    csv_path = "/opt/airflow/data/raw/bookings.csv"
    result = ingest_csv_to_bronze(csv_path)
    logger.info(f"Ingestion result: {result}")

    # Push metrics to XCom for the summary task
    context["ti"].xcom_push(key="ingestion_result", value=result)
    return result


ingest_to_bronze = PythonOperator(
    task_id="ingest_to_bronze",
    python_callable=run_ingestion,
    dag=dag,
)


# === Tasks 2-3: dbt Silver + Gold (grouped) ===
with TaskGroup("dbt_transformations", dag=dag) as dbt_group:

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="bash /opt/airflow/scripts/run_dbt.sh run staging",
        dag=dag,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="bash /opt/airflow/scripts/run_dbt.sh run marts",
        dag=dag,
    )

    dbt_run_silver >> dbt_run_gold


# === Task 4: dbt Tests (quality gate) ===
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="bash /opt/airflow/scripts/run_dbt.sh test",
    dag=dag,
)


# === Task 5: Log Summary ===
def log_summary(**context):
    """Logs pipeline execution summary."""
    ti = context["ti"]
    ingestion_result = ti.xcom_pull(task_ids="ingest_to_bronze", key="ingestion_result")

    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    if ingestion_result:
        logger.info(f"  Bronze rows inserted: {ingestion_result.get('inserted', 'N/A')}")
        logger.info(f"  Duplicates skipped:   {ingestion_result.get('duplicates', 'N/A')}")
        logger.info(f"  Total Bronze rows:    {ingestion_result.get('total', 'N/A')}")
    logger.info(f"  Pipeline completed at: {datetime.now()}")
    logger.info("=" * 60)


log_summary_task = PythonOperator(
    task_id="log_summary",
    python_callable=log_summary,
    dag=dag,
)

# === Task Dependencies ===
ingest_to_bronze >> dbt_group >> dbt_test >> log_summary_task
