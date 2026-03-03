FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root dbt_project/ /opt/airflow/dbt_project/
COPY --chown=airflow:root data/ /opt/airflow/data/
COPY --chown=airflow:root tests/ /opt/airflow/tests/