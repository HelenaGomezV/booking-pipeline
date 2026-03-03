#!/bin/bash
set -euo pipefail
DBT_PROJECT_DIR="${DBT_PROFILES_DIR:-/opt/airflow/dbt_project}"
COMMAND="${1:-run}"
SELECT="${2:-}"

cd "${DBT_PROJECT_DIR}"
DBT_CMD="dbt ${COMMAND} --profiles-dir ${DBT_PROJECT_DIR}"
[ -n "${SELECT}" ] && DBT_CMD="${DBT_CMD} --select ${SELECT}"

echo "Running: ${DBT_CMD}"
eval "${DBT_CMD}"