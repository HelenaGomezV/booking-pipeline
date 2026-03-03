#!/bin/bash
set -e

echo "=== 1. LINT ==="
flake8 scripts/ tests/ dags/ --max-line-length=120 --ignore=E501,W503
echo "PASS"

echo "=== 2. FORMAT ==="
black --check scripts/ tests/ dags/ --line-length=120
echo "PASS"

echo "=== 3. TESTS ==="
python -m pytest tests/ -v
echo "PASS"

echo "=== 4. DAG CHECK ==="
python -c "from dags.booking_pipeline import dag; print(f'DAG OK: {dag.dag_id}, {len(dag.tasks)} tasks')"
echo "PASS"

echo "=== ALL CHECKS PASSED ==="
