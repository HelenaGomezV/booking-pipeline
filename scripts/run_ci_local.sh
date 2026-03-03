#!/bin/bash
set -e

echo "=== 1. FORMAT (auto-fix) ==="
black scripts/ tests/ dags/ --line-length=120
echo "PASS"

echo "=== 2. LINT ==="
flake8 scripts/ tests/ dags/ --max-line-length=120 --ignore=E501,W503
echo "PASS"

echo "=== 3. TESTS ==="
python -m pytest tests/ -v
echo "PASS"

echo ""
echo "=== ALL CHECKS PASSED ==="
echo "Safe to commit and push!"
