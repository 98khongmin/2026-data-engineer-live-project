#!/usr/bin/env bash
set -euo pipefail

JOB_SCRIPT="${1:-amazon_reviews_etl.py}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
if [[ $# -gt 0 ]]; then shift; fi

# 수집은 Spark 정제가 필요 없으므로 컨테이너의 Python으로 실행합니다.
exec python3 -u "/opt/spark/app/$JOB_SCRIPT" "$@"
