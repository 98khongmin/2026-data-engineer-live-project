#!/usr/bin/env bash
set -euo pipefail

JOB_SCRIPT="${1:-amazon_reviews_etl.py}"
JOB_SCRIPT="${JOB_SCRIPT#jobs/}"
if [[ $# -gt 0 ]]; then shift; fi

# 수집과 후속 정제 모두 같은 방식으로 Python 작업을 실행합니다.
exec python3 -u "/opt/spark/app/$JOB_SCRIPT" "$@"
