"""Spark 기반 원본 분산 로딩, review_id 중복 제거, 월별 Parquet 저장 및 PostgreSQL 월별 집계 파이프라인.

설계 원칙:
파티션별 manifest와 완료 marker를 기반으로 정상 산출물을 판정하고,
staging·backup·quarantine·startup recovery를 통해 예외 및 비정상 종료 이후에도 안전하게 재실행 가능한 구조를 제공합니다.
"""
import argparse
import contextlib
from datetime import datetime, timezone
import fcntl
import json
import os
import shutil
from urllib.parse import quote
import uuid

from pyspark.sql import SparkSession, functions as F, types as T

NAMESPACE_URL = uuid.NAMESPACE_URL

# 원본 JSONL 데이터 스키마 정의 (스키마 추론 오버헤드 방지 및 타입 안정성 확보)
RAW_REVIEW_SCHEMA = T.StructType([
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("text", T.StringType(), True),
    T.StructField("asin", T.StringType(), True),
    T.StructField("parent_asin", T.StringType(), True),
    T.StructField("user_id", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("helpful_vote", T.LongType(), True),
    T.StructField("verified_purchase", T.BooleanType(), True),
])


@F.udf(returnType=T.StringType())
def make_review_id(user_id, parent_asin, timestamp):
    """user_id, parent_asin, timestamp를 결합해 고유한 UUID5 review_id를 생성합니다."""
    if not user_id or not parent_asin or timestamp is None:
        return None
    key = json.dumps([user_id, parent_asin, timestamp], ensure_ascii=False, separators=(",", ":"))
    return str(uuid.uuid5(NAMESPACE_URL, key))


def send_slack_alert(job_name, error_message, category=None, year_month=None, step=None):
    """Spark 작업/워커 실패 시 Slack Webhook으로 장애 알림을 전송합니다."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    try:
        import json
        import urllib.request

        err_str = str(error_message)
        if len(err_str) > 800:
            err_str = err_str[:800] + "\n... (생략)"

        fields = [
            {"title": "Spark Job", "value": f"`{job_name}`", "short": True},
            {"title": "실행 단계 (Step)", "value": f"`{step or 'all'}`", "short": True},
        ]
        if category:
            fields.append({"title": "카테고리", "value": f"`{category}`", "short": True})
        if year_month:
            fields.append({"title": "대상 년월", "value": f"`{year_month}`", "short": True})

        fields.append({"title": "오류/장애 상세 내용", "value": f"```{err_str}```", "short": False})

        payload = {
            "text": f"🚨 [Spark 장애 알림] {job_name} 작업 실패 감지",
            "attachments": [
                {
                    "color": "#E01E5A",
                    "title": f"🚨 Spark 작업/워커 장애 감지: {job_name}",
                    "fields": fields,
                    "mrkdwn_in": ["text", "fields"],
                }
            ],
        }

        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                print(f"[Slack 알림 전송 완료] Spark 장애 알림이 성공적으로 전송되었습니다.", flush=True)
            else:
                print(f"[Slack 알림 응답 경고] HTTP {response.status}", flush=True)
    except Exception as notify_err:
        print(f"[Slack 알림 전송 실패] 알림 전송 중 오류: {notify_err}", flush=True)


def write_atomic_file(target_path, content=""):
    """임시 파일에 작성 후 fsync를 거쳐 os.replace로 원자적으로 교체합니다."""
    dir_name = os.path.dirname(target_path)
    os.makedirs(dir_name, exist_ok=True)
    tmp_path = os.path.join(dir_name, f".{os.path.basename(target_path)}.tmp_{uuid.uuid4().hex[:6]}")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, target_path)


def get_manifest_data(dir_path):
    """파티션 디렉터리의 _manifest.json 내용을 읽어 반환합니다."""
    manifest_path = os.path.join(dir_path, "_manifest.json")
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def is_partition_valid(dir_path, expected_ym=None):
    """비재귀 단일 레벨 검증: 디렉터리, 직하위 .parquet 파일, _SUCCESS, _manifest.json 정밀 검증."""
    if not (os.path.exists(dir_path) and os.path.isdir(dir_path)):
        return False

    success_path = os.path.join(dir_path, "_SUCCESS")
    manifest_path = os.path.join(dir_path, "_manifest.json")
    if not (os.path.exists(success_path) and os.path.exists(manifest_path)):
        return False

    try:
        direct_files = os.listdir(dir_path)
        has_parquet = any(f.endswith(".parquet") and not f.startswith(".") for f in direct_files)
        if not has_parquet:
            return False

        manifest = get_manifest_data(dir_path)
        if not manifest:
            return False

        if expected_ym and manifest.get("year_month") != expected_ym:
            return False

        row_count = manifest.get("row_count")
        if not isinstance(row_count, int) or row_count < 0:
            return False

        if manifest.get("schema_version") != "1.0":
            return False

        if not manifest.get("run_id") or not manifest.get("created_at"):
            return False

        return True
    except Exception:
        return False


def is_all_partitions_valid(dir_path):
    """루트 완료 marker와 모든 월별 파티션의 산출물 무결성을 검증합니다."""
    if not (os.path.isdir(dir_path) and os.path.isfile(os.path.join(dir_path, "_SUCCESS"))):
        return False

    try:
        partition_dirs = sorted(
            d for d in os.listdir(dir_path)
            if d.startswith("review_year_month=")
            and os.path.isdir(os.path.join(dir_path, d))
        )
        if not partition_dirs:
            return False
        return all(
            is_partition_valid(os.path.join(dir_path, d), expected_ym=d.split("=", 1)[1])
            for d in partition_dirs
        )
    except OSError:
        return False


@contextlib.contextmanager
def file_lock(lock_path):
    """POSIX fcntl 기반 배타적 비차단 파일 락 (Docker bind-mount 지원, fd 종료 시 자동 해제)."""
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            raise BlockingIOError(f"해당 리소스({lock_path})에 대한 작업이 이미 다른 프로세스에서 실행 중입니다.")
        yield lock_fd
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


def cleanup_stale_staging(processed_dir, target_ym):
    """이전 실패로 남겨진 stale staging 디렉토리들을 정리합니다."""
    if not os.path.exists(processed_dir):
        return
    staging_prefix = f"._staging_review_year_month_{target_ym}_"
    for d in os.listdir(processed_dir):
        if d.startswith(staging_prefix):
            stale_path = os.path.join(processed_dir, d)
            print(f"[시작 정리] 이전 실패 잔여 staging 디렉토리 삭제: {stale_path}", flush=True)
            shutil.rmtree(stale_path, ignore_errors=True)


def cleanup_stale_all_staging(processed_dir):
    """전체 실행 실패로 남겨진 루트 staging 디렉터리를 정리합니다."""
    parent_dir = os.path.dirname(processed_dir)
    if not os.path.isdir(parent_dir or "."):
        return
    prefix = f"{os.path.basename(processed_dir)}._staging_all_"
    for entry in os.listdir(parent_dir or "."):
        if entry.startswith(prefix):
            stale_path = os.path.join(parent_dir, entry)
            print(f"[시작 정리] 이전 실패 전체 staging 디렉터리 삭제: {stale_path}", flush=True)
            shutil.rmtree(stale_path, ignore_errors=True)


def recover_all_state(processed_dir, run_id):
    """전체 결과의 불완전한 target을 유효한 backup으로 복구합니다."""
    parent_dir = os.path.dirname(processed_dir)
    base_name = os.path.basename(processed_dir)
    target_ok = is_all_partitions_valid(processed_dir)
    backup_entries = [
        os.path.join(parent_dir, entry)
        for entry in os.listdir(parent_dir or ".")
        if entry.startswith(f"{base_name}._backup_all_")
    ]
    valid_backups = [b for b in backup_entries if is_all_partitions_valid(b)]
    valid_backups.sort(key=lambda path: os.path.getmtime(path), reverse=True)

    if target_ok:
        for backup in backup_entries:
            shutil.rmtree(backup, ignore_errors=True)
        return True

    if valid_backups:
        best_backup = valid_backups[0]
        if os.path.exists(processed_dir):
            quarantine = os.path.join(
                parent_dir,
                f"{base_name}._quarantine_all_{run_id}",
            )
            os.rename(processed_dir, quarantine)
        os.rename(best_backup, processed_dir)
        if is_all_partitions_valid(processed_dir):
            for backup in backup_entries:
                if backup != best_backup:
                    shutil.rmtree(backup, ignore_errors=True)
            print(f"[Startup 복구 완료] 전체 결과를 백업에서 복구했습니다: {processed_dir}", flush=True)
            return True

    if os.path.exists(processed_dir) and not is_all_partitions_valid(processed_dir):
        quarantine = os.path.join(parent_dir, f"{base_name}._quarantine_all_{run_id}")
        os.rename(processed_dir, quarantine)
    return False


def recover_partition_state(processed_dir, target_ym, run_id):
    """비정상 종료로 남아있는 backup, quarantine 및 target 파티션 상태를 추론하여 자동 복구합니다."""
    target_partition = os.path.join(processed_dir, f"review_year_month={target_ym}")
    backup_prefix = f"._backup_review_year_month_{target_ym}_"
    all_backups = [
        os.path.join(processed_dir, d)
        for d in os.listdir(processed_dir)
        if d.startswith(backup_prefix) or d == f".backup_review_year_month_{target_ym}"
    ]

    target_ok = is_partition_valid(target_partition, expected_ym=target_ym)
    valid_backups = [b for b in all_backups if is_partition_valid(b, expected_ym=target_ym)]
    valid_backups.sort(key=lambda b: get_manifest_data(b).get("created_at", ""), reverse=True)

    if target_ok:
        for b in all_backups:
            print(f"[Startup 복구] 정상 target 확인됨. 잔여 백업 디렉토리 정리: {b}", flush=True)
            shutil.rmtree(b, ignore_errors=True)
        return True

    if valid_backups:
        best_backup = valid_backups[0]
        print(f"[Startup 복구] 최신 유효 백업({best_backup})을 target으로 복구합니다.", flush=True)
        if os.path.exists(target_partition):
            quarantine_path = os.path.join(processed_dir, f"._quarantine_review_year_month_{target_ym}_{run_id}")
            print(f"[Startup 복구] 불완전한 target을 quarantine({quarantine_path})으로 격리합니다.", flush=True)
            os.rename(target_partition, quarantine_path)

        os.rename(best_backup, target_partition)
        if is_partition_valid(target_partition, expected_ym=target_ym):
            print(f"[Startup 복구 완료] target 파티션이 백업으로부터 성공적으로 복구되었습니다.", flush=True)
            for b in all_backups:
                if b != best_backup and os.path.exists(b):
                    shutil.rmtree(b, ignore_errors=True)
            return True
        else:
            print(f"[Startup 복구 실패] 백업 복구 후 target 검증 실패.", flush=True)
            return False

    if os.path.exists(target_partition) and not target_ok:
        quarantine_path = os.path.join(processed_dir, f"._quarantine_review_year_month_{target_ym}_{run_id}")
        print(f"[Startup 복구] 불완전한 target 파티션을 quarantine({quarantine_path})으로 격리 후 신규 처리 대기합니다.", flush=True)
        os.rename(target_partition, quarantine_path)

    return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--step",
        choices=["all", "refine_store_parquet", "refine_parquet", "aggregate_monthly", "convert_raw_to_parquet"],
        default="all",
        help="실행할 파이프라인 단계: refine_store_parquet, aggregate_monthly, convert_raw_to_parquet, all",
    )
    parser.add_argument("--year-month", default=None,
                        help="처리할 대상 년월 (YYYY-MM). 지정 시 해당 월만 증분 추출/적재, 'all' 또는 미지정 시 전체 처리")
    parser.add_argument("--category", default=os.getenv("CATEGORY", None),
                        help="처리할 대상 카테고리/데이터셋 이름 (예: All_Beauty, Appliances 등)")
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/spark/data/raw"),
                        help="수집기가 적재한 원본 JSON/JSONL 디렉토리 또는 파일 경로")
    parser.add_argument("--raw-parquet-dir", default=os.getenv("RAW_PARQUET_DIR", "/opt/airflow/data/raw_parquet"),
                        help="월별 파티션 Raw Parquet 디렉토리 경로 (Full Scan 제거용)")
    parser.add_argument("--processed-dir", default=os.getenv("PROCESSED_DIR", "/opt/spark/data/processed/reviews"),
                        help="정제 완료 원본을 저장할 Parquet 경로")
    parser.add_argument("--summary-table", default="public.monthly_review_summary",
                        help="월별 집계를 저장할 PostgreSQL 테이블명")
    parser.add_argument("--partitions", type=int, default=16, help="Spark 처리 파티션 수")
    args = parser.parse_args()

    # 카테고리/데이터셋 이름 결정 (명시적 인자 -> raw_dir 파일명 추론 -> 기본값)
    category = args.category
    if not category:
        if os.path.isfile(args.raw_dir):
            category = os.path.splitext(os.path.basename(args.raw_dir))[0]
        elif os.path.isdir(args.raw_dir):
            jsonl_files = [
                f for f in os.listdir(args.raw_dir)
                if (f.endswith(".jsonl") or f.endswith(".json")) and not f.startswith(".")
            ]
            primary = [f for f in jsonl_files if not f.endswith("_duplicate.jsonl")]
            if primary:
                category = os.path.splitext(primary[0])[0]
            elif jsonl_files:
                category = os.path.splitext(jsonl_files[0])[0]
    if not category:
        category = "All_Beauty"

    # processed_dir 및 raw_parquet_dir 카테고리 네임스페이스 반영
    if args.processed_dir.endswith("/reviews"):
        args.processed_dir = os.path.join(os.path.dirname(args.processed_dir), category)
    elif not args.processed_dir.endswith(f"/{category}") and os.path.basename(args.processed_dir) != category:
        args.processed_dir = os.path.join(args.processed_dir, category)

    if not args.raw_parquet_dir.endswith(f"/{category}") and os.path.basename(args.raw_parquet_dir) != category:
        args.raw_parquet_dir = os.path.join(args.raw_parquet_dir, category)

    # raw_dir가 디렉토리이고 해당 카테고리 JSONL 파일이 존재하면 직접 지정
    if os.path.isdir(args.raw_dir):
        cat_file = os.path.join(args.raw_dir, f"{category}.jsonl")
        if os.path.exists(cat_file):
            args.raw_dir = cat_file

    database = dict(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )

    spark = None

    def get_spark():
        nonlocal spark
        if spark is None:
            spark = (
                SparkSession.builder
                .appName(f"amazon-reviews-pipeline-{args.step}")
                .config("spark.sql.shuffle.partitions", str(args.partitions))
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")
        return spark

    try:
        all_run_lock = None
        all_run_lock_acquired = False
        is_all_run = not args.year_month or args.year_month.lower() in ("all", "auto")
        if is_all_run and args.step in ("all", "refine_store_parquet", "refine_parquet"):
            os.makedirs(os.path.dirname(args.processed_dir) or ".", exist_ok=True)
            lock_dir = os.path.dirname(args.processed_dir) or "."
            lock_name = f".{os.path.basename(args.processed_dir)}.lock_all"
            all_run_lock = file_lock(os.path.join(lock_dir, lock_name))
            all_run_lock.__enter__()
            all_run_lock_acquired = True
        refined_df = None

        # 0. Raw Parquet 월별 1회 변환 단계 (Full Scan 원천 차단용 유틸리티)
        if args.step == "convert_raw_to_parquet":
            raw_parent_dir = os.path.dirname(args.raw_parquet_dir) or "."
            os.makedirs(raw_parent_dir, exist_ok=True)
            lock_path = os.path.join(raw_parent_dir, f".{os.path.basename(args.raw_parquet_dir)}.lock_convert_raw")
            with file_lock(lock_path):
                run_id = uuid.uuid4().hex[:8]
                cleanup_stale_all_staging(args.raw_parquet_dir)
                target_ready = recover_all_state(args.raw_parquet_dir, run_id)
                if target_ready or is_all_partitions_valid(args.raw_parquet_dir):
                    print("=" * 70, flush=True)
                    print(
                        f"[Raw Parquet 준비 완료] 루트 _SUCCESS 및 모든 월별 파티션 무결성 검증 완료: "
                        f"{args.raw_parquet_dir}\n"
                        f"Spark JSONL 로딩 및 변환을 생략하고 즉시 정상 종료합니다.",
                        flush=True,
                    )
                    print("=" * 70, flush=True)
                    return

                print("=" * 70, flush=True)
                print(
                    f"[Raw 변환 시작] Raw Parquet 부재 또는 불완전 상태 감지.\n"
                    f"원본 JSONL({args.raw_dir}) -> Raw Parquet({args.raw_parquet_dir}) 전체 변환을 시작합니다.",
                    flush=True,
                )
                print("=" * 70, flush=True)

                sp = get_spark()
                staging_raw = f"{args.raw_parquet_dir}._staging_all_{run_id}"
                backup_raw = f"{args.raw_parquet_dir}._backup_all_{run_id}"
                quarantine_raw = f"{args.raw_parquet_dir}._quarantine_all_{run_id}"

                try:
                    raw_df = sp.read.schema(RAW_REVIEW_SCHEMA).json(args.raw_dir)
                    raw_df = (
                        raw_df.filter(F.col("timestamp").isNotNull())
                        .withColumn(
                            "review_year_month",
                            F.date_format(F.to_timestamp(F.col("timestamp") / 1000), "yyyy-MM"),
                        )
                    )
                    (
                        raw_df
                        .repartition(args.partitions, "review_year_month")
                        .sortWithinPartitions("review_year_month")
                        .write
                        .mode("overwrite")
                        .option("partitionOverwriteMode", "dynamic")
                        .partitionBy("review_year_month")
                        .parquet(staging_raw)
                    )

                    created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    staged_df = sp.read.parquet(staging_raw)
                    month_counts = {
                        row["review_year_month"]: row["count"]
                        for row in staged_df.groupBy("review_year_month").count().collect()
                    }

                    for entry in sorted(os.listdir(staging_raw)):
                        if entry.startswith("review_year_month=") and os.path.isdir(os.path.join(staging_raw, entry)):
                            month_staging = os.path.join(staging_raw, entry)
                            ym = entry.split("=", 1)[1]
                            manifest = {
                                "year_month": ym,
                                "row_count": int(month_counts.get(ym, 0)),
                                "schema_version": "1.0",
                                "run_id": run_id,
                                "created_at": created_at_utc,
                            }
                            write_atomic_file(os.path.join(month_staging, "_manifest.json"), json.dumps(manifest, indent=2))
                            write_atomic_file(os.path.join(month_staging, "_SUCCESS"), "")

                    write_atomic_file(os.path.join(staging_raw, "_SUCCESS"), "")

                    if not is_all_partitions_valid(staging_raw):
                        raise RuntimeError("staging 전체 Raw Parquet 파티션 무결성 검증 실패")

                    has_backup = False
                    if os.path.exists(args.raw_parquet_dir):
                        os.rename(args.raw_parquet_dir, backup_raw)
                        has_backup = True

                    try:
                        os.rename(staging_raw, args.raw_parquet_dir)
                        if not is_all_partitions_valid(args.raw_parquet_dir):
                            raise RuntimeError("신규 전체 Raw Parquet 파티션 무결성 검증 실패")
                        if has_backup:
                            shutil.rmtree(backup_raw, ignore_errors=True)
                        print("[Raw 변환 완료] 전체 월별 Raw Parquet 파티션 staging 발행 및 무결성 검증 완료!", flush=True)
                    except Exception as swap_err:
                        print(f"[교체 실패] 전체 Raw Parquet 교체 실패: {swap_err}. 격리 및 백업 롤백을 수행합니다.", flush=True)
                        if os.path.exists(args.raw_parquet_dir):
                            os.rename(args.raw_parquet_dir, quarantine_raw)
                        if has_backup and os.path.exists(backup_raw):
                            os.rename(backup_raw, args.raw_parquet_dir)
                        raise
                finally:
                    if os.path.exists(staging_raw):
                        shutil.rmtree(staging_raw, ignore_errors=True)
                return

        # 1. 정제 및 Parquet 저장 단계
        spark = get_spark()
        if args.step in ("all", "refine_store_parquet", "refine_parquet"):
            skip_refine = False
            target_ym = args.year_month if (args.year_month and args.year_month.lower() not in ("all", "auto")) else None
            run_id = uuid.uuid4().hex[:8]

            if target_ym:
                os.makedirs(args.processed_dir, exist_ok=True)
                lock_path = os.path.join(args.processed_dir, f".lock_{target_ym}")
                with file_lock(lock_path):
                    # 시작 시 비정상 종료 상태 추론 복구 및 잔여 Staging 정리
                    cleanup_stale_staging(args.processed_dir, target_ym)
                    target_ready = recover_partition_state(args.processed_dir, target_ym, run_id)

                    target_partition = os.path.join(args.processed_dir, f"review_year_month={target_ym}")
                    if target_ready or is_partition_valid(target_partition, expected_ym=target_ym):
                        print(f"[정제 스킵] 대상 월({target_ym}) 유효한 파티션(_SUCCESS 및 manifest) 확인. 정제를 생략합니다.", flush=True)
                        skip_refine = True

                    if not skip_refine:
                        # Raw 소스 선택: 월별 Raw Parquet 존재 시 직접 지정 로딩 (Full Scan 제거)
                        month_raw_path = os.path.join(args.raw_parquet_dir, f"review_year_month={target_ym}")
                        if is_partition_valid(month_raw_path, expected_ym=target_ym):
                            print(f"[정제 단계] Raw Parquet 파티션 직접 로딩: {month_raw_path} (Full Scan 우회)", flush=True)
                            raw_df = spark.read.parquet(month_raw_path)
                        else:
                            print(
                                f"[정제 단계 경고] 대상 월({target_ym}) Raw Parquet 부재 또는 무결성 검증 미달({month_raw_path}) -> "
                                f"원본 JSONL({args.raw_dir}) 전체 스캔 폴백 로딩을 수행합니다.",
                                flush=True,
                            )
                            raw_df = spark.read.schema(RAW_REVIEW_SCHEMA).json(args.raw_dir)
                            # JSON 물리 스캔은 유지하되, UUID5 UDF와 중복 제거 셔플은 대상 월로 제한합니다.
                            raw_df = raw_df.filter(
                                F.col("timestamp").isNotNull() &
                                (
                                    F.date_format(
                                        F.to_timestamp(F.col("timestamp") / 1000),
                                        "yyyy-MM",
                                    ) == target_ym
                                )
                            )

                        print(f"[정제 단계] 필수값 필터링 및 review_id 기준 dropDuplicates 수행 (월: {target_ym})", flush=True)
                        valid_df = (
                            raw_df.filter(
                                F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != "") &
                                F.col("parent_asin").isNotNull() & (F.trim(F.col("parent_asin")) != "") &
                                F.col("timestamp").isNotNull() &
                                F.col("asin").isNotNull() & (F.trim(F.col("asin")) != "") &
                                F.col("text").isNotNull() & (F.trim(F.col("text")) != "") &
                                F.col("rating").isNotNull() & F.col("rating").between(1, 5)
                            )
                            .withColumn("review_id", make_review_id(F.col("user_id"), F.col("parent_asin"), F.col("timestamp")))
                            .dropDuplicates(["review_id"])
                        )

                        refined_df = (
                            valid_df.select(
                                F.col("review_id"),
                                F.trim(F.col("user_id")).alias("user_id"),
                                F.trim(F.col("parent_asin")).alias("parent_asin"),
                                F.trim(F.col("asin")).alias("product_id"),
                                F.col("timestamp"),
                                F.to_timestamp(F.col("timestamp") / 1000).alias("review_time"),
                                F.to_date(F.to_timestamp(F.col("timestamp") / 1000)).alias("review_date"),
                                F.date_format(F.to_timestamp(F.col("timestamp") / 1000), "yyyy-MM").alias("review_year_month"),
                                F.col("rating").cast("double").alias("rating"),
                                F.trim(F.col("title")).alias("review_title"),
                                F.trim(F.col("text")).alias("review_text"),
                                F.coalesce(F.col("verified_purchase"), F.lit(False)).alias("verified_purchase"),
                            )
                            .filter(F.col("review_year_month") == target_ym)
                        )

                        refined_df = refined_df.repartition(args.partitions)
                        refined_df.cache()
                        refined_count = refined_df.count()
                        print(f"[정제 단계] 대상 월({target_ym}) 정제 및 중복 제거 후 레코드 수: {refined_count:,}건", flush=True)

                        # Staging 디렉토리 저장
                        staging_root = os.path.join(args.processed_dir, f"._staging_review_year_month_{target_ym}_{run_id}")
                        staging_partition = os.path.join(staging_root, f"review_year_month={target_ym}")
                        backup_dir = os.path.join(args.processed_dir, f"._backup_review_year_month_{target_ym}_{run_id}")
                        quarantine_dir = os.path.join(args.processed_dir, f"._quarantine_review_year_month_{target_ym}_{run_id}")

                        try:
                            print(f"[정제 단계] Staging 경로({staging_root})에 partitionBy 저장 중...", flush=True)
                            (
                                refined_df.write
                                .mode("overwrite")
                                .option("partitionOverwriteMode", "dynamic")
                                .partitionBy("review_year_month")
                                .parquet(staging_root)
                            )

                            # Manifest 및 _SUCCESS 파일 원자적 생성
                            created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                            manifest = {
                                "year_month": target_ym,
                                "row_count": refined_count,
                                "schema_version": "1.0",
                                "run_id": run_id,
                                "created_at": created_at_utc,
                            }
                            write_atomic_file(os.path.join(staging_partition, "_manifest.json"), json.dumps(manifest, indent=2))
                            write_atomic_file(os.path.join(staging_partition, "_SUCCESS"), "")

                            # Two-phase 교체 및 롤백
                            has_backup = False
                            if os.path.exists(target_partition):
                                os.rename(target_partition, backup_dir)
                                has_backup = True

                            try:
                                os.rename(staging_partition, target_partition)
                                if not is_partition_valid(target_partition, expected_ym=target_ym):
                                    raise RuntimeError(f"새 파티션 무결성 검증 실패: {target_partition}")

                                if has_backup and os.path.exists(backup_dir):
                                    shutil.rmtree(backup_dir, ignore_errors=True)
                                print(f"[정제 단계 완료] 대상 월({target_ym}) 안전한 Staging 발행 및 검증 완료!", flush=True)
                            except Exception as swap_err:
                                print(f"[교체 실패] target 교체 실패: {swap_err}. 격리 및 롤백을 수행합니다.", flush=True)
                                if os.path.exists(target_partition):
                                    os.rename(target_partition, quarantine_dir)
                                if has_backup and os.path.exists(backup_dir):
                                    os.rename(backup_dir, target_partition)
                                    is_valid = is_partition_valid(target_partition, expected_ym=target_ym)
                                    if is_valid and os.path.exists(quarantine_dir):
                                        shutil.rmtree(quarantine_dir, ignore_errors=True)
                                raise swap_err
                        finally:
                            if os.path.exists(staging_root):
                                shutil.rmtree(staging_root, ignore_errors=True)
            else:
                # 전체(all) 실행 경로: 루트 단위 staging 발행 및 복구
                os.makedirs(os.path.dirname(args.processed_dir) or ".", exist_ok=True)
                cleanup_stale_all_staging(args.processed_dir)
                target_ready = recover_all_state(args.processed_dir, run_id)
                if target_ready or is_all_partitions_valid(args.processed_dir):
                    print("[정제 스킵] 루트 _SUCCESS 및 모든 월별 파티션 무결성 확인. 전체 정제를 생략합니다.", flush=True)
                    skip_refine = True

                if not skip_refine:
                    staging_dir = f"{args.processed_dir}._staging_all_{run_id}"
                    backup_dir = f"{args.processed_dir}._backup_all_{run_id}"
                    quarantine_dir = f"{args.processed_dir}._quarantine_all_{run_id}"
                    print("=" * 70, flush=True)
                    if is_all_partitions_valid(args.raw_parquet_dir):
                        print(f"[정제 단계] Raw Parquet({args.raw_parquet_dir}) 전체 파티션 직접 로딩 (Full Scan 우회)", flush=True)
                        raw_df = spark.read.parquet(args.raw_parquet_dir)
                    else:
                        print(
                            f"[정제 단계 경고] Raw Parquet 부재 또는 무결성 검증 미달({args.raw_parquet_dir}) -> "
                            f"원본 JSONL({args.raw_dir}) 전체 스캔 폴백 로딩 수행",
                            flush=True,
                        )
                        raw_df = spark.read.schema(RAW_REVIEW_SCHEMA).json(args.raw_dir)
                    valid_df = (
                        raw_df.filter(
                            F.col("user_id").isNotNull() & (F.trim(F.col("user_id")) != "") &
                            F.col("parent_asin").isNotNull() & (F.trim(F.col("parent_asin")) != "") &
                            F.col("timestamp").isNotNull() &
                            F.col("asin").isNotNull() & (F.trim(F.col("asin")) != "") &
                            F.col("text").isNotNull() & (F.trim(F.col("text")) != "") &
                            F.col("rating").isNotNull() & F.col("rating").between(1, 5)
                        )
                        .withColumn("review_id", make_review_id(F.col("user_id"), F.col("parent_asin"), F.col("timestamp")))
                        .dropDuplicates(["review_id"])
                    )

                    refined_df = (
                        valid_df.select(
                            F.col("review_id"),
                            F.trim(F.col("user_id")).alias("user_id"),
                            F.trim(F.col("parent_asin")).alias("parent_asin"),
                            F.trim(F.col("asin")).alias("product_id"),
                            F.col("timestamp"),
                            F.to_timestamp(F.col("timestamp") / 1000).alias("review_time"),
                            F.to_date(F.to_timestamp(F.col("timestamp") / 1000)).alias("review_date"),
                            F.date_format(F.to_timestamp(F.col("timestamp") / 1000), "yyyy-MM").alias("review_year_month"),
                            F.col("rating").cast("double").alias("rating"),
                            F.trim(F.col("title")).alias("review_title"),
                            F.trim(F.col("text")).alias("review_text"),
                            F.coalesce(F.col("verified_purchase"), F.lit(False)).alias("verified_purchase"),
                        )
                    )
                    refined_df = (
                        refined_df
                        .repartition(args.partitions, "review_year_month")
                        .sortWithinPartitions("review_year_month")
                    )
                    try:
                        print(f"[정제 단계] Staging 경로({staging_dir})에 월별 파티션 쓰기 시작 (버퍼 메모리 최소화 정렬 적용)", flush=True)
                        (
                            refined_df.write
                            .mode("overwrite")
                            .option("partitionOverwriteMode", "dynamic")
                            .partitionBy("review_year_month")
                            .parquet(staging_dir)
                        )

                        created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        staged_df = spark.read.parquet(staging_dir)
                        month_counts = {
                            row["review_year_month"]: row["count"]
                            for row in staged_df.groupBy("review_year_month").count().collect()
                        }
                        total_refined_count = sum(month_counts.values())
                        print(
                            f"[정제 단계] 전체 정제 및 중복 제거 완료. 총 레코드 수: {total_refined_count:,}건 "
                            f"({len(month_counts)}개 월 파티션 동시 발행 완료)",
                            flush=True,
                        )
                        for entry in os.listdir(staging_dir):
                            if entry.startswith("review_year_month="):
                                p_dir = os.path.join(staging_dir, entry)
                                ym = entry.split("=", 1)[1]
                                manifest = {
                                    "year_month": ym,
                                    "row_count": int(month_counts.get(ym, 0)),
                                    "schema_version": "1.0",
                                    "run_id": run_id,
                                    "created_at": created_at_utc,
                                }
                                write_atomic_file(os.path.join(p_dir, "_manifest.json"), json.dumps(manifest, indent=2))
                                write_atomic_file(os.path.join(p_dir, "_SUCCESS"), "")

                        write_atomic_file(os.path.join(staging_dir, "_SUCCESS"), "")
                        if not is_all_partitions_valid(staging_dir):
                            raise RuntimeError("staging 전체 파티션 무결성 검증 실패")

                        has_backup = False
                        if os.path.exists(args.processed_dir):
                            os.rename(args.processed_dir, backup_dir)
                            has_backup = True
                        try:
                            os.rename(staging_dir, args.processed_dir)
                            if not is_all_partitions_valid(args.processed_dir):
                                raise RuntimeError("신규 전체 파티션 무결성 검증 실패")
                            if has_backup:
                                shutil.rmtree(backup_dir, ignore_errors=True)
                            print("[정제 단계 완료] 전체 결과 staging 발행 및 무결성 검증 완료!", flush=True)
                            refined_df = None
                        except Exception as swap_err:
                            print(f"[교체 실패] 전체 결과 교체 실패: {swap_err}. 롤백을 수행합니다.", flush=True)
                            if os.path.exists(args.processed_dir):
                                os.rename(args.processed_dir, quarantine_dir)
                            if has_backup and os.path.exists(backup_dir):
                                os.rename(backup_dir, args.processed_dir)
                            raise
                    finally:
                        if os.path.exists(staging_dir):
                            shutil.rmtree(staging_dir, ignore_errors=True)
                else:
                    if args.step in ("refine_store_parquet", "refine_parquet"):
                        print("[정제 단계] Parquet 파티션이 이미 준비되어 있으므로 작업을 성공적으로 마칩니다.", flush=True)
                        return

        # 2. 월별 집계 및 DB 적재 단계
        if args.step in ("all", "aggregate_monthly"):
            if refined_df is None:
                print("=" * 70, flush=True)
                print(f"[집계 단계] {args.processed_dir} 정제 Parquet 데이터 로딩 중...", flush=True)
                if args.year_month and args.year_month.lower() not in ("all", "auto"):
                    print(f"[집계 단계] 대상 월({args.year_month}) 파티션 프루닝 로딩", flush=True)
                    refined_df = spark.read.parquet(args.processed_dir).filter(F.col("review_year_month") == args.year_month)
                else:
                    refined_df = spark.read.parquet(args.processed_dir)

            print("-" * 70, flush=True)
            print(f"[집계 단계] ({category}) review_year_month 기준 리뷰 수, 평균 평점, 구매인증 수 월별 집계 생성", flush=True)
            monthly_summary = (
                refined_df.groupBy("review_year_month")
                .agg(
                    F.count("*").alias("total_reviews"),
                    F.round(F.avg("rating"), 2).alias("avg_rating"),
                    F.sum(F.when(F.col("verified_purchase") == True, 1).otherwise(0)).alias("verified_reviews"),
                    F.countDistinct("product_id").alias("unique_products"),
                    F.countDistinct("user_id").alias("unique_users"),
                )
                .withColumn("category", F.lit(category))
                .withColumn("updated_at", F.current_timestamp())
                .orderBy("review_year_month")
            )

            monthly_count = monthly_summary.count()
            print(f"[집계 단계] 총 집계 월수: {monthly_count:,}개월", flush=True)

            if monthly_count == 0:
                print(f"[집계 단계 알림] 집계 대상 레코드가 0건이어서 DB 적재를 건너뜁니다.", flush=True)
            else:
                jdbc_url = f"jdbc:postgresql://{database['host']}:{database['port']}/{quote(database['dbname'], safe='')}"
                staging_table = f"{args.summary_table}_staging"
                print(f"[집계 단계] PostgreSQL 스테이징 테이블({staging_table})에 분산 적재 중...", flush=True)
                (
                    monthly_summary.write
                    .format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", staging_table)
                    .option("user", database["user"])
                    .option("password", database["password"])
                    .option("driver", "org.postgresql.Driver")
                    .mode("overwrite")
                    .save()
                )

                print(f"[집계 단계] PostgreSQL {args.summary_table} 대상 UPSERT(ON CONFLICT DO UPDATE) 병합 수행 중...", flush=True)
                try:
                    import psycopg2
                    conn = psycopg2.connect(
                        host=database["host"],
                        port=database["port"],
                        dbname=database["dbname"],
                        user=database["user"],
                        password=database["password"],
                    )
                    try:
                        with conn.cursor() as cur:
                            upsert_sql = f"""
                                CREATE TABLE IF NOT EXISTS {args.summary_table} (
                                    category VARCHAR(100) NOT NULL,
                                    review_year_month VARCHAR(7) NOT NULL,
                                    total_reviews BIGINT NOT NULL,
                                    avg_rating DOUBLE PRECISION NOT NULL,
                                    verified_reviews BIGINT NOT NULL,
                                    unique_products BIGINT NOT NULL,
                                    unique_users BIGINT NOT NULL,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    PRIMARY KEY (category, review_year_month)
                                );

                                DO $$
                                BEGIN
                                    IF NOT EXISTS (
                                        SELECT 1 FROM information_schema.columns
                                        WHERE table_name = split_part('{args.summary_table}', '.', CASE WHEN '{args.summary_table}' LIKE '%.%' THEN 2 ELSE 1 END)
                                          AND column_name = 'category'
                                    ) THEN
                                        ALTER TABLE {args.summary_table} ADD COLUMN IF NOT EXISTS category VARCHAR(100) DEFAULT '{category}' NOT NULL;
                                        ALTER TABLE {args.summary_table} DROP CONSTRAINT IF EXISTS monthly_review_summary_pkey;
                                        ALTER TABLE {args.summary_table} ADD PRIMARY KEY (category, review_year_month);
                                    END IF;
                                END $$;

                                CREATE INDEX IF NOT EXISTS monthly_review_summary_cat_ym_idx
                                    ON {args.summary_table}(category, review_year_month);

                                INSERT INTO {args.summary_table} (
                                    category, review_year_month, total_reviews, avg_rating,
                                    verified_reviews, unique_products, unique_users, updated_at
                                )
                                SELECT
                                    category, review_year_month, total_reviews, avg_rating,
                                    verified_reviews, unique_products, unique_users, updated_at
                                FROM {staging_table}
                                ON CONFLICT (category, review_year_month) DO UPDATE SET
                                    total_reviews = EXCLUDED.total_reviews,
                                    avg_rating = EXCLUDED.avg_rating,
                                    verified_reviews = EXCLUDED.verified_reviews,
                                    unique_products = EXCLUDED.unique_products,
                                    unique_users = EXCLUDED.unique_users,
                                    updated_at = EXCLUDED.updated_at;

                                DROP TABLE IF EXISTS {staging_table};
                            """
                            cur.execute(upsert_sql)
                        conn.commit()
                        print(
                            f"[집계 단계 완료] PostgreSQL {args.summary_table} UPSERT 적재 완료 "
                            f"(총 {monthly_count:,}행 멱등성 보장)",
                            flush=True,
                        )
                    finally:
                        conn.close()
                except ImportError:
                    print("[집계 단계 알림] psycopg2가 설치되지 않은 환경이라 스테이징 완료 후 직접 쿼리가 생략되었습니다.", flush=True)

                print("=" * 70, flush=True)
                print("집계 결과:")
                monthly_summary.orderBy(F.col("review_year_month").desc()).show(10, truncate=False)
                print("=" * 70, flush=True)

    except Exception as e:
        print(f"[Spark 작업 실패 감지] 오류 발생: {e}", flush=True)
        send_slack_alert(
            job_name=f"AmazonReview_RefineReviews_{category}",
            error_message=e,
            category=category,
            year_month=args.year_month,
            step=args.step,
        )
        raise e
    finally:
        if all_run_lock is not None and all_run_lock_acquired:
            all_run_lock.__exit__(None, None, None)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
