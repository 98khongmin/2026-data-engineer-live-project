#!/usr/bin/env python3
"""All_Beauty 원본 데이터를 기반으로 2배, 5배, 10배 부하 테스트용 데이터셋을 생성하는 스크립트.

원본(1배) 데이터는 전혀 수정하지 않고, 각 배수(2x, 5x, 10x) 데이터에 대해
1부터 시작하는 복제 회차 번호(k=1, 2, ...)를 user_id에 고유하게 부여하여
Spark 중복 제거(dropDuplicates)를 정상 통과하고 정확한 선형 확장을 보장합니다.
"""

from datetime import datetime
import json
from pathlib import Path
import sys
import time


def generate_scaled_dataset(source_path: Path, target_path: Path, multiplier: int):
    """원본 JSONL을 읽어 multiplier 배수 크기의 데이터셋을 생성합니다."""
    print(f"\n[{multiplier}x 데이터셋 생성 시작] 대상 파일: {target_path.name}")
    start_time = time.time()
    total_written = 0

    with target_path.open("w", encoding="utf-8") as out_f:
        # 1부터 multiplier까지 순차적으로 복제 회차 부여
        for k in range(1, multiplier + 1):
            batch_count = 0
            with source_path.open("r", encoding="utf-8") as in_f:
                for line in in_f:
                    line_str = line.strip()
                    if not line_str:
                        continue
                    row = json.loads(line_str)
                    original_user = row.get("user_id")
                    if original_user:
                        # 1부터 시작하는 고유 식별 번호 부여 (예: USERID_1, USERID_2...)
                        row["user_id"] = f"{original_user}_{k}"
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    batch_count += 1
            total_written += batch_count
            print(f"  -> 복제 회차 {k}/{multiplier} 완료 ({batch_count:,}건 누적: {total_written:,}건)")

    elapsed = time.time() - start_time
    file_size_mb = target_path.stat().st_size / (1024 * 1024)
    print(
        f"[{multiplier}x 완료] 총 {total_written:,}건 쓰기 완료 | "
        f"크기: {file_size_mb:.2f} MB | 소요 시간: {elapsed:.2f}초"
    )


def main():
    project_root = Path(__file__).resolve().parent.parent
    raw_dir = project_root / "data" / "raw"
    source_file = raw_dir / "All_Beauty.jsonl"

    if not source_file.exists():
        print(f"[오류] 원본 파일이 존재하지 않습니다: {source_file}", file=sys.stderr)
        sys.exit(1)

    with source_file.open("r", encoding="utf-8") as f:
        original_count = sum(1 for line in f if line.strip())

    print("=" * 70)
    print("All_Beauty 2x, 5x, 10x 부하 테스트용 데이터셋 생성 도구")
    print(f"시작 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"원본 파일: {source_file} (총 {original_count:,}건 - 수정 없음)")
    print("=" * 70)

    scales = [2, 5, 10]
    for mult in scales:
        target_file = raw_dir / f"{mult}x_All_Beauty.jsonl"
        generate_scaled_dataset(source_file, target_file, mult)

    print("\n" + "=" * 70)
    print("모든 배수(2x, 5x, 10x) 데이터셋 생성이 성공적으로 완료되었습니다!")
    print("=" * 70)


if __name__ == "__main__":
    main()

