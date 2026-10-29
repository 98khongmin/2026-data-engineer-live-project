"""외부 원본 JSONL을 읽어 데이터 레이크의 1단계(Raw Bronze)인 data/raw/ 에 적재하는 수집기 코드."""
import argparse
import json
import os
from pathlib import Path
import shutil



def load_state(state_file, input_file):
    """이전에 저장을 완료한 줄 번호를 읽습니다."""
    if not state_file.exists():
        return 0

    state = json.loads(state_file.read_text(encoding="utf-8"))
    if (state.get("source_file") != input_file.name
            or state.get("source_size") != input_file.stat().st_size):
        return 0
    return state.get("last_line", 0)


def save_state(state_file, input_file, last_line, completed=False):
    """임시 파일에 쓴 뒤 교체하여 상태 파일이 반쪽만 저장되는 것을 방지합니다."""
    state = {
        "source_file": input_file.name,
        "source_size": input_file.stat().st_size,
        "last_line": last_line,
        "completed": completed,
    }
    temporary = state_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(state_file)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=os.getenv("INPUT_PATH", "/opt/spark/data/All_Beauty.jsonl"))
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/spark/data/raw"))
    parser.add_argument("--states-dir", default=os.getenv("STATES_DIR", "/opt/spark/states"))
    parser.add_argument("--reset-state", action="store_true", help="처음부터 다시 수집")
    args = parser.parse_args()

    input_file = Path(args.input)
    if not input_file.exists():
        # 로컬 환경 상대경로 대응
        alt_input = Path("data") / input_file.name
        if alt_input.exists():
            input_file = alt_input
        else:
            raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {args.input}")

    raw_dir = Path(args.raw_dir)
    if not raw_dir.is_absolute() and not raw_dir.exists():
        raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    states_dir = Path(args.states_dir)
    if not states_dir.is_absolute() and not states_dir.exists():
        states_dir = Path("states")
    states_dir.mkdir(parents=True, exist_ok=True)

    target_file = raw_dir / input_file.name
    state_file = states_dir / f"collector_{input_file.stem}.json"

    # 이미 동일한 파일이 raw_dir에 완전하게 존재하고 크기가 일치하는 경우
    if target_file.exists() and not args.reset_state:
        if target_file.stat().st_size == input_file.stat().st_size and target_file.resolve() != input_file.resolve():
            with target_file.open(encoding="utf-8", newline="") as f:
                line_count = sum(1 for _ in f)
            print(f"[수집기] 이미 {target_file} 에 수집 완료되어 있습니다: 총 {line_count:,}행 ({target_file.stat().st_size:,} 바이트)", flush=True)
            save_state(state_file, input_file, line_count, completed=True)
            return

    # 입력 파일과 대상 파일이 동일 경로가 아닌 경우 복사/적재 진행
    if target_file.resolve() != input_file.resolve():
        print(f"[수집기] 원본 {input_file} -> {target_file} 적재 시작", flush=True)
        # 빠른 파일 복사 및 검증
        shutil.copy2(input_file, target_file)

    with target_file.open(encoding="utf-8", newline="") as f:
        line_count = sum(1 for _ in f)

    save_state(state_file, input_file, line_count, completed=True)
    print(f"[수집기] 원본 데이터 수집(Landing) 완료: {target_file.name}, 총 {line_count:,}행", flush=True)


if __name__ == "__main__":
    main()
