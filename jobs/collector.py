"""외부 원본 JSONL(로컬 파일 또는 Hugging Face URL)을 읽어 데이터 레이크 1단계(Raw Bronze)인 data/raw/ 에 적재하는 수집기 코드."""
import argparse
import json
import os
from pathlib import Path
import shutil
import urllib.parse
import urllib.request

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

DEFAULT_HUGGINGFACE_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/All_Beauty.jsonl"
)


def load_state(state_file, file_name, file_size):
    """이전에 저장을 완료한 줄 번호를 읽습니다."""
    if not state_file.exists():
        return 0

    state = json.loads(state_file.read_text(encoding="utf-8"))
    if (state.get("source_file") != file_name
            or state.get("source_size") != file_size):
        return 0
    return state.get("last_line", 0)


def save_state(state_file, source_file_name, source_size, last_line, completed=False):
    """임시 파일에 쓴 뒤 교체하여 상태 파일이 반쪽만 저장되는 것을 방지합니다."""
    state = {
        "source_file": source_file_name,
        "source_size": source_size,
        "last_line": last_line,
        "completed": completed,
    }
    temporary = state_file.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(state_file)


def download_from_url(url: str, target_file: Path, chunk_size: int = 1024 * 1024):
    """URL에서 파일을 스트리밍 방식으로 다운로드하고 진행률을 표시합니다."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    hf_token = os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")
    if hf_token:
        headers["Authorization"] = f"Bearer {hf_token}"
    req = urllib.request.Request(url, headers=headers)
    temp_file = target_file.with_suffix(".download")

    print(f"[수집기] Hugging Face 원본 다운로드 시작: {url}", flush=True)
    try:
        with urllib.request.urlopen(req) as response:
            content_length = response.headers.get("Content-Length")
            total_size = int(content_length) if content_length and content_length.isdigit() else 0

            with temp_file.open("wb") as f:
                if tqdm and total_size > 0:
                    with tqdm(total=total_size, unit="B", unit_scale=True, unit_divisor=1024, desc=f"다운로드 {target_file.name}") as pbar:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)
                            pbar.update(len(chunk))
                else:
                    downloaded = 0
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"\r[수집기] 다운로드 중: {downloaded / (1024 * 1024):.1f}MB / {total_size / (1024 * 1024):.1f}MB ({percent:.1f}%)", end="", flush=True)
                    if total_size > 0:
                        print("", flush=True)

        temp_file.replace(target_file)
        print(f"[수집기] 다운로드 완료 및 임시 파일 교체 성공: {target_file}", flush=True)
    except Exception as e:
        if temp_file.exists():
            temp_file.unlink(missing_ok=True)
        print(f"[수집기 오류] 다운로드 실패: {e}", flush=True)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=os.getenv("INPUT_PATH", DEFAULT_HUGGINGFACE_URL),
        help="수집할 원본 파일의 URL(HTTP/HTTPS) 또는 로컬 파일 경로",
    )
    parser.add_argument("--raw-dir", default=os.getenv("RAW_DIR", "/opt/spark/data/raw"))
    parser.add_argument("--states-dir", default=os.getenv("STATES_DIR", "/opt/spark/states"))
    parser.add_argument("--reset-state", action="store_true", help="처음부터 다시 수집/다운로드")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        local_raw = Path("data/raw")
        if local_raw.exists() or not raw_dir.parent.exists():
            raw_dir = local_raw
    raw_dir.mkdir(parents=True, exist_ok=True)

    states_dir = Path(args.states_dir)
    if not states_dir.exists():
        local_states = Path("states")
        if local_states.exists() or not states_dir.parent.exists():
            states_dir = local_states
    states_dir.mkdir(parents=True, exist_ok=True)

    is_url = args.input.startswith(("http://", "https://"))
    if is_url:
        parsed_url = urllib.parse.urlparse(args.input)
        file_name = Path(parsed_url.path).name or "All_Beauty.jsonl"
        target_file = raw_dir / file_name
        input_file = None
    else:
        input_file = Path(args.input)
        if not input_file.exists():
            # 로컬 환경 및 상대경로 대응 (data/raw -> data 순으로 탐색)
            alt_raw = Path("data/raw") / input_file.name
            alt_data = Path("data") / input_file.name
            if alt_raw.exists():
                input_file = alt_raw
            elif alt_data.exists():
                input_file = alt_data
            else:
                raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {args.input}")
        target_file = raw_dir / input_file.name

    state_file = states_dir / f"collector_{target_file.stem}.json"

    # 이미 동일한 파일이 raw_dir에 존재하고 크기가 0보다 크며 reset_state가 아닌 경우 스킵
    if target_file.exists() and not args.reset_state and target_file.stat().st_size > 0:
        if is_url or (input_file.resolve() == target_file.resolve() or input_file.stat().st_size == target_file.stat().st_size):
            with target_file.open(encoding="utf-8", newline="") as f:
                line_count = sum(1 for _ in f)
            print(f"[수집기] 이미 {target_file} 에 파일이 존재하여 다운로드를 스킵합니다: 총 {line_count:,}행 ({target_file.stat().st_size:,} 바이트)", flush=True)
            save_state(state_file, target_file.name, target_file.stat().st_size, line_count, completed=True)
            return

    # 파일 획득 (URL 다운로드 또는 로컬 복사)
    if is_url:
        download_from_url(args.input, target_file)
    elif target_file.resolve() != input_file.resolve():
        print(f"[수집기] 원본 {input_file} -> {target_file} 적재 시작", flush=True)
        shutil.copy2(input_file, target_file)

    with target_file.open(encoding="utf-8", newline="") as f:
        line_count = sum(1 for _ in f)

    save_state(state_file, target_file.name, target_file.stat().st_size, line_count, completed=True)
    print(f"[수집기] 원본 데이터 수집(Landing) 완료: {target_file.name}, 총 {line_count:,}행", flush=True)


if __name__ == "__main__":
    main()
