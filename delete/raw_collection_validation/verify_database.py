import json,subprocess
from pathlib import Path
query='SELECT coalesce(json_agg(row_to_json(r)),\'[]\'::json) FROM (SELECT user_id,parent_asin,"timestamp",raw_payload FROM public.raw_reviews_collection_check ORDER BY user_id) r'
cmd=['docker','exec','amazon-review-pipeline-postgres-1','sh','-c','exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "$1"','sh',query]
rows=json.loads(subprocess.check_output(cmd,text=True))
source=Path('data/_raw_collection_check.jsonl').read_bytes().splitlines(keepends=True)
expected={}
for line in source:
    row=json.loads(line)
    key=(row['user_id'],row['parent_asin'],row['timestamp'])
    expected.setdefault(key,line.decode())
assert len(rows)==len(expected)==3
for row in rows:
    key=(row['user_id'],row['parent_asin'],row['timestamp'])
    assert row['raw_payload']==expected[key]
state=json.loads(Path('states/_validation/_raw_collection_check.json').read_text())
assert state['completed'] and state['lines_committed']==4
print('PASS: actual DB composite-key deduplication; exact raw payload including whitespace, CRLF, Unicode and unknown fields; 4 input rows / 3 stored rows.')
