from pathlib import Path
import json,subprocess
query='''SELECT json_build_object(
'rows', (SELECT count(*) FROM public.raw_reviews),
'unique_keys', (SELECT count(DISTINCT (user_id,parent_asin,"timestamp")) FROM public.raw_reviews),
'pk', (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='public.raw_reviews'::regclass AND contype='p'),
'samples', (SELECT json_agg(row_to_json(r)) FROM (SELECT source_line,raw_payload FROM public.raw_reviews WHERE source_line IN (1,350000,701528) ORDER BY source_line) r)
)'''
cmd=['docker','exec','amazon-review-pipeline-postgres-1','sh','-c','exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "$1"','sh',query]
result=json.loads(subprocess.check_output(cmd,text=True))
state=json.loads(Path('states/All_Beauty.json').read_text())
assert result['rows']==result['unique_keys']==state['inserted_rows']==694252
assert state['completed'] and state['lines_committed']==701528 and state['skipped_rows']==7276
assert result['pk']=='PRIMARY KEY (user_id, parent_asin, "timestamp")'
samples={row['source_line']:row['raw_payload'] for row in result['samples']}
assert samples
with Path('data/All_Beauty.jsonl').open('rb') as source:
    for number,line in enumerate(source,1):
        if number in samples:
            assert line.decode('utf-8')==samples[number]
print('PASS: 701,528 input lines; 694,252 raw rows; 7,276 key conflicts skipped; PK verified; production payload samples match source exactly; checkpoint completed.')
Path('delete/raw_collection_validation/RESULTS.json').write_text(json.dumps({'input_lines':701528,'stored_rows':result['rows'],'key_conflicts_skipped':7276,'pk':result['pk'],'payload_sample_lines_checked':list(samples),'completed':True},ensure_ascii=False,indent=2)+'\n')
