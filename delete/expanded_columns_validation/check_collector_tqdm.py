import io
import sys
class ProgressTerminal(io.StringIO):
    def isatty(self):
        return True
original_stderr = sys.stderr
progress_output = ProgressTerminal()
sys.stderr = progress_output
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import psycopg2

spec=importlib.util.spec_from_file_location('collect','/opt/spark/app/amazon_reviews_etl.py')
c=importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
real=psycopg2.connect(host='postgres',port=5432,dbname=os.getenv('POSTGRES_DB','reviews'),user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
with real.cursor() as cur:
    cur.execute('CREATE TEMP TABLE raw_reviews (LIKE public.raw_reviews INCLUDING ALL)')
real.commit()
class Connection:
    def cursor(self): return real.cursor()
    def commit(self): real.commit()
    def close(self): real.rollback()
proxy=Connection()
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder)
    source=root/'reviews.jsonl'
    states=root/'states'
    records=[
      dict(user_id='u1',parent_asin='p1',timestamp=1700000000001,rating=4.0,title='  title  ',text='한글\n본문\t',images=[{'url':'https://example.invalid/image','extra':None}],asin='a1',helpful_vote=0,verified_purchase=False),
      dict(user_id='u2',parent_asin='p2',timestamp=1700000000002,rating=None,title=None,text='',images=[],asin=None,helpful_vote=None,verified_purchase=None),
      dict(user_id='u1',parent_asin='p1',timestamp=1700000000001,text='changed'),
      dict(user_id='u3',parent_asin='p3',timestamp=1700000000003,text='last',images=None),
    ]
    source.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in records),encoding='utf-8')
    def run(*extra):
        argv=['collect','--input',str(source),'--states-dir',str(states),'--batch-size','2',*extra]
        with patch.object(c.psycopg2,'connect',return_value=proxy), patch.object(sys,'argv',argv),contextlib.redirect_stdout(io.StringIO()):
            c.main()
    run('--max-batches','1')
    assert json.loads((states/'reviews.json').read_text())['last_line']==2
    run()
    state=json.loads((states/'reviews.json').read_text())
    assert state['last_line']==4 and state['completed']
    run('--reset-state')
    with real.cursor() as cur:
        cur.execute('SELECT user_id,parent_asin,"timestamp",rating,title,text,images,asin,helpful_vote,verified_purchase FROM raw_reviews ORDER BY user_id')
        rows=cur.fetchall()
    assert len(rows)==3
    assert rows[0]==('u1','p1',1700000000001,4.0,'  title  ','한글\n본문\t',records[0]['images'],'a1',0,False)
    assert rows[1]==('u2','p2',1700000000002,None,None,'',[],None,None,None)
    assert rows[2][5]=='last' and rows[2][6] is None
    # Unknown source fields must not be silently lost.
    source.write_text(json.dumps(dict(user_id='new',parent_asin='p',timestamp=1,new_field=1))+'\n')
    try:
        run('--reset-state')
        raise AssertionError('expected unknown-column failure')
    except ValueError:
        pass
    with real.cursor() as cur:
        cur.execute('SELECT count(*) FROM raw_reviews')
        assert cur.fetchone()[0]==3
real.close()
print('PASS: actual main loop; pause/resume; duplicate skipping; all typed fields; Unicode/whitespace; JSON arrays; NULL/empty/0/False; unknown fields rejected. Production rows untouched.')

text = progress_output.getvalue()
sys.stderr = original_stderr
assert "\r" in text and "50%" in text and "100%" in text
assert "저장" in text and "중복" in text
print("PASS: tqdm updates in place; resumed progress starts at checkpoint; counters displayed.")
