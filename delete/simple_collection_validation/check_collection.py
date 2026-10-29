"""Run inside the collector image. Uses a separate temporary PostgreSQL schema."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import psycopg2
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('collect', '/opt/spark/app/amazon_reviews_etl.py')
c=importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
conn=psycopg2.connect(host='postgres',port=5432,dbname=os.getenv('POSTGRES_DB','reviews'),user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
# A temp table takes precedence over public.raw_reviews on this connection only.
with conn.cursor() as cur:
    cur.execute('CREATE TEMP TABLE raw_reviews (LIKE public.raw_reviews INCLUDING ALL)')
conn.commit()
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder)
    source=root/'input.jsonl'
    state=root/'input.json'
    lines=[
        '{ "user_id":"u1", "parent_asin":"p1", "timestamp":1,"extra":null }\r\n',
        '{"user_id":"u2","parent_asin":"p2","timestamp":2,"text":"한글"}\n',
        '{"user_id":"u1","parent_asin":"p1","timestamp":1,"changed":true}\n',
    ]
    source.write_bytes(''.join(lines).encode())
    def record(line, number):
        r=json.loads(line)
        return r['user_id'],r['parent_asin'],r['timestamp'],line,source.name,number
    assert c.load_state(state,source)==0
    c.save_batch(conn,[record(lines[0],1),record(lines[1],2)],state,source)
    assert c.load_state(state,source)==2
    c.save_batch(conn,[record(lines[2],3)],state,source)
    with conn.cursor() as cur:
        cur.execute('SELECT raw_payload FROM raw_reviews ORDER BY source_line')
        assert [r[0] for r in cur.fetchall()]==lines[:2]
    # Simulate a crash between commit and checkpoint, then retry the same row.
    fourth='{"user_id":"u4","parent_asin":"p4","timestamp":4}\n'
    with patch.object(c,'save_state',side_effect=OSError('simulated checkpoint failure')):
        try:
            c.save_batch(conn,[record(fourth,4)],state,source)
            raise AssertionError('expected checkpoint failure')
        except OSError:
            pass
    assert c.load_state(state,source)==3
    c.save_batch(conn,[record(fourth,4)],state,source)
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM raw_reviews')
        assert cur.fetchone()[0]==3
    # SQL failure must not advance the checkpoint or partially insert a batch.
    bad=[('u5','p5',5,'{}',source.name,5),(None,'p6',6,'{}',source.name,6)]
    try:
        c.save_batch(conn,bad,state,source)
        raise AssertionError('expected NOT NULL failure')
    except psycopg2.IntegrityError:
        conn.rollback()
    assert c.load_state(state,source)==4
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM raw_reviews')
        assert cur.fetchone()[0]==3
conn.close()
print('PASS: raw text preservation, checkpoint resume, key conflicts, crash replay, and failed-batch rollback. Production table unchanged.')
