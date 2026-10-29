import importlib.util
from datetime import datetime,timezone
import os
import uuid
import psycopg2

spec=importlib.util.spec_from_file_location('refine','/opt/spark/app/refine_reviews.py')
r=importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
key=('user|1','parent|2',1704067200123)
uid=r.make_review_id(*key)
assert uuid.UUID(uid).version==5
assert uid==r.make_review_id(*key)
assert uid!=r.make_review_id(key[0],key[1],key[2]+1)
assert r.make_review_id('a|b','c',1)!=r.make_review_id('a','b|c',1)
row=(*key,' asin ','  본문  ',4.0,' title ',False)
clean=r.transform(row)
assert clean[:4]==(uid,*key)
assert clean[4:8]==('asin','본문',4.0,'title')
assert clean[8]==datetime(2024,1,1,0,0,0,123000,tzinfo=timezone.utc)
assert clean[9] is False
for bad in [(*key,'a',' ',4,None,True),(*key,'a','x',None,None,True),(*key,'a','x',6,None,True),(*key,None,'x',4,None,True),(*key,'a','x',float('nan'),None,True)]:
    assert r.transform(bad) is None
conn=psycopg2.connect(host='postgres',port=5432,dbname=os.getenv('POSTGRES_DB','reviews'),user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
with conn.cursor() as cur:
    cur.execute('CREATE TEMP TABLE amazon_reviews (LIKE public.amazon_reviews INCLUDING ALL)')
conn.commit()
assert r.save_batch(conn,[clean])==1
assert r.save_batch(conn,[clean])==0
with conn.cursor() as cur:
    cur.execute('SELECT review_id,user_id,parent_asin,"timestamp",product_id,review_text,rating,review_title,review_time,verified_purchase FROM amazon_reviews')
    saved=cur.fetchone()
assert str(saved[0])==uid
assert saved[1:8]==clean[1:8]
assert saved[8]==clean[8] and saved[9] is False
conn.close()
print('PASS: UUID5 stable identity and key boundaries; exact milliseconds; trim/filter rules; PostgreSQL UUID type; rerun skips duplicates. Production rows untouched.')
