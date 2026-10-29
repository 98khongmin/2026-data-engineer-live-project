import importlib.util
import json
import os
import psycopg2
spec=importlib.util.spec_from_file_location('refine','/opt/spark/app/refine_reviews.py')
r=importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
conn=psycopg2.connect(host='postgres',port=5432,dbname=os.getenv('POSTGRES_DB','reviews'),user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'])
with conn.cursor() as cur:
    cur.execute('''SELECT (SELECT count(*) FROM raw_reviews),count(*),count(DISTINCT review_id),
                   count(*) FILTER (WHERE substr(review_id::text,15,1)<>'5') FROM amazon_reviews''')
    raw,refined,unique,not_v5=cur.fetchone()
    assert raw==694252 and refined==unique==693547 and not_v5==0
    cur.execute('''SELECT r.user_id,r.parent_asin,r."timestamp",r.asin,r.text,r.rating,r.title,r.verified_purchase,
                         a.review_id::text,a.user_id,a.parent_asin,a."timestamp",a.product_id,a.review_text,
                         a.rating,a.review_title,a.review_time,a.verified_purchase
                  FROM amazon_reviews a JOIN raw_reviews r USING (user_id,parent_asin,"timestamp") LIMIT 10''')
    samples=cur.fetchall()
    assert len(samples)==10
    for row in samples:
        assert r.transform(row[:8])==tuple(row[8:])
conn.close()
print(json.dumps(dict(raw_rows=raw,refined_rows=refined,unique_review_ids=unique,non_uuid5_ids=not_v5,matched_samples=len(samples))))
