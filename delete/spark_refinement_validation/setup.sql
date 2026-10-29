INSERT INTO public.raw_reviews (
 user_id,parent_asin,"timestamp",rating,title,text,images,asin,
 helpful_vote,verified_purchase,source_file,source_line
)
SELECT 'test_u' || i,'test_p' || i,1704067200000 + i,
 CASE WHEN i=7 THEN 6 ELSE 4 END,' title ',
 CASE WHEN i=8 THEN '   ' ELSE ' text ' END,
 '[]'::jsonb,'asin' || i,0,false,'test.jsonl',i
FROM generate_series(1,8) AS i;
