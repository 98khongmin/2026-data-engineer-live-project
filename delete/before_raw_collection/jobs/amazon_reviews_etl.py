"""Amazon JSONL -> normalized SHA-256 identity -> staging JDBC -> PostgreSQL.

Run using spark-submit. No third-party Python dependencies are needed.
"""
import os
import uuid
from urllib.parse import quote, urlencode

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T


OUTPUT_COLUMNS = [
    'review_id', 'user_id', 'product_id', 'review_text', 'rating',
    'review_title', 'review_time', 'verified_purchase',
]
HASHABLE_COLUMNS = OUTPUT_COLUMNS[1:]
# Explicit schema avoids full-file inference and supports both Amazon formats.
INPUT_SCHEMA = T.StructType([
    T.StructField(name, T.StringType(), True) for name in [
        'user_id', 'reviewerID', 'asin', 'text', 'reviewText', 'title', 'summary',
        'rating', 'overall', 'timestamp', 'unixReviewTime',
        'verified_purchase', 'verified',
    ]
])


def parse_hash_columns(value):
    columns = [part.strip() for part in value.split(',')]
    if (not columns or len(columns) != len(set(columns))
            or any(c not in HASHABLE_COLUMNS for c in columns)):
        raise ValueError('HASH_COLUMNS must be unique normalized columns from: '
                         + ','.join(HASHABLE_COLUMNS))
    return columns


def clean_text(column):
    value = F.trim(F.col(column))
    # PostgreSQL TEXT rejects NUL characters; remove them before writing/hashing.
    value = F.regexp_replace(value, '\u0000', '')
    return F.when(F.length(F.trim(value)) > 0, F.trim(value))


def transform(raw, hash_columns):
    # 2023 timestamp is milliseconds; legacy unixReviewTime is seconds.
    epoch_seconds = F.coalesce(
        F.col('timestamp').cast('double') / F.lit(1000.0),
        F.col('unixReviewTime').cast('double'),
    )
    normalized = raw.select(
        F.coalesce(clean_text('user_id'), clean_text('reviewerID')).alias('user_id'),
        clean_text('asin').alias('product_id'),
        F.coalesce(clean_text('text'), clean_text('reviewText')).alias('review_text'),
        F.coalesce(F.col('rating').cast('double'), F.col('overall').cast('double')).alias('rating'),
        F.coalesce(clean_text('title'), clean_text('summary')).alias('review_title'),
        F.timestamp_seconds(epoch_seconds).alias('review_time'),
        F.coalesce(F.col('verified_purchase').cast('boolean'),
                   F.col('verified').cast('boolean')).alias('verified_purchase'),
    )
    # Null removal concerns required fields and chosen hash fields only.
    required = list(dict.fromkeys(['user_id', 'product_id', 'review_text', 'rating'] + hash_columns))
    valid = normalized.na.drop(subset=required).filter(F.col('rating').between(1, 5))
    identity = F.to_json(F.struct(*[F.col(c) for c in hash_columns]),
                         options={'ignoreNullFields': 'false'})
    hashed = valid.withColumn('review_id', F.sha2(identity, 256))
    # Avoid ambiguous concat delimiters and arbitrary dropDuplicates winners.
    # Prefer latest review_time, then lexicographically greatest full row JSON.
    ordering = Window.partitionBy('review_id').orderBy(
        F.col('review_time').desc_nulls_last(),
        F.to_json(F.struct(*[F.col(c) for c in OUTPUT_COLUMNS])).desc(),
    )
    return (hashed.withColumn('_rank', F.row_number().over(ordering))
            .filter(F.col('_rank') == 1).select(*OUTPUT_COLUMNS))


def positive_int(name, default):
    value = int(os.getenv(name, default))
    if value < 1:
        raise ValueError(f'{name} must be positive')
    return value


def open_connection(spark, url, user, password):
    jvm = spark.sparkContext._jvm
    # Instantiate directly instead of relying on DriverManager classloader lookup.
    props = jvm.java.util.Properties()
    props.setProperty('user', user)
    props.setProperty('password', password)
    return jvm.org.postgresql.Driver().connect(url, props)


def load_postgres(spark, cleaned, partitions, batch_size):
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = positive_int('POSTGRES_PORT', '5432')
    database = quote(os.getenv('POSTGRES_DB', 'reviews'), safe='')
    query = urlencode({'options': '-c TimeZone=UTC', 'connectTimeout': '15'})
    url = f'jdbc:postgresql://{host}:{port}/{database}?{query}'
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    # Each attempt gets its own staging table. It has no unique constraint so a
    # retried Spark partition may append again without failing on duplicate keys.
    stage = 'pipeline.stage_' + uuid.uuid4().hex
    columns = ', '.join(OUTPUT_COLUMNS)
    conn = open_connection(spark, url, user, password)
    statement = conn.createStatement()
    try:
        statement.executeUpdate(
            f'CREATE TABLE {stage} AS SELECT {columns} '
            'FROM public.amazon_reviews WITH NO DATA'
        )
        print(f'Staging table: {stage}', flush=True)
        (cleaned.coalesce(partitions).write.format('jdbc')
         .option('url', url).option('dbtable', stage)
         .option('user', user).option('password', password)
         .option('driver', 'org.postgresql.Driver')
         .option('numPartitions', partitions).option('batchsize', batch_size)
         .option('isolationLevel', 'READ_COMMITTED')
         .mode('append').save())
        # Merge and staging cleanup are one DB transaction. Existing IDs win:
        # this starter intentionally does not update already-loaded reviews.
        conn.setAutoCommit(False)
        statement.executeUpdate(
            f'INSERT INTO public.amazon_reviews ({columns}) '
            f'SELECT DISTINCT ON (review_id) {columns} FROM {stage} '
            'ORDER BY review_id '
            'ON CONFLICT (review_id) DO NOTHING'
        )
        statement.executeUpdate(f'DROP TABLE {stage}')
        conn.commit()
    except Exception:
        if not conn.getAutoCommit():
            conn.rollback()
        print(f'Load failed. Inspect/remove staging table if present: {stage}', flush=True)
        raise
    finally:
        statement.close()
        conn.close()


def main():
    columns = parse_hash_columns(os.getenv('HASH_COLUMNS', 'user_id,product_id,review_text'))
    partitions = positive_int('JDBC_PARTITIONS', '2')
    batch_size = positive_int('JDBC_BATCH_SIZE', '1000')
    spark = SparkSession.builder.appName('amazon-reviews-etl').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    cleaned = None
    try:
        raw = (spark.read.schema(INPUT_SCHEMA).option('mode', 'FAILFAST')
               .json(os.getenv('INPUT_PATH', '/opt/spark/data/All_Beauty.jsonl')))
        cleaned = transform(raw, columns).persist(StorageLevel.MEMORY_AND_DISK)
        print(f'Valid distinct reviews: {cleaned.count()}', flush=True)
        load_postgres(spark, cleaned, partitions, batch_size)
        print('Load committed successfully.', flush=True)
    finally:
        if cleaned is not None:
            cleaned.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
