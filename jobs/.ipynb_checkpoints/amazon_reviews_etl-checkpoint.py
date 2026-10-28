"""Collect JSONL verbatim into PostgreSQL; resume via atomic JSON state files.

The historical filename is retained for ./submit-job.sh compatibility.
Spark is used only to obtain its already-installed PostgreSQL JDBC connection.
No Spark normalization, filtering, hashing of reviews, or timestamp conversion.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import quote, urlencode


DDL = """CREATE TABLE IF NOT EXISTS public.{table} (
    user_id TEXT NOT NULL,
    parent_asin TEXT NOT NULL,
    "timestamp" BIGINT NOT NULL,
    raw_payload TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_line BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, parent_asin, "timestamp")
)"""


def source_info(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        before = os.fstat(source.fileno())
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
        after = os.fstat(source.fileno())
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError('Input changed while fingerprinting; use a stable source file.')
    return {'name': path.name, 'size_bytes': after.st_size,
            'mtime_ns': after.st_mtime_ns, 'sha256': digest.hexdigest()}


def atomic_state(path, state):
    # fsync + rename means a crash cannot leave a partially-written checkpoint.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                         dir=path.parent, prefix='.' + path.name + '.',
                                         suffix='.tmp', delete=False) as output:
            temporary = Path(output.name)
            json.dump(state, output, ensure_ascii=False, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def initial_state(source, target):
    return {'version': 1, 'source': source, 'target': target,
            'next_byte': 0, 'lines_committed': 0,
            'inserted_rows': 0, 'skipped_rows': 0,
            'completed': False, 'last_key': None}


def load_state(path, source, target, reset=False):
    if reset or not path.exists():
        return initial_state(source, target)
    state = json.loads(path.read_text(encoding='utf-8'))
    if state.get('version') != 1 or state.get('source') != source:
        raise ValueError('Source changed or state format differs. Use --reset-state to reread from the start.')
    if state.get('target') != target:
        raise ValueError('State belongs to another database/table. Use a separate states directory or --reset-state.')
    for key in ['next_byte', 'lines_committed', 'inserted_rows', 'skipped_rows']:
        if type(state.get(key)) is not int or state[key] < 0:
            raise ValueError('Invalid checkpoint field: ' + key)
    if state['next_byte'] > source['size_bytes']:
        raise ValueError('Checkpoint offset exceeds source file size.')
    if state['inserted_rows'] + state['skipped_rows'] != state['lines_committed']:
        raise ValueError('Invalid checkpoint counters.')
    if state.get('completed') and state['next_byte'] != source['size_bytes']:
        raise ValueError('Invalid completed checkpoint.')
    return state


def parse_record(raw, line_number):
    # Store the original decoded line, including whitespace and line ending.
    # json.loads is only used to extract the three keys, not to rewrite payload.
    payload = raw.decode('utf-8')
    try:
        record = json.loads(payload)
    except (ValueError, TypeError) as error:
        raise ValueError(f'Line {line_number}: invalid JSON') from error
    if not isinstance(record, dict):
        raise ValueError(f'Line {line_number}: expected a JSON object')
    for key in ['user_id', 'parent_asin']:
        if not isinstance(record.get(key), str) or record[key] == '':
            raise ValueError(f'Line {line_number}: {key} must be a nonempty string')
    timestamp = record.get('timestamp')
    if type(timestamp) is not int or not -(2**63) <= timestamp < 2**63:
        raise ValueError(f'Line {line_number}: timestamp must be a non-null BIGINT integer')
    if '\x00' in payload:
        raise ValueError(f'Line {line_number}: PostgreSQL TEXT cannot preserve a literal NUL byte')
    return record['user_id'], record['parent_asin'], timestamp, payload, line_number


class JdbcStore:
    def __init__(self, spark, host, port, database, user, password, table):
        if not re.fullmatch(r'[a-z_][a-z0-9_]*', table):
            raise ValueError('Invalid destination table name')
        self.table = table
        self.target = {'host': host, 'port': port, 'database': database, 'table': table}
        jvm = spark.sparkContext._jvm
        props = jvm.java.util.Properties()
        props.setProperty('user', user)
        props.setProperty('password', password)
        query = urlencode({'connectTimeout': '15', 'options': '-c TimeZone=UTC'})
        url = f'jdbc:postgresql://{host}:{port}/{quote(database, safe="")}?{query}'
        self.conn = jvm.org.postgresql.Driver().connect(url, props)
        statement = self.conn.createStatement()
        try:
            statement.executeUpdate(DDL.format(table=table))
        finally:
            statement.close()
        self.conn.setAutoCommit(False)

    def verify_last_key(self, key):
        if key is None:
            return
        statement = self.conn.prepareStatement(
            f'SELECT 1 FROM public.{self.table} '
            'WHERE user_id=? AND parent_asin=? AND "timestamp"=?')
        try:
            statement.setString(1, key[0])
            statement.setString(2, key[1])
            statement.setLong(3, key[2])
            result = statement.executeQuery()
            try:
                if not result.next():
                    raise RuntimeError('Checkpoint row is missing in DB. If DB was reset, use --reset-state.')
            finally:
                result.close()
        finally:
            statement.close()
            self.conn.rollback()

    def write_batch(self, records, source_name):
        statement = self.conn.prepareStatement(
            f'INSERT INTO public.{self.table} '
            '(user_id, parent_asin, "timestamp", raw_payload, source_file, source_line) '
            'VALUES (?, ?, ?, ?, ?, ?) '
            'ON CONFLICT (user_id, parent_asin, "timestamp") DO NOTHING')
        try:
            for user_id, parent_asin, timestamp, payload, line_number in records:
                statement.setString(1, user_id)
                statement.setString(2, parent_asin)
                statement.setLong(3, timestamp)
                statement.setString(4, payload)
                statement.setString(5, source_name)
                statement.setLong(6, line_number)
                statement.addBatch()
            counts = list(statement.executeBatch())
            if any(count not in (0, 1) for count in counts):
                raise RuntimeError('Unexpected JDBC batch update counts; refusing to advance checkpoint.')
            self.conn.commit()
            return sum(counts)
        except Exception:
            self.conn.rollback()
            raise
        finally:
            statement.close()

    def close(self):
        self.conn.close()


def collect(path, states_dir, store, batch_size=1000, max_batches=None, reset=False):
    path = Path(path).resolve(strict=True)
    states_dir = Path(states_dir)
    states_dir.mkdir(parents=True, exist_ok=True)
    state_path = states_dir / (path.stem + '.json')
    # A persistent lock file prevents concurrent checkpoint writers. Kernel
    # releases the advisory lock even if the process is terminated unexpectedly.
    with (states_dir / (path.stem + '.lock')).open('a') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError('Another collector is already using this state file.') from error
        info = source_info(path)
        state = load_state(state_path, info, store.target, reset=reset)
        store.verify_last_key(state['last_key'])
        print(f'Source: {path.name}; resume line: {state["lines_committed"] + 1}; state: {state_path}', flush=True)
        batches = 0
        with path.open('rb') as source:
            initial_stat = os.fstat(source.fileno())
            if (initial_stat.st_size, initial_stat.st_mtime_ns) != (info['size_bytes'], info['mtime_ns']):
                raise RuntimeError('Source changed before collection started.')
            source.seek(state['next_byte'])
            while True:
                batch = []
                for _ in range(batch_size):
                    raw = source.readline()
                    if not raw:
                        break
                    batch.append(parse_record(raw, state['lines_committed'] + len(batch) + 1))
                current_stat = os.fstat(source.fileno())
                if (current_stat.st_size, current_stat.st_mtime_ns) != (initial_stat.st_size, initial_stat.st_mtime_ns):
                    raise RuntimeError('Source changed during collection; checkpoint was not advanced.')
                if not batch:
                    state['completed'] = True
                    atomic_state(state_path, state)
                    break
                inserted = store.write_batch(batch, path.name)
                # Advance only AFTER the DB commit. A crash between commit and
                # state rename causes replay; the composite PK safely skips it.
                state = dict(state, next_byte=source.tell(),
                             lines_committed=state['lines_committed'] + len(batch),
                             inserted_rows=state['inserted_rows'] + inserted,
                             skipped_rows=state['skipped_rows'] + len(batch) - inserted,
                             last_key=list(batch[-1][:3]),
                             completed=source.tell() == info['size_bytes'])
                atomic_state(state_path, state)
                batches += 1
                print(f'Committed lines: {state["lines_committed"]:,}; '
                      f'inserted this batch: {inserted}; skipped: {len(batch) - inserted}', flush=True)
                if state['completed'] or (max_batches is not None and batches >= max_batches):
                    break
        print(f'Collection {"completed" if state["completed"] else "paused"}: '
              f'{state["lines_committed"]:,} lines; state saved.', flush=True)
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', default=os.getenv('INPUT_PATH', '/opt/spark/data/All_Beauty.jsonl'))
    parser.add_argument('--states-dir', default=os.getenv('STATES_DIR', '/opt/spark/states'))
    parser.add_argument('--batch-size', type=int, default=int(os.getenv('COLLECT_BATCH_SIZE', '1000')))
    parser.add_argument('--max-batches', type=int, help='Pause after this many committed batches; useful for resume demos')
    parser.add_argument('--reset-state', action='store_true', help='Reread from start; does not delete DB records')
    parser.add_argument('--table', default='raw_reviews', help='Destination table; default: raw_reviews')
    args = parser.parse_args()
    if args.batch_size < 1 or (args.max_batches is not None and args.max_batches < 1):
        parser.error('Batch size and max batches must be positive')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('amazon-reviews-raw-collection').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    store = None
    try:
        store = JdbcStore(spark, os.getenv('POSTGRES_HOST', 'postgres'),
                          int(os.getenv('POSTGRES_PORT', '5432')),
                          os.getenv('POSTGRES_DB', 'reviews'),
                          os.environ['POSTGRES_USER'], os.environ['POSTGRES_PASSWORD'], args.table)
        collect(args.input, args.states_dir, store, args.batch_size, args.max_batches, args.reset_state)
    finally:
        if store is not None:
            store.close()
        spark.stop()


if __name__ == '__main__':
    main()
