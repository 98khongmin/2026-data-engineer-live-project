import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('collector', ROOT / 'jobs/amazon_reviews_etl.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)

class Store:
    target = {'database':'test','table':'raw_reviews'}
    def __init__(self):
        self.rows = {}
    def verify_last_key(self, key):
        if key is not None and tuple(key) not in self.rows:
            raise RuntimeError('missing checkpoint row')
    def write_batch(self, records, name):
        inserted = 0
        for r in records:
            if r[:3] not in self.rows:
                self.rows[r[:3]] = r[3]
                inserted += 1
        return inserted

class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / 'reviews.jsonl'
        self.lines = [
            b'{ "user_id": "u1", "parent_asin":"p1", "timestamp":1700000000001, "rating":null }\r\n',
            b'{"user_id":"u2","parent_asin":"p2","timestamp":1700000000002,"extra":{"keep":true}}\n',
            b'{"user_id":"u1","parent_asin":"p1","timestamp":1700000000001,"different":true}\n',
            '{"user_id":"u3","parent_asin":"p3","timestamp":1700000000003,"text":"한글"}'.encode(),
        ]
        self.source.write_bytes(b''.join(self.lines))
        self.states = self.root / 'states'
        self.store = Store()
    def tearDown(self):
        self.tmp.cleanup()
    def collect(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return c.collect(self.source, self.states, self.store, batch_size=2, **kwargs)
    def test_resume_and_verbatim_and_conflict(self):
        first = self.collect(max_batches=1)
        self.assertEqual(first['lines_committed'],2)
        self.assertFalse(first['completed'])
        final = self.collect()
        self.assertEqual((final['lines_committed'],final['inserted_rows'],final['skipped_rows']),(4,3,1))
        self.assertTrue(final['completed'])
        self.assertEqual(self.store.rows[('u1','p1',1700000000001)], self.lines[0].decode())
        self.assertEqual(self.store.rows[('u3','p3',1700000000003)], self.lines[3].decode())
        self.collect()
        self.assertEqual(len(self.store.rows),3)
    def test_crash_after_db_commit_before_state(self):
        with patch.object(c,'atomic_state',side_effect=OSError('simulated crash')):
            with self.assertRaises(OSError):
                self.collect()
        self.assertEqual(len(self.store.rows),2)
        self.assertFalse((self.states/'reviews.json').exists())
        final=self.collect()
        self.assertTrue(final['completed'])
        self.assertEqual(len(self.store.rows),3)
    def test_invalid_key_never_commits_partial_batch(self):
        self.source.write_bytes(self.lines[0]+b'{"user_id":null,"parent_asin":"p","timestamp":1}\n')
        with self.assertRaises(ValueError):
            self.collect()
        self.assertEqual(self.store.rows,{})
        self.assertFalse((self.states/'reviews.json').exists())
    def test_changed_source_is_rejected(self):
        self.collect(max_batches=1)
        self.source.write_bytes(self.source.read_bytes()+self.lines[0])
        with self.assertRaisesRegex(ValueError,'Source changed'):
            self.collect()
    def test_reset_rereads_without_duplicates(self):
        self.collect()
        state=self.collect(reset=True)
        self.assertEqual((state['inserted_rows'],state['skipped_rows']),(0,4))
        self.assertEqual(len(self.store.rows),3)
    def test_database_reset_is_detected(self):
        self.collect(max_batches=1)
        self.store.rows.clear()
        with self.assertRaises(RuntimeError):
            self.collect()
    def test_null_timestamp_is_rejected(self):
        for value in [None,True,1.1,'1000']:
            raw=json.dumps(dict(user_id='u',parent_asin='p',timestamp=value)).encode()
            with self.assertRaises(ValueError):
                c.parse_record(raw,1)

if __name__=='__main__':
    unittest.main()
