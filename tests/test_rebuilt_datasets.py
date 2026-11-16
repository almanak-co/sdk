"""The rebuilt-dataset comparison admits only fewer retained reads, never changed records."""

import json
import sqlite3
from contextlib import closing

import pytest

from tests.rebuilt_datasets import assert_rebuilt_dataset

SOURCE = '{"source":"pool"}'
READS = {"a": '{"read":"a"}', "b": '{"read":"b"}', "c": '{"read":"c"}'}


def write(path, *, payload=1, record_reads=("a", "b"), range_reads=("a", "b", "c"), reads=READS):
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE preparation (id INTEGER PRIMARY KEY, body TEXT, sealed INTEGER)")
        db.execute("CREATE TABLE inputs (source TEXT, time INTEGER, body TEXT)")
        db.execute("CREATE TABLE ranges (source TEXT, start INTEGER, end INTEGER, body TEXT)")
        db.execute("CREATE TABLE evidence (digest TEXT PRIMARY KEY, body TEXT)")
        db.execute("INSERT INTO preparation VALUES (1, '{}', 1)")
        record = {"payload": payload, "evidence": list(record_reads)}
        db.execute("INSERT INTO inputs VALUES (?, 10, ?)", (SOURCE, json.dumps(record)))
        span = {"start": 10, "evidence": list(range_reads)}
        db.execute("INSERT INTO ranges VALUES (?, 10, 19, ?)", (SOURCE, json.dumps(span)))
        db.executemany("INSERT INTO evidence VALUES (?, ?)", reads.items())
        db.commit()


def manifest(evidence_records, records=1):
    return {
        "records": records,
        "database_sha256": "0" * 64,
        "database_bytes": 1,
        "evidence_records": evidence_records,
    }


@pytest.fixture
def retained(tmp_path):
    path = tmp_path / "source.sqlite"
    write(path)
    return path.read_bytes()


def test_rebuild_naming_fewer_retained_reads_is_equivalent(tmp_path, retained):
    write(
        tmp_path / "data.sqlite", record_reads=("a",), range_reads=("a",), reads={"a": READS["a"]}
    )
    assert_rebuilt_dataset(tmp_path, manifest(1), manifest(3), retained)


@pytest.mark.parametrize(
    "rebuilt, summary",
    [
        ({"payload": 2}, manifest(3)),
        ({"record_reads": ("a", "a")}, manifest(3)),
        ({"range_reads": ("a", "d"), "reads": {**READS, "d": "{}"}}, manifest(4)),
        ({"reads": {**READS, "c": '{"read":"changed"}'}}, manifest(3)),
        ({}, manifest(3, records=2)),
        ({}, manifest(2)),
    ],
    ids=["payload", "duplicate", "new-read", "changed-read", "manifest", "count"],
)
def test_rebuild_with_changed_content_is_refused(tmp_path, retained, rebuilt, summary):
    write(tmp_path / "data.sqlite", **rebuilt)
    with pytest.raises(AssertionError):
        assert_rebuilt_dataset(tmp_path, summary, manifest(3), retained)
