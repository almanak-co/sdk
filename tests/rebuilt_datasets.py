"""Compare a prepared dataset rebuilt today with one retained by earlier collection.

Two accepted collection changes alter which archival reads a prepared dataset keeps
as evidence, never what it records:

- d643f3820 (#107) keeps a collection pass's shared preflight reads with the first
  range it commits; datasets prepared before it repeated them on every range.
- 0bf62c6d5 (#115) carries unchanged V3-family ticks between samples, so a sample
  names only the reads it made at its block; earlier datasets read every tick at
  every sample. Its own measurement found identical sample payloads.

Either change gives the database file a different digest, size and evidence count,
so a dataset retained before them cannot match its rebuild byte for byte. The
rebuild must still record exactly what was retained: the same preparation, the same
records with the same payloads, times and block references, and the same ranges,
while every read it names is one the retained dataset already holds with an
identical body. Opening the rebuilt dataset re-verifies its payloads against that
evidence, so dropping a read cannot hide a changed value.
"""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

DATABASE_FIELDS = frozenset({"database_sha256", "database_bytes", "evidence_records"})


def tables(path: Path) -> dict:
    with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as db:
        return {
            "preparation": list(db.execute("SELECT id, body, sealed FROM preparation ORDER BY id")),
            "inputs": {
                (source, time): json.loads(body)
                for source, time, body in db.execute("SELECT source, time, body FROM inputs")
            },
            "ranges": {
                (source, start, end): json.loads(body)
                for source, start, end, body in db.execute(
                    "SELECT source, start, end, body FROM ranges"
                )
            },
            "evidence": dict(db.execute("SELECT digest, body FROM evidence")),
        }


def without(value: dict, fields: frozenset[str]) -> dict:
    return {k: v for k, v in value.items() if k not in fields}


def split_evidence(rows: dict) -> tuple[dict, dict]:
    bodies = {key: without(row, frozenset({"evidence"})) for key, row in rows.items()}
    return bodies, {key: row["evidence"] for key, row in rows.items()}


def assert_named_reads_retained(rebuilt: dict, retained: dict) -> None:
    """Each rebuilt row names a duplicate-free subset of its retained row's reads."""
    for key, digests in rebuilt.items():
        assert len(set(digests)) == len(digests), key
        assert set(digests) <= set(retained[key]), key


def assert_same_records(rebuilt: dict, retained: dict) -> None:
    assert rebuilt["preparation"] == retained["preparation"]
    for table in ("inputs", "ranges"):
        rebuilt_bodies, rebuilt_reads = split_evidence(rebuilt[table])
        retained_bodies, retained_reads = split_evidence(retained[table])
        assert rebuilt_bodies == retained_bodies
        assert_named_reads_retained(rebuilt_reads, retained_reads)
    # A digest names its body, so equal items mean the rebuild read nothing new.
    assert rebuilt["evidence"].items() <= retained["evidence"].items()


def assert_rebuilt_dataset(
    directory: Path, manifest: dict, retained_manifest: dict, retained_database: bytes
) -> None:
    """Compare the rebuilt dataset in `directory` with the retained manifest and database."""
    assert without(manifest, DATABASE_FIELDS) == without(retained_manifest, DATABASE_FIELDS)
    retained_path = directory / "retained-data.sqlite"
    retained_path.write_bytes(retained_database)
    rebuilt = tables(directory / "data.sqlite")
    assert_same_records(rebuilt, tables(retained_path))
    assert len(rebuilt["evidence"]) == manifest["evidence_records"]
