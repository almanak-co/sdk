"""Atomic acquisition ranges and publication of a completed immutable dataset."""

import os
import sqlite3
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import (
    DataKey,
    DataRequirement,
    DatasetReader,
    Digest,
    InputRecord,
    RawEvidence,
    StoredInput,
    file_digest,
)
from almanak.backtest.engine import sync_directory
from almanak.backtest.types import Name, UnixTime
from almanak.models import Model, UInt
from almanak.sandbox import readable_paths


class Preparation(Model):
    schema_version: Literal[1] = 1
    model: Name
    policy_digest: Digest | None = None
    acquisition_revision: Name
    requirements: Annotated[tuple[DataRequirement, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def distinct(self):
        if len({r.key for r in self.requirements}) != len(self.requirements):
            raise ValueError("prepared requirements must have distinct source/product identities")
        return self

    def requirement(self, key: DataKey) -> DataRequirement:
        for requirement in self.requirements:
            if requirement.key == key:
                return requirement
        raise ValueError("input has no declared data requirement")


class AcquiredRange(Model):
    key: DataKey
    start: UnixTime
    end: UnixTime
    evidence: Annotated[tuple[Digest, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def ordered(self):
        if self.end < self.start:
            raise ValueError("acquisition range is reversed")
        return self


class DatasetManifest(Model):
    schema_version: Literal[1] = 1
    status: Literal["complete"] = "complete"
    preparation: Preparation
    database_sha256: Digest
    database_bytes: UInt
    records: UInt
    evidence_records: UInt

    def open(self, directory: Path) -> DatasetReader:
        path = directory / "data.sqlite"
        if not path.is_file() or path.stat().st_size != self.database_bytes:
            raise ValueError("prepared database size differs from its manifest")
        reader = DatasetReader(path, self.database_sha256)
        try:
            row = reader.db.execute("SELECT body, sealed FROM preparation WHERE id=1").fetchone()
            if row != (self.preparation.model_dump_json(), 1):
                raise ValueError("prepared database differs from its completed manifest")
            actual = (
                reader.db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0],
                reader.db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
            )
            if actual != (self.records, self.evidence_records):
                raise ValueError("prepared record counts differ from the manifest")
            return reader
        except BaseException:
            reader.close()
            raise


def load_dataset(directory: Path, expected_manifest_sha256: str | None = None):
    require_private_directory(directory)
    path = directory / "dataset.json"
    if not path.is_file():
        raise ValueError("dataset is incomplete: completed manifest is missing")
    if expected_manifest_sha256 is not None and file_digest(path) != expected_manifest_sha256:
        raise ValueError("dataset manifest differs from the frozen run")
    manifest = DatasetManifest.model_validate_json(path.read_text())
    return manifest, manifest.open(directory)


# Files a fresh dataset directory may already hold: the coordinator lock and the
# resolution a public selection retains beside the dataset before provisioning.
PROVISIONAL = frozenset({"acquisition.lock", "resolution.json", "resolution.lock", "private"})


def acquisition_request(identity, resolution_facts=None) -> dict:
    """The dataset identity, bound to the resolved facts of a public selection when present."""
    request = {"identity": identity}
    if resolution_facts is not None:
        request["resolution_facts"] = resolution_facts
    return request


def require_private_directory(directory: Path):
    if any(directory.resolve().is_relative_to(path.resolve()) for path in readable_paths()):
        raise ValueError("prepared datasets must stay outside strategy-readable directories")


class DatasetWriter:
    def __init__(self, directory: Path, preparation: Preparation):
        require_private_directory(directory)
        self.directory = directory
        self.preparation = preparation
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / "data.sqlite"
        # Evidence this writer has read back and hashed. Records of one pass name the
        # same large evidence again and again; each body is verified once per commit
        # history, and again from the start when the dataset is published.
        self.verified: set[str] = set()
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.execute("PRAGMA synchronous=FULL")
        try:
            self.initialize()
        except BaseException:
            self.close()
            raise

    def initialize(self):
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS preparation "
                "(id INTEGER PRIMARY KEY, body TEXT NOT NULL, sealed INTEGER NOT NULL)"
            )
            self.db.execute(
                "INSERT OR IGNORE INTO preparation VALUES (1, ?, 0)",
                (self.preparation.model_dump_json(),),
            )
            row = self.db.execute("SELECT body FROM preparation WHERE id=1").fetchone()
            if row[0] != self.preparation.model_dump_json():
                raise ValueError("resume cannot change prepared sources, policy, window or model")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS inputs (source TEXT, time INTEGER, body TEXT NOT NULL, "
                "PRIMARY KEY(source, time))"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS ranges (source TEXT, start INTEGER, end INTEGER, "
                "body TEXT NOT NULL, PRIMARY KEY(source, start))"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS evidence (digest TEXT PRIMARY KEY, body TEXT NOT NULL)"
            )

    def ranges(self, key: DataKey) -> tuple[AcquiredRange, ...]:
        return self.indexed_ranges(key, "", ())

    def overlapping(self, span: AcquiredRange) -> tuple[AcquiredRange, ...]:
        # Filter on the indexed bounds so a commit parses only the ranges it can
        # conflict with; parsing every prior range made acquisition quadratic.
        return self.indexed_ranges(
            span.key, " AND start<=? AND end>=?", (int(span.end), int(span.start))
        )

    def indexed_ranges(self, key: DataKey, bounds: str, values: tuple[int, ...]):
        result = []
        for start, end, body in self.db.execute(
            f"SELECT start, end, body FROM ranges WHERE source=?{bounds} ORDER BY start",
            (key.model_dump_json(), *values),
        ):
            span = AcquiredRange.model_validate_json(body)
            if (span.key, span.start, span.end) != (key, start, end):
                raise ValueError("acquisition range differs from its indexed identity")
            result.append(span)
        return tuple(result)

    def missing(self, key: DataKey) -> tuple[tuple[int, int], ...]:
        requirement = self.preparation.requirement(key)
        start = int(requirement.start)
        missing = []
        for span in self.ranges(key):
            if span.start > start:
                missing.append((start, span.start - 1))
            start = span.end + 1
        if start <= requirement.end:
            missing.append((start, int(requirement.end)))
        return tuple(missing)

    def commit_range(
        self,
        span: AcquiredRange,
        records: tuple[InputRecord, ...],
        evidence: tuple[RawEvidence, ...],
    ):
        requirement = self.preparation.requirement(span.key)
        self.validate_batch(requirement, span, records)
        try:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                if self.db.execute("SELECT sealed FROM preparation WHERE id=1").fetchone()[0]:
                    raise ValueError("completed datasets are immutable")
                self.validate_overlap(span, records)
                for item in evidence:
                    self.retain_evidence(item)
                self.require_evidence(span.evidence)
                for record in records:
                    self.retain_record(record)
                self.db.execute(
                    "INSERT OR IGNORE INTO ranges VALUES (?, ?, ?, ?)",
                    (
                        span.key.model_dump_json(),
                        int(span.start),
                        int(span.end),
                        span.model_dump_json(),
                    ),
                )
        except BaseException:
            # A rolled-back commit may have verified evidence that is no longer stored.
            self.verified.clear()
            raise

    def validate_batch(self, requirement, span, records):
        if not requirement.start <= span.start <= span.end <= requirement.end:
            raise ValueError("acquisition range lies outside the required window")
        times = tuple(int(record.available_at) for record in records)
        if times != tuple(sorted(set(times))):
            raise ValueError("range inputs must be unique and ordered")
        for record in records:
            record.require(requirement)
            if not span.start <= record.available_at <= span.end:
                raise ValueError("record lies outside the committed acquisition range")
        if requirement.sample_seconds is not None:
            first = span.start + (requirement.start - span.start) % requirement.sample_seconds
            expected = tuple(range(first, span.end + 1, requirement.sample_seconds))
            if times != expected:
                raise ValueError("acquisition range is missing a required rate sample")
        if span.start == requirement.start and (not times or times[0] != requirement.start):
            raise ValueError("acquisition requires an initialization record")

    def validate_overlap(self, span, records):
        for prior in self.overlapping(span):
            if prior != span:
                raise ValueError("conflicting or overlapping acquisition range")
            if prior == span:
                self.validate_retry(span, records)

    def validate_retry(self, span, records):
        retained = tuple(
            row[0]
            for row in self.db.execute(
                "SELECT body FROM inputs WHERE source=? AND time BETWEEN ? AND ? ORDER BY time",
                (span.key.model_dump_json(), int(span.start), int(span.end)),
            )
        )
        if retained != tuple(record.model_dump_json() for record in records):
            raise ValueError("conflicting prepared input in a completed acquisition range")

    def retain_evidence(self, evidence: RawEvidence):
        if evidence.digest in self.verified:
            return
        self.db.execute(
            "INSERT OR IGNORE INTO evidence VALUES (?, ?)", (evidence.digest, evidence.body)
        )
        body = self.db.execute(
            "SELECT body FROM evidence WHERE digest=?", (evidence.digest,)
        ).fetchone()[0]
        if body != evidence.body:
            raise ValueError("conflicting retained raw evidence")
        self.verified.add(evidence.digest)

    def require_evidence(self, digests):
        for digest in digests:
            if digest in self.verified:
                continue
            row = self.db.execute("SELECT body FROM evidence WHERE digest=?", (digest,)).fetchone()
            if row is None or RawEvidence(body=row[0]).digest != digest:
                raise ValueError("acquisition evidence is missing or corrupt")
            self.verified.add(digest)

    def retain_record(self, record):
        self.require_evidence(record.evidence)
        key, body = record.key.model_dump_json(), record.model_dump_json()
        self.db.execute(
            "INSERT OR IGNORE INTO inputs VALUES (?, ?, ?)", (key, int(record.available_at), body)
        )
        retained = self.db.execute(
            "SELECT body FROM inputs WHERE source=? AND time=?", (key, int(record.available_at))
        ).fetchone()[0]
        if retained != body:
            raise ValueError("conflicting prepared input at the same source/time")

    def validate_complete(self):
        # Publication reads every retained body back once, whatever was verified before.
        self.verified.clear()
        for requirement in self.preparation.requirements:
            if self.missing(requirement.key):
                raise ValueError("dataset has missing acquisition ranges")
            for span in self.ranges(requirement.key):
                records = self.range_records(span)
                self.validate_batch(requirement, span, records)
                self.require_evidence(span.evidence)
                for record in records:
                    self.require_evidence(record.evidence)
            self.validate_freshness(requirement)

    def range_records(self, span):
        result = []
        for time, body in self.db.execute(
            "SELECT time, body FROM inputs WHERE source=? AND time BETWEEN ? AND ? ORDER BY time",
            (span.key.model_dump_json(), int(span.start), int(span.end)),
        ):
            record = StoredInput.model_validate_json(body)
            if record.available_at != time or record.key != span.key:
                raise ValueError("prepared input differs from its indexed identity")
            result.append(record)
        return tuple(result)

    def validate_freshness(self, requirement):
        rows = tuple(
            self.db.execute(
                "SELECT time, body FROM inputs WHERE source=? ORDER BY time",
                (requirement.key.model_dump_json(),),
            )
        )
        for i, (_, body) in enumerate(rows):
            record = StoredInput.model_validate_json(body)
            last_use = rows[i + 1][0] - 1 if i + 1 < len(rows) else requirement.end
            if last_use - record.source_time > requirement.max_age_seconds:
                raise ValueError("prepared coverage would carry an input beyond its permitted age")

    def publish(self) -> DatasetManifest:
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            self.validate_complete()
            self.db.execute("UPDATE preparation SET sealed=1 WHERE id=1 AND sealed=0")
        manifest = DatasetManifest(
            preparation=self.preparation,
            database_sha256=file_digest(self.path),
            database_bytes=self.path.stat().st_size,
            records=self.db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0],
            evidence_records=self.db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
        )
        publish_manifest(self.directory, manifest)
        return manifest

    def close(self):
        self.db.close()


def publish_manifest(directory: Path, manifest: DatasetManifest):
    path = directory / "dataset.json"
    body = manifest.model_dump_json(indent=2) + "\n"
    if path.exists():
        if path.read_text() != body:
            raise ValueError("completed dataset manifest cannot change")
        return
    temporary = directory / "dataset.json.tmp"
    with temporary.open("w") as output:
        output.write(body)
        output.flush()
        os.fsync(output.fileno())
    # Publication is exclusive. A completed manifest is never replaced on resume.
    os.link(temporary, path)
    temporary.unlink()
    sync_directory(directory)
