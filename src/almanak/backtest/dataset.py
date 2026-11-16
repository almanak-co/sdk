"""Typed prepared inputs and read-only timelines without venue or VM assumptions."""

import hashlib
import sqlite3
from bisect import bisect_right
from functools import cached_property
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from almanak.backtest.types import ConnectorId, Name, UnixTime
from almanak.models import Chain, Model
from almanak.triggers import PositiveSeconds

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DataKey(Model):
    connector: ConnectorId
    chain: Chain
    source: Name
    product: Name


class InputGap(Model):
    key: DataKey
    time: UnixTime
    reason: str


class InputUnavailable(ValueError):
    def __init__(self, key: DataKey, time: int, reason: str):
        self.gap = InputGap(key=key, time=UnixTime(time), reason=reason)
        super().__init__(f"{reason}: {key.model_dump_json()} at {time}")


class DataRequirement(Model):
    key: DataKey
    start: UnixTime
    end: UnixTime
    semantics: Literal["sampled", "changes"]
    sample_seconds: PositiveSeconds | None = None
    max_age_seconds: PositiveSeconds
    unit: Name
    visibility: Literal["end_of_block", "published", "retrospective"]
    payload_revision: Name

    @model_validator(mode="after")
    def window(self):
        if self.end < self.start:
            raise ValueError("data requirement has a reversed window")
        if (self.semantics == "sampled") != (self.sample_seconds is not None):
            raise ValueError("only sampled products require a sampling interval")
        return self


class InputEnvelope(Model):
    key: DataKey
    unit: Name
    source_time: UnixTime
    available_at: UnixTime
    retrieved_at: Annotated[float, Field(allow_inf_nan=False, ge=0)]
    period_end: UnixTime | None = None
    evidence: Annotated[tuple[Digest, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def visibility(self):
        if self.available_at < self.source_time:
            raise ValueError("an input cannot be available before its source time")
        if self.retrieved_at < self.available_at:
            raise ValueError("historical input was retrieved before it became available")
        if self.period_end is not None:
            if not self.source_time <= self.period_end <= self.available_at:
                raise ValueError("averaged input cannot be available before its period finishes")
        return self

    def require(self, requirement: DataRequirement):
        if self.key != requirement.key or self.unit != requirement.unit:
            raise ValueError("input source or units differ from the data requirement")
        if not requirement.start <= self.available_at <= requirement.end:
            raise ValueError("input lies outside the required window")
        if self.available_at - self.source_time > requirement.max_age_seconds:
            raise ValueError("input exceeds the permitted source observation age")


class InputRecord[Payload: Model, Reference: Model](InputEnvelope):
    reference: Reference
    payload: Payload


class StoredInput(InputEnvelope):
    """Envelope validation does not imply connector payload or revision support."""

    reference: dict[str, JsonValue]
    payload: dict[str, JsonValue]


class RawEvidence(Model):
    # Retain source response text unchanged, including its request/provenance envelope.
    body: str

    # A continuity proof can be megabytes and every sample names it, so the immutable
    # body is hashed once.
    @cached_property
    def digest(self) -> str:
        return hashlib.sha256(self.body.encode()).hexdigest()


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class Timeline[Record: InputEnvelope]:
    def __init__(self, rows: tuple[Record, ...], requirement: DataRequirement):
        times = tuple(int(row.available_at) for row in rows)
        if not rows or any(a >= b for a, b in pairwise(times)):
            raise ValueError("prepared timeline requires unique increasing observations")
        for record in rows:
            record.require(requirement)
        if times[0] != requirement.start:
            raise ValueError("prepared timeline lacks its initialization input")
        self.rows = rows
        self.times = times
        self.requirement = requirement

    def at(self, time: int) -> Record:
        if not self.requirement.start <= time <= self.requirement.end:
            raise InputUnavailable(
                self.requirement.key, time, "requested time is outside prepared coverage"
            )
        index = bisect_right(self.times, time) - 1
        record = self.rows[index]
        if time - record.source_time > self.requirement.max_age_seconds:
            raise InputUnavailable(
                self.requirement.key,
                time,
                "carried input exceeds the permitted source observation age",
            )
        return record

    def next(self, time: int) -> int | None:
        index = bisect_right(self.times, time)
        return self.times[index] if index < len(self.times) else None

    def expiry(self, time: int) -> int | None:
        index = bisect_right(self.times, time) - 1
        if index < 0:
            return None
        expiry = self.rows[index].source_time + self.requirement.max_age_seconds + 1
        following = self.next(time)
        if time < expiry <= self.requirement.end and (following is None or expiry < following):
            return expiry
        return None


class DatasetReader:
    def __init__(self, path: Path, expected_digest: str):
        if not path.is_file() or file_digest(path) != expected_digest:
            raise ValueError("prepared dataset is missing or its content changed")
        self.db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
        if self.db.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            self.db.close()
            raise ValueError("prepared dataset failed SQLite integrity validation")
        self.verified: set[str] = set()

    def timeline[Record: InputEnvelope](
        self, requirement: DataRequirement, record_type: type[Record]
    ) -> Timeline[Record]:
        records = tuple(
            (timestamp, record_type.model_validate_json(body))
            for timestamp, body in self.db.execute(
                "SELECT time, body FROM inputs WHERE source=? ORDER BY time",
                (requirement.key.model_dump_json(),),
            )
        )
        for timestamp, record in records:
            if record.key != requirement.key or record.available_at != timestamp:
                raise ValueError("prepared record differs from its indexed identity")
        return Timeline(tuple(r for _, r in records), requirement)

    def evidence(self, digest: str) -> RawEvidence:
        row = self.db.execute("SELECT body FROM evidence WHERE digest=?", (digest,)).fetchone()
        if row is None or (evidence := RawEvidence(body=row[0])).digest != digest:
            raise ValueError("prepared raw evidence is missing or corrupt")
        self.verified.add(digest)
        return evidence

    def verify(self, digest: str) -> None:
        """Require retained evidence, reading and hashing each body of this immutable
        dataset once however many records name it."""
        if digest not in self.verified:
            self.evidence(digest)

    def close(self):
        self.db.close()
