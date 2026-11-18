"""Prepared-data integrity, recovery and visibility without EVM or lending payloads."""

import json
import sqlite3

import pytest

from almanak.backtest.dataset import (
    DataKey,
    DataRequirement,
    InputRecord,
    RawEvidence,
    Timeline,
    file_digest,
)
from almanak.backtest.prepared import (
    AcquiredRange,
    DatasetManifest,
    DatasetWriter,
    Preparation,
    load_dataset,
)
from almanak.backtest.types import ConnectorId, UnixTime
from almanak.caip import ChainId
from almanak.models import Chain, Model


class Quote(Model):
    quantity: int


class Slot(Model):
    height: int


Record = InputRecord[Quote, Slot]
KEY = DataKey(
    connector=ConnectorId("swap_example"),
    chain=Chain(chain_id=ChainId("cosmos:cosmoshub-4"), environment="test"),
    source="declared_pool",
    product="quote",
)
EVIDENCE = RawEvidence(body='{"request":{"source":"declared_pool"},"result":{"quantity":3}}')


def requirement(**changes):
    return DataRequirement.model_validate(
        dict(
            key=KEY,
            start=UnixTime(100),
            end=UnixTime(120),
            semantics="sampled",
            sample_seconds=10,
            max_age_seconds=20,
            unit="base_units",
            visibility="published",
            payload_revision="quote-v1",
            **changes,
        )
    )


def spec(req=None):
    return Preparation(
        model="swap-example-v1",
        acquisition_revision="fixture-v1",
        requirements=(req or requirement(),),
    )


def record(at=100, **changes):
    values = dict(
        key=KEY,
        unit="base_units",
        source_time=UnixTime(at),
        available_at=UnixTime(at),
        retrieved_at=200.0,
        evidence=(EVIDENCE.digest,),
        reference=Slot(height=at // 2),
        payload=Quote(quantity=at),
    )
    return Record.model_validate(values | changes)


def span(start=100, end=120, **changes):
    return AcquiredRange.model_validate(
        dict(
            key=KEY,
            start=UnixTime(start),
            end=UnixTime(end),
            evidence=(EVIDENCE.digest,),
        )
        | changes
    )


def fill(writer):
    writer.commit_range(span(), tuple(record(t) for t in (100, 110, 120)), (EVIDENCE,))


def test_interrupted_ranges_resume_and_publish_immutable_dataset(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    writer.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    assert writer.missing(KEY) == ((110, 120),)
    with pytest.raises(ValueError, match="missing acquisition"):
        writer.publish()
    writer.close()
    with pytest.raises(ValueError, match="incomplete"):
        load_dataset(tmp_path)
    resumed = DatasetWriter(tmp_path, spec())
    resumed.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    resumed.commit_range(span(110, 120), (record(110), record(120)), (EVIDENCE,))
    manifest = resumed.publish()
    assert manifest.records == 3 and manifest.evidence_records == 1
    digest = file_digest(tmp_path / "dataset.json")
    assert resumed.publish() == manifest
    with pytest.raises(ValueError, match="immutable"):
        resumed.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    resumed.close()
    loaded, reader = load_dataset(tmp_path, digest)
    assert loaded == manifest
    timeline = reader.timeline(requirement(), Record)
    assert timeline.at(109).payload.quantity == 100
    assert timeline.at(110).payload.quantity == 110
    assert timeline.next(109) == 110 and timeline.next(120) is None
    assert reader.evidence(EVIDENCE.digest) == EVIDENCE
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        reader.db.execute("DELETE FROM inputs")
    reader.close()
    reopened = DatasetWriter(tmp_path, spec())
    assert reopened.publish() == manifest
    reopened.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("model", "other-model"),
        ("acquisition_revision", "other-source-decoder"),
        ("requirements", (requirement().model_copy(update={"end": UnixTime(130)}),)),
        ("requirements", (requirement().model_copy(update={"sample_seconds": 5}),)),
    ],
)
def test_resume_freezes_sources_model_coverage_and_resolution(tmp_path, field, value):
    DatasetWriter(tmp_path, spec()).close()
    with pytest.raises(ValueError, match="resume cannot change"):
        DatasetWriter(tmp_path, spec().model_copy(update={field: value}))


@pytest.mark.parametrize(
    "records,message",
    [
        ((record(), record(120)), "missing a required rate sample"),
        ((record(), record()), "unique and ordered"),
        ((record(110), record()), "unique and ordered"),
        ((record(99),), "outside the required window"),
        ((record(unit="different"),), "units differ"),
        ((record(key=KEY.model_copy(update={"source": "other_pool"})),), "source or units"),
    ],
)
def test_rejected_batches_leave_no_completed_range(tmp_path, records, message):
    writer = DatasetWriter(tmp_path, spec())
    with pytest.raises(ValueError, match=message):
        writer.commit_range(span(), records, (EVIDENCE,))
    assert writer.missing(KEY) == ((100, 120),)
    assert writer.db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0] == 0
    writer.close()


def test_missing_evidence_rolls_back_partial_records_and_raw_inputs(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    records = (record(), record(110, evidence=("0" * 64,)), record(120))
    with pytest.raises(ValueError, match="evidence is missing"):
        writer.commit_range(span(), records, (EVIDENCE,))
    for table in ("inputs", "ranges", "evidence"):
        assert writer.db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    fill(writer)
    writer.publish()
    writer.close()


def test_duplicates_are_idempotent_but_conflicts_and_overlap_fail(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    writer.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    with pytest.raises(ValueError, match="overlapping"):
        writer.commit_range(span(100, 119), (record(), record(110)), (EVIDENCE,))
    with pytest.raises(ValueError, match="conflicting prepared input"):
        writer.commit_range(span(100, 109), (record(payload=Quote(quantity=200)),), (EVIDENCE,))
    assert writer.missing(KEY) == ((110, 120),)
    writer.close()


def test_overlap_check_reads_only_ranges_that_intersect_the_commit(tmp_path):
    writer = DatasetWriter(tmp_path / "dataset", spec())
    writer.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    writer.commit_range(span(110, 119), (record(110),), (EVIDENCE,))
    assert writer.overlapping(span(120, 120)) == ()
    assert writer.overlapping(span(110, 119)) == (span(110, 119),)
    assert writer.overlapping(span(105, 115)) == (span(100, 109), span(110, 119))
    assert writer.overlapping(span(109, 110)) == (span(100, 109), span(110, 119))


def test_change_ranges_retain_empty_log_evidence_and_initialization(tmp_path):
    req = requirement().model_copy(update={"semantics": "changes", "sample_seconds": None})
    writer = DatasetWriter(tmp_path, spec(req))
    with pytest.raises(ValueError, match="initialization"):
        writer.commit_range(span(100, 109), (), (EVIDENCE,))
    writer.commit_range(span(100, 109), (record(), record(106)), (EVIDENCE,))
    writer.commit_range(span(110, 120), (), (EVIDENCE,))
    manifest = writer.publish()
    assert manifest.records == 2
    writer.close()
    _, reader = load_dataset(tmp_path)
    assert reader.timeline(req, Record).at(120).source_time == 106
    reader.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"source_time": UnixTime(101)},
        {"retrieved_at": 99.0},
        {"retrieved_at": float("nan")},
        {"period_end": UnixTime(101)},
        {"period_end": UnixTime(99)},
        {"evidence": ()},
    ],
)
def test_future_averaged_and_invalid_inputs_are_rejected(changes):
    with pytest.raises(ValueError):
        record(**changes)


def test_average_availability_and_carried_age_are_not_reset():
    req = requirement().model_copy(
        update={
            "semantics": "changes",
            "sample_seconds": None,
            "max_age_seconds": 15,
        }
    )
    first = record(source_time=UnixTime(90), period_end=UnixTime(99))
    timeline = Timeline((first, record(110)), req)
    assert timeline.at(105).source_time == 90
    with pytest.raises(ValueError, match="source observation age"):
        timeline.at(106)
    for time in (99, 121):
        with pytest.raises(ValueError, match="outside prepared coverage"):
            timeline.at(time)


def test_publication_checks_stale_tail_and_cleanup_gaps(tmp_path):
    req = requirement().model_copy(update={"semantics": "changes", "sample_seconds": None})
    writer = DatasetWriter(tmp_path, spec(req))
    writer.commit_range(span(), (record(source_time=UnixTime(99)),), (EVIDENCE,))
    with pytest.raises(ValueError, match="permitted age"):
        writer.publish()
    assert not (tmp_path / "dataset.json").exists()
    writer.close()


@pytest.mark.parametrize("target", ["data.sqlite", "dataset.json"])
def test_frozen_content_detects_file_tampering(tmp_path, target):
    writer = DatasetWriter(tmp_path, spec())
    fill(writer)
    writer.publish()
    writer.close()
    manifest_digest = file_digest(tmp_path / "dataset.json")
    path = tmp_path / target
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="differs|content changed"):
        load_dataset(tmp_path, manifest_digest)


def test_lost_manifest_publication_can_resume_without_mutating_database(tmp_path, monkeypatch):
    writer = DatasetWriter(tmp_path, spec())
    fill(writer)
    import almanak.backtest.prepared as module

    original = module.publish_manifest

    def interrupted(*args):
        raise SystemExit("died after sealing")

    monkeypatch.setattr(module, "publish_manifest", interrupted)
    with pytest.raises(SystemExit):
        writer.publish()
    before = file_digest(writer.path)
    writer.close()
    monkeypatch.setattr(module, "publish_manifest", original)
    resumed = DatasetWriter(tmp_path, spec())
    manifest = resumed.publish()
    assert manifest.database_sha256 == before
    resumed.close()


def test_reader_checks_indexed_identity_and_raw_evidence(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    fill(writer)
    writer.publish()
    writer.db.execute("UPDATE inputs SET time=101 WHERE time=100")
    writer.db.commit()
    raw = json.loads((tmp_path / "dataset.json").read_text())
    raw["database_sha256"] = file_digest(writer.path)
    manifest = DatasetManifest.model_validate_json(json.dumps(raw))
    reader = manifest.open(tmp_path)
    with pytest.raises(ValueError, match="indexed identity"):
        reader.timeline(requirement(), Record)
    with pytest.raises(ValueError, match="missing or corrupt"):
        reader.evidence("0" * 64)
    reader.close()
    writer.close()


def test_retry_cannot_add_or_omit_events_in_a_completed_change_range(tmp_path):
    req = requirement().model_copy(update={"semantics": "changes", "sample_seconds": None})
    writer = DatasetWriter(tmp_path, spec(req))
    writer.commit_range(span(), (record(), record(110)), (EVIDENCE,))
    for records in ((record(),), (record(), record(105), record(110))):
        with pytest.raises(ValueError, match="conflicting prepared input"):
            writer.commit_range(span(), records, (EVIDENCE,))
    assert writer.publish().records == 2
    writer.close()


def test_completed_manifest_cannot_claim_a_different_preparation(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    fill(writer)
    manifest = writer.publish()
    writer.close()
    incorrect = manifest.model_copy(
        update={
            "preparation": spec().model_copy(update={"model": "unvalidated-model"}),
        }
    )
    with pytest.raises(ValueError, match="completed manifest"):
        incorrect.open(tmp_path)
    with pytest.raises(ValueError, match="record counts"):
        manifest.model_copy(update={"records": 99}).open(tmp_path)


def test_data_directory_must_remain_inaccessible_to_strategy_workers(tmp_path, monkeypatch):
    monkeypatch.setattr("almanak.backtest.prepared.readable_paths", lambda: (tmp_path,))
    with pytest.raises(ValueError, match="strategy-readable"):
        DatasetWriter(tmp_path / "data", spec())


@pytest.mark.parametrize("table", ["inputs", "ranges"])
def test_publication_refuses_corrupted_preparation_indexes(tmp_path, table):
    req = requirement().model_copy(update={"semantics": "changes", "sample_seconds": None})
    writer = DatasetWriter(tmp_path, spec(req))
    writer.commit_range(span(), (record(),), (EVIDENCE,))
    if table == "inputs":
        writer.db.execute("UPDATE inputs SET time=101")
    else:
        writer.db.execute("UPDATE ranges SET end=119")
    writer.db.commit()
    with pytest.raises(ValueError, match="indexed identity"):
        writer.publish()
    assert not (tmp_path / "dataset.json").exists()
    writer.close()


def evidence_reads(db):
    """Count the statements that read an evidence body back from `db`."""
    seen: list[str] = []
    db.set_trace_callback(lambda sql: seen.append(sql) if "body FROM evidence" in sql else None)
    return seen


def test_evidence_is_read_back_once_per_writer_and_again_at_publication(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    reads = evidence_reads(writer.db)
    writer.commit_range(span(100, 109), (record(),), (EVIDENCE,))
    writer.commit_range(span(110, 119), (record(110),), (EVIDENCE,))
    writer.commit_range(span(120, 120), (record(120),), ())
    assert len(reads) == 1
    writer.publish()
    assert len(reads) == 2
    writer.close()
    _, reader = load_dataset(tmp_path)
    reads = evidence_reads(reader.db)
    for _ in range(3):
        reader.verify(EVIDENCE.digest)
    assert len(reads) == 1
    with pytest.raises(ValueError, match="missing or corrupt"):
        reader.verify("0" * 64)
    reader.close()


def test_publication_refuses_evidence_changed_after_it_was_verified(tmp_path):
    writer = DatasetWriter(tmp_path, spec())
    fill(writer)
    writer.db.execute("UPDATE evidence SET body='{}'")
    writer.db.commit()
    with pytest.raises(ValueError, match="evidence is missing or corrupt"):
        writer.publish()
    writer.close()
