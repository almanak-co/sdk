"""Prepared samples cannot silently diverge from their archived source or policy."""

import gzip
import json

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.history import ReadEvidence
from almanak.connectors.aerodrome.economic_acquisition import sample
from almanak.connectors.aerodrome.economic_book import PoolBook, RetainedReader
from almanak.connectors.aerodrome.economic_config import DataPolicy
from almanak.connectors.aerodrome.economic_prepare import validate_policy
from tests.test_aerodrome_prepared_runner import EVIDENCE, ROOT, ROWS, inputs, restore


@pytest.fixture
def retained(tmp_path):
    row = ROWS[0]
    config, settings, _, _ = inputs(row)
    restore(tmp_path, row, ("data.sqlite", "dataset.json"))
    book = PoolBook(tmp_path, config, settings)
    try:
        target = config.target(config.allocations[0].name)
        yield config, settings, book, target, book.timelines[target].rows[0]
    finally:
        book.close()


@pytest.mark.parametrize("change", ["pool", "time", "age", "payload", "evidence"])
def test_sample_payload_requires_matching_time_source_and_raw_reads(retained, change):
    _, _, book, target, row = retained
    if change == "pool":
        target = target.model_copy(
            update={"pool": target.pool.model_copy(update={"stable": False})}
        )
    elif change == "time":
        row = row.model_copy(update={"available_at": row.available_at + 1})
    elif change == "age":
        reference = row.reference.model_copy(update={"timestamp": row.source_time - 121})
        row = row.model_copy(
            update={
                "reference": reference,
                "source_time": reference.timestamp,
                "payload": row.payload.model_copy(
                    update={"pool": row.payload.pool.model_copy(update={"block": reference})}
                ),
            }
        )
    elif change == "payload":
        row = row.model_copy(
            update={
                "payload": row.payload.model_copy(
                    update={
                        "pool": row.payload.pool.model_copy(
                            update={"reserve0": row.payload.pool.reserve0 + 1}
                        )
                    }
                )
            }
        )
    else:
        row = row.model_copy(update={"evidence": ()})
    with pytest.raises(ValueError, match="prepared"):
        book.validate_row(target, row, 0)


@pytest.mark.parametrize("change", ["code", "block", "index"])
def test_samples_require_stable_contracts_and_monotonic_fee_indices(retained, change):
    _, _, book, _, row = retained
    if change == "code":
        later = row.model_copy(update={"payload": row.payload.model_copy(update={"contracts": ()})})
    elif change == "block":
        later = row.model_copy(
            update={
                "reference": row.reference.model_copy(update={"number": row.reference.number - 1})
            }
        )
    else:
        later = row.model_copy(
            update={
                "payload": row.payload.model_copy(
                    update={
                        "pool": row.payload.pool.model_copy(
                            update={"index0": row.payload.pool.index0 - 1}
                        )
                    }
                )
            }
        )
    with pytest.raises(ValueError, match="prepared"):
        book.validate_continuity(row, later)


def test_retained_reads_require_exact_chain_nonconflicting_values_and_pinned_header(retained):
    _, _, book, target, row = retained
    raw = tuple(book.reader.evidence(d) for d in row.evidence)
    reader = RetainedReader(target.fund.chain, raw)
    assert reader.pin_block(row.reference.number) == row.reference
    assert sample(reader, target, row.reference) == row.payload
    first = ReadEvidence.model_validate_json(raw[0].body)
    wrong = first.model_copy(
        update={
            "request": first.request.model_copy(
                update={"chain": first.request.chain.model_copy(update={"environment": "another"})}
            )
        }
    )
    from almanak.backtest.dataset import RawEvidence

    with pytest.raises(ValueError, match="another chain"):
        RetainedReader(target.fund.chain, (RawEvidence(body=wrong.model_dump_json()),))
    changed = first.model_copy(update={"result": None if first.result is not None else "changed"})
    with pytest.raises(ValueError, match="conflicting"):
        RetainedReader(target.fund.chain, (raw[0], RawEvidence(body=changed.model_dump_json())))


def test_dataset_policy_limits_and_source_age_remain_explicit(retained, tmp_path):
    config, settings, book, target, _ = retained
    with pytest.raises(InputUnavailable):
        book.at(target, settings.schedule.start - 1)
    with pytest.raises(ValueError, match="outside prepared"):
        book.at(target.model_copy(update={"allocation": "another"}), settings.schedule.start)
    bounded = settings.model_copy(
        update={"data": settings.data.model_copy(update={"max_samples": 9})}
    )
    with pytest.raises(ValueError, match="sample limit"):
        validate_policy(config, bounded)
    with pytest.raises(ValueError, match="model or policy"):
        PoolBook(tmp_path, config, bounded)
    with pytest.raises(ValueError, match="shorter"):
        DataPolicy(
            source="aerodrome_archive", sample_seconds=60, max_age_seconds=59, max_samples=10
        )


def test_retained_datasets_and_runs_match_the_sealed_manifest():
    import hashlib

    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for entry in manifest["files"]:
        raw = (EVIDENCE / entry["path"]).read_bytes()
        assert len(raw) == entry["bytes"] and hashlib.sha256(raw).hexdigest() == entry["sha256"]
    for row in ROWS:
        published = json.loads(gzip.decompress((ROOT / row["files"]["dataset.json"]).read_bytes()))
        assert published == row["manifest"]
