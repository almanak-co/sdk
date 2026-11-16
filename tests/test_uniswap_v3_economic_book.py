"""Publication and offline reload use captured pool state, not a temporal-path proof."""

from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.connectors.concentrated_policy import Coverage
from almanak.connectors.uniswap_v3.economic_book import PoolBook, identity
from almanak.connectors.uniswap_v3.economic_config import DataPolicy
from almanak.connectors.uniswap_v3.economic_data import (
    MODEL,
    REVISION,
    PreparedSample,
    Record,
    key,
    requirement,
)
from almanak.connectors.uniswap_v3.history import BoundaryEvidence
from tests.test_uniswap_v3_economic_simulation import setup


def publish(directory, index=0, *, corrupt_gross=False):
    config, captured, position = setup(index)
    target, sample, block = position.target, captured.payload, captured.reference
    at = block.timestamp
    policy = DataPolicy(
        source="uniswap_v3_archive",
        sample_seconds=60,
        max_age_seconds=60,
        max_samples=1,
        max_bitmap_words=100,
        max_initialized_ticks=10000,
        max_liquidity_events=100,
        max_liquidity_log_requests=100,
        coverage=(Coverage(allocation=target.allocation, lower=sample.lower, upper=sample.upper),),
    )
    settings = SimpleNamespace(
        data=policy, schedule=SimpleNamespace(start=at, deadline=at), history=()
    )
    ticks = {t.index: t.gross for t in sample.ticks}
    gross = tuple(ticks[t] + int(corrupt_gross) for t in (target.range.lower, target.range.upper))
    # The zero-duration fixture isolates retained boundary/sample consistency.
    baseline = BoundaryEvidence(
        target=target,
        first=block,
        last=block,
        initial_gross=gross,
        final_gross=gross,
        events=(),
        contracts=(),
        inputs=(),
    )
    proof = RawEvidence(body=baseline.model_dump_json())
    raw = RawEvidence(body=sample.model_dump_json())
    payload = PreparedSample(**sample.model_dump(), baseline_digest=proof.digest)
    record = Record(
        key=key(target),
        unit="uniswap_v3_integer_pool",
        payload=payload,
        reference=block,
        source_time=at,
        available_at=at,
        retrieved_at=float(at),
        evidence=(proof.digest, raw.digest),
    )
    preparation = Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=identity(config, settings),
        requirements=(requirement(target, settings),),
    )
    writer = DatasetWriter(directory, preparation)
    try:
        writer.commit_range(
            AcquiredRange(key=key(target), start=at, end=at, evidence=record.evidence),
            (record,),
            (proof, raw),
        )
        writer.publish()
    finally:
        writer.close()
    return config, settings, record


@pytest.mark.parametrize("index", [0, 1])
def test_published_v3_pool_reloads_exactly_offline(tmp_path, monkeypatch, index):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared book cannot contact a provider")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    config, settings, record = publish(tmp_path, index)
    book = PoolBook(tmp_path, config, settings)
    try:
        assert book.at(config.target("position"), record.available_at) == record
        with pytest.raises(ValueError, match="outside prepared coverage"):
            book.at(config.target("position"), record.available_at + 1)
    finally:
        book.close()
    settings.data = settings.data.model_copy(update={"max_initialized_ticks": 9999})
    with pytest.raises(ValueError, match="frozen model or policy"):
        PoolBook(tmp_path, config, settings)


def test_valid_hashes_cannot_hide_inconsistent_boundary_measurement(tmp_path):
    config, settings, _ = publish(tmp_path, corrupt_gross=True)
    with pytest.raises(ValueError, match="range differs from its continuity"):
        PoolBook(tmp_path, config, settings)
