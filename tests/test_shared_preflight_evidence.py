"""Window-wide preflight evidence is retained once per collection pass, not per sample."""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord, RawEvidence
from almanak.backtest.prepared import AcquiredRange, DatasetWriter, Preparation
from almanak.backtest.types import ConnectorId, UnixTime
from almanak.caip import ChainId
from almanak.connectors import v3_economic_collection as v3_family
from almanak.connectors.curve import economic_collection as curve
from almanak.connectors.uniswap_v4 import economic_prepare as uniswap_v4
from almanak.models import Chain, Model


class Quote(Model):
    quantity: int


class Slot(Model):
    height: int
    # The V3 family learns which reads repeat from each record's block reference.
    hash: str = "0x" + "00" * 32


Record = InputRecord[Quote, Slot]
KEY = DataKey(
    connector=ConnectorId("swap_example"),
    chain=Chain(chain_id=ChainId("cosmos:cosmoshub-4"), environment="test"),
    source="declared_pool",
    product="quote",
)
SHARED = tuple(RawEvidence(body=f'{{"preflight":{i}}}') for i in range(3))
TARGET = SimpleNamespace(allocation="position")
SETTINGS = SimpleNamespace(
    schedule=SimpleNamespace(start=100, deadline=120),
    data=SimpleNamespace(sample_seconds=10, max_samples=10),
)


def sampled(at):
    raw = RawEvidence(body=f'{{"sample":{at}}}')
    record = Record(
        key=KEY,
        unit="base_units",
        source_time=UnixTime(at),
        available_at=UnixTime(at),
        retrieved_at=float(at),
        evidence=(raw.digest,),
        reference=Slot(height=at),
        payload=Quote(quantity=at),
    )
    return record, (raw,)


def dataset_writer(tmp_path):
    requirement = DataRequirement(
        key=KEY,
        start=UnixTime(100),
        end=UnixTime(120),
        semantics="sampled",
        sample_seconds=10,
        max_age_seconds=20,
        unit="base_units",
        visibility="published",
        payload_revision="quote-v1",
    )
    preparation = Preparation(
        model="swap-example-v1", acquisition_revision="fixture-v1", requirements=(requirement,)
    )
    return DatasetWriter(tmp_path / "dataset", preparation)


def collect_v3(writer, monkeypatch):
    monkeypatch.setattr(v3_family, "evidence", lambda reader: SHARED)
    # The family reads ahead through its history; these samples make no reads to repeat.
    history = SimpleNamespace(
        reader=SimpleNamespace(used=set(), cache={}), prefetch_floors=lambda times: None
    )
    v3_family.collect(
        writer,
        history,
        TARGET,
        SETTINGS,
        key=lambda target: KEY,
        preflight=lambda *args: SimpleNamespace(
            events=(), model_dump_json=lambda: '{"baseline":1}'
        ),
        prepared_sample=lambda history, target, at, data, baseline, **carried: sampled(at),
    )


def collect_v4(writer, monkeypatch):
    monkeypatch.setattr(uniswap_v4, "evidence", lambda reader: SHARED)
    monkeypatch.setattr(uniswap_v4, "key", lambda target: KEY)
    monkeypatch.setattr(uniswap_v4, "preflight", lambda *args: "baseline")
    monkeypatch.setattr(
        uniswap_v4, "sample", lambda history, target, at, data, baseline: sampled(at)
    )
    uniswap_v4.collect(writer, SimpleNamespace(reader=None), TARGET, SETTINGS)


def collect_curve(writer, monkeypatch):
    path = SimpleNamespace(events=(), model_dump_json=lambda: '{"path":"unchanged"}')
    monkeypatch.setattr(curve, "evidence", lambda reader: SHARED)
    monkeypatch.setattr(curve, "key", lambda target: KEY)
    monkeypatch.setattr(curve, "preflight", lambda *args: path)
    monkeypatch.setattr(curve, "prepared_sample", lambda history, target, at, path: sampled(at))
    curve.collect(writer, SimpleNamespace(reader=None), TARGET, SETTINGS)


@pytest.mark.parametrize("collect", [collect_v3, collect_v4, collect_curve])
def test_preflight_evidence_is_retained_with_the_first_range_only(tmp_path, monkeypatch, collect):
    writer = dataset_writer(tmp_path)
    collect(writer, monkeypatch)
    spans = writer.ranges(KEY)
    shared = {evidence.digest for evidence in SHARED}
    assert [int(span.start) for span in spans] == [100, 110, 120]
    assert shared <= set(spans[0].evidence)
    assert all(not shared & set(span.evidence) for span in spans[1:])
    for span in spans:
        assert sampled(int(span.start))[1][0].digest in span.evidence
    writer.publish()
    assert writer.db.execute("SELECT COUNT(*) FROM evidence").fetchone()[0] >= len(SHARED) + 3
    writer.close()


@pytest.mark.parametrize("collect", [collect_v3, collect_v4, collect_curve])
def test_resumed_pass_retains_preflight_evidence_with_its_first_range(
    tmp_path, monkeypatch, collect
):
    writer = dataset_writer(tmp_path)
    record, raw = sampled(100)
    writer.commit_range(
        AcquiredRange(
            key=KEY,
            start=UnixTime(100),
            end=UnixTime(109),
            evidence=tuple(e.digest for e in raw),
        ),
        (record,),
        raw,
    )
    collect(writer, monkeypatch)
    spans = writer.ranges(KEY)
    shared = {evidence.digest for evidence in SHARED}
    assert not shared & set(spans[0].evidence)
    assert shared <= set(spans[1].evidence)
    assert not shared & set(spans[2].evidence)
    writer.publish()
    writer.close()
