"""A prepared V3 sample binds its block, identities, reads and continuity proof."""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import RawEvidence
from almanak.connectors import v3_fee_growth
from almanak.connectors.v3_prepared_acquisition import prepared_sample
from almanak.models import Block
from tests.test_backtest_history import CHAIN

BLOCK = Block(chain=CHAIN, number=7, hash="0x" + "07" * 32, timestamp=95)
DATA = SimpleNamespace(
    PreparedSample=lambda **fields: fields,
    Record=lambda **fields: SimpleNamespace(**fields),
    key=lambda target: "key",
    UNIT="unit",
)
POLICY = SimpleNamespace(
    for_target=lambda target: SimpleNamespace(lower=-100, upper=100),
    max_bitmap_words=4,
    max_initialized_ticks=50,
)


def read(name, retrieved_at):
    return SimpleNamespace(
        model_dump_json=lambda: f'{{"read":"{name}"}}', retrieved_at=retrieved_at
    )


class History:
    def __init__(self):
        # A read left over from the sample before must not enter this one's evidence.
        self.reader = SimpleNamespace(used={"stale"}, cache={"stale": read("stale", 1.0)})

    def floor(self, at):
        assert at == 100
        return BLOCK

    def pin(self, block):
        self.reader.cache["header"] = read("header", 200.0)
        self.reader.used.add("header")
        return block


def sampler(calls):
    def sample(reader, pool, block, **options):
        calls.append((pool, block, options))
        reader.cache["slot0"] = read("slot0", 300.0)
        reader.used.add("slot0")
        return SimpleNamespace(model_dump=lambda: {"ticks": ()})

    return sample


def prepare(target, baseline, calls, **options):
    return prepared_sample(
        History(),
        target,
        100,
        POLICY,
        baseline,
        sample=sampler(calls),
        data=DATA,
        contract_identities=lambda reader, target, block: ("pool", "factory"),
        **options,
    )


def baseline(contracts=("pool", "factory")):
    return SimpleNamespace(contracts=contracts, model_dump_json=lambda: '{"baseline":1}')


def test_a_sample_retains_its_own_reads_and_names_the_continuity_proof():
    calls = []
    target = SimpleNamespace(pool="pool", range=None)
    record, raw = prepare(target, baseline(), calls, carry="carry", audit=True)
    proof = RawEvidence(body='{"baseline":1}')
    assert [e.body for e in raw] == ['{"read":"header"}', '{"read":"slot0"}', proof.body]
    assert record.evidence == tuple(e.digest for e in raw)
    assert record.payload == {"ticks": (), "baseline_digest": proof.digest}
    assert (record.reference, record.source_time, record.available_at) == (BLOCK, 95, 100)
    assert (record.key, record.unit, record.retrieved_at) == ("key", "unit", 300.0)
    # Coverage limits and the carry options reach the connector's sampler unchanged.
    assert calls == [
        (
            "pool",
            BLOCK,
            dict(lower=-100, upper=100, max_words=4, max_ticks=50, carry="carry", audit=True),
        )
    ]


def test_a_supplied_proof_is_used_without_serializing_the_baseline_again():
    proof = RawEvidence(body='{"baseline":1}')
    unserializable = SimpleNamespace(contracts=("pool", "factory"))
    record, raw = prepare(SimpleNamespace(pool="pool", range=None), unserializable, [], proof=proof)
    assert raw[-1] is proof and record.payload["baseline_digest"] == proof.digest


def test_a_fixed_range_sample_must_measure_growth_inside_its_range(monkeypatch):
    seen = []
    monkeypatch.setattr(v3_fee_growth, "sampled_inside", lambda *args: seen.append(args))
    target = SimpleNamespace(pool="pool", range=SimpleNamespace(lower=-10, upper=10))
    prepare(target, baseline(), [])
    assert len(seen) == 1 and seen[0][1] is target


def test_changed_contract_identities_refuse_the_sample():
    calls = []
    with pytest.raises(ValueError, match="changed V3 implementation identities"):
        prepare(SimpleNamespace(pool="pool", range=None), baseline(("other",)), calls)
    assert calls == []
