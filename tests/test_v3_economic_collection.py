"""The V3 family carries ticks over its retained liquidity events, and audits each range."""

from types import SimpleNamespace

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.history import ReadRequest
from almanak.backtest.prepared import AcquiredRange
from almanak.backtest.types import UnixTime
from almanak.connectors import v3_economic_collection as family
from almanak.connectors.v3_prepared_acquisition import TICK_SELECTOR, Carry
from tests.test_backtest_history import CHAIN, TARGET
from tests.test_shared_preflight_evidence import KEY, SETTINGS, SHARED, dataset_writer, sampled


def history():
    return SimpleNamespace(
        reader=SimpleNamespace(used=set(), cache={}), prefetch_floors=lambda times: None
    )


def collect(writer, monkeypatch, target, baseline):
    calls = []

    def prepared_sample(history, target, at, data, baseline, **carried):
        carry = carried.get("carry")
        calls.append((at, carried, None if carry is None else carry.block))
        if carry is not None:
            carry.retain(SimpleNamespace(number=at), 0, {}, direct=carry.block is None)
        return sampled(at)

    monkeypatch.setattr(family, "evidence", lambda reader: SHARED)
    monkeypatch.setattr(family, "liquidity_event", lambda target, event: (*event.ticks, 1))
    family.collect(
        writer,
        history(),
        target,
        SETTINGS,
        key=lambda target: KEY,
        preflight=lambda *args: baseline,
        prepared_sample=prepared_sample,
    )
    return calls


def baseline(*events):
    return SimpleNamespace(
        events=tuple(
            SimpleNamespace(block=SimpleNamespace(number=number), ticks=ticks)
            for number, ticks in events
        ),
        model_dump_json=lambda: '{"baseline":1}',
    )


def test_a_target_carries_ticks_and_audits_the_last_sample_of_each_range(tmp_path, monkeypatch):
    writer = dataset_writer(tmp_path)
    record, raw = sampled(110)
    writer.commit_range(
        AcquiredRange(key=KEY, start=UnixTime(110), end=UnixTime(119), evidence=(raw[0].digest,)),
        (record,),
        raw,
    )
    target = SimpleNamespace(allocation="position")
    calls = collect(writer, monkeypatch, target, baseline((7, (-10, 10)), (9, (20, 30))))
    # Two ranges remain around the committed one. Each starts from no carried block and
    # audits its last sample; here each holds one sample.
    assert [(at, carried["audit"], block) for at, carried, block in calls] == [
        (100, True, None),
        (120, True, None),
    ]
    carry = calls[0][1]["carry"]
    assert isinstance(carry, Carry) and carry is calls[1][1]["carry"]
    assert carry.changes == ((7, -10, 10), (9, 20, 30))
    proof = RawEvidence(body='{"baseline":1}')
    assert all(carried["proof"] == proof for _, carried, _ in calls)
    assert calls[0][1]["proof"] is calls[1][1]["proof"]
    writer.publish()
    writer.close()


def test_only_the_last_sample_of_a_range_is_audited(tmp_path, monkeypatch):
    writer = dataset_writer(tmp_path)
    calls = collect(writer, monkeypatch, SimpleNamespace(allocation="position"), baseline())
    assert [(at, carried["audit"]) for at, carried, _ in calls] == [
        (100, False),
        (110, False),
        (120, True),
    ]
    # The carry runs through the range: only its first sample starts without a block.
    assert [block is None for _, _, block in calls] == [True, False, False]
    writer.close()


def tick_request(tick):
    data = TICK_SELECTOR + tick.to_bytes(32, "big", signed=True).hex()
    reference = {"blockHash": "0x" + "22" * 32, "requireCanonical": True}
    return ReadRequest(
        chain=CHAIN, method="eth_call", params=({"to": TARGET, "data": data}, reference)
    )


def test_only_the_ticks_beside_the_price_are_expected_to_repeat():
    other = ReadRequest(chain=CHAIN, method="eth_getCode", params=(TARGET, "latest"))
    carry = Carry(())
    carry.retain(SimpleNamespace(number=1), 5, dict.fromkeys(range(-50, 51, 10)), direct=True)
    keep = family.repeated_reads(carry)
    assert keep(other)
    assert [tick for tick in range(-50, 51, 10) if keep(tick_request(tick))] == [-10, 0, 10, 20]
