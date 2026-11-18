"""Carried V3 tick state equals a direct read of every tick, with far fewer reads."""

import random
from types import SimpleNamespace

import pytest

from almanak.backtest.history import ReadRequest
from almanak.connectors import v3_prepared_acquisition as acquisition
from almanak.connectors.v3_prepared_acquisition import (
    Carry,
    carried_tick_state,
    changed_ticks,
    confirm_carried,
    frontier,
    tick_read,
)
from almanak.connectors.v3_prepared_data import TickState
from almanak.gateway.ethereum import DataError
from almanak.models import Block
from tests.band_pool_ledger import Ledger
from tests.test_backtest_history import CHAIN

SPACING = 10
SOURCE = SimpleNamespace(pool="0x" + "11" * 20, tick_spacing=SPACING)
BOUNDS = (-4000, 4000, 8, 1000)


def block(number):
    return Block(chain=CHAIN, number=number, hash="0x" + f"{number:064x}", timestamp=number)


def states(ledger):
    return {
        index: TickState(index=index, gross=s[0], net=s[1], outside0=s[2], outside1=s[3])
        for index, s in sorted(ledger.ticks.items())
    }


def walk(seed, samples=40):
    """A seeded pool history as (block, tick, tick states) samples and its liquidity events.

    Intervals range from idle to busy, and some price moves leave and return without any
    fee accrual, so a crossed tick can end an interval exactly as it began.
    """
    rng = random.Random(seed)
    ledger = Ledger(tick=5)
    for lower, upper, liquidity in ((-3000, 3000, 10**30), (-100, 50, 10**18), (0, 200, 10**18)):
        ledger.modify(lower, upper, liquidity)
    for index in range(-40, 41, 4):
        ledger.modify(index * SPACING, (index + 3) * SPACING, 10**15)
    # Positions this walk opened; a burn never takes more than its own position holds.
    held: dict[tuple[int, int], int] = {}
    number, events = 1, []
    taken = [(block(number), ledger.tick, states(ledger))]
    for _ in range(samples - 1):
        for _ in range(rng.choice((0, 0, 1, 3, 12))):
            number += 1
            action = rng.random()
            if action < 0.4:
                ledger.accrue(rng.randrange(0, 2**100), rng.randrange(0, 2**100))
            elif action < 0.6:
                origin = ledger.tick
                ledger.move(max(-2900, min(2900, origin + rng.randrange(-400, 401))))
                ledger.move(origin)
            elif action < 0.85:
                ledger.move(max(-2900, min(2900, ledger.tick + rng.randrange(-80, 81))))
            else:
                lower = rng.randrange(-40, 30) * SPACING
                upper = lower + rng.randrange(1, 10) * SPACING
                if held and rng.random() < 0.6:
                    lower, upper = rng.choice(sorted(held))
                    change = -held.pop((lower, upper))
                else:
                    change = rng.randrange(1, 10**12)
                    held[lower, upper] = held.get((lower, upper), 0) + change
                ledger.modify(lower, upper, change)
                events.append((number, lower, upper))
        number += 1
        taken.append((block(number), ledger.tick, states(ledger)))
    return taken, tuple(events)


def counting(direct, reads):
    def read(indices):
        reads.extend(indices)
        return {index: direct[index] for index in indices}

    return read


@pytest.mark.parametrize("seed", range(25))
def test_carried_ticks_equal_a_direct_read_of_every_tick(seed):
    taken, events = walk(seed)
    carry = Carry(events)
    at, tick, direct = taken[0]
    carry.retain(at, tick, dict(direct), direct=True)
    total = carried = 0
    for at, tick, direct in taken[1:]:
        reads: list[int] = []
        read = counting(direct, reads)
        result, kept = changed_ticks(sorted(direct), tick, carry, carry.touched(at), read)
        assert len(reads) == len(set(reads)) and not set(reads) & set(kept)
        confirm_carried(kept, carry.states, counting(direct, []))
        result.update((index, carry.states[index]) for index in kept)
        assert result == direct
        total, carried = total + len(direct), carried + len(kept)
        carry.retain(at, tick, result, direct=False)
    # Most ticks of most samples are never read again.
    assert carried > total / 2


def test_a_quiet_interval_reads_only_the_ticks_beside_the_price():
    taken, _ = walk(0, samples=1)
    at, tick, direct = taken[0]
    carry = Carry(())
    carry.retain(at, tick, dict(direct), direct=True)
    reads: list[int] = []
    result, kept = changed_ticks(sorted(direct), tick, carry, frozenset(), counting(direct, reads))
    ordered = sorted(direct)
    below = [index for index in ordered if index <= tick][-2:]
    above = [index for index in ordered if index > tick][:2]
    assert sorted(reads) == below + above
    assert sorted([*result, *kept]) == ordered


def test_touched_ticks_are_the_events_after_the_carried_block_through_the_sample():
    carry = Carry(((5, -10, 10), (7, 20, 30), (7, 40, 50), (9, 60, 70)))
    carry.retain(block(5), 0, {}, direct=True)
    assert carry.touched(block(6)) == frozenset()
    assert carry.touched(block(7)) == {20, 30, 40, 50}
    assert carry.touched(block(12)) == {20, 30, 40, 50, 60, 70}
    with pytest.raises(ValueError, match="block order"):
        Carry(((7, 0, 10), (5, 0, 10)))


def quiet_pair():
    taken, _ = walk(3, samples=1)
    at, tick, direct = taken[0]
    carry = Carry(())
    carry.retain(at, tick, dict(direct), direct=True)
    return carry, tick, direct


def test_liquidity_changed_without_an_event_is_refused():
    carry, tick, direct = quiet_pair()
    beside = max(index for index in direct if index <= tick)
    changed = dict(direct)
    changed[beside] = direct[beside].model_copy(update={"gross": direct[beside].gross + 1})
    with pytest.raises(DataError, match="without a retained liquidity event"):
        changed_ticks(sorted(direct), tick, carry, frozenset(), counting(changed, []))


def test_a_tick_initialized_or_cleared_without_an_event_is_refused():
    carry, tick, direct = quiet_pair()
    far = min(direct)
    fewer = sorted(index for index in direct if index != far)
    with pytest.raises(DataError, match="initialized or cleared without"):
        changed_ticks(fewer, tick, carry, frozenset(), counting(direct, []))
    more = dict(direct) | {far - SPACING: direct[far].model_copy(update={"index": far - SPACING})}
    with pytest.raises(DataError, match="initialized or cleared without"):
        changed_ticks(sorted(more), tick, carry, frozenset(), counting(more, []))
    # The same changes are read directly when a liquidity event names the tick.
    reads: list[int] = []
    touched = frozenset({far - SPACING})
    result, kept = changed_ticks(sorted(more), tick, carry, touched, counting(more, reads))
    assert far - SPACING in reads and far - SPACING not in kept
    result, kept = changed_ticks(fewer, tick, carry, frozenset({far}), counting(direct, []))
    assert far not in result and far not in kept


def test_growth_changed_beyond_an_unchanged_tick_is_refused():
    carry, tick, direct = quiet_pair()
    second = sorted(index for index in direct if index <= tick)[-2]
    changed = dict(direct)
    changed[second] = direct[second].model_copy(update={"outside0": direct[second].outside0 + 1})
    with pytest.raises(DataError, match="beyond an unchanged tick"):
        changed_ticks(sorted(direct), tick, carry, frozenset(), counting(changed, []))


def test_an_audit_refuses_a_carried_tick_that_differs_from_its_direct_read():
    carry, tick, direct = quiet_pair()
    far = min(direct)
    changed = dict(direct)
    changed[far] = direct[far].model_copy(update={"outside1": direct[far].outside1 + 1})
    _, kept = changed_ticks(sorted(direct), tick, carry, frozenset(), counting(changed, []))
    assert far in kept
    with pytest.raises(DataError, match="differs from its direct read"):
        confirm_carried(kept, carry.states, counting(changed, []))


def test_the_frontier_widens_until_a_tick_is_unchanged():
    side = [90, 80, 70, 60, 50, 40, 30, 20, 10]
    previous = {i: TickState(index=i, gross=5, net=1, outside0=1, outside1=1) for i in side}
    # The five ticks nearest the price changed their growth; the sixth did not.
    now = {
        i: state.model_copy(update={"outside0": 2}) if i >= 50 else state
        for i, state in previous.items()
    }
    reads: list[list[int]] = []

    def read(indices):
        reads.append(list(indices))
        return {index: now[index] for index in indices}

    found: dict[int, TickState] = {}
    assert frontier(side, previous, found, read) == [30, 20, 10]
    assert reads == [[90, 80], [70, 60, 50, 40]]
    # No tick on the side is unchanged: every one was read and none is carried.
    assert frontier(side[:5], previous, {}, read) == []


class PoolReader:
    """Answers bitmap words from a sample's tick states; tick reads go through `read_tick`."""

    def __init__(self):
        self.direct: dict[int, TickState] = {}
        self.reads: list[int] = []

    def call(self, to, signature, at, *, inputs=(), args=(), outputs=()):
        assert (to, signature) == (SOURCE.pool, "tickBitmap(int16)")
        bitmap = 0
        for index in self.direct:
            if (index // SPACING) >> 8 == args[0]:
                bitmap |= 1 << ((index // SPACING) & 255)
        return (bitmap,)

    def read_tick(self, reader, source, at, index):
        self.reads.append(index)
        state = self.direct[index]
        return state.gross, state.net, state.outside0, state.outside1


def test_a_pass_reads_every_tick_first_then_few_and_audits_on_schedule(monkeypatch):
    monkeypatch.setattr(acquisition, "AUDIT_SAMPLES", 4)
    taken, events = walk(11, samples=12)
    carry, reader, counts = Carry(events), PoolReader(), []
    for position, (at, tick, direct) in enumerate(taken):
        reader.direct, reader.reads = direct, []
        words, ticks = carried_tick_state(
            reader,
            SOURCE,
            at,
            tick,
            BOUNDS,
            carry,
            audit=position == len(taken) - 1,
            tick_reader=reader.read_tick,
        )
        assert ticks == tuple(direct[index] for index in sorted(direct))
        assert sum(bin(word.bitmap).count("1") for word in words) == len(direct)
        counts.append((len(reader.reads), len(direct)))
    # The first sample, every fourth carried sample and the audited last one read all.
    full = [position for position, (reads, size) in enumerate(counts) if reads == size]
    assert full == [0, 4, 8, 11]
    assert sum(reads for reads, _ in counts) < sum(size for _, size in counts) / 2
    carry.reset()
    assert (carry.block, carry.states, carry.carried) == (None, {}, 0)


def test_tick_reads_name_their_tick_and_other_reads_name_none():
    def call(data):
        return ReadRequest(
            chain=CHAIN,
            method="eth_call",
            params=({"to": SOURCE.pool, "data": data}, {"blockHash": "0x" + "22" * 32}),
        )

    assert tick_read(call(acquisition.TICK_SELECTOR + "00" * 32)) == 0
    assert tick_read(call(acquisition.TICK_SELECTOR + "ff" * 31 + "f6")) == -10
    assert tick_read(call(acquisition.TICK_SELECTOR + "00" * 30 + "0bb8")) == 3000
    assert tick_read(call(acquisition.TICK_SELECTOR)) is None
    assert tick_read(call("0x3850c7bd")) is None
    code = ReadRequest(chain=CHAIN, method="eth_getCode", params=(SOURCE.pool, "latest"))
    assert tick_read(code) is None


def test_the_ticks_beside_the_price_are_the_next_sample_s_first_reads():
    carry = Carry(())
    ticks = {index: None for index in (-30, -20, -10, 0, 10, 20, 30)}
    carry.retain(block(1), 5, ticks, direct=True)
    assert carry.beside() == {-10, 0, 10, 20}
    carry.retain(block(2), -25, ticks, direct=False)
    assert carry.beside() == {-30, -20, -10}
    carry.retain(block(3), 40, ticks, direct=False)
    assert carry.beside() == {20, 30}
