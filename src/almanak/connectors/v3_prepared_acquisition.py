"""Verified V3 bounded bitmap/tick acquisition and fixed-growth samples.

A sample holds the state of every initialized tick its bitmap words cover. Reading each
of them at every sample repeats, for almost every tick, the read of the sample before.
A collection pass that retains the pool's liquidity events reads each tick directly once
and then only the ticks that can have changed; the others keep the state last read.

A pool writes a tick in two places. Mint and Burn change its liquidity, and the retained
events name both ticks of every one. A swap that crosses an initialized tick replaces its
outside growth with the global growth minus it. So between two samples a tick that no
event names changes only by being crossed, and:

- the price moved from one side of it to the other: it was crossed and is read;
- otherwise it was crossed an even number of times, and its outside growth rose by the
  global growth accrued while the price was beyond it. A tick farther from the price on
  the same side is beyond the price only while the nearer one is, and global growth does
  not fall. So where a tick's outside growth is unchanged, every tick farther out on that
  side is unchanged too.

Ticks are therefore read outward from the price on each side until one is unchanged, and
the rest are carried. The rule needs only those two writes, which the verified V3 pool
revisions share; periodic audits read every carried tick and refuse a difference.
"""

from bisect import bisect_right
from dataclasses import dataclass, field

from eth_abi import encode
from eth_utils import keccak

from almanak.backtest.history import ReadRequest
from almanak.connectors.concentrated_pool import Word, initialized_ticks
from almanak.connectors.v3_prepared_data import TickState
from almanak.gateway.ethereum import DataError
from almanak.models import Block

# Ticks read on each side of the price before looking for an unchanged one. The second
# costs one read and checks the rule itself: it lies beyond the first, so it must be
# unchanged whenever the first is.
FRONTIER_TICKS = 2
# A sample that reads every carried tick directly after this many carried samples.
AUDIT_SAMPLES = 256
TICK_SELECTOR = "0x" + keccak(text="ticks(int24)")[:4].hex()


def tick_read(request: ReadRequest) -> int | None:
    """The tick a retained request reads from a pool, or None for any other read."""
    call = request.params[0] if request.method == "eth_call" and request.params else None
    data = str(call.get("data", "")) if isinstance(call, dict) else ""
    if not data.startswith(TICK_SELECTOR) or len(data) != len(TICK_SELECTOR) + 64:
        return None
    return int.from_bytes(bytes.fromhex(data[len(TICK_SELECTOR) :]), "big", signed=True)


@dataclass
class Carry:
    """The tick state one collection pass carries from each sample to the next.

    `changes` are the pool's retained liquidity events as (block number, lower tick,
    upper tick) in block order. They must cover every block the pass samples, or a
    liquidity change would be carried over unseen.
    """

    changes: tuple[tuple[int, int, int], ...]
    block: Block | None = None
    tick: int = 0
    states: dict[int, TickState] = field(default_factory=dict)
    carried: int = 0

    def __post_init__(self):
        self.numbers = [number for number, _, _ in self.changes]
        if self.numbers != sorted(self.numbers):
            raise ValueError("liquidity events must retain canonical block order")

    def touched(self, block: Block) -> frozenset[int]:
        """Ticks a liquidity event names after the carried block through `block`."""
        assert self.block is not None
        first = bisect_right(self.numbers, self.block.number)
        last = bisect_right(self.numbers, block.number)
        return frozenset(tick for change in self.changes[first:last] for tick in change[1:])

    def retain(self, block: Block, tick: int, states: dict[int, TickState], direct: bool):
        self.block, self.tick, self.states = block, tick, states
        self.carried = 0 if direct else self.carried + 1

    def beside(self) -> frozenset[int]:
        """The ticks nearest the carried price, which the next sample reads first."""
        ordered = sorted(self.states)
        split = bisect_right(ordered, self.tick)
        return frozenset(ordered[max(0, split - FRONTIER_TICKS) : split + FRONTIER_TICKS])

    def reset(self):
        self.block, self.states, self.carried = None, {}, 0


def read_tick(reader, source, block, tick):
    result = reader.call(
        source.pool,
        "ticks(int24)",
        block,
        inputs=("int24",),
        args=(tick,),
        outputs=("uint128", "int128", "uint256", "uint256", "int56", "uint160", "uint32", "bool"),
    )
    if bool(result[0]) != result[7]:
        raise DataError("inconsistent", "V3 tick initialization differs from its gross liquidity")
    return result


def prefetch(reader, source, block, signature, argument_type, values):
    if not hasattr(reader, "prefetch"):
        return
    reader.prefetch(
        tuple(
            ReadRequest(
                chain=block.chain,
                method="eth_call",
                params=(
                    {
                        "to": source.pool,
                        "data": "0x"
                        + (keccak(text=signature)[:4] + encode((argument_type,), (value,))).hex(),
                    },
                    {"blockHash": block.hash, "requireCanonical": True},
                ),
            )
            for value in values
        )
    )


def bitmap_words(reader, source, block, lower, upper, max_words, max_ticks):
    spacing = source.tick_spacing
    indices = range((lower // spacing) >> 8, ((upper // spacing) >> 8) + 1)
    if not 0 < len(indices) <= max_words or max_ticks <= 0:
        raise ValueError("tick coverage exceeds the explicit bitmap collection limit")
    prefetch(reader, source, block, "tickBitmap(int16)", "int16", indices)
    words = tuple(
        Word(
            index=index,
            bitmap=reader.call(
                source.pool,
                "tickBitmap(int16)",
                block,
                inputs=("int16",),
                args=(index,),
                outputs=("uint256",),
            )[0],
        )
        for index in indices
    )
    initialized = initialized_ticks(words, spacing)
    if len(initialized) > max_ticks:
        raise ValueError("pool exceeds the explicit initialized-tick collection limit")
    return words, initialized


def read_ticks(reader, source, block, indices, tick_reader):
    """Direct state of each tick in `indices` at `block`, read in one batch."""
    prefetch(reader, source, block, "ticks(int24)", "int24", indices)
    states = {}
    for index in indices:
        result = tick_reader(reader, source, block, index)
        states[index] = TickState(
            index=index, gross=result[0], net=result[1], outside0=result[2], outside1=result[3]
        )
    return states


def tick_state(reader, source, block, lower, upper, max_words, max_ticks, *, tick_reader=read_tick):
    words, initialized = bitmap_words(reader, source, block, lower, upper, max_words, max_ticks)
    states = read_ticks(reader, source, block, initialized, tick_reader)
    return words, tuple(states[index] for index in initialized)


def carried_tick_state(reader, source, block, tick, bounds, carry, *, audit, tick_reader):
    """Every initialized tick at `block`, reading only those that can differ from `carry`."""
    words, initialized = bitmap_words(reader, source, block, *bounds)
    if carry.block is None:
        states = read_ticks(reader, source, block, initialized, tick_reader)
        carry.retain(block, tick, states, direct=True)
        return words, tuple(states[index] for index in initialized)

    def read(indices):
        return read_ticks(reader, source, block, indices, tick_reader)

    states, kept = changed_ticks(initialized, tick, carry, carry.touched(block), read)
    direct = audit or carry.carried + 1 >= AUDIT_SAMPLES
    if direct:
        confirm_carried(kept, carry.states, read)
    states.update((index, carry.states[index]) for index in kept)
    carry.retain(block, tick, states, direct)
    return words, tuple(states[index] for index in initialized)


def changed_ticks(initialized, tick, carry, touched, read):
    """Direct state of the ticks that can have changed, and the ticks proven unchanged."""
    previous = carry.states
    low, high = sorted((carry.tick, tick))
    # A tick is initialized or cleared only by a liquidity event that names it.
    if (previous.keys() ^ set(initialized)) - touched:
        raise DataError(
            "inconsistent", "V3 tick was initialized or cleared without a retained liquidity event"
        )
    # New ticks, ticks a liquidity event names and ticks the price moved across.
    changed = [t for t in initialized if t not in previous or t in touched or low < t <= high]
    exposed = set(changed)
    stable = [t for t in initialized if t not in exposed]
    sides = (
        [t for t in reversed(stable) if t <= low],
        [t for t in stable if t > high],
    )
    states = read([*changed, *(t for side in sides for t in side[:FRONTIER_TICKS])])
    kept: list[int] = []
    for side in sides:
        kept += frontier(side, previous, states, read)
    return states, kept


def frontier(side, previous, states, read):
    """Read one side outward from the price until a tick is unchanged; the ticks past it.

    `side` runs outward from the price and holds only ticks no liquidity event names, so
    each must keep its liquidity, and one that keeps its outside growth proves the same
    of every tick after it.
    """
    width, start = FRONTIER_TICKS, 0
    while start < len(side):
        group = side[start : start + width]
        states.update(read([index for index in group if index not in states]))
        for offset, index in enumerate(group):
            before, now = previous[index], states[index]
            if (before.gross, before.net) != (now.gross, now.net):
                raise DataError(
                    "inconsistent", "V3 tick liquidity changed without a retained liquidity event"
                )
            if before == now:
                if any(previous[later] != states[later] for later in group[offset + 1 :]):
                    raise DataError(
                        "inconsistent", "V3 tick growth changed beyond an unchanged tick"
                    )
                return side[start + len(group) :]
        start, width = start + len(group), width * 2
    return []


def confirm_carried(kept, previous, read):
    """Read every tick about to be carried and refuse one that differs from its carry."""
    direct = read(kept)
    if any(direct[index] != previous[index] for index in kept):
        raise DataError("inconsistent", "carried V3 tick state differs from its direct read")


def sample(
    reader,
    source,
    block,
    *,
    lower,
    upper,
    max_words,
    max_ticks,
    read_pool,
    sample_type,
    tick_reader=read_tick,
    carry=None,
    audit=False,
):
    if not -887272 <= lower < upper <= 887272:
        raise ValueError("prepared V3 coverage must have ordered valid tick bounds")
    pool = read_pool(reader, source, block)
    bounds = (lower, upper, max_words, max_ticks)
    if carry is None:
        words, ticks = tick_state(reader, source, block, *bounds, tick_reader=tick_reader)
    else:
        words, ticks = carried_tick_state(
            reader, source, block, pool.tick, bounds, carry, audit=audit, tick_reader=tick_reader
        )
    growth = tuple(
        reader.call(source.pool, f"feeGrowthGlobal{index}X128()", block)[0] for index in (0, 1)
    )
    return sample_type(
        pool=pool, lower=lower, upper=upper, words=words, ticks=ticks, global_growth=growth
    )


def evidence(reader):
    from almanak.backtest.dataset import RawEvidence

    return tuple(RawEvidence(body=reader.cache[k].model_dump_json()) for k in sorted(reader.used))


def prepared_sample(
    history,
    target,
    at,
    policy,
    baseline,
    *,
    sample,
    data,
    contract_identities,
    proof=None,
    **carried,
):
    """One retained sample. `proof` is the continuity proof when the caller already has it:
    the body names every liquidity event and continuity read of the window, so a pass
    serializes it once instead of once per sample."""
    from almanak.backtest.dataset import RawEvidence
    from almanak.backtest.types import UnixTime
    from almanak.connectors.v3_fee_growth import sampled_inside

    reader = history.reader
    anchor = history.floor(UnixTime(at))
    reader.used.clear()
    block = history.pin(anchor)
    if contract_identities(reader, target, block) != baseline.contracts:
        raise ValueError("prepared sample has changed V3 implementation identities")
    bounds = policy.for_target(target)
    observed = sample(
        reader,
        target.pool,
        block,
        lower=bounds.lower,
        upper=bounds.upper,
        max_words=policy.max_bitmap_words,
        max_ticks=policy.max_initialized_ticks,
        **carried,
    )
    if target.range is not None:
        sampled_inside(observed, target)
    if proof is None:
        proof = RawEvidence(body=baseline.model_dump_json())
    payload = data.PreparedSample(**observed.model_dump(), baseline_digest=proof.digest)
    raw = (*evidence(reader), proof)
    return data.Record(
        key=data.key(target),
        unit=data.UNIT,
        payload=payload,
        reference=block,
        source_time=UnixTime(block.timestamp),
        available_at=UnixTime(at),
        retrieved_at=max(reader.cache[k].retrieved_at for k in reader.used),
        evidence=tuple(e.digest for e in raw),
    ), raw
