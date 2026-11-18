"""Strategy price series built from a pool's own on-chain state, in every run mode.

A point is dated by a candle boundary `t`. Its close is the pool's price at the end of
the last block at or before `t`, read from the pool's state at that block, so a quiet
interval still has a close and a series has no gaps. A swap candle also opens at the
previous boundary's close and spans every trade the pool emitted between the two
boundary blocks. Prices are the declared asset in its quote, another coin of the pool,
decimal-adjusted and rounded to `PRECISION` significant digits. How each kind of pool
is read and priced is `pool_venues`.

Retained evidence holds only what rebuilding and checking a point needs: the resolved
pool, each boundary's two headers and raw state reads, and each log window's trade
events in compact form with a digest of the provider's whole answer.
"""

import hashlib
import json
from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import DataRequirement, Digest, RawEvidence
from almanak.backtest.history import ReadRequest
from almanak.backtest.history_logs import MAX_LOG_BLOCKS
from almanak.backtest.pool_venues import (
    LAYOUTS,
    Pricing,
    coin_values,
    event_fields,
    state_calls,
    state_values,
    trade,
)
from almanak.backtest.pricing import (
    PoolPricePoint,
    PoolPriceRecord,
    PoolPriceReference,
    PoolSwapCandles,
)
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.connectors.curve.stable_math import StableRevision
from almanak.models import Address, Asset, Block, Chain, Hash32, Model, UInt

PRECISION = 30
UNIT = "quote_per_asset"
REVISIONS = {"pool_state": "pool-state-closes-v1", "pool_swaps": "pool-swap-candles-v1"}
# A price series' swap window may span far more blocks than the historical gateway's
# other log reads (`MAX_LOG_BLOCKS`): providers bound a long range's answer by its log
# count, not its blocks (Alchemy answers at most 10,000 logs beyond 10,000 blocks). A
# window longer than `MAX_LOG_BLOCKS` holds at most `MAX_SWAP_LOGS` logs; a larger
# answer, or a provider's refusal of the range, splits it (`pool_reads.read_swaps`).
MAX_SWAP_BLOCKS = 500_000
MAX_SWAP_LOGS = 10_000
# An event topic, or the alternatives one log filter position matches.
Topic = Hash32 | tuple[Hash32, ...]
Topics = Annotated[tuple[Topic, ...], Field(min_length=1, max_length=2)]
# Coins of each pricing kind; Curve StableSwap pools hold two to eight.
COINS = {"curve_stableswap": range(2, 9), "curve_tricrypto": range(3, 4)}


class PricePool(Model):
    """A pool its connector resolved and verified, with the contracts its price reads use."""

    kind: Literal["price_pool"] = "price_pool"
    connector: str
    pool: Address | Hash32
    block: Block
    # The pool's coins in its own order.
    assets: Annotated[tuple[Asset, ...], Field(min_length=2, max_length=8)]
    pricing: Pricing
    # A Curve StableSwap pool's integer revision, which its invariant follows.
    revision: StableRevision | None = Field(default=None, exclude_if=lambda v: v is None)
    # The contract holding the pool price: the pool, or the Uniswap V4 StateView.
    state: Address
    # The contract emitting the pool's trade events: the pool, or the V4 PoolManager.
    emitter: Address
    swap_topics: Topics

    @model_validator(mode="after")
    def coins(self):
        if len(self.assets) not in COINS.get(self.pricing, range(2, 3)):
            raise ValueError(f"a {self.pricing} pool cannot hold {len(self.assets)} coins")
        if (self.pricing == "curve_stableswap") != (self.revision is not None):
            raise ValueError("only a Curve StableSwap pool names its integer revision")
        return self

    def pair(self, asset: AssetType, quote: AssetType | None = None) -> tuple[int, int]:
        """The coin indices of the declared asset and its quote.

        The quote defaults to the other coin of a two-coin pool; a pool with more
        coins needs it declared.
        """
        coins: list = [item.identifier for item in self.assets]
        if asset not in coins:
            raise ValueError(f"price asset {asset} is not one of the pool's tokens")
        if quote is None and len(coins) > 2:
            raise ValueError(
                f"a pool of {len(coins)} coins prices {asset} only in a declared quote asset"
            )
        if quote is None:
            quote = coins[1 - coins.index(asset)]
        if quote not in coins or quote == asset:
            raise ValueError(f"price quote {quote} is not another of the pool's tokens")
        return coins.index(asset), coins.index(quote)

    def quote(self, asset: AssetType, quote: AssetType | None = None) -> Asset:
        return self.assets[self.pair(asset, quote)[1]]

    def price(self, pair: tuple[int, int], state: tuple[int, ...]) -> Decimal:
        """The asset in its quote from the pool's raw state, decimal-adjusted."""
        values = coin_values(self, state)
        return rounded(values[pair[0]] / values[pair[1]])

    def state_requests(self, block: Block) -> tuple[ReadRequest, ...]:
        return tuple(
            ReadRequest(
                chain=self.block.chain,
                method="eth_call",
                params=(
                    {"to": to, "data": data},
                    {"blockHash": block.hash, "requireCanonical": True},
                ),
            )
            for to, data in state_calls(self)
        )

    def window(self, first: int, last: Block) -> "SwapWindow":
        return SwapWindow(
            chain=self.block.chain,
            address=self.emitter,
            topics=self.swap_topics,
            first=first,
            last=last.number,
            last_hash=last.hash,
        )


def rounded(value: Fraction) -> Decimal:
    with localcontext(prec=PRECISION, rounding=ROUND_HALF_EVEN):
        return Decimal(value.numerator) / Decimal(value.denominator)


class BoundaryState(Model):
    """One boundary's close: the floor block, its successor and the raw state reads."""

    kind: Literal["boundary_state"] = "boundary_state"
    time: UnixTime
    block: Block
    following: Block
    requests: Annotated[tuple[ReadRequest, ...], Field(min_length=1)]
    results: tuple[Annotated[str, Field(pattern=r"^0x(?:[0-9a-f]{2})*$")], ...]

    @model_validator(mode="after")
    def floor(self):
        if self.following.number != self.block.number + 1 or self.following.chain != (
            self.block.chain
        ):
            raise ValueError("boundary evidence needs the floor block and its successor")
        if not self.block.timestamp <= self.time < self.following.timestamp:
            raise ValueError("boundary block is not the last block at or before its time")
        anchor = {"blockHash": self.block.hash, "requireCanonical": True}
        if len(self.results) != len(self.requests) or any(
            request.method != "eth_call" or not request.params or request.params[-1] != anchor
            for request in self.requests
        ):
            raise ValueError("boundary price read is not anchored to its floor block")
        return self

    def state(self, pool: PricePool) -> tuple[int, ...]:
        """The pool's raw state at this boundary; the reads must be the pool's own."""
        if self.requests != pool.state_requests(self.block):
            raise ValueError("boundary price read differs from its pool's state reads")
        return state_values(pool, self.results)


class SwapWindow(Model):
    """One bounded Swap log read: blocks `first` through `last` of one pool's emitter."""

    chain: Chain
    address: Address
    topics: Topics
    first: UInt
    last: UInt
    # The hash of block `last`; logs in that block must name it.
    last_hash: Hash32

    @model_validator(mode="after")
    def bounded(self):
        if not 0 <= self.last - self.first < MAX_SWAP_BLOCKS:
            raise ValueError(f"a swap window spans 1 to {MAX_SWAP_BLOCKS} blocks")
        return self

    @property
    def blocks(self) -> int:
        return self.last - self.first + 1

    @property
    def long(self) -> bool:
        """Longer than the historical log bound, so its answer is bounded by log count."""
        return self.blocks > MAX_LOG_BLOCKS

    @property
    def key(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def params(self) -> list:
        return [
            {
                "address": self.address,
                "topics": [list(t) if isinstance(t, tuple) else t for t in self.topics],
                "fromBlock": hex(self.first),
                "toBlock": hex(self.last),
            }
        ]


# (block, log index, *fields) of one trade event; `pool_venues.LAYOUTS` names the
# fields of each event, such as (amount0, amount1, sqrtPriceX96) of a V3 Swap.
Swap = Annotated[tuple[int, ...], Field(min_length=4, max_length=6)]


class PoolSwaps(Model):
    """A trade window's logs in compact form; `response` hashes the provider's answer."""

    kind: Literal["pool_swaps"] = "pool_swaps"
    request: SwapWindow
    swaps: tuple[Swap, ...]
    response: Digest
    retrieved_at: Annotated[float, Field(allow_inf_nan=False, ge=0)]

    @model_validator(mode="after")
    def ordered(self):
        orders = [swap[:2] for swap in self.swaps]
        if any(min(order) < 0 for order in orders) or orders != sorted(set(orders)):
            raise ValueError("swap evidence must keep distinct canonical log order")
        if any(not self.request.first <= swap[0] <= self.request.last for swap in self.swaps):
            raise ValueError("swap evidence lies outside its log window")
        return self


PoolEvidence = Annotated[PricePool | BoundaryState | PoolSwaps, Field(discriminator="kind")]


def topic_matches(topics: list[str], expected) -> bool:
    if len(topics) < len(expected):
        return False
    return all(
        found in wanted if isinstance(wanted, tuple) else found == wanted
        for found, wanted in zip(topics, expected, strict=False)
    )


def compact_swaps(window: SwapWindow, raw, retrieved_at: float) -> PoolSwaps:
    """Check a provider's trade logs against their window and keep what candles need."""
    if not isinstance(raw, list):
        raise ValueError("swap logs must be an array")
    swaps = []
    for item in raw:
        if not isinstance(item, dict) or item.get("removed") is not False:
            raise ValueError("swap logs must be canonical, non-removed events")
        try:
            number = int(item["blockNumber"], 16)
            index = int(item["logIndex"], 16)
            topics = [str(topic).lower() for topic in item["topics"]]
            data = bytes.fromhex(str(item["data"])[2:])
            emitter = str(item["address"]).lower()
        except (KeyError, TypeError, ValueError):
            raise ValueError("swap log is missing required event evidence") from None
        if emitter != window.address or not topic_matches(topics, window.topics):
            raise ValueError("swap log differs from its requested pool or event")
        if topics[0] not in LAYOUTS:
            raise ValueError("swap log differs from its requested pool or event")
        if not window.first <= number <= window.last:
            raise ValueError("swap log lies outside its requested block range")
        if number == window.last and item.get("blockHash") != window.last_hash:
            raise ValueError("swap log block hash differs from the window's canonical header")
        swaps.append((number, index, *event_fields(topics[0], data)))
    body = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return PoolSwaps(
        request=window,
        swaps=tuple(swaps),
        response=hashlib.sha256(body.encode()).hexdigest(),
        retrieved_at=retrieved_at,
    )


def series_start(binding, start: int) -> int:
    """The boundary of the first point a backtest from `start` can show: the warm-up."""
    source = binding.source
    seconds = source.seconds
    warm = start - binding.lookback_seconds - source.availability_lag_seconds
    return warm - warm % seconds


def last_boundary(binding, cutoff: int) -> int:
    """The newest boundary whose point is available at `cutoff`."""
    source = binding.source
    visible = cutoff - source.availability_lag_seconds
    return visible - visible % source.seconds


def boundary_times(binding, first: int, last: int) -> range:
    """Boundaries whose states points from `first` through `last` need, opens included."""
    source = binding.source
    opening = source.seconds if isinstance(source, PoolSwapCandles) else 0
    return range(first - opening, last + 1, source.seconds)


def pool_requirement(binding, start: int, end: int) -> DataRequirement:
    source = binding.source
    return DataRequirement(
        key=binding.key,
        start=UnixTime(series_start(binding, start) + source.availability_lag_seconds),
        end=UnixTime(end),
        # Every boundary has a close, so a point is due every interval.
        semantics="sampled",
        sample_seconds=source.seconds,
        max_age_seconds=source.max_age_seconds,
        unit=UNIT,
        visibility="end_of_block",
        payload_revision=REVISIONS[source.kind],
    )


def build_point(binding, pool, close: BoundaryState, opening: BoundaryState | None, swaps):
    """A point's payload and reference from its boundary states and the trades between."""
    source = binding.source
    pair = pool.pair(binding.asset, source.quote)
    quote = pool.assets[pair[1]]
    state = close.state(pool)
    closing = pool.price(pair, state)
    point: dict[str, object] = {"asset": binding.asset, "quote": quote.identifier}
    point |= {"source": source, "close": closing}
    read: dict[str, object] = {"block": close.block}
    read |= {"sqrt_price_x96": state[0]} if pool.pricing == "sqrt_price" else {"state": state}
    if isinstance(source, PoolSwapCandles):
        if opening is None:
            raise ValueError("a swap candle needs its opening boundary")
        point |= candle(pool, pair, pool.price(pair, opening.state(pool)), closing, swaps)
        read |= {"opened": opening.block, "swaps": point.pop("trades")}
    return PoolPricePoint.model_validate(point), PoolPriceReference.model_validate(read)


def candle(pool, pair, opening: Decimal, closing: Decimal, swaps) -> dict:
    """Open, high, low and quote volume of the trades between two closes, and their count."""
    prices = [opening, closing]
    volume, count = Decimal(0), 0
    for record in swaps:
        price, amount = trade(pool, record, pair)
        if price is not None:
            prices.append(rounded(price))
        if amount is not None:
            volume += Decimal(amount).scaleb(-pool.assets[pair[1]].decimals)
            count += 1
    return {
        "open": opening,
        "high": max(prices),
        "low": min(prices),
        "volume": volume,
        "trades": count,
    }


def window_swaps(windows, after: int, through: int):
    """Swaps in blocks `after + 1` through `through`, and the windows holding them."""
    selected: list[Swap] = []
    used = []
    for window in windows:
        if window.request.last <= after or window.request.first > through:
            continue
        used.append(window)
        selected.extend(swap for swap in window.swaps if after < swap[0] <= through)
    if not windows_cover(used, after, through):
        raise ValueError("swap evidence does not cover a candle's blocks")
    return selected, used


def windows_cover(windows, after: int, through: int) -> bool:
    reached = after
    for first, last in sorted((w.request.first, w.request.last) for w in windows):
        if first > reached + 1:
            return False
        reached = max(reached, last)
    return reached >= through


class SeriesEvidence:
    """A pool series' evidence bodies, by boundary and by log window."""

    def __init__(self, pool: PricePool, states: Iterable[BoundaryState], windows):
        self.pool = pool
        self.bodies: dict[str, RawEvidence] = {}
        self.digests: dict[int, str] = {}
        self.pool_digest = self.retain(pool)
        self.states = {int(state.time): state for state in states}
        self.windows = sorted(windows, key=lambda item: item.request.first)
        for item in (*self.states.values(), *self.windows):
            self.digests[id(item)] = self.retain(item)

    def retain(self, item) -> str:
        evidence = RawEvidence(body=item.model_dump_json())
        self.bodies[evidence.digest] = evidence
        return evidence.digest


def series_records(binding, evidence: SeriesEvidence, times: Iterable[int], retrieved_at: float):
    """Prepared rows for the points at `times`, each naming the evidence it came from."""
    source = binding.source
    rows = []
    for time in times:
        close = evidence.states[time]
        opening, swaps, windows = None, [], []
        if isinstance(source, PoolSwapCandles):
            opening = evidence.states[time - source.seconds]
            swaps, windows = window_swaps(
                evidence.windows, opening.block.number, close.block.number
            )
        payload, reference = build_point(binding, evidence.pool, close, opening, swaps)
        digests = [evidence.pool_digest] + [
            evidence.digests[id(item)] for item in (close, opening, *windows) if item is not None
        ]
        rows.append(
            PoolPriceRecord(
                key=binding.key,
                unit=UNIT,
                source_time=UnixTime(time),
                available_at=UnixTime(time + source.availability_lag_seconds),
                retrieved_at=retrieved_at,
                evidence=tuple(dict.fromkeys(digests)),
                reference=reference,
                payload=payload,
            )
        )
    return tuple(rows)


def validate_pool_requirement(requirement, binding, start, end) -> None:
    expected = pool_requirement(binding, start, end)
    if requirement.start > expected.start or requirement.end < end:
        raise ValueError("pool price dataset does not cover the requested window")
    if requirement.model_dump(exclude={"start", "end"}) != expected.model_dump(
        exclude={"start", "end"}
    ):
        raise ValueError("pool price dataset differs from the requested semantics")


def validate_pool_row(row, binding) -> None:
    source = binding.source
    payload, reference = row.payload, row.reference
    if payload.asset != binding.asset or payload.source != source:
        raise ValueError("pool price differs from its declared asset and source")
    if row.source_time % source.seconds or reference.block.timestamp > row.source_time:
        raise ValueError("pool price is not dated by its candle boundary")
    if reference.block.chain != binding.chain:
        raise ValueError("pool price was read on another chain")
    if row.available_at < row.source_time + source.availability_lag_seconds:
        raise ValueError("pool price precedes its configured availability")
    candle = (payload.open, payload.high, payload.low, payload.volume, reference.opened)
    swaps = isinstance(source, PoolSwapCandles)
    if any(value is None for value in candle) == swaps or (reference.swaps is None) == swaps:
        raise ValueError("pool price members differ from its source kind")


def verify_rows(binding, rows, evidence_body) -> None:
    """Rebuild every row from the evidence it names; refuse any difference.

    `evidence_body(digest)` returns a retained body. Each body is parsed once.
    """
    from pydantic import TypeAdapter

    adapter: TypeAdapter = TypeAdapter(PoolEvidence)
    parsed: dict[str, PricePool | BoundaryState | PoolSwaps] = {}

    def load(digest):
        if digest not in parsed:
            parsed[digest] = adapter.validate_json(evidence_body(digest).body)
        return parsed[digest]

    source = binding.source
    for row in rows:
        items = [load(digest) for digest in row.evidence]
        pools = [item for item in items if isinstance(item, PricePool)]
        states = {int(item.time): item for item in items if isinstance(item, BoundaryState)}
        windows = [item for item in items if isinstance(item, PoolSwaps)]
        if (
            len(pools) != 1
            or pools[0].connector != source.connector
            or pools[0].pool != (source.pool)
        ):
            raise ValueError("pool price evidence names another pool")
        close = states.get(int(row.source_time))
        opening = states.get(int(row.source_time) - source.seconds)
        if close is None or (isinstance(source, PoolSwapCandles) and opening is None):
            raise ValueError("pool price evidence lacks its boundary state")
        swaps: list = []
        if isinstance(source, PoolSwapCandles):
            assert opening is not None
            swaps, _ = window_swaps(windows, opening.block.number, close.block.number)
        payload, reference = build_point(binding, pools[0], close, opening, swaps)
        if (payload, reference) != (row.payload, row.reference):
            raise ValueError("pool price differs from the evidence it names")
