"""On-chain reads behind a pool price series: pool resolution, boundary blocks, states, swaps.

The same reads serve every mode. A backtest reads through its historical gateway, which
resolves each declared pool with its connector's own resolution and keeps swap logs in
compact form; a fork or live gateway reads its chain directly. On a chain sealing blocks
at a fixed interval a boundary block is derived from the previous boundary and its
successor and confirmed by the two headers its evidence needs anyway; elsewhere, or when
the derivation does not hold, it is found by a bracketed search from the previous
boundary. Swap logs are read in windows sized from the pool's trade density.
"""

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from importlib import import_module
from itertools import islice

from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.backtest.history_logs import MAX_LOG_BLOCKS
from almanak.backtest.pool_prices import (
    MAX_SWAP_BLOCKS,
    MAX_SWAP_LOGS,
    BoundaryState,
    PoolSwaps,
    PricePool,
    SeriesEvidence,
    SwapWindow,
    boundary_times,
    compact_swaps,
)
from almanak.backtest.pool_venues import (
    AERODROME_SWAP,
    CRYPTO_EXCHANGE,
    PANCAKE_SWAP,
    STABLE_EXCHANGE,
    SYNC,
    V3_SWAP,
    V4_SWAP,
    pool_facts,
)
from almanak.backtest.pricing import PoolSwapCandles
from almanak.caip import AssetType
from almanak.chains import evm_network
from almanak.gateway.ethereum import DataError, EthereumReader, ProviderBusy, UnreadableAnswer
from almanak.gateway.evm import native_asset
from almanak.models import ZERO_ADDRESS, Address, Block, Chain, Hash32, Model

# Swap windows a backtest reads at once, one gateway request each.
WINDOWS_AHEAD = 8
# A provider refuses a long log range as too large with one of these JSON-RPC codes:
# invalid params (Alchemy, QuickNode), limit exceeded (EIP-1474, Infura), a server
# error (geth and Erigon's "query returned more than 10000 results") or an internal
# error (an Anvil fork forwarding its upstream's refusal).
RANGE_REFUSALS = frozenset((-32602, -32005, -32000, -32603))
# A long swap window the provider refused, or whose answer exceeds `MAX_SWAP_LOGS`.
TOO_LARGE = "swap_window_too_large"
REFUSED = json.dumps(TOO_LARGE)


class PricePoolScope(Model):
    """A declared price pool: the reads a historical gateway admits for it."""

    chain: Chain
    connector: str
    pool: Address | Hash32

    @property
    def v4(self) -> bool:
        return self.connector == "uniswap_v4"

    @property
    def state(self) -> str:
        return self.roles.state_view if self.v4 else self.pool

    @property
    def emitter(self) -> str:
        return self.roles.pool_manager if self.v4 else self.pool

    @property
    def admitted(self) -> tuple[tuple, ...]:
        """The trade event filters the pool's series may read; a Curve pool's depends on
        its family, which resolution decides."""
        if self.v4:
            return ((V4_SWAP, self.pool),)
        return {
            "pancakeswap_v3": ((PANCAKE_SWAP,),),
            "aerodrome": (((SYNC, AERODROME_SWAP),),),
            "curve": ((STABLE_EXCHANGE,), (CRYPTO_EXCHANGE,)),
        }.get(self.connector, ((V3_SWAP,),))

    @property
    def roles(self):
        from almanak.connectors.uniswap_v4.networks import roles_for

        return roles_for(self.chain.chain_id)

    def require_deployment(self) -> None:
        """The connector has a reviewed deployment on the chain; DataError otherwise."""
        networks = import_module(f"almanak.connectors.{self.connector}.networks")
        if self.v4:
            networks.roles_for(self.chain.chain_id)
        elif self.connector == "curve":
            networks.factories_for(self.chain.chain_id)
        else:
            networks.deployment_for(self.chain.chain_id)


def pool_scope(binding) -> PricePoolScope:
    return PricePoolScope(
        chain=binding.chain, connector=binding.source.connector, pool=binding.source.pool
    )


class PoolRequest(Model):
    """Resolve one declared pool with its connector at a canonical block."""

    scope: PricePoolScope
    block: Block

    @property
    def key(self) -> str:
        import hashlib

        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


def resolve_pool(reader: EthereumReader, scope: PricePoolScope, block: Block) -> PricePool:
    """The connector's own resolution of the pool at `block`, verified deployment included."""
    chain = scope.chain.chain_id
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "price pool anchor is no longer canonical")
    module = import_module(f"almanak.connectors.{scope.connector}.resolution")
    if scope.v4:
        roles = scope.roles
        currency0, currency1, fee, spacing, hooks = reader.call(
            roles.position_manager,
            "poolKeys(bytes25)",
            block,
            inputs=("bytes25",),
            args=(bytes.fromhex(scope.pool[2:52]),),
            outputs=("address", "address", "uint24", "int24", "address"),
        )
        if currency0 == currency1:
            raise DataError(
                "unresolved_pool", "Uniswap V4 pool id is unknown to the reviewed PositionManager"
            )
        selection = module.MarketSelection(
            connector="uniswap_v4",
            chain=chain,
            currency0=currency_asset(chain, currency0),
            currency1=currency_asset(chain, currency1),
            fee=fee,
            tick_spacing=spacing,
            hooks=hooks.lower(),
        )
    else:
        selection = module.MarketSelection(connector=scope.connector, chain=chain, pool=scope.pool)
    resolved = module.resolve_market(reader, selection, block)
    if scope.v4:
        from almanak.connectors.uniswap_v4.pool import pool_id

        if pool_id(resolved.pool) != scope.pool:
            raise DataError("unresolved_pool", "resolved Uniswap V4 pool key has another id")
    facts = {"swap_topics": scope.admitted[0]} | pool_facts(
        scope.connector, resolved, reader, block
    )
    return PricePool(
        connector=scope.connector,
        pool=scope.pool,
        block=block,
        state=scope.state,
        emitter=scope.emitter,
        **facts,
    )


def currency_asset(chain, value: str) -> AssetType:
    value = value.lower()
    if value == ZERO_ADDRESS:
        return native_asset(chain)
    return AssetType(f"{chain}/erc20:{value}")


def fetch_swaps(reader: EthereumReader, window: SwapWindow) -> PoolSwaps:
    """A window's trades; a long window refused or answered beyond its log bound is
    TOO_LARGE, which its reader splits. A shorter window fails as before."""
    try:
        raw = reader._rpc("eth_getLogs", window.params)
    except DataError as error:
        if window.long and error.code in RANGE_REFUSALS:
            raise DataError(TOO_LARGE, "provider refused the swap log range") from None
        raise
    if window.long and isinstance(raw, list) and len(raw) > MAX_SWAP_LOGS:
        raise DataError(TOO_LARGE, f"a long swap window holds more than {MAX_SWAP_LOGS} logs")
    try:
        return compact_swaps(window, raw, time.time())
    except ValueError as error:
        raise DataError("malformed", str(error)) from None


class GatewayReads:
    """A backtest's reads through its historical gateway; `history` is its `EvmHistory`."""

    def __init__(self, history):
        self.history = history
        self.reader = history.reader

    @property
    def upper(self) -> Block:
        return self.history.upper

    def header(self, number: int) -> Block:
        return self.history.header(number)

    def first_floor(self, at: int) -> Block:
        return self.history.floor(at)

    def call(self, request: ReadRequest):
        return self.reader._rpc(request.method, list(request.params))

    def warm(self, requests) -> None:
        from almanak.backtest.history import warm

        warm(self.reader, requests)

    def warm_headers(self, numbers) -> None:
        self.history.retain_headers(sorted(set(numbers) - self.history.headers.keys()))

    def pool(self, scope: PricePoolScope, block: Block) -> PricePool:
        result = self.reader.send("price_pool", PoolRequest(scope=scope, block=block))
        return PricePool.model_validate_json(json.dumps(result))

    def swaps(self, windows: list[SwapWindow]) -> list[PoolSwaps | None]:
        """Each window's swaps, None for one too large; windows are independent, so
        several are read at once."""

        def read(window):
            return too_large(
                lambda: PoolSwaps.model_validate_json(
                    json.dumps(self.reader.send("swap_read", window))
                )
            )

        with ThreadPoolExecutor(WINDOWS_AHEAD) as pool:
            return list(pool.map(read, windows))


class DirectReads:
    """A fork or live gateway's reads of its own chain, up to the head pinned at creation."""

    def __init__(self, reader: EthereumReader):
        self.reader = reader
        self.headers: dict[int, Block] = {}
        self.upper = reader.pin_block("latest")
        self.headers[self.upper.number] = self.upper

    def header(self, number: int) -> Block:
        if number not in self.headers:
            self.headers[number] = self.reader.pin_block(number)
        return self.headers[number]

    def first_floor(self, at: int) -> Block:
        return floor_from(self, at, self.header(0))[0]

    def call(self, request: ReadRequest):
        return self.reader._rpc(request.method, list(request.params))

    def warm(self, requests) -> None:
        return None

    def warm_headers(self, numbers) -> None:
        return None

    def pool(self, scope: PricePoolScope, block: Block) -> PricePool:
        with self.reader.retained_pin():
            return resolve_pool(self.reader, scope, block)

    def swaps(self, windows: list[SwapWindow]) -> list[PoolSwaps | None]:
        """Each window's swaps, None for one too large; windows are independent, so
        several are read at once, each in a copy of the caller's context so its reads
        are counted against the caller's request."""

        def read(window):
            return too_large(lambda: fetch_swaps(self.reader, window))

        with ThreadPoolExecutor(WINDOWS_AHEAD) as pool:
            futures = [pool.submit(copy_context().run, read, window) for window in windows]
            return [future.result() for future in futures]


def too_large(read):
    """The read's swaps, or None when its window is too large."""
    try:
        return read()
    except DataError as error:
        if error.kind == TOO_LARGE:
            return None
        raise


def floor_from(reads, at: int, low: Block) -> tuple[Block, Block]:
    """The last block at or before `at` and its successor, searched above `low`.

    Interpolation on block timestamps guesses the block; when a guess fails to halve
    the bracket the next probe bisects it. The probes depend only on `low` and the
    pinned upper header, so a resumed or offline search reads the same headers.
    """
    high = reads.upper
    if low.timestamp > at:
        raise DataError("unavailable_history", "boundary precedes its search bracket")
    if high.timestamp <= at:
        raise DataError("unavailable_history", "no block follows the boundary yet")
    bisect = False
    while high.number - low.number > 1:
        span = high.number - low.number
        if bisect:
            guess = low.number + span // 2
        else:
            guess = low.number + (at - low.timestamp) * span // (high.timestamp - low.timestamp)
        probe = reads.header(min(max(guess, low.number + 1), high.number - 1))
        if probe.timestamp <= at:
            low = probe
        else:
            high = probe
        bisect = not bisect and high.number - low.number > span // 2
    return low, high


def fixed_interval(chain: Chain) -> bool:
    try:
        return evm_network(chain.chain_id).fixed_block_interval
    except ValueError:
        return False


def derived_floor(reads, at: int, anchor: Block, following: Block) -> tuple[Block, Block] | None:
    """On a chain sealing blocks at a fixed interval, the last block at or before `at` and
    its successor, derived from an anchor block and its successor and confirmed by both
    headers; None when the derivation does not hold.

    A confirmed pair is the unique floor, so it is the block a search would find.
    """
    interval = following.timestamp - anchor.timestamp
    if following.number != anchor.number + 1 or interval <= 0:
        return None
    number = anchor.number + (at - anchor.timestamp) // interval
    if not 0 <= number < reads.upper.number:
        return None
    block, successor = reads.header(number), reads.header(number + 1)
    if block.timestamp <= at < successor.timestamp:
        return block, successor
    return None


def boundary_blocks(reads, times, known=None, fixed=False) -> dict[int, tuple[Block, Block]]:
    """Each boundary's floor block and successor, from the boundary before it.

    With `fixed` (the chain's blocks are sealed at a fixed interval) the block is first
    derived from the previous boundary, or from the pinned upper header and its
    predecessor; a derivation that does not hold falls back to the search.
    """
    found: dict[int, tuple[Block, Block]] = dict(known or {})
    ordered = sorted(set(times))
    previous: tuple[Block, Block] | None = None
    for at in ordered:
        if at in found:
            previous = found[at]
            continue
        if previous is None:
            earlier = [found[t] for t in found if t < at]
            previous = max(earlier, key=lambda pair: pair[0].number) if earlier else None
        pair = None
        if previous is None:
            if fixed:
                upper = reads.upper
                pair = derived_floor(reads, at, reads.header(upper.number - 1), upper)
            if pair is None:
                floor = reads.first_floor(at)
                pair = floor, reads.header(floor.number + 1)
            ahead(reads, *pair, [t for t in ordered if t > at and t not in found], fixed)
        else:
            if fixed:
                pair = derived_floor(reads, at, *previous)
            if pair is None:
                pair = floor_from(reads, at, previous[0])
        found[at] = previous = pair
    return {at: found[at] for at in ordered}


def ahead(reads, floor: Block, following: Block, times, fixed=False) -> None:
    """Read ahead the headers the derivations or searches will read first."""
    upper = reads.upper
    if not times or upper.timestamp <= floor.timestamp:
        return
    interval = following.timestamp - floor.timestamp
    if fixed and interval > 0:
        guesses = {floor.number + (t - floor.timestamp) // interval for t in times}
    else:
        pace = (upper.number - floor.number) / (upper.timestamp - floor.timestamp)
        guesses = {floor.number + int((t - floor.timestamp) * pace) for t in times}
    reads.warm_headers(n for g in guesses for n in (g, g + 1) if floor.number < n < upper.number)


def read_states(reads, pool: PricePool, blocks) -> dict[int, BoundaryState]:
    requests = {at: pool.state_requests(block) for at, (block, _) in blocks.items()}
    reads.warm(tuple(request for group in requests.values() for request in group))
    states = {}
    for at, (block, following) in blocks.items():
        results = tuple(reads.call(request) for request in requests[at])
        if not all(isinstance(result, str) for result in results):
            raise DataError("malformed", "pool price read returned no data")
        states[at] = BoundaryState(
            time=at, block=block, following=following, requests=requests[at], results=results
        )
    return states


class SwapPace:
    """The length of the swap windows planned next.

    Until a window is read, the historical log bound; then the length holding about
    half of `MAX_SWAP_LOGS` at the trade density read so far, never shorter than the
    historical bound and shorter than any window found too large. A busy pool reads as
    it would under the historical bound; a quiet one in far fewer windows.
    """

    def __init__(self):
        self.blocks = self.logs = 0
        self.ceiling = MAX_SWAP_BLOCKS

    @property
    def limit(self) -> int:
        if not self.blocks:
            return MAX_LOG_BLOCKS
        wanted = self.blocks * (MAX_SWAP_LOGS // 2) // self.logs if self.logs else self.ceiling
        return max(MAX_LOG_BLOCKS, min(wanted, self.ceiling))

    def read(self, swaps: PoolSwaps) -> None:
        self.blocks += swaps.request.blocks
        self.logs += len(swaps.swaps)

    def refused(self, window: SwapWindow) -> None:
        self.ceiling = min(self.ceiling, window.blocks - 1)


def plan_windows(reads, pool: PricePool, intervals, pace: SwapPace | None = None):
    """Log windows covering each interval's blocks, in order, merged up to the length
    `pace` sets when each window is planned (the historical log bound without one).

    A window ends on an interval's last block or, inside a long interval, on a block
    whose header is read so the window's last logs can be checked against it. Windows
    are planned lazily, so a later window follows the pace of those read before it.
    """

    def limit() -> int:
        return pace.limit if pace else MAX_LOG_BLOCKS

    pending: tuple[int, Block] | None = None
    for after, through in intervals:
        first = after.number + 1
        if pending is not None and through.number - pending[0] < limit():
            pending = pending[0], through
            continue
        if pending is not None:
            yield pool.window(*pending)
            pending = None
        while through.number - first >= limit():
            cut = reads.header(first + limit() - 1)
            yield pool.window(first, cut)
            first = cut.number + 1
        if first <= through.number:
            pending = first, through
    if pending is not None:
        yield pool.window(*pending)


def read_swaps(reads, pool: PricePool, intervals) -> list[PoolSwaps]:
    """Every interval's trades. The first window measures the pool's density, later
    ones are read several at once; a window too large is split and read again."""
    pace = SwapPace()
    known = {block.number: block for pair in intervals for block in pair}
    planned = plan_windows(reads, pool, intervals, pace)
    found: list[PoolSwaps] = []
    count = 1
    while windows := list(islice(planned, count)):
        found += settle(reads, pool, windows, known, pace)
        count = WINDOWS_AHEAD
    return found


def settle(reads, pool, windows, known, pace) -> list[PoolSwaps]:
    found: list[PoolSwaps] = []
    for window, swaps in zip(windows, reads.swaps(windows), strict=True):
        if swaps is None:
            pace.refused(window)
            found += settle(reads, pool, halves(reads, pool, window, known), known, pace)
        else:
            pace.read(swaps)
            found.append(swaps)
    return found


def halves(reads, pool: PricePool, window: SwapWindow, known) -> list[SwapWindow]:
    """A long window split near its middle, on a known boundary block when one lies in
    its middle half, so the cut costs no header read."""
    if not window.long:
        raise DataError("malformed", "a swap window within the log bound cannot be split")
    quarter = window.blocks // 4
    middle = (window.first + window.last) // 2
    near = [n for n in known if window.first + quarter <= n <= window.last - quarter]
    cut = min(near, key=lambda n: (abs(n - middle), n)) if near else middle

    def header(number):
        return known[number] if number in known else reads.header(number)

    return [
        pool.window(window.first, header(cut)),
        pool.window(cut + 1, header(window.last)),
    ]


def acquire(reads, binding, times, known=None, pool: PricePool | None = None) -> SeriesEvidence:
    """The evidence for the points at `times`: boundaries, states and, for candles, swaps."""
    source = binding.source
    scope = pool_scope(binding)
    needed = boundary_times(binding, min(times), max(times))
    blocks = boundary_blocks(reads, needed, known, fixed_interval(binding.chain))
    if pool is None:
        pool = reads.pool(scope, blocks[needed[0]][0])
    try:
        pool.pair(binding.asset, source.quote)
    except ValueError as error:
        raise DataError("unresolved_pool", str(error)) from None
    states = read_states(reads, pool, blocks)
    windows: list[PoolSwaps] = []
    if isinstance(source, PoolSwapCandles):
        intervals = [
            (blocks[t - source.seconds][0], blocks[t][0])
            for t in times
            if blocks[t][0].number > blocks[t - source.seconds][0].number
        ]
        windows = read_swaps(reads, pool, intervals)
    return SeriesEvidence(pool, states.values(), windows)


class PoolGateway:
    """A historical gateway's price pool operations, within the declared pools only.

    The gateway resolves a pool with its connector's trusted resolution code, reading
    through its own capture so an offline run replays the same reads. Swap logs are
    kept compact; the provider's whole answer is represented by its digest.
    """

    def __init__(self, gateway, scopes: tuple[PricePoolScope, ...]):
        self.gateway = gateway
        self.scopes = set(scopes)
        with gateway.capture.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS pool_reads (key TEXT PRIMARY KEY, body TEXT)")

    def retained(self, key):
        with self.gateway.capture.connect() as db:
            row = db.execute("SELECT body FROM pool_reads WHERE key=?", (key,)).fetchone()
        if row is not None:
            self.gateway.capture.metric("pool_cache_hits")
        return None if row is None else row[0]

    def retain(self, key, body):
        with self.gateway.capture.connect() as db:
            db.execute("INSERT OR IGNORE INTO pool_reads VALUES (?, ?)", (key, body))
            retained = db.execute("SELECT body FROM pool_reads WHERE key=?", (key,)).fetchone()
        return retained[0]

    def pool(self, request: PoolRequest) -> PricePool:
        if request.scope not in self.scopes:
            raise ValueError("price pool is outside the declared strategy series")
        body = self.retained(request.key)
        if body is None:
            with CapturedReader(self.gateway, request.scope.chain) as reader:
                resolved = resolve_pool(reader, request.scope, request.block)
            body = self.retain(request.key, resolved.model_dump_json())
        return PricePool.model_validate_json(body)

    def swaps(self, window: SwapWindow) -> PoolSwaps:
        if not any(
            (scope.chain, scope.emitter) == (window.chain, window.address)
            and window.topics in scope.admitted
            for scope in self.scopes
        ):
            raise ValueError("swap window is outside the declared strategy series")
        header = self.gateway.dispatch(
            "read",
            ReadRequest(
                chain=window.chain,
                method="eth_getBlockByNumber",
                params=(hex(window.last), False),
            ),
        ).result
        if not isinstance(header, dict) or header.get("hash") != window.last_hash:
            raise DataError("changed_history", "swap window ends on a noncanonical block")
        body = self.retained(window.key)
        if body is None:
            try:
                body = self.retain(window.key, self.fetch(window).model_dump_json())
            except DataError as error:
                if error.kind != TOO_LARGE:
                    raise
                # Retained, so an offline run splits the same windows.
                body = self.retain(window.key, REFUSED)
        if body == REFUSED:
            raise DataError(TOO_LARGE, "provider refused the swap log window as too large")
        swaps = PoolSwaps.model_validate_json(body)
        if swaps.request != window:
            raise DataError("corrupt_capture", "captured swaps differ from their window")
        return swaps

    def fetch(self, window: SwapWindow) -> PoolSwaps:
        chain = window.chain
        if chain not in self.gateway.readers:
            raise DataError("missing_capture", "offline run is missing required swap logs")
        reader = self.gateway.readers[chain]
        limits = self.gateway.limits.get(chain)
        retries = limits.max_retries if limits is not None else 3
        self.gateway.capture.metric("swap_log_requests")
        print(
            json.dumps({"event": "historical_provider_swaps", "request": window.key}),
            file=sys.stderr,
            flush=True,
        )
        for attempt in range(retries + 1):
            try:
                with self.gateway.provider_gates[chain].admit():
                    return fetch_swaps(reader, window)
            except DataError as error:
                retryable = error.kind in ("http", "transport", "rpc")
                if attempt == retries or not (retryable or isinstance(error, UnreadableAnswer)):
                    raise
                wait = getattr(error, "retry_after", None) if isinstance(error, ProviderBusy) else 0
                time.sleep(min(max(wait or 0.0, 0.5 * 2**attempt), 8.0))
        raise AssertionError("unreachable retry state")


class CapturedReader(EthereumReader):
    """The gateway's own reads for trusted pool resolution, retained in its capture."""

    def __init__(self, gateway, chain: Chain):
        super().__init__(chain, "https://captured-gateway.invalid")
        self.gateway = gateway

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        evidence: ReadEvidence | None = self.gateway.capture.get(request)
        if evidence is None:
            if self.chain not in self.gateway.readers:
                raise DataError("missing_capture", "offline run is missing a pool resolution read")
            result = self.gateway.fetch(self.gateway.readers[self.chain], request)
            evidence = self.gateway.capture.put(request, result)
        if evidence.result is None and not allow_none:
            raise DataError("unavailable", f"{method}: no result")
        return evidence.result


def series_scopes(bindings) -> tuple[PricePoolScope, ...]:
    return tuple(dict.fromkeys(pool_scope(binding) for binding in bindings))
