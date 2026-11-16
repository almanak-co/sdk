"""Declared strategy price series read from a pool's on-chain state.

A synthetic chain fixes block times, pool prices and Swap logs, so boundary blocks,
closes, candles and their evidence are checked against values computed here.
"""

from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.dataset import DataKey, RawEvidence
from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import Capture
from almanak.backtest.history_logs import MAX_LOG_BLOCKS
from almanak.backtest.pool_prices import (
    MAX_SWAP_BLOCKS,
    MAX_SWAP_LOGS,
    PoolSwaps,
    compact_swaps,
    pool_requirement,
    series_records,
    validate_pool_requirement,
    validate_pool_row,
    verify_rows,
)
from almanak.backtest.pool_reads import (
    TOO_LARGE,
    DirectReads,
    GatewayReads,
    PoolGateway,
    PoolRequest,
    SwapPace,
    acquire,
    boundary_blocks,
    fetch_swaps,
    plan_windows,
    pool_scope,
)
from almanak.backtest.pricing import (
    CoinGeckoPrice,
    PoolPrice,
    PoolSwapCandles,
    PriceBinding,
    ProtocolPrice,
)
from almanak.backtest.strategy_prices import HistoricalSeries, acquire_series, point_times
from almanak.backtest.types import ConnectorId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block
from almanak.price_series import series_at
from tests.pool_price_chain import (
    CBBTC,
    CHAIN,
    GENESIS,
    HOUR,
    POOL,
    PRICE_POOL,
    USDC,
    WETH,
    FakeChain,
    sqrt_price,
)


def closes(**changes):
    values = {
        "kind": "pool_state",
        "connector": "uniswap_v3",
        "pool": POOL,
        "timeframe": "1h",
        "max_age_seconds": 2 * HOUR,
        "availability_lag_seconds": 60,
    }
    return PoolPrice(**{**values, **changes})


def candles(**changes):
    return PoolSwapCandles(**{**closes().model_dump(), "kind": "pool_swaps", **changes})


def binding(window=4, source=None, asset=WETH, purpose="strategy"):
    return PriceBinding(
        purpose=purpose, chain=CHAIN, asset=asset, window=window, source=source or closes()
    )


def coin_price():
    return CoinGeckoPrice(
        kind="coingecko",
        provider="market_data",
        coin_id="weth",
        quote="USD",
        interval="hourly",
        max_age_seconds=3700,
        availability="retrospective_timestamp",
        availability_lag_seconds=10,
    )


def test_a_series_names_a_supported_pool_and_bounds_its_age():
    assert binding(window=120).window == 120
    v4 = closes(connector="uniswap_v4", pool="0x" + "ab" * 32)
    assert v4.pool == "0x" + "ab" * 32
    with pytest.raises(ValueError, match="names its pool id"):
        closes(connector="uniswap_v4")
    with pytest.raises(ValueError, match="names its pool id"):
        closes(pool="0x" + "ab" * 32)
    assert closes(connector="curve").connector == "curve"
    with pytest.raises(ValueError):
        closes(connector="gmx_v2")
    with pytest.raises(ValueError, match="maximum age must cover"):
        closes(timeframe="4h")
    with pytest.raises(ValueError, match="only a strategy price declares a series window"):
        binding(window=2, purpose="valuation")
    with pytest.raises(ValueError, match="only a strategy price source"):
        binding(window=1, purpose="valuation")
    with pytest.raises(ValueError, match="reads a pool"):
        binding(window=2, source=coin_price())
    oracle = ProtocolPrice(
        kind="protocol_oracle",
        key=DataKey(connector=ConnectorId("aave_v4"), chain=CHAIN, source="spoke", product="price"),
    )
    with pytest.raises(ValueError, match="reads a pool"):
        binding(window=2, source=oracle)


def test_single_prices_keep_their_identity_and_series_key_by_their_pool():
    for purpose in ("strategy", "valuation"):
        single = binding(window=1, purpose=purpose, source=coin_price())
        assert "window" not in single.model_dump(mode="json")
        # The key a retained market_chart dataset was prepared under.
        assert single.key.source == (
            "433fd99c6400cba625a9122bddbf2f6fc00571d7b9c0782452086f50f25b902a"
        )
    assert binding().key == binding(window=120).key  # the same closes, read further back
    assert str(binding().key.connector) == "uniswap_v3" and binding().key.product == "price"
    assert binding().key != binding(source=candles()).key
    assert PriceBinding.model_validate_json(binding().model_dump_json()) == binding()


def test_a_backtest_series_starts_on_the_boundary_its_first_window_needs():
    start, end = 100 * HOUR, 110 * HOUR
    series = binding(window=24)
    assert series.lookback_seconds == 23 * HOUR
    requirement = pool_requirement(series, start, end)
    # At the start the 99:00 close is the newest available (100:00 needs 60 s more).
    assert requirement.start == 76 * HOUR + 60
    assert (requirement.semantics, requirement.sample_seconds) == ("sampled", HOUR)
    times = point_times(series, start, end)
    assert times[0] == 76 * HOUR and times[-1] == 109 * HOUR  # 110:00 is visible at 110:01
    lagless = binding(window=24, source=closes(availability_lag_seconds=0))
    assert pool_requirement(lagless, start, end).start == 77 * HOUR


def test_boundary_blocks_are_the_last_at_or_before_each_boundary():
    uniform = FakeChain()
    times = [GENESIS + h * HOUR for h in range(1, 18)]
    found = boundary_blocks(DirectReads(uniform), times)
    for at, (block, following) in found.items():
        assert block.number == uniform.floor(at) and following.number == block.number + 1
        assert block.timestamp <= at < following.timestamp
    # A bracketed search from the previous boundary costs a few headers per boundary.
    assert uniform.reads["eth_getBlockByNumber"] <= 40 + 4 * len(times)
    # Irregular block times, with several blocks sharing a timestamp.
    stamps, now = [], GENESIS
    for n in range(40000):
        now += (0, 0, 1, 3, 7)[n * 7919 % 5]
        stamps.append(now)
    ragged = FakeChain(timestamps=stamps)
    times = [GENESIS + h * 1800 for h in range(1, 40)]
    for at, (block, following) in boundary_blocks(DirectReads(ragged), times).items():
        assert block.number == ragged.floor(at)
        assert following.timestamp > at


def test_fixed_interval_boundaries_are_derived_and_confirmed_by_their_own_headers():
    times = [GENESIS + h * HOUR for h in range(1, 18)]
    uniform = FakeChain()
    derived = boundary_blocks(DirectReads(uniform), times, fixed=True)
    assert derived == boundary_blocks(DirectReads(FakeChain()), times)
    # The pinned head and its predecessor give the interval; then each boundary costs
    # only the block and successor its evidence holds.
    assert uniform.reads["eth_getBlockByNumber"] == 2 + 2 * len(times)
    # Where the interval changes (as at Optimism's Bedrock upgrade) or blocks are
    # irregular, a derivation that does not hold falls back to the search.
    changed = [GENESIS + n for n in range(9000)]
    changed += [changed[-1] + 2 * n for n in range(1, 30000)]
    stamps, now = [], GENESIS
    for n in range(40000):
        now += (0, 0, 1, 3, 7)[n * 7919 % 5]
        stamps.append(now)
    for chain in (FakeChain(timestamps=changed), FakeChain(timestamps=stamps)):
        times = [GENESIS + h * 1800 for h in range(1, 30)]
        found = boundary_blocks(DirectReads(chain), times, fixed=True)
        assert all(block.number == chain.floor(at) for at, (block, _) in found.items())
        assert found == boundary_blocks(DirectReads(chain), times)


def price(asset, raw):
    return PRICE_POOL.price(PRICE_POOL.pair(asset), (raw,))


def test_closes_price_the_asset_in_the_pool_s_other_token():
    raw = sqrt_price(Decimal(2500))
    weth = price(WETH, raw)
    assert abs(weth - 2500) < Decimal("1e-20")
    assert abs(price(USDC, raw) * weth - 1) < Decimal("1e-25")
    assert PRICE_POOL.quote(WETH).identifier == USDC
    assert PRICE_POOL.pair(WETH, USDC) == (0, 1)
    # Exact ratio, decimal-adjusted, rounded to 30 significant digits.
    assert price(WETH, 2**96) == Decimal("1000000000000.00000000000000000")
    with pytest.raises(ValueError, match="not one of the pool's tokens"):
        PRICE_POOL.pair(CBBTC)
    with pytest.raises(ValueError, match="not another of the pool's tokens"):
        PRICE_POOL.pair(WETH, CBBTC)
    with pytest.raises(ValueError, match="no price"):
        price(WETH, 0)


def evidence(chain, series, times):
    return acquire(DirectReads(chain), series, times, pool=PRICE_POOL)


def test_closes_are_the_pool_state_at_each_boundary_block():
    chain = FakeChain()
    series = binding(window=3)
    times = range(GENESIS + 2 * HOUR, GENESIS + 6 * HOUR, HOUR)
    found = evidence(chain, series, times)
    rows = series_records(series, found, times, 0.0 + 10**10)
    for row in rows:
        floor = chain.floor(row.source_time)
        assert row.reference.block.number == floor
        assert row.payload.close == price(WETH, chain.price(floor))
        assert row.payload.quote == USDC and row.payload.open is None
        assert row.available_at == row.source_time + 60
    # A closes series reads no swap logs.
    assert chain.reads["eth_getLogs"] == 0


def swap_chain():
    """Two swaps in the second hour and none after; the pool price moves only by swaps."""
    first = HOUR // 2  # the block at the first boundary, an hour after genesis
    swaps = [
        (first + 100, -5 * 10**17, 1_300_000_000, sqrt_price(Decimal(2600))),
        (first + 900, 2 * 10**18, -3_900_000_000, sqrt_price(Decimal(1950))),
    ]

    def price(number):
        moved = [swap[3] for swap in swaps if swap[0] <= number]
        return moved[-1] if moved else sqrt_price(Decimal(2000))

    return FakeChain(swaps=swaps, price=price), first


def test_swap_candles_open_at_the_previous_close_and_span_every_swap():
    chain, first = swap_chain()
    series = binding(window=2, source=candles())
    t1, t2 = GENESIS + 2 * HOUR, GENESIS + 3 * HOUR
    found = evidence(chain, series, [t1, t2])
    traded, quiet = series_records(series, found, [t1, t2], 10.0**10)
    open_price = price(WETH, chain.price(chain.floor(t1 - HOUR)))
    close_price = price(WETH, chain.price(chain.floor(t1)))
    assert traded.payload.open == open_price and traded.payload.close == close_price
    assert traded.payload.high == price(WETH, sqrt_price(Decimal(2600)))
    assert traded.payload.low == price(WETH, sqrt_price(Decimal(1950)))
    # Volume: absolute USDC amounts, in whole USDC.
    assert traded.payload.volume == Decimal("5200")
    assert traded.reference.swaps == 2 and traded.reference.opened.number == chain.floor(t1 - HOUR)
    # No swaps: the pool still has a price, and the candle is flat with no volume.
    assert quiet.payload.open == quiet.payload.high == quiet.payload.low == close_price
    assert quiet.payload.close == close_price and quiet.payload.volume == 0
    assert quiet.reference.swaps == 0


def test_a_point_is_visible_only_after_its_boundary_and_lag():
    chain = FakeChain()
    series = binding(window=3)
    times = range(GENESIS + HOUR, GENESIS + 6 * HOUR, HOUR)
    rows = series_records(series, evidence(chain, series, times), times, 10.0**10)
    boundary = GENESIS + 5 * HOUR
    before = series_at(series, rows, boundary + 59)
    assert before.latest.source_time == boundary - HOUR and before.complete
    after = series_at(series, rows, boundary + 60)
    assert [p.source_time for p in after.points] == [boundary - 2 * HOUR, boundary - HOUR, boundary]
    assert after.quote == USDC and after.values[-1] == rows[-1].payload.close
    with pytest.raises(ValueError, match="requires a pool swap candle series"):
        after.highs  # noqa: B018
    short = series_at(binding(window=6), rows, boundary + 60)
    stale = series_at(series, rows, boundary + 3 * HOUR)
    assert not short.complete and not stale.complete and not stale.gaps


def test_an_asset_outside_the_pool_is_refused():
    with pytest.raises(DataError, match="not one of the pool's tokens"):
        evidence(FakeChain(), binding(asset=CBBTC), [GENESIS + 2 * HOUR])


def prepared_rows(series, chain=None):
    """Rows a backtest prepares through its historical gateway, and their evidence."""
    chain = chain or FakeChain()
    start, end = GENESIS + 6 * HOUR, GENESIS + 9 * HOUR
    rows, bodies = acquire_series(EvmHistory(chain), series, start, end)
    return rows, {item.digest: item for item in bodies}, (start, end), chain


def test_prepared_rows_are_rebuilt_from_the_evidence_they_name():
    for source in (closes(), candles()):
        series = binding(window=3, source=source)
        chain, _ = swap_chain()
        rows, bodies, window, chain = prepared_rows(series, chain)
        requirement = pool_requirement(series, *window)
        assert rows[0].available_at == requirement.start
        validate_pool_requirement(requirement, series, *window)
        for row in rows:
            validate_pool_row(row, series)
        verify_rows(series, rows, bodies.__getitem__)
        changed = rows[-1].model_copy(
            update={"payload": rows[-1].payload.model_copy(update={"close": Decimal(1)})}
        )
        with pytest.raises(ValueError, match="differs from the evidence"):
            verify_rows(series, (*rows[:-1], changed), bodies.__getitem__)
        missing = rows[-1].model_copy(update={"evidence": rows[-1].evidence[:1]})
        with pytest.raises(ValueError, match="lacks its boundary state"):
            verify_rows(series, (missing,), bodies.__getitem__)
        other = binding(window=3, source=source.model_copy(update={"availability_lag_seconds": 30}))
        with pytest.raises(ValueError, match="declared asset and source"):
            validate_pool_row(rows[0], other)
        tight = requirement.model_copy(update={"max_age_seconds": HOUR * 3})
        with pytest.raises(ValueError, match="differs from the requested semantics"):
            validate_pool_requirement(tight, series, *window)


def test_a_prepared_candle_names_only_the_swap_windows_of_its_blocks():
    series = binding(window=3, source=candles())
    chain, first = swap_chain()
    rows, bodies, _, chain = prepared_rows(series, chain)
    # One window per candle here (an hour is 1800 blocks): each row names its own.
    windows = [
        PoolSwaps.model_validate_json(bodies[d].body)
        for row in rows
        for d in row.evidence
        if '"kind":"pool_swaps"' in bodies[d].body
    ]
    assert len(windows) == len(rows)
    assert chain.reads["swap_read"] >= 1


def window(first=10, last=20):
    return PRICE_POOL.window(first, FakeChain().header(last))


def test_swap_logs_are_checked_and_kept_compact():
    chain = FakeChain(swaps=[(15, -1, 2, sqrt_price(Decimal(2000)))])
    good = chain._rpc("eth_getLogs", window().params)
    compact = compact_swaps(window(), good, 1.0)
    assert compact.swaps == ((15, 0, -1, 2, sqrt_price(Decimal(2000))),)
    assert len(compact.response) == 64
    tampered = [
        ({"removed": True}, "non-removed"),
        ({"address": "0x" + "11" * 20}, "requested pool or event"),
        ({"topics": ["0x" + "22" * 32]}, "requested pool or event"),
        ({"blockNumber": hex(21)}, "outside its requested block range"),
        ({"data": "0x00"}, "lacks its event fields"),
    ]
    for change, message in tampered:
        with pytest.raises(ValueError, match=message):
            compact_swaps(window(), [good[0] | change], 1.0)
    last = good[0] | {"blockNumber": hex(20), "blockHash": "0x" + "33" * 32}
    with pytest.raises(ValueError, match="block hash differs"):
        compact_swaps(window(), [last], 1.0)
    with pytest.raises(ValueError, match="distinct canonical log order"):
        compact_swaps(window(), good + good, 1.0)
    far = Block(chain=CHAIN, number=10 + MAX_SWAP_BLOCKS, hash="0x" + "55" * 32, timestamp=1)
    with pytest.raises(ValueError, match=f"spans 1 to {MAX_SWAP_BLOCKS} blocks"):
        PRICE_POOL.window(10, far)


def test_swap_windows_merge_short_intervals_and_split_long_ones():
    chain = FakeChain(blocks=12000)
    reads = DirectReads(chain)
    short = [(chain.header(n), chain.header(n + 600)) for n in range(0, 2400, 600)]
    spans = [(w.first, w.last) for w in plan_windows(reads, PRICE_POOL, short)]
    assert spans == [(1, 1800), (1801, 2400)]
    long = [(chain.header(0), chain.header(4500))]
    spans = [(w.first, w.last) for w in plan_windows(reads, PRICE_POOL, long)]
    assert spans == [(1, 2000), (2001, 4000), (4001, 4500)]


def test_the_swap_window_pace_follows_the_trade_density_within_its_bounds():
    pace = SwapPace()
    assert pace.limit == MAX_LOG_BLOCKS  # nothing read yet
    pace.blocks, pace.logs = 2000, 0
    assert pace.limit == MAX_SWAP_BLOCKS
    pace.logs = 20  # 1 log per 100 blocks: windows of about half the log bound
    assert pace.limit == 100 * MAX_SWAP_LOGS // 2
    pace.logs = 20_000  # a busy pool keeps the historical bound
    assert pace.limit == MAX_LOG_BLOCKS
    pace.logs = 20
    pace.refused(PRICE_POOL.window(1, FakeChain().header(30_000)))
    assert pace.limit == 29_999


def candle_chain(per_hour, hours=30, log_cap=10_000):
    """A pool with `per_hour` swaps spread over each hour, its price set by each swap."""
    step = 1800 // per_hour
    swaps = [
        (n, 10**15, -2_000_000, sqrt_price(Decimal(2000 + (n % 97))))
        for n in range(step, hours * 1800, step)
    ]

    def price(number):
        return sqrt_price(Decimal(2000 + (number - number % step) % 97))

    return FakeChain(blocks=hours * 1800 + 2, swaps=swaps, price=price, log_cap=log_cap)


def candle_rows(chain, times):
    series = binding(window=len(times), source=candles())
    found = acquire(DirectReads(chain), series, times, pool=PRICE_POOL)
    windows = [(w.request.first, w.request.last) for w in found.windows]
    assert all(a[1] + 1 == b[0] for a, b in zip(windows, windows[1:], strict=False))
    return [r.payload for r in series_records(series, found, times, 1e10)], windows


def test_quiet_pools_read_long_swap_windows_and_split_those_refused():
    times = range(GENESIS + 2 * HOUR, GENESIS + 29 * HOUR, HOUR)
    quiet = candle_chain(per_hour=4)
    rows, windows = candle_rows(quiet, times)
    # The first window measures the pool; the rest is one window.
    assert len(windows) == 2 and windows[0][1] - windows[0][0] < MAX_LOG_BLOCKS
    assert quiet.reads["eth_getLogs"] == 2
    # A provider refusing long ranges of more than 20 logs: the long window is split
    # on boundary blocks until each part is accepted; the points are the same.
    strict = candle_chain(per_hour=4, log_cap=20)
    assert candle_rows(strict, times)[0] == rows
    refused = [r for r in strict.log_ranges if r not in candle_rows(candle_chain(4), times)[1]]
    assert refused and strict.reads["eth_getLogs"] > 2
    assert all(last - first < MAX_SWAP_BLOCKS for first, last in strict.log_ranges)


def test_busy_pools_keep_the_historical_swap_windows():
    times = range(GENESIS + 2 * HOUR, GENESIS + 8 * HOUR, HOUR)
    busy = candle_chain(per_hour=1800, hours=9)
    busy.swaps = [(n, *rest) for n, *rest in busy.swaps for _ in range(3)]
    candle_rows(busy, times)
    # Three trades a block: windows stay within the historical log bound, one an hour.
    assert all(last - first < MAX_LOG_BLOCKS for first, last in busy.log_ranges)
    assert len(busy.log_ranges) == len(times)


def test_a_long_window_beyond_its_log_bound_is_too_large_and_a_short_one_fails():
    chain = FakeChain(swaps=[(n, 1, -1, sqrt_price(Decimal(2000))) for n in range(1, 10_010)])
    long = PRICE_POOL.window(1, chain.header(MAX_LOG_BLOCKS + 10))
    chain.log_cap = 10**9  # a provider answering every log of a long range
    with pytest.raises(DataError, match=f"more than {MAX_SWAP_LOGS} logs") as error:
        fetch_swaps(chain, PRICE_POOL.window(1, chain.header(10_020)))
    assert error.value.kind == TOO_LARGE
    chain.log_cap = 10
    with pytest.raises(DataError) as error:
        fetch_swaps(chain, long)
    assert error.value.kind == TOO_LARGE

    def refuse(method, params):
        raise DataError("http", "eth_getLogs: HTTP 400", code=-32602)

    # A window within the historical bound fails as before: it is never split.
    short = PRICE_POOL.window(1, chain.header(MAX_LOG_BLOCKS))
    with pytest.raises(DataError) as error:
        fetch_swaps(SimpleNamespace(_rpc=refuse), short)
    assert error.value.kind == "http"
    with pytest.raises(DataError) as error:
        fetch_swaps(SimpleNamespace(_rpc=refuse), long)
    assert error.value.kind == TOO_LARGE


def test_a_provider_refusal_carries_its_json_rpc_code():
    def refuse(request):
        body = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "too big"}}
        return httpx.Response(400, json=body)

    client = httpx.Client(transport=httpx.MockTransport(refuse))
    reader = EthereumReader(CHAIN, "https://provider.invalid", client=client)
    with pytest.raises(DataError, match="HTTP 400") as error:
        reader._rpc("eth_getLogs", [{}])
    assert (error.value.kind, error.value.code) == ("http", -32602)


def test_a_backtest_reads_the_pool_once_through_its_gateway():
    rows, _, _, chain = prepared_rows(binding(window=3))
    assert chain.reads["price_pool"] == 1 and chain.reads["swap_read"] == 0
    assert all(row.payload.quote == USDC for row in rows)
    rows_again, _, _, _ = prepared_rows(binding(window=3))
    assert [r.payload for r in rows_again] == [r.payload for r in rows]


def test_a_gateway_resolves_and_reads_only_declared_pools(tmp_path):
    chain = FakeChain(swaps=[(15, -1, 2, sqrt_price(Decimal(2000)))])
    gateway = SimpleNamespace(
        capture=Capture(tmp_path / "capture.sqlite"),
        readers={CHAIN: chain},
        limits={},
        provider_gates={CHAIN: SimpleNamespace(admit=nullcontext)},
        dispatch=lambda method, request: SimpleNamespace(
            result=chain._rpc(request.method, list(request.params))
        ),
    )
    pools = PoolGateway(gateway, (pool_scope(binding()),))
    first = pools.swaps(window())
    assert pools.swaps(window()) == first and chain.reads["eth_getLogs"] == 1  # retained
    other = window().model_copy(update={"address": "0x" + "11" * 20})
    with pytest.raises(ValueError, match="outside the declared strategy series"):
        pools.swaps(other)
    foreign = pool_scope(binding(source=closes(pool="0x" + "12" * 20)))
    with pytest.raises(ValueError, match="outside the declared strategy series"):
        pools.pool(PoolRequest(scope=foreign, block=chain.header(1)))
    stale = window().model_copy(update={"last_hash": "0x" + "44" * 32})
    with pytest.raises(DataError, match="noncanonical block"):
        pools.swaps(stale)
    # A long window the provider refused is retained as refused, so an offline run
    # splits the same windows.
    chain.log_cap = 0
    long = PRICE_POOL.window(10, chain.header(10 + MAX_LOG_BLOCKS))
    for readers in ({CHAIN: chain}, {}):
        gateway.readers = readers
        with pytest.raises(DataError, match="refused the swap log window") as error:
            pools.swaps(long)
        assert error.value.kind == TOO_LARGE
    assert chain.reads["eth_getLogs"] == 2
    gateway.capture.close()


def test_a_historical_run_slices_the_series_it_read_at_open():
    chain = FakeChain()
    settings = SimpleNamespace(
        schedule=SimpleNamespace(start=GENESIS + 6 * HOUR, deadline=GENESIS + 9 * HOUR),
        data=SimpleNamespace(prices=(binding(window=3),)),
    )
    prices = HistoricalSeries({CHAIN: EvmHistory(chain)}, settings)
    (first,) = prices.at(GENESIS + 6 * HOUR)
    assert first.complete and first.latest.source_time == GENESIS + 5 * HOUR
    reads = sum(chain.reads.values())
    (later,) = prices.at(GENESIS + 6 * HOUR + 60)
    assert later.latest.source_time == GENESIS + 6 * HOUR and sum(chain.reads.values()) == reads


def test_gateway_and_direct_reads_see_the_same_points():
    series = binding(window=3, source=candles())
    times = range(GENESIS + 3 * HOUR, GENESIS + 6 * HOUR, HOUR)
    chain, _ = swap_chain()
    direct = series_records(
        series, acquire(DirectReads(chain), series, times, pool=PRICE_POOL), times, 1e10
    )
    chain, _ = swap_chain()
    gateway = series_records(
        series, acquire(GatewayReads(EvmHistory(chain)), series, times), times, 1e10
    )
    assert [r.payload for r in direct] == [r.payload for r in gateway]
    assert [r.reference for r in direct] == [r.reference for r in gateway]


def test_raw_evidence_is_compact():
    series = binding(window=3, source=candles())
    chain, _ = swap_chain()
    _, bodies, _, _ = prepared_rows(series, chain)
    assert all(isinstance(item, RawEvidence) for item in bodies.values())
    # No header body or raw log is retained: only boundary blocks, reads and swaps.
    assert all(
        '"logsBloom"' not in item.body and '"transactionHash"' not in item.body
        for item in bodies.values()
    )


def test_swap_logs_answered_with_a_page_that_is_not_json_are_read_again(tmp_path, monkeypatch):
    from almanak.backtest import pool_reads
    from almanak.gateway.ethereum import UnreadableAnswer

    monkeypatch.setattr(pool_reads.time, "sleep", lambda seconds: None)
    chain = FakeChain(swaps=[(15, -1, 2, sqrt_price(Decimal(2000)))])
    read = chain._rpc
    failures = [UnreadableAnswer("malformed", "eth_getLogs: response is not JSON")]

    def flaky(method, params, **kwargs):
        if method == "eth_getLogs" and failures:
            raise failures.pop()
        return read(method, params, **kwargs)

    chain._rpc = flaky
    gateway = SimpleNamespace(
        capture=Capture(tmp_path / "capture.sqlite"),
        readers={CHAIN: chain},
        limits={},
        provider_gates={CHAIN: SimpleNamespace(admit=nullcontext)},
        dispatch=lambda method, request: SimpleNamespace(
            result=read(request.method, list(request.params))
        ),
    )
    pools = PoolGateway(gateway, (pool_scope(binding()),))
    assert pools.swaps(window()).request == window() and not failures
    # A malformed answer that is JSON is not a provider failure, so it is not read again.
    failures.append(DataError("malformed", "eth_getLogs: invalid log"))
    with pytest.raises(DataError, match="invalid log"):
        pools.fetch(window())
    gateway.capture.close()
