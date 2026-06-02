"""VIB-4845 (T3-C / T3-D) — RPC/gRPC provider wiring in MarketSnapshot.

Covers the four readers wired by this PR so the corresponding accessors go live:

  - pool_reserves()                  -> GatewayPoolReserveReader (eth_call proxy)
  - liquidity_depth()                -> LiquidityDepthReader (eth_call proxy)
  - estimate_slippage()              -> SlippageEstimator (eth_call proxy)
  - lending_rate_history() /
    funding_rate_history()           -> RateHistoryReader (gateway RateHistoryService)

The epic's acceptance is a parametrized sweep: the LIVE path returns real data
(mock the gateway stub / eth_call) and the BACKTEST path raises
``DataSourceUnavailable``. Both halves are asserted here.

Also covers the two explicitly-deferred accessors:
  - pool_history()        — stays NOT wired (VIB-4755 D-4 / VIB-4730 / VIB-4863).
  - yield_opportunities() — stays NOT wired (no gateway Yield service in proto).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.defi.gateway_pool_reader import GatewayPoolReserveReader
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.null_readers import (
    NullLiquidityDepthReader,
    NullPoolReserveReader,
    NullRateHistoryReader,
    NullSlippageEstimator,
)
from almanak.framework.data.pools.liquidity import LiquidityDepthReader, SlippageEstimator
from almanak.framework.data.rates.history import RateHistoryReader
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.runner.failure_kind import FailureKind, classify_failure


@pytest.fixture(autouse=True)
def _restore_event_loop():
    """Re-establish a current event loop after each test in this module.

    The live accessors here call MarketSnapshot methods (pool_reserves, etc.)
    that bridge to async via ``asyncio.run()`` (``snapshot.py::_run_async_bridged``),
    which sets the current event loop to ``None`` on exit. Without restoring it,
    a later test on the same xdist worker that uses the deprecated
    ``asyncio.get_event_loop()`` pattern (e.g.
    ``tests/framework/observability/test_snapshot_accounting.py``) fails with
    "There is no current event loop in thread 'MainThread'". Clean up after
    ourselves so this module never leaks loop state to co-located tests.
    """
    yield
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


# --------------------------------------------------------------------------- #
# eth_call byte-crafting helpers
# --------------------------------------------------------------------------- #


def _word(value: int, *, signed: bool = False) -> str:
    """Encode an int as one right-aligned 32-byte hex word (no 0x)."""
    return value.to_bytes(32, byteorder="big", signed=signed).hex()


def _addr_word(addr: str) -> str:
    """Encode a 20-byte address as a left-padded 32-byte word."""
    return addr.lower().removeprefix("0x").zfill(64)


# Selector -> handler. Selectors mirror the production reader modules.
_SLOT0 = "0x3850c7bd"
_LIQUIDITY = "0x1a686502"
_TOKEN0 = "0x0dfe1681"
_TOKEN1 = "0xd21220a7"
_FEE = "0xddca3f43"
_DECIMALS = "0x313ce567"
_BALANCE_OF = "0x70a08231"
_TICK_SPACING = "0xd0c93a7c"
_TICK_BITMAP = "0x5339c296"

_TOKEN0_ADDR = "0x1111111111111111111111111111111111111111"  # < token1 (token0)
_TOKEN1_ADDR = "0x2222222222222222222222222222222222222222"

# A realistic sqrtPriceX96 for ~price; value is opaque to these tests (we only
# assert reserves/tick/liquidity flow through, not the derived price).
_SQRT_PRICE_X96 = 79228162514264337593543950336  # = 2**96 -> price 1.0 (raw)
_TICK = -100
_LIQUIDITY_VAL = 123456789
_FEE_TIER = 500
_RESERVE0_RAW = 5_000_000_000  # 5,000 token0 @ 6 decimals
_RESERVE1_RAW = 3_000_000_000_000_000_000  # 3 token1 @ 18 decimals


def _crafted_eth_call(chain: str, to: str, data: str) -> str:  # noqa: ARG001
    """Return crafted hex for the pool / token reads used by the live readers."""
    to_l = to.lower()
    selector = data[:10].lower()

    if selector == _SLOT0:
        # slot0 returns 7 words; only the first two (sqrtPriceX96, tick) are read.
        return "0x" + _word(_SQRT_PRICE_X96) + _word(_TICK, signed=True) + _word(0) * 5
    if selector == _LIQUIDITY:
        return "0x" + _word(_LIQUIDITY_VAL)
    if selector == _TOKEN0:
        return "0x" + _addr_word(_TOKEN0_ADDR)
    if selector == _TOKEN1:
        return "0x" + _addr_word(_TOKEN1_ADDR)
    if selector == _FEE:
        return "0x" + _word(_FEE_TIER)
    if selector == _TICK_SPACING:
        return "0x" + _word(10, signed=True)
    if selector == _TICK_BITMAP:
        return "0x" + _word(0)  # empty word -> no initialized ticks
    if selector == _DECIMALS:
        # token0 -> 6 decimals, token1 -> 18 decimals.
        return "0x" + _word(6 if to_l == _TOKEN0_ADDR else 18)
    if selector == _BALANCE_OF:
        # balanceOf(pool): token0 holds reserve0, token1 holds reserve1.
        return "0x" + _word(_RESERVE0_RAW if to_l == _TOKEN0_ADDR else _RESERVE1_RAW)
    return "0x"


class _FakeRateHistoryStub:
    """Stand-in for GatewayClient.rate_history with lending + funding RPCs."""

    def __init__(self, lending_points=None, funding_points=None):
        self._lending_points = lending_points or []
        self._funding_points = funding_points or []
        self.lending_calls = []
        self.funding_calls = []

    def GetLendingRateHistory(self, request):  # noqa: N802 — mirrors gRPC stub
        self.lending_calls.append(request)
        return SimpleNamespace(success=True, error="", points=list(self._lending_points))

    def GetFundingRateHistory(self, request):  # noqa: N802 — mirrors gRPC stub
        self.funding_calls.append(request)
        return SimpleNamespace(success=True, error="", points=list(self._funding_points))


class _FakeGatewayClient:
    def __init__(self, eth_call_fn=None, rate_history=None):
        self._eth_call_fn = eth_call_fn
        self._rate_history = rate_history
        self.is_connected = True

    def connect(self):
        self.is_connected = True

    @property
    def rate_history(self):
        if self._rate_history is None:
            raise RuntimeError("Gateway client not connected")
        return self._rate_history

    def eth_call(self, chain, to, data, block=None):  # noqa: ARG002
        return self._eth_call_fn(chain, to, data) if self._eth_call_fn else None


class _FakePriceOracle:
    def __init__(self, prices: dict[str, Decimal]):
        self._prices = prices
        self.calls = []

    async def get_aggregated_price(self, token: str, quote: str = "USD", *, chain: str | None = None):
        self.calls.append((token, quote, chain))
        return SimpleNamespace(price=self._prices[token])


class _FakeTokenResolver:
    def resolve(self, token_address: str, chain: str, log_errors: bool = False):  # noqa: ARG002
        if token_address.lower() == _TOKEN0_ADDR:
            return SimpleNamespace(symbol="USDC", name="USD Coin", decimals=6)
        if token_address.lower() == _TOKEN1_ADDR:
            return SimpleNamespace(symbol="WETH", name="Wrapped Ether", decimals=18)
        raise KeyError(token_address)


# --------------------------------------------------------------------------- #
# 1. Builder wiring — live path wires the correct reader TYPES
# --------------------------------------------------------------------------- #


def test_for_strategy_runner_wires_live_rpc_grpc_readers():
    oracle = _FakePriceOracle({})
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40, price_oracle=oracle)
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    assert isinstance(snap._pool_reader, GatewayPoolReserveReader)
    assert snap._pool_reader._price_oracle is oracle
    assert isinstance(snap._liquidity_depth_reader, LiquidityDepthReader)
    assert isinstance(snap._slippage_estimator, SlippageEstimator)
    assert isinstance(snap._rate_history_reader, RateHistoryReader)
    # Honest provenance for the eth_call-backed readers.
    assert snap._liquidity_depth_reader._source_name == "gateway_rpc"
    assert snap._slippage_estimator._source_name == "gateway_rpc"
    # The slippage estimator shares the wired registry (carries the TokenResolver).
    assert snap._slippage_estimator._pool_reader_registry is snap._pool_reader_registry


def test_for_strategy_runner_multi_chain_wires_live_rpc_grpc_readers():
    # The multi-chain branch (chains=) must wire the same four live readers as the
    # single-chain branch (card invariant #1: "single + multi-chain").
    oracle = _FakePriceOracle({})
    strategy = SimpleNamespace(
        chain="base",
        wallet_address="0x" + "0" * 40,
        _multi_chain_price_oracle=oracle,
    )
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)

    snap = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy, gateway_client=gw, chain="base", chains=("base", "arbitrum")
    )

    assert snap.chains == ("base", "arbitrum")
    assert isinstance(snap._pool_reader, GatewayPoolReserveReader)
    assert isinstance(snap._liquidity_depth_reader, LiquidityDepthReader)
    assert isinstance(snap._slippage_estimator, SlippageEstimator)
    assert isinstance(snap._rate_history_reader, RateHistoryReader)
    assert snap._liquidity_depth_reader._source_name == "gateway_rpc"
    assert snap._slippage_estimator._pool_reader_registry is snap._pool_reader_registry


def test_for_strategy_runner_honors_reader_overrides():
    custom_pool = MagicMock()
    custom_depth = MagicMock()
    custom_slip = MagicMock()
    custom_rates = MagicMock()
    strategy = SimpleNamespace(
        chain="base",
        wallet_address="0x" + "0" * 40,
        _pool_reader=custom_pool,
        _liquidity_depth_reader=custom_depth,
        _slippage_estimator=custom_slip,
        _rate_history_reader=custom_rates,
    )
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    assert snap._pool_reader is custom_pool
    assert snap._liquidity_depth_reader is custom_depth
    assert snap._slippage_estimator is custom_slip
    assert snap._rate_history_reader is custom_rates


def test_for_strategy_runner_no_gateway_leaves_ethcall_readers_none_but_builds_rate_history():
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=None, chain="base")

    # eth_call-backed readers cannot be built without a gateway -> stay None so
    # the accessor raises a clear "not configured" error.
    assert snap._pool_reader is None
    assert snap._liquidity_depth_reader is None
    assert snap._slippage_estimator is None
    # RateHistoryReader resolves its own connected client lazily -> always built.
    assert isinstance(snap._rate_history_reader, RateHistoryReader)

    with pytest.raises(ValueError, match="No pool reader configured"):
        snap.pool_reserves("0xpool")
    with pytest.raises(ValueError, match="No liquidity depth reader configured"):
        snap.liquidity_depth("0xpool")
    with pytest.raises(ValueError, match="No slippage estimator configured"):
        snap.estimate_slippage("WETH", "USDC", Decimal("1"))


def test_disconnected_gateway_client_fails_fast_at_rpc_boundary():
    # CR3: the shared eth_call closure must fail loud when the client explicitly
    # reports a dead channel, rather than deferring an opaque failure into the
    # decoders. ``default=True`` means clients that omit the signal still work.
    from almanak.framework.market.builders import _make_gateway_rpc_call

    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    gw.is_connected = False
    rpc_call = _make_gateway_rpc_call(gw)
    with pytest.raises(Exception, match="not connected"):  # noqa: PT011
        rpc_call("base", _TOKEN0_ADDR, _SLOT0)

    # A client that does not expose is_connected at all is treated as usable.
    class _NoSignalClient:
        def eth_call(self, chain, to, data, block=None):  # noqa: ARG002
            return _crafted_eth_call(chain, to, data)

    rpc_call_no_signal = _make_gateway_rpc_call(_NoSignalClient())
    assert rpc_call_no_signal("base", _TOKEN0_ADDR, _DECIMALS)  # does not raise


# --------------------------------------------------------------------------- #
# 2. LIVE data — each accessor returns real data through the gateway
# --------------------------------------------------------------------------- #


def test_live_pool_reserves_returns_real_data():
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    reserves = snap.pool_reserves("0xPoolAddress0000000000000000000000000000", chain="base")

    assert reserves.dex == "uniswap_v3"
    assert reserves.fee_tier == _FEE_TIER
    assert reserves.tick == _TICK
    assert reserves.liquidity == _LIQUIDITY_VAL
    assert reserves.sqrt_price_x96 == _SQRT_PRICE_X96
    # reserve0 = 5,000,000,000 / 10**6 ; reserve1 = 3e18 / 10**18.
    assert reserves.reserve0 == Decimal(_RESERVE0_RAW) / Decimal(10**6)
    assert reserves.reserve1 == Decimal(_RESERVE1_RAW) / Decimal(10**18)
    assert reserves.token0.decimals == 6
    assert reserves.token1.decimals == 18


def test_gateway_pool_reserve_reader_computes_tvl_from_price_oracle():
    oracle = _FakePriceOracle({"USDC": Decimal("1"), "WETH": Decimal("3000")})
    reader = GatewayPoolReserveReader(
        rpc_call=lambda chain, to, data: _to_bytes(_crafted_eth_call(chain, to, data)),
        token_resolver=_FakeTokenResolver(),
        price_oracle=oracle,
    )

    reserves = reader._read_pool_reserves_sync("0xPoolAddress0000000000000000000000000000", "base")

    assert reserves.tvl_usd == Decimal("14000")
    assert oracle.calls == [("USDC", "USD", "base"), ("WETH", "USD", "base")]


def test_live_liquidity_depth_returns_real_envelope():
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    env = snap.liquidity_depth("0xPoolAddress0000000000000000000000000000", chain="base")

    depth = env.value
    assert depth.current_tick == _TICK
    assert depth.total_liquidity == _LIQUIDITY_VAL
    assert depth.tick_spacing == 10
    # Empty bitmap -> no initialized ticks, but a well-formed depth envelope.
    assert depth.ticks == []
    assert env.meta.source == "gateway_rpc"


def test_live_estimate_slippage_delegates_to_wired_estimator():
    # Drive the snapshot's estimate_slippage through an injected estimator so the
    # delegation contract is asserted without crafting a full tick-walk fixture.
    captured = {}
    sentinel = MagicMock(name="slippage_envelope")

    class _Estimator:
        def estimate_slippage(self, token_in, token_out, amount, chain, protocol=None):
            captured.update(token_in=token_in, token_out=token_out, amount=amount, chain=chain, protocol=protocol)
            return sentinel

    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40, _slippage_estimator=_Estimator())
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    result = snap.estimate_slippage("WETH", "USDC", Decimal("2.5"), chain="Base")

    assert result is sentinel
    assert captured["chain"] == "base"  # snapshot lowercases chain
    assert captured["amount"] == Decimal("2.5")


def test_live_estimator_reads_real_data_through_gateway():
    # End-to-end through the REAL SlippageEstimator + LiquidityDepthReader +
    # PoolReaderRegistry over the crafted gateway eth_call. token0=6dec,
    # token1=18dec, empty tick bitmap so the v3 walk completes against in-range
    # liquidity only — proving the eth_call-backed estimator produces real data.
    from almanak.framework.data.pools.reader import PoolReaderRegistry

    rpc_call = lambda chain, to, data: _to_bytes(_crafted_eth_call(chain, to, data))  # noqa: E731
    depth_reader = LiquidityDepthReader(rpc_call=rpc_call, source_name="gateway_rpc")
    registry = PoolReaderRegistry(rpc_call=rpc_call, source_name="gateway_rpc")
    estimator = SlippageEstimator(
        liquidity_reader=depth_reader, pool_reader_registry=registry, source_name="gateway_rpc"
    )
    env = estimator.estimate_slippage(
        token_in=_TOKEN0_ADDR,
        token_out=_TOKEN1_ADDR,
        amount=Decimal("100"),
        chain="base",
        protocol="uniswap_v3",
        pool_address="0xpool00000000000000000000000000000000000a",
    )
    assert env.value is not None
    assert env.meta.source == "gateway_rpc"


def _to_bytes(raw: str) -> bytes:
    if not raw or raw == "0x":
        return b""
    return bytes.fromhex(raw.removeprefix("0x"))


def test_live_lending_rate_history_returns_real_snapshots():
    # Field names mirror the gateway RateHistoryService lending point proto:
    # supply side carries supply_apy_pct + utilization_pct; the reader issues
    # both a supply and a borrow lookup and merges by timestamp.
    point = SimpleNamespace(
        timestamp=1_700_000_000,
        supply_apy_pct="3.0",
        borrow_apy_pct="5.0",
        utilization_pct="80.0",
    )
    rh = _FakeRateHistoryStub(lending_points=[point])
    strategy = SimpleNamespace(chain="arbitrum", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(rate_history=rh)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="arbitrum")

    # The RateHistoryReader resolves its OWN connected gateway client via the
    # module singleton — point it at our fake.
    import almanak.framework.data.rates.history as hist
    from almanak.gateway.proto import gateway_pb2

    orig = hist._rate_history_get_connected_gateway_client
    hist._rate_history_get_connected_gateway_client = lambda: (gw, gateway_pb2)
    try:
        env = snap.lending_rate_history(protocol="aave_v3", token="USDC", chain="arbitrum", days=30)
    finally:
        hist._rate_history_get_connected_gateway_client = orig

    assert len(env.value) >= 1
    assert rh.lending_calls, "GetLendingRateHistory was not called"


def test_live_funding_rate_history_returns_real_snapshots():
    # Field names mirror the gateway RateHistoryService funding point proto.
    point = SimpleNamespace(
        timestamp=1_700_000_000,
        rate_hourly="0.0001",
        rate_annualized="0.36",
    )
    rh = _FakeRateHistoryStub(funding_points=[point])
    strategy = SimpleNamespace(chain="arbitrum", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(rate_history=rh)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="arbitrum")

    import almanak.framework.data.rates.history as hist
    from almanak.gateway.proto import gateway_pb2

    orig = hist._rate_history_get_connected_gateway_client
    hist._rate_history_get_connected_gateway_client = lambda: (gw, gateway_pb2)
    try:
        env = snap.funding_rate_history(venue="hyperliquid", market_symbol="ETH-USD", hours=48)
    finally:
        hist._rate_history_get_connected_gateway_client = orig

    assert len(env.value) >= 1
    assert rh.funding_calls, "GetFundingRateHistory was not called"


# --------------------------------------------------------------------------- #
# 3. BACKTEST path — every factory injects Null stubs that fail deterministically
# --------------------------------------------------------------------------- #


def _pnl_snapshot():
    state = SimpleNamespace()
    return MarketSnapshotBuilder.for_pnl_backtest_state(chain="base", wallet_address="0x" + "0" * 40, state=state)


def _paper_snapshot():
    fork = SimpleNamespace(get_rpc_url=lambda: "http://127.0.0.1:8545", current_block=1)
    return MarketSnapshotBuilder.for_paper_fork(chain="base", wallet_address="0x" + "0" * 40, fork_manager=fork)


def _http_snapshot():
    spec = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    return MarketSnapshotBuilder.for_http_backtest_spec(spec=spec)


@pytest.mark.parametrize("factory", [_pnl_snapshot, _paper_snapshot, _http_snapshot])
def test_backtest_factories_wire_null_rpc_grpc_readers(factory):
    snap = factory()
    assert isinstance(snap._pool_reader, NullPoolReserveReader)
    assert isinstance(snap._liquidity_depth_reader, NullLiquidityDepthReader)
    assert isinstance(snap._slippage_estimator, NullSlippageEstimator)
    assert isinstance(snap._rate_history_reader, NullRateHistoryReader)


@pytest.mark.parametrize("factory", [_pnl_snapshot, _paper_snapshot, _http_snapshot])
def test_backtest_pool_reserves_raises_data_unavailable(factory):
    snap = factory()
    with pytest.raises(Exception) as excinfo:  # noqa: PT011 — wrapped type asserted below
        snap.pool_reserves("0xpool")
    # The snapshot wraps the reader exception; the cause chain classifies as
    # DATA_UNAVAILABLE (the runner's HOLD-inference signal).
    assert _has_backtest_cause(excinfo.value)


@pytest.mark.parametrize("factory", [_pnl_snapshot, _paper_snapshot, _http_snapshot])
def test_backtest_liquidity_depth_raises_data_unavailable(factory):
    snap = factory()
    with pytest.raises(Exception) as excinfo:  # noqa: PT011
        snap.liquidity_depth("0xpool")
    assert _has_backtest_cause(excinfo.value)


@pytest.mark.parametrize("factory", [_pnl_snapshot, _paper_snapshot, _http_snapshot])
def test_backtest_estimate_slippage_raises_data_unavailable(factory):
    snap = factory()
    with pytest.raises(Exception) as excinfo:  # noqa: PT011
        snap.estimate_slippage("WETH", "USDC", Decimal("1"))
    assert _has_backtest_cause(excinfo.value)


@pytest.mark.parametrize("factory", [_pnl_snapshot, _paper_snapshot, _http_snapshot])
def test_backtest_rate_history_raises_data_unavailable(factory):
    snap = factory()
    with pytest.raises(Exception) as excinfo:  # noqa: PT011
        snap.lending_rate_history(protocol="aave_v3", token="USDC", chain="base")
    assert _has_backtest_cause(excinfo.value)
    with pytest.raises(Exception) as excinfo2:  # noqa: PT011
        snap.funding_rate_history(venue="hyperliquid", market_symbol="ETH-USD")
    assert _has_backtest_cause(excinfo2.value)


def _has_backtest_cause(exc: BaseException) -> bool:
    """True if the exception (or its __cause__ chain) is a backtest DataSourceUnavailable."""
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, DataSourceUnavailable) and "backtest" in str(cur):
            return True
        cur = cur.__cause__
    return False


# --------------------------------------------------------------------------- #
# 4. Null readers raise DataSourceUnavailable("backtest") directly
# --------------------------------------------------------------------------- #


def test_null_pool_reserve_reader_raises_directly():
    import asyncio

    # Dedicated loop (not asyncio.run, which nulls the current loop on exit and
    # breaks later same-worker tests using the deprecated asyncio.get_event_loop()).
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(DataSourceUnavailable, match="backtest"):
            loop.run_until_complete(NullPoolReserveReader().get_pool_reserves("0xpool", "base"))
    finally:
        loop.close()


def test_null_liquidity_depth_reader_raises_directly():
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        NullLiquidityDepthReader().read_liquidity_depth("0xpool", "base")


def test_null_slippage_estimator_raises_directly():
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        NullSlippageEstimator().estimate_slippage("WETH", "USDC", Decimal("1"), "base")


def test_null_rate_history_reader_raises_directly():
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        NullRateHistoryReader().get_lending_rate_history("aave_v3", "USDC", "base")
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        NullRateHistoryReader().get_funding_rate_history("hyperliquid", "ETH-USD")
    assert NullRateHistoryReader().health() == {}


# --------------------------------------------------------------------------- #
# 5. Snapshot wrapper preserves DATA_UNAVAILABLE classification
# --------------------------------------------------------------------------- #


def test_backtest_pool_reserves_classifies_as_data_unavailable():
    snap = _pnl_snapshot()
    with pytest.raises(Exception) as excinfo:  # noqa: PT011
        snap.pool_reserves("0xpool")
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


# --------------------------------------------------------------------------- #
# 6. Explicitly-deferred accessors stay NOT configured
# --------------------------------------------------------------------------- #


def test_pool_history_stays_deferred_on_live_runner():
    # VIB-4755 D-4: live cut-over gated on VIB-4730 + VIB-4863. for_strategy_runner
    # injects nothing -> the accessor raises "not configured".
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")
    assert snap._pool_history_reader is None
    with pytest.raises(ValueError, match="No pool history reader configured"):
        snap.pool_history("0xpool", protocol="uniswap_v3")


def test_yield_opportunities_stays_deferred_on_live_runner():
    # No gateway Yield service in the proto -> no aggregator wired -> accessor
    # raises "not configured" (tracked as a follow-up new-service ticket).
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(eth_call_fn=_crafted_eth_call)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")
    assert snap._yield_aggregator is None
    with pytest.raises(ValueError, match="No yield aggregator configured"):
        snap.yield_opportunities("USDC")
