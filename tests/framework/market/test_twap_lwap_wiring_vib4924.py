"""VIB-4924 / ALM-2770 — twap()/lwap() wiring in the live/hosted runner.

Covers the design's §7 test plan:

1. Gateway twap adapter — success (source="on_chain", window mapping),
   success=false → PoolPriceUnavailableError, unconnected stub → error.
2. Builder regression — for_strategy_runner wires the gateway aggregator +
   a PoolReaderRegistry with source_name="gateway_rpc" (H3).
3. C1 best-pool resolution — picks the highest-liquidity pool, not default 3000.
4. End-to-end twap (mocked gateway) — resolve best pool then GetDexTwap.
5. Unsupported protocol (F4) — surfaced via the gateway's success=false.
6. lwap (L2) — inherited aggregator carries source="gateway_rpc"; snapshot.lwap
   pre-check raises a structured error for a known-unsupported protocol (F3).
7. Backtest determinism — Null stubs raise DataSourceUnavailable("backtest").
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification
from almanak.framework.data.null_readers import NullPoolReaderRegistry, NullPriceAggregator
from almanak.framework.data.pools.reader import (
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
)
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.market.errors import PoolPriceUnavailableError
from almanak.framework.market.gateway_price_aggregator import GatewayMarketPriceAggregator
from almanak.framework.market.snapshot import MarketSnapshot

# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


def _twap_resp(*, success=True, price="2500.5", source="on_chain", error="", obs=10):
    return SimpleNamespace(
        success=success,
        error=error,
        source=source,
        point=SimpleNamespace(price=price, tick_observation_count=obs),
    )


def _lwap_resp(*, success=True, price="2500.5", source="gateway_rpc", error="", pool_count=1):
    return SimpleNamespace(
        success=success,
        error=error,
        source=source,
        point=SimpleNamespace(price=price, pool_count=pool_count),
    )


class _FakeRateHistory:
    """Stand-in for GatewayClient.rate_history with GetDexTwap + GetDexLwap."""

    def __init__(self, response=None, raise_exc=None, lwap_response=None, lwap_raise=None):
        self._response = response
        self._raise = raise_exc
        self._lwap_response = lwap_response
        self._lwap_raise = lwap_raise
        self.last_request = None
        self.last_lwap_request = None

    def GetDexTwap(self, request):  # noqa: N802 — mirrors the gRPC stub method name
        self.last_request = request
        if self._raise is not None:
            raise self._raise
        return self._response

    def GetDexLwap(self, request):  # noqa: N802 — mirrors the gRPC stub method name
        self.last_lwap_request = request
        if self._lwap_raise is not None:
            raise self._lwap_raise
        return self._lwap_response


class _FakeGatewayClient:
    def __init__(self, rate_history=None, eth_call_fn=None):
        self._rate_history = rate_history
        self._eth_call_fn = eth_call_fn

    @property
    def rate_history(self):
        # Mirror the real stub: raises RuntimeError when unconnected.
        if self._rate_history is None:
            raise RuntimeError("Gateway client not connected")
        return self._rate_history

    def eth_call(self, chain, to, data, block=None):
        return self._eth_call_fn(chain, to, data) if self._eth_call_fn else None


# --------------------------------------------------------------------------- #
# 1. Gateway twap adapter
# --------------------------------------------------------------------------- #


def test_gateway_twap_adapter_success_on_chain_source_and_window_mapping():
    rh = _FakeRateHistory(response=_twap_resp(price="2500.5", source="on_chain"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=MagicMock(),
        rpc_call=lambda *a: b"",
    )

    env = agg.twap(pool_address="0xpool", chain="base", window_seconds=300, protocol="uniswap_v3")

    assert env.value.method == "twap"
    assert env.value.price == Decimal("2500.5")
    assert env.value.pool_count == 1
    # F2 / M3: honest provenance — NOT "alchemy_rpc".
    assert env.meta.source == "on_chain"
    assert env.classification == DataClassification.EXECUTION_GRADE
    # §7.4 window/dex/pool mapping.
    req = rh.last_request
    assert req.dex == "uniswap_v3"
    assert req.chain == "base"
    assert req.pool_address == "0xpool"
    assert req.secs_ago_start == 300
    assert req.secs_ago_end == 0


def test_gateway_twap_adapter_requires_decimals_false():
    # The gateway returns a human-readable price; the snapshot must skip decimal
    # resolution for this aggregator (§6.3).
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=_FakeRateHistory(response=_twap_resp())),
        pool_registry=MagicMock(),
        rpc_call=lambda *a: b"",
    )
    assert agg.requires_decimals is False


def test_gateway_twap_adapter_success_false_raises():
    rh = _FakeRateHistory(response=_twap_resp(success=False, error="unsupported dex 'aerodrome' on base"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=MagicMock(),
        rpc_call=lambda *a: b"",
    )
    with pytest.raises(PoolPriceUnavailableError, match="unsupported dex"):
        agg.twap(pool_address="0xpool", chain="base", protocol="aerodrome")


def test_gateway_twap_adapter_unconnected_stub_raises():
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=None),  # property raises RuntimeError
        pool_registry=MagicMock(),
        rpc_call=lambda *a: b"",
    )
    with pytest.raises(PoolPriceUnavailableError, match="RPC failed"):
        agg.twap(pool_address="0xpool", chain="base")


# --------------------------------------------------------------------------- #
# 2. Builder regression
# --------------------------------------------------------------------------- #


def test_for_strategy_runner_wires_gateway_aggregator_and_registry():
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(rate_history=_FakeRateHistory(response=_twap_resp()))

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    assert isinstance(snap._price_aggregator, GatewayMarketPriceAggregator)
    assert isinstance(snap._pool_reader_registry, PoolReaderRegistry)
    # H3: provenance label must be gateway_rpc, not the alchemy_rpc default.
    assert snap._pool_reader_registry._source_name == "gateway_rpc"
    assert snap._price_aggregator._source_name == "gateway_rpc"


def test_for_strategy_runner_honors_strategy_override():
    custom_agg = MagicMock()
    custom_reg = MagicMock()
    strategy = SimpleNamespace(
        chain="base",
        wallet_address="0x" + "0" * 40,
        _price_aggregator=custom_agg,
        _pool_reader_registry=custom_reg,
    )
    gw = _FakeGatewayClient(rate_history=_FakeRateHistory(response=_twap_resp()))

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    assert snap._price_aggregator is custom_agg
    assert snap._pool_reader_registry is custom_reg


def test_for_strategy_runner_no_gateway_leaves_providers_none():
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=None, chain="base")
    assert snap._price_aggregator is None
    assert snap._pool_reader_registry is None
    # The "not configured" raise is preserved.
    with pytest.raises(ValueError, match="No price aggregator configured"):
        snap.twap("WETH/USDC")


# --------------------------------------------------------------------------- #
# 3. C1 best-pool resolution (headline regression)
# --------------------------------------------------------------------------- #


def test_resolve_best_pool_picks_highest_liquidity_not_default_3000():
    reader = UniswapV3PoolPriceReader(rpc_call=lambda *a: b"")
    # WETH/USDC resolves at 100/500/3000; the 500 tier is the deepest.
    pools = {100: "0xfee100", 500: "0xfee500", 3000: "0xfee3000", 10000: None}
    liquidity = {"0xfee100": 50, "0xfee500": 9_000, "0xfee3000": 100}

    reader.resolve_pool_address = lambda a, b, c, fee: pools.get(fee)  # type: ignore[method-assign]
    reader.read_pool_price = lambda addr, chain: SimpleNamespace(  # type: ignore[method-assign]
        value=SimpleNamespace(liquidity=liquidity[addr])
    )

    best = reader.resolve_best_pool_address("WETH", "USDC", "base")
    # Must be the deepest tier, NOT the default-3000 pool (the C1 bug).
    assert best == "0xfee500"
    assert best != "0xfee3000"


def test_resolve_best_pool_none_when_nothing_resolves():
    reader = UniswapV3PoolPriceReader(rpc_call=lambda *a: b"")
    reader.resolve_pool_address = lambda a, b, c, fee: None  # type: ignore[method-assign]
    assert reader.resolve_best_pool_address("ABC", "XYZ", "base") is None


# --------------------------------------------------------------------------- #
# 4. End-to-end twap (snapshot resolves best pool then calls gateway)
# --------------------------------------------------------------------------- #


def test_snapshot_twap_resolves_best_pool_then_calls_gateway():
    reader = MagicMock()
    reader.resolve_best_pool_address.return_value = "0xbest"
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(response=_twap_resp(price="2500", source="on_chain"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    snap = MarketSnapshot(chain="base", price_aggregator=agg, pool_reader_registry=registry)

    env = snap.twap("WETH/USDC", window_seconds=600)

    # requires_decimals=False ⇒ no decimal resolution eth_call needed.
    reader._get_pool_metadata.assert_not_called()
    assert rh.last_request.pool_address == "0xbest"
    assert rh.last_request.secs_ago_start == 600
    assert env.meta.source == "on_chain"
    assert env.value.price == Decimal("2500")


# --------------------------------------------------------------------------- #
# 5. Unsupported protocol surfaced via gateway success=false (F4)
# --------------------------------------------------------------------------- #


def test_snapshot_twap_unsupported_protocol_via_gateway_success_false():
    reader = MagicMock()
    reader.resolve_best_pool_address.return_value = "0xpool"
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(response=_twap_resp(success=False, error="dex 'aerodrome' not TWAP-capable"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    snap = MarketSnapshot(chain="base", price_aggregator=agg, pool_reader_registry=registry)

    with pytest.raises(PoolPriceUnavailableError, match="not TWAP-capable"):
        snap.twap("WETH/USDC", protocol="aerodrome")


# --------------------------------------------------------------------------- #
# 6. lwap (L2): inherited aggregator carries gateway_rpc; F3 pre-check
# --------------------------------------------------------------------------- #


# Distinct stub addresses so the gateway pair-filter forwarding has valid strings.
_STUB_ADDR = {"WETH": "0x" + "1" * 40, "USDC": "0x" + "2" * 40, "ABC": "0x" + "3" * 40, "XYZ": "0x" + "4" * 40}


def _stub_resolve(sym, chain):
    return _STUB_ADDR.get(sym.upper())


def test_lwap_routes_through_gateway_getdexlwap():
    # Framework resolves candidate pools; the gateway does the reads (L3 / VIB-4948).
    reader = MagicMock()
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xpool500" if fee == 500 else None
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(price="2500.5", pool_count=1, source="gateway_rpc"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    env = agg.lwap("WETH", "USDC", "base")

    assert env.value.method == "lwap"
    assert env.value.price == Decimal("2500.5")
    assert env.value.pool_count == 1
    assert env.meta.source == "gateway_rpc"
    # The framework-resolved pool was forwarded to GetDexLwap.
    req = rh.last_lwap_request
    assert "0xpool500" in list(req.pool_addresses)
    assert req.dex == "uniswap_v3"
    # B2 follow-on: the requested pair addresses ride along so the gateway can
    # drop foreign-pair pools.
    assert req.base_token == _STUB_ADDR["WETH"]
    assert req.quote_token == _STUB_ADDR["USDC"]


def test_lwap_no_pools_resolved_raises():
    reader = MagicMock()
    reader.resolve_pool_address.return_value = None  # nothing resolves
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    registry.get_reader.return_value = reader

    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=_FakeRateHistory(lwap_response=_lwap_resp())),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    with pytest.raises(PoolPriceUnavailableError, match="No pools resolved"):
        agg.lwap("ABC", "XYZ", "base")


def test_lwap_gateway_success_false_raises():
    reader = MagicMock()
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xpool500" if fee == 500 else None
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(success=False, error="no readable V3 pools"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    with pytest.raises(PoolPriceUnavailableError, match="no readable V3 pools"):
        agg.lwap("WETH", "USDC", "base")


def test_lwap_pinned_aerodrome_slipstream_dispatches_uniswap_v3_profile():
    """#1 regression: pinning protocols=["aerodrome_slipstream"] must still
    produce an LWAP. The gateway read is protocol-agnostic (slot0/liquidity), so
    a pinned non-uniswap protocol dispatches under the uniswap_v3 read profile
    instead of dex="aerodrome_slipstream" — which has no registered LWAP provider
    and hard-fails with 'unsupported dex (lwap)' (the live Base bug)."""
    reader = MagicMock()
    # Slipstream USDC/CBBTC pool keyed by tick spacing 100 (not a uni fee tier).
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xslip100" if fee == 100 else None
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3", "aerodrome_slipstream"]
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(price="64000.0", pool_count=1))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )

    env = agg.lwap("WETH", "USDC", "base", protocols=["aerodrome_slipstream"])

    assert env.value.price == Decimal("64000.0")
    req = rh.last_lwap_request
    assert req.dex == "uniswap_v3"  # the fix — NOT "aerodrome_slipstream"
    assert "0xslip100" in list(req.pool_addresses)


def test_lwap_default_sweep_covers_aerodrome_tick_spacings():
    """#1: the default fee_tiers sweep includes Aerodrome tick spacings (e.g.
    200), so spacing-200 Slipstream pools resolve — a pure Uniswap fee-tier
    sweep ([100, 500, 3000, 10000]) would skip them."""
    reader = MagicMock()
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xspacing200" if fee == 200 else None
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["aerodrome_slipstream"]
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(price="64000.0", pool_count=1))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )

    agg.lwap("WETH", "USDC", "base")
    req = rh.last_lwap_request
    assert "0xspacing200" in list(req.pool_addresses)  # 200 = tick spacing, not uni fee tier
    assert req.dex == "uniswap_v3"


def test_lwap_excludes_non_slot0_reader_kinds():
    """PR #3204 (codex P1): GetDexLwap reads slot0()+liquidity(), so pools of a
    non-slot0 reader kind (Curve's get_dy shape) must never be shipped in
    pool_addresses — they are unreadable under the uniswap_v3 profile. The
    framework PriceAggregator lane covers those protocols with their own
    reader."""
    v3_reader = MagicMock()
    v3_reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xpool500" if fee == 500 else None
    v3_reader._resolve_to_address.side_effect = _stub_resolve
    curve_reader = MagicMock()
    curve_reader.resolve_pool_address.return_value = "0xcurvepool"
    curve_reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.protocols_for_chain.return_value = ["curve", "uniswap_v3"]  # sorted() puts curve first
    registry.reader_kind.side_effect = lambda p: "curve_pool" if p == "curve" else "v3_slot0"
    registry.get_reader.side_effect = lambda chain, proto: curve_reader if proto == "curve" else v3_reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(price="2500.5", pool_count=1, source="gateway_rpc"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )

    agg.lwap("WETH", "USDC", "base")

    req = rh.last_lwap_request
    assert "0xpool500" in list(req.pool_addresses)
    assert "0xcurvepool" not in list(req.pool_addresses)
    # The curve reader is skipped before resolution — no doomed server-side reads.
    curve_reader.resolve_pool_address.assert_not_called()


def test_snapshot_lwap_normalizes_protocols_without_registry():
    # CodeRabbit: a snapshot with a price_aggregator but NO registry must still
    # forward lowercase protocol names (the downstream dispatch is exact-match).
    aggregator = MagicMock()
    aggregator.lwap.return_value = _twap_resp()  # any object; orientation is a no-op on base
    snap = MarketSnapshot(chain="base", price_aggregator=aggregator, pool_reader_registry=None)

    snap.lwap("WETH/USDC", protocols=["Uniswap_V3", "PancakeSwap_V3"])

    assert aggregator.lwap.call_args.kwargs["protocols"] == ["uniswap_v3", "pancakeswap_v3"]


def test_snapshot_lwap_unsupported_protocol_precheck_raises():
    registry = PoolReaderRegistry(rpc_call=lambda *a: b"", source_name="gateway_rpc")
    aggregator = MagicMock()
    snap = MarketSnapshot(chain="base", price_aggregator=aggregator, pool_reader_registry=registry)

    with pytest.raises(PoolPriceUnavailableError, match="unsupported protocol"):
        snap.lwap("WETH/USDC", protocols=["curve_stable_not_a_reader"])

    # F3: the pre-check fires BEFORE delegating to the aggregator.
    aggregator.lwap.assert_not_called()


# --------------------------------------------------------------------------- #
# 7. Backtest determinism
# --------------------------------------------------------------------------- #


def test_for_pnl_backtest_state_injects_null_twap_lwap():
    snap = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="base", wallet_address="0x" + "0" * 40, state=SimpleNamespace()
    )
    assert isinstance(snap._price_aggregator, NullPriceAggregator)
    assert isinstance(snap._pool_reader_registry, NullPoolReaderRegistry)


def test_for_paper_fork_injects_null_twap_lwap():
    fork = SimpleNamespace(get_rpc_url=lambda: "http://fork", current_block=123)
    snap = MarketSnapshotBuilder.for_paper_fork(chain="base", wallet_address="0x" + "0" * 40, fork_manager=fork)
    assert isinstance(snap._price_aggregator, NullPriceAggregator)
    assert isinstance(snap._pool_reader_registry, NullPoolReaderRegistry)


def test_null_stubs_raise_data_source_unavailable_backtest():
    agg = NullPriceAggregator()
    reg = NullPoolReaderRegistry()
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        agg.twap(pool_address="0xp", chain="base")
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        agg.lwap("WETH", "USDC", "base")
    with pytest.raises(DataSourceUnavailable, match="backtest"):
        reg.get_reader("base", "uniswap_v3")
    # The null aggregator skips decimal resolution too.
    assert agg.requires_decimals is False
    assert reg.supported_protocols == []


def test_snapshot_backtest_twap_lwap_wrap_data_unavailable():
    """Through the snapshot wrapper, twap/lwap surface PoolPriceUnavailableError
    whose __cause__ is the deterministic DataSourceUnavailable — mirroring the
    pool_history backtest contract."""
    snap = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="base", wallet_address="0x" + "0" * 40, state=SimpleNamespace()
    )
    with pytest.raises(PoolPriceUnavailableError) as twap_err:
        snap.twap("WETH/USDC")
    assert isinstance(twap_err.value.__cause__, DataSourceUnavailable)

    with pytest.raises(PoolPriceUnavailableError) as lwap_err:
        snap.lwap("WETH/USDC")
    assert isinstance(lwap_err.value.__cause__, DataSourceUnavailable)


def test_null_registry_protocols_for_chain_is_empty():
    # CodeRabbit: snapshot.lwap's protocol pre-check calls protocols_for_chain;
    # the Null registry must expose it (return []) so a backtest lwap with
    # explicit protocols fails closed instead of raising AttributeError.
    reg = NullPoolReaderRegistry()
    assert reg.protocols_for_chain("base") == []


def test_backtest_lwap_with_explicit_protocols_does_not_crash():
    snap = MarketSnapshotBuilder.for_pnl_backtest_state(
        chain="base", wallet_address="0x" + "0" * 40, state=SimpleNamespace()
    )
    # Must raise a typed PoolPriceUnavailableError (not AttributeError) when
    # explicit protocols are passed against the Null registry.
    with pytest.raises(PoolPriceUnavailableError):
        snap.lwap("WETH/USDC", protocols=["uniswap_v3"])


def test_null_types_construct_no_client_primitives():
    # Mirror the spirit of the 38-primitive determinism proof: the Null stubs are
    # thin shells holding no gateway client / socket-opening primitive.
    agg = NullPriceAggregator()
    reg = NullPoolReaderRegistry()
    assert not hasattr(agg, "_gateway_client")
    assert not hasattr(agg, "_rpc_call")
    assert not hasattr(reg, "_rpc_call")


# --------------------------------------------------------------------------- #
# 8. B1 — symbol resolution is actually wired (the blocker the mocks hid)
# --------------------------------------------------------------------------- #


def test_for_strategy_runner_wires_token_resolver_into_registry():
    # B1 regression: the live registry MUST carry a TokenResolver, else
    # resolve_instrument's canonical *symbols* never map to pool addresses.
    strategy = SimpleNamespace(chain="base", wallet_address="0x" + "0" * 40)
    gw = _FakeGatewayClient(rate_history=_FakeRateHistory(response=_twap_resp()))

    snap = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, gateway_client=gw, chain="base")

    assert snap._pool_reader_registry._token_resolver is not None


def test_registry_with_resolver_resolves_symbol_pair_to_known_pool():
    # End-to-end through the REAL reader + REAL token resolver (no reader mock):
    # WETH/USDC 0.05% on Base is a known pool, resolvable only when the registry
    # has a token_resolver to map the symbols → addresses (B1).
    from almanak.framework.data.tokens import get_token_resolver

    reg = PoolReaderRegistry(
        rpc_call=lambda *a: b"",
        token_resolver=get_token_resolver(),
        source_name="gateway_rpc",
    )
    reader = reg.get_reader("base", "uniswap_v3")
    addr = reader.resolve_pool_address("WETH", "USDC", "base", 500)
    assert addr is not None
    assert addr.lower() == "0xd0b53d9277642d899df5c87a3966a349a798f224"


def test_registry_without_resolver_cannot_resolve_symbols():
    # The B1 bug: no resolver ⇒ symbol pairs resolve to no pool ⇒ perpetual HOLD.
    reg = PoolReaderRegistry(rpc_call=lambda *a: b"", source_name="gateway_rpc")
    reader = reg.get_reader("base", "uniswap_v3")
    assert reader.resolve_pool_address("WETH", "USDC", "base", 500) is None


# --------------------------------------------------------------------------- #
# 9. B2 — quote/base orientation (inverse-ordered pools)
# --------------------------------------------------------------------------- #


def _orient_snapshot(chain, *, twap_price=None, lwap_price=None):
    """Build a snapshot whose gateway returns a fixed pool-native price."""
    reader = MagicMock()
    reader.resolve_best_pool_address.return_value = "0xpool"
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0xpool500" if fee == 500 else None
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.get_reader.return_value = reader
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    rh = _FakeRateHistory(
        response=_twap_resp(price=twap_price or "0", source="on_chain"),
        lwap_response=_lwap_resp(price=lwap_price or "0", source="gateway_rpc"),
    )
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    return MarketSnapshot(chain=chain, price_aggregator=agg, pool_reader_registry=registry)


def test_twap_inverts_when_base_is_token1_ethereum():
    # Ethereum WETH/USDC: token0 = USDC (0xa0b8… < 0xc02a… WETH), so the pool
    # returns token1/token0 = WETH/USDC (~0.00033). twap("WETH/USDC") must invert
    # to USDC/WETH (~3000) — the B2 blocker.
    snap = _orient_snapshot("ethereum", twap_price="0.00033")
    env = snap.twap("WETH/USDC")
    assert env.value.price == Decimal(1) / Decimal("0.00033")
    assert env.value.price > Decimal("3000")


def test_twap_no_inversion_when_base_is_token0_base():
    # Base WETH/USDC: token0 = WETH (0x4200 < 0x8335 USDC) → token1/token0 is
    # already USDC/WETH; no inversion (this is why the Base QA missed B2).
    snap = _orient_snapshot("base", twap_price="3000")
    env = snap.twap("WETH/USDC")
    assert env.value.price == Decimal("3000")


def test_lwap_inverts_when_base_is_token1_ethereum():
    snap = _orient_snapshot("ethereum", lwap_price="0.00033")
    env = snap.lwap("WETH/USDC")
    assert env.value.price == Decimal(1) / Decimal("0.00033")
    assert env.value.price > Decimal("3000")


def test_lwap_no_inversion_when_base_is_token0_base():
    snap = _orient_snapshot("base", lwap_price="3000")
    env = snap.lwap("WETH/USDC")
    assert env.value.price == Decimal("3000")


def test_twap_inverts_source_contributions_too():
    # Orientation must invert the per-pool contribution price as well, so the
    # provenance breakdown stays consistent with the aggregate.
    snap = _orient_snapshot("ethereum", twap_price="0.00033")
    env = snap.twap("WETH/USDC")
    assert env.value.sources  # twap carries one contribution
    assert env.value.sources[0].price == Decimal(1) / Decimal("0.00033")


# --------------------------------------------------------------------------- #
# 10. V4 bytes32 PoolIds never reach the gateway slot0 LWAP batch
# --------------------------------------------------------------------------- #


def test_lwap_skips_bytes32_v4_pool_ids_but_forwards_contract_addresses():
    # uniswap_v4 resolve returns a synthetic bytes32 PoolId — no contract to
    # slot0-read, so it must be dropped from the gateway batch. The 20-byte
    # V3 pool (and any non-bytes32 identifier) still forwards unchanged.
    v4_pool_id = "0x" + "ab" * 32  # 64 hex chars
    reader = MagicMock()
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: {500: "0xpool500", 3000: v4_pool_id}.get(fee)
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    registry.get_reader.return_value = reader

    rh = _FakeRateHistory(lwap_response=_lwap_resp(price="2500.5", pool_count=1, source="gateway_rpc"))
    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=rh),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    agg.lwap("WETH", "USDC", "base")

    forwarded = list(rh.last_lwap_request.pool_addresses)
    assert "0xpool500" in forwarded
    assert v4_pool_id not in forwarded


def test_lwap_all_candidates_bytes32_raises_no_pools():
    # If every resolved candidate is a synthetic PoolId, the gateway batch is
    # empty and lwap must fail loudly, not issue guaranteed-dead reads.
    reader = MagicMock()
    reader.resolve_pool_address.side_effect = lambda a, b, c, fee: "0x" + "cd" * 32
    reader._resolve_to_address.side_effect = _stub_resolve
    registry = MagicMock()
    registry.reader_kind.return_value = "v3_slot0"
    registry.protocols_for_chain.return_value = ["uniswap_v3"]
    registry.get_reader.return_value = reader

    agg = GatewayMarketPriceAggregator(
        gateway_client=_FakeGatewayClient(rate_history=_FakeRateHistory(lwap_response=_lwap_resp())),
        pool_registry=registry,
        rpc_call=lambda *a: b"",
    )
    with pytest.raises(PoolPriceUnavailableError, match="No pools resolved"):
        agg.lwap("WETH", "USDC", "base")
