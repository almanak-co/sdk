"""Framework PoolAnalyticsReader (thin gRPC client) tests (VIB-4727).

Covers the framework-side half of the UAT card
``docs/internal/uat-cards/VIB-4727.md`` — D1.S2, D3.F1, D3.F2 — and the
spec-drift D1 source-inspection check (no ``import aiohttp`` in
``analytics.py``).

The reader's only external dependency is the gRPC stub exposed by
``GatewayClient.pool_analytics`` — every test stubs that one surface; no
real gRPC channel, no real gateway boot, no live HTTP.
"""

from __future__ import annotations

import inspect
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.market_snapshot import PoolAnalyticsUnavailableError
from almanak.framework.data.models import DataEnvelope
from almanak.framework.data.pools.analytics import PoolAnalytics, PoolAnalyticsReader
from almanak.framework.runner.failure_kind import FailureKind, classify_failure
from almanak.gateway.proto import gateway_pb2

_ANTONIS_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"


def _make_response(
    *,
    success: bool = True,
    error: str = "",
    tvl_usd: str = "1210000.0",
    fee_apr: str = "12.5",
    source: str = "defillama",
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
    is_live_data: bool = True,
    observed_at: int = 0,
) -> gateway_pb2.PoolAnalyticsResponse:
    return gateway_pb2.PoolAnalyticsResponse(
        pool_address=_ANTONIS_POOL,
        chain=chain,
        protocol=protocol,
        tvl_usd=tvl_usd,
        volume_24h_usd="850000.0" if success else "",
        volume_7d_usd="6100000.0" if success else "",
        fee_apr=fee_apr,
        fee_apy="12.6" if success else "",
        utilization_rate="",
        token0_weight="",
        token1_weight="",
        source=source,
        observed_at=observed_at,
        is_live_data=is_live_data,
        success=success,
        error=error,
    )


def _fake_gateway_with_stub(stub: MagicMock) -> MagicMock:
    gateway = MagicMock()
    gateway.pool_analytics = stub
    return gateway


# ============================================================================
# Constructor guard — PoolAnalyticsReader() with no args MUST raise TypeError
# (positive proof old aiohttp-owning constructor is gone).
# ============================================================================


def test_pool_analytics_reader_requires_gateway_client():
    """D1.S2: ``PoolAnalyticsReader()`` raises TypeError; the new ctor requires a gateway client."""
    with pytest.raises(TypeError):
        PoolAnalyticsReader()  # type: ignore[call-arg]

    with pytest.raises(TypeError, match="GatewayClient"):
        PoolAnalyticsReader(gateway_client=None)  # type: ignore[arg-type]


# ============================================================================
# D1.S2 — happy path through the gateway stub
# ============================================================================


def test_get_pool_analytics_routes_through_gateway():
    """D1.S2: returned envelope is DataEnvelope[PoolAnalytics] with the
    string-decimal wire values parsed at the framework boundary."""
    stub = MagicMock()
    stub.GetPoolAnalytics.return_value = _make_response()
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_analytics(
        pool_address=_ANTONIS_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
    )

    assert isinstance(envelope, DataEnvelope)
    assert isinstance(envelope.value, PoolAnalytics)
    assert envelope.value.tvl_usd == Decimal("1210000.0")
    assert envelope.value.fee_apr == 12.5
    assert envelope.meta.source == "defillama"
    assert envelope.meta.cache_hit is False  # is_live_data=True -> not cached

    # Verify the framework constructed the right gRPC request shape.
    sent_request = stub.GetPoolAnalytics.call_args.args[0]
    assert sent_request.pool_address == _ANTONIS_POOL
    assert sent_request.chain == "arbitrum"
    assert sent_request.protocol == "uniswap_v3"


# ============================================================================
# Spec-drift D1: source inspection — no `import aiohttp` in analytics.py
# ============================================================================


def test_analytics_module_source_has_no_aiohttp_import():
    """Spec-drift D1 (2026-05-21): ``analytics.py`` must not import aiohttp.

    Original assertion was a ``sys.modules`` check; that was tightened to
    source inspection because the sibling ``pools/history.py`` independently
    imports aiohttp (tracked as VIB-4728) and re-exports through
    ``pools/__init__.py``. The intent is "the framework PoolAnalyticsReader
    itself uses no HTTP client," which source inspection captures precisely.
    """
    import almanak.framework.data.pools.analytics as analytics_mod

    src = Path(analytics_mod.__file__).read_text()
    assert "import aiohttp" not in src
    assert "from aiohttp" not in src

    # The same intent re-asserted via inspect.getsource on the hot method.
    method_src = inspect.getsource(PoolAnalyticsReader.get_pool_analytics)
    assert "aiohttp" not in method_src


# ============================================================================
# D3.F1 — gateway gRPC channel returns UNAVAILABLE -> typed DataSourceUnavailable
# ============================================================================


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details
        super().__init__(details)

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def test_gateway_unavailable_raises_datasource_unavailable():
    """D3.F1: gRPC UNAVAILABLE -> DataSourceUnavailable with cause chain
    preserved; MarketSnapshot wraps in PoolAnalyticsUnavailableError;
    classify_failure walks the chain to DATA_UNAVAILABLE."""
    stub = MagicMock()
    stub.GetPoolAnalytics.side_effect = _FakeRpcError(
        grpc.StatusCode.UNAVAILABLE,
        "channel closed",
    )
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_analytics(
            pool_address=_ANTONIS_POOL,
            chain="arbitrum",
            protocol="uniswap_v3",
        )
    assert isinstance(excinfo.value.__cause__, grpc.RpcError)
    # classify_failure on the typed direct exception.
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE

    # MarketSnapshot wraps; walk __cause__ to confirm typed origin survives.
    from almanak.framework.market import MarketSnapshot

    snap = MarketSnapshot(chain="arbitrum", pool_analytics_reader=reader)
    with pytest.raises(PoolAnalyticsUnavailableError) as wrapped:
        snap.pool_analytics(pool_address=_ANTONIS_POOL, protocol="uniswap_v3")
    assert isinstance(wrapped.value.__cause__, DataSourceUnavailable)
    assert classify_failure(wrapped.value) == FailureKind.DATA_UNAVAILABLE


# ============================================================================
# D3.F2 — gateway returned success=False with both-provider errors
# ============================================================================


def test_all_providers_unavailable_maps_to_datasource_unavailable():
    """D3.F2 (framework side): gateway returned success=False -> framework
    raises DataSourceUnavailable with the gateway's error message."""
    stub = MagicMock()
    stub.GetPoolAnalytics.return_value = _make_response(
        success=False,
        error="defillama: timeout; coingecko_onchain: 503",
        tvl_usd="",
        fee_apr="",
        source="",
    )
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_analytics(
            pool_address=_ANTONIS_POOL,
            chain="arbitrum",
            protocol="uniswap_v3",
    )
    assert "defillama" in excinfo.value.reason
    assert "coingecko_onchain" in excinfo.value.reason
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


# ============================================================================
# Regression guards (Codex audit findings)
# ============================================================================


def test_measured_zero_token_weight_is_not_defaulted_to_balanced():
    """A measured ``"0"`` weight (e.g. one side of a 0/100 pool) must
    survive as ``0.0`` — not get rounded to ``0.5`` by an Empty != Zero
    fallback. An unmeasured ``""`` weight still falls back to balanced
    0.5 (no signal => assume mid-pool)."""
    measured_zero = MagicMock()
    measured_zero.GetPoolAnalytics.return_value = _make_response(
        # token0_weight non-default below; reach into the response shape:
    )
    # Build a response with token weights explicitly set on the wire.
    response = _make_response()
    response.token0_weight = "0"
    response.token1_weight = "1.0"
    measured_zero.GetPoolAnalytics.return_value = response
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(measured_zero))

    envelope = reader.get_pool_analytics(
        pool_address=_ANTONIS_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
    )
    assert envelope.value.token0_weight == 0.0
    assert envelope.value.token1_weight == 1.0

    # Unmeasured -> default balanced.
    unmeasured = MagicMock()
    response2 = _make_response()
    response2.token0_weight = ""
    response2.token1_weight = ""
    unmeasured.GetPoolAnalytics.return_value = response2
    reader2 = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(unmeasured))
    envelope2 = reader2.get_pool_analytics(
        pool_address=_ANTONIS_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
    )
    assert envelope2.value.token0_weight == 0.5
    assert envelope2.value.token1_weight == 0.5


def test_disconnected_gateway_client_raises_datasource_unavailable():
    """CodeRabbit PR #2389: ``GatewayClient.pool_analytics`` raises
    ``RuntimeError`` when not connected. The reader must map that to
    ``DataSourceUnavailable`` (not leak the RuntimeError) so the runner's
    HOLD path fires via the same DATA_UNAVAILABLE classification as a
    real outage."""
    fake_gateway = MagicMock()
    type(fake_gateway).pool_analytics = property(
        lambda _self: (_ for _ in ()).throw(RuntimeError("Gateway client not connected")),
    )
    reader = PoolAnalyticsReader(gateway_client=fake_gateway)

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.get_pool_analytics(
            pool_address=_ANTONIS_POOL,
            chain="arbitrum",
            protocol="uniswap_v3",
        )
    assert "not connected" in excinfo.value.reason
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


def test_unmeasured_fields_surface_via_metadata_and_decay_confidence():
    """Blocker #2 from the multi-auditor audit: callers must be able to
    distinguish measured zero from unmeasured without re-parsing the
    wire. The PoolAnalytics dataclass exposes ``unmeasured_fields`` and
    ``DataMeta.confidence`` decays from 0.85 by ~0.15 per missing field."""
    stub = MagicMock()
    response = _make_response()
    # Simulate a partial provider response: TVL measured, both volume
    # fields and fee APR/APY unmeasured.
    response.tvl_usd = "1210000.0"
    response.volume_24h_usd = ""
    response.volume_7d_usd = ""
    response.fee_apr = ""
    response.fee_apy = ""
    stub.GetPoolAnalytics.return_value = response
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.get_pool_analytics(
        pool_address=_ANTONIS_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
    )

    # tvl_usd is measured -> not in unmeasured_fields.
    assert "tvl_usd" not in envelope.value.unmeasured_fields
    # The four empty fields ARE in the set.
    assert {"volume_24h_usd", "volume_7d_usd", "fee_apr", "fee_apy"} <= envelope.value.unmeasured_fields
    # Confidence decayed from 0.85 baseline by 4 * 0.15.
    assert envelope.meta.confidence < 0.85
    assert abs(envelope.meta.confidence - 0.25) < 0.01

    # Fully-measured response keeps confidence at 0.85 and the set empty.
    full = MagicMock()
    full.GetPoolAnalytics.return_value = _make_response()  # all populated
    reader2 = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(full))
    envelope2 = reader2.get_pool_analytics(
        pool_address=_ANTONIS_POOL,
        chain="arbitrum",
        protocol="uniswap_v3",
    )
    assert envelope2.value.unmeasured_fields == frozenset()
    assert abs(envelope2.meta.confidence - 0.85) < 0.01


def test_solana_pool_address_is_not_lowercased_by_framework_reader():
    """Blocker #1: framework reader must preserve case for Solana addresses.
    Lowercasing a base58 string yields a different address."""
    solana_addr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    stub = MagicMock()
    stub.GetPoolAnalytics.return_value = _make_response()
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    reader.get_pool_analytics(
        pool_address=solana_addr,
        chain="solana",
        protocol="raydium_clmm",
    )

    sent_request = stub.GetPoolAnalytics.call_args.args[0]
    assert sent_request.pool_address == solana_addr  # case preserved


def test_best_pool_raises_datasource_unavailable_for_hold_path():
    """``best_pool`` is deferred to VIB-4729 (SearchPools RPC). Until then
    it must raise ``DataSourceUnavailable`` so the runner's HOLD path fires
    via ``classify_failure`` — never ``NotImplementedError`` (which would
    crash the iteration loop)."""
    stub = MagicMock()
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    with pytest.raises(DataSourceUnavailable) as excinfo:
        reader.best_pool(
            token_a="USDC",
            token_b="WETH",
            chain="arbitrum",
        )
    assert "VIB-4729" in excinfo.value.reason
    assert classify_failure(excinfo.value) == FailureKind.DATA_UNAVAILABLE


# =============================================================================
# VIB-6599 — list_token_pools + rank_token_pools
# =============================================================================


_BTT = "0xc669928185dbce49d2230cc9b0979be6dc797957"


def _token_pools_response(
    *,
    success: bool = True,
    error: str = "",
    source: str = "coingecko_onchain",
    complete: bool = True,
    product_distinct: bool = True,
    rows: list[tuple[str, str, str, str]] | None = None,
) -> gateway_pb2.TokenPoolsResponse:
    """Rows are ``(pool_address, dex_id, reserve_usd, volume_24h_usd)``."""
    rows = (
        rows
        if rows is not None
        else [
            ("0x2d0ba902badaa82592f0e1c04c71d66cea21d921", "uniswap_v2", "211219.19", "84.35"),
            ("0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59", "uniswap_v3", "10.76", "0"),
        ]
    )
    return gateway_pb2.TokenPoolsResponse(
        chain="ethereum",
        token_address=_BTT,
        pools=[
            gateway_pb2.TokenPoolRow(
                pool_address=address,
                dex_id=dex_id,
                name="BTT / WETH",
                reserve_usd=reserve,
                volume_24h_usd=volume,
            )
            for address, dex_id, reserve, volume in rows
        ],
        source=source,
        observed_at=0,
        success=success,
        error=error,
        complete=complete,
        product_distinct_dex_id=product_distinct,
    )


def test_list_token_pools_decodes_rows_and_carries_both_caveats():
    stub = MagicMock()
    stub.ListTokenPools.return_value = _token_pools_response()
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.list_token_pools(token_address=_BTT.upper(), chain="Ethereum")

    result = envelope.value
    assert result.source == "coingecko_onchain"
    assert result.complete is True
    assert result.product_distinct_dex_id is True
    assert [p.dex_id for p in result.pools] == ["uniswap_v2", "uniswap_v3"]
    assert result.pools[0].reserve_usd == Decimal("211219.19")
    # A measured zero survives as Decimal(0) — NOT as None.
    assert result.pools[1].volume_24h_usd == Decimal("0")

    # EVM address lower-cased before it reaches the gateway. Not cosmetic: the
    # upstream answers a checksummed address with an EMPTY LIST rather than an
    # error, so a missed lowercase reads as "this token has no venues at all".
    assert stub.ListTokenPools.call_args.args[0].token_address == _BTT
    # Strict by default: opting in is the caller's explicit choice.
    assert stub.ListTokenPools.call_args.args[0].allow_fallback_provider is False


def test_list_token_pools_keeps_unmeasured_money_as_none():
    """Empty != Zero: an unreported reserve must not arrive as ``Decimal(0)``.

    ``PoolAnalytics`` collapses unmeasured to ``Decimal(0)`` for pre-VIB-4727
    callers; this surface deliberately does not, because telling a dead venue
    from an unreported one is the entire job.
    """
    stub = MagicMock()
    stub.ListTokenPools.return_value = _token_pools_response(
        rows=[("0x2d0ba902badaa82592f0e1c04c71d66cea21d921", "uniswap_v2", "", "")]
    )
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    pool = reader.list_token_pools(token_address=_BTT, chain="ethereum").value.pools[0]

    assert pool.reserve_usd is None
    assert pool.volume_24h_usd is None


def test_list_token_pools_forwards_the_fallback_opt_in():
    stub = MagicMock()
    stub.ListTokenPools.return_value = _token_pools_response(source="dexscreener", product_distinct=False)
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    envelope = reader.list_token_pools(token_address=_BTT, chain="ethereum", allow_fallback_provider=True)

    assert stub.ListTokenPools.call_args.args[0].allow_fallback_provider is True
    assert envelope.value.product_distinct_dex_id is False
    # Both caveats are also legible without unpacking the dataclass.
    assert envelope.meta.confidence < 0.85


def test_list_token_pools_empty_venue_list_is_success_not_failure():
    """"No venues on this chain" is a measured answer a caller must be able to act on."""
    stub = MagicMock()
    stub.ListTokenPools.return_value = _token_pools_response(rows=[])
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(stub))

    assert reader.list_token_pools(token_address=_BTT, chain="ethereum").value.pools == ()


def test_list_token_pools_maps_failures_to_the_hold_path():
    """Both wire failure channels must classify as DATA_UNAVAILABLE, not crash the loop."""
    unsuccessful = MagicMock()
    unsuccessful.ListTokenPools.return_value = _token_pools_response(success=False, error="providers exhausted")
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(unsuccessful))
    with pytest.raises(DataSourceUnavailable) as envelope_failure:
        reader.list_token_pools(token_address=_BTT, chain="ethereum")
    assert classify_failure(envelope_failure.value) == FailureKind.DATA_UNAVAILABLE

    rpc_error = MagicMock()
    rpc_error.ListTokenPools.side_effect = grpc.RpcError("gateway down")
    reader = PoolAnalyticsReader(gateway_client=_fake_gateway_with_stub(rpc_error))
    with pytest.raises(DataSourceUnavailable) as transport_failure:
        reader.list_token_pools(token_address=_BTT, chain="ethereum")
    assert classify_failure(transport_failure.value) == FailureKind.DATA_UNAVAILABLE


def test_rank_token_pools_sorts_deepest_first_with_unmeasured_last():
    from almanak.framework.data.pools.analytics import TokenPool, rank_token_pools

    def _pool(name: str, reserve: Decimal | None) -> TokenPool:
        return TokenPool(
            pool_address=f"0x{name}",
            dex_id="uniswap_v3",
            name=name,
            reserve_usd=reserve,
            volume_24h_usd=None,
            base_token_address="",
            quote_token_address="",
        )

    pools = (
        _pool("unknown", None),
        _pool("shallow", Decimal("10")),
        _pool("empty", Decimal("0")),
        _pool("deep", Decimal("211219")),
    )

    # Unmeasured sorts LAST, not as zero: ranking an unknown depth below a
    # measured-empty pool would assert something the data does not say.
    assert [p.name for p in rank_token_pools(pools)] == ["deep", "shallow", "empty", "unknown"]

    # With a floor, unmeasured is DROPPED — "unknown" does not satisfy ">= X".
    assert [p.name for p in rank_token_pools(pools, Decimal("5"))] == ["deep", "shallow"]
