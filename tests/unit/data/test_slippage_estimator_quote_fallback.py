"""SlippageEstimator routing through connector-owned executable quotes.

A Curve 2crv LP strategy gates LP_OPEN on ``market.estimate_slippage(..., protocol="curve")``
and fails closed (permanent HOLD) when it raises. Curve has no V3 tick reader, so the
V3-only estimator could never produce an estimate. These tests pin:

1. The pre-fix failure on the no-quote-wiring path (regression guard).
2. The fix: a registered connector swap quoter yields a usable SlippageEstimate.
3. Fail-loud preserved: a failing/absent quoter does NOT fabricate zero slippage.
4. Concentrated-liquidity estimates use quotes without loading tick depth.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._strategy_base.swap_quote_registry import (
    SLIPPAGE_REFERENCE_STABLE_PARITY,
    SLIPPAGE_REFERENCE_UNSUPPORTED,
    SLIPPAGE_REFERENCE_V3_SPOT,
)
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.pools.liquidity import (
    LiquidityDepthReader,
    SlippageEstimator,
)
from almanak.framework.data.pools.reader import PoolReaderRegistry

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _StubResolved:
    def __init__(self, address: str, decimals: int) -> None:
        self.address = address
        self.decimals = decimals


class _StubTokenResolver:
    """Resolves the two stable symbols to addresses + 6 decimals."""

    _MAP = {
        "USDC.E": ("0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", 6),
        "USDC": ("0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
        "USDT": ("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", 6),
        "WETH": ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", 18),
        "ETH": ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", 18),
    }

    def resolve_for_swap(self, token: str, chain: str) -> _StubResolved:  # noqa: ARG002
        addr, dec = self._MAP[token.upper()]
        return _StubResolved(addr, dec)


class _FakeQuoteResult:
    def __init__(self, amount_out: int, *, metadata: dict | None = None) -> None:
        self.amount_out = amount_out
        self.source = "fake"
        self.metadata = {} if metadata is None else metadata


class _FakeSwapQuoteRegistry:
    """Minimal SWAP_QUOTE_REGISTRY double recording calls and returning a quote."""

    def __init__(self, *, registered: bool, result=None, raises: Exception | None = None) -> None:
        self._registered = registered
        self._result = result
        self._raises = raises
        self.call_count = 0
        self.last_request = None

    def get(self, protocol: str):  # noqa: ARG002
        return object() if self._registered else None

    def quote_swap(self, ctx, request):  # noqa: ARG002
        self.call_count += 1
        self.last_request = request
        if self._raises is not None:
            raise self._raises
        return self._result


_CURVE = {
    "token_in": "USDC.e",
    "token_out": "USDT",
    "amount": Decimal("5"),
    "chain": "arbitrum",
    "protocol": "curve",
}
_CURVE_POOL = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"


def _stable_curve_quote(amount_out: int) -> _FakeQuoteResult:
    return _FakeQuoteResult(
        amount_out=amount_out,
        metadata={
            "pool_address": _CURVE_POOL,
            "pool_type": "stableswap",
            "slippage_reference": SLIPPAGE_REFERENCE_STABLE_PARITY,
        },
    )


class _StubRegistryTokenResolver:
    """Pool-reader-registry-side resolver (duck type: ``.resolve`` -> ``.address``).

    Mirrors the live builder wiring, where the PoolReaderRegistry carries a
    TokenResolver — the configuration under which the curated Curve 2pool
    actually resolves for the USDC.e/USDT pair.
    """

    def resolve(self, token: str, chain: str) -> _StubResolved:  # noqa: ARG002
        addr, dec = _StubTokenResolver._MAP[token.upper()]
        return _StubResolved(addr, dec)


def _estimator(*, swap_quote_registry=None, quote_ctx=None, token_resolver=None, pool_registry=None):
    reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)
    return SlippageEstimator(
        liquidity_reader=reader,
        pool_reader_registry=pool_registry or PoolReaderRegistry(rpc_call=lambda *a: b"\x00" * 32),
        swap_quote_registry=swap_quote_registry,
        quote_ctx=quote_ctx,
        token_resolver=token_resolver,
    )


# ---------------------------------------------------------------------------
# 1. Pre-fix behaviour (no quote wiring) — reproduces the HOLD
# ---------------------------------------------------------------------------


def test_curve_without_quote_wiring_raises_unavailable():
    """Without the swap-quote fallback wired, protocol='curve' is unavailable.

    This is exactly the production path that left the strategy in permanent HOLD:
    no V3 reader for Curve, no fallback -> DataUnavailableError.
    """
    est = _estimator()  # no swap_quote_registry/quote_ctx/token_resolver
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)


# ---------------------------------------------------------------------------
# 2. The fix — registered quoter yields a usable estimate
# ---------------------------------------------------------------------------


def test_curve_with_quote_fallback_returns_estimate():
    """5 USDC.e -> 4.995 USDT (6 dec) => 10 bps impact, finite slippage."""
    registry = _FakeSwapQuoteRegistry(registered=True, result=_stable_curve_quote(4_995_000))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )

    envelope = est.estimate_slippage(**_CURVE)
    estimate = envelope.value

    assert registry.call_count == 1
    assert estimate.price_impact_bps == 10  # 0.1%
    assert estimate.effective_slippage_bps == 10
    assert estimate.expected_price == Decimal("0.999")
    assert estimate.recommended_max_size > 0
    # The strategy gate divides price_impact_bps by 10000 -> finite slippage -> LP_OPEN.
    assert Decimal(estimate.price_impact_bps) / Decimal("10000") == Decimal("0.001")


def test_curve_catalog_free_resolution_uses_quote_fallback():
    """Curve pair lookup stays absent after removing the SDK-wide catalog.

    A deployment-selected exact pool belongs to the Curve connector's quote
    path, not the generic V3 pool reader. The connector quote fallback must
    therefore remain usable when pair-only pool resolution returns ``None``.
    """
    pool_registry = PoolReaderRegistry(
        rpc_call=lambda *a: b"\x00" * 32,
        token_resolver=_StubRegistryTokenResolver(),
    )
    # No exact deployment pool was supplied, so pair-only discovery cannot
    # nominate a Curve target.
    curve_reader = pool_registry.get_reader("arbitrum", "curve")
    assert curve_reader.resolve_pool_address("USDC.e", "USDT", "arbitrum") is None

    quote_registry = _FakeSwapQuoteRegistry(registered=True, result=_stable_curve_quote(4_995_000))
    est = _estimator(
        swap_quote_registry=quote_registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
        pool_registry=pool_registry,
    )

    envelope = est.estimate_slippage(**_CURVE)

    assert quote_registry.call_count == 1
    assert envelope.value.price_impact_bps == 10


def test_protocol_sweep_never_returns_non_v3_pool():
    """The protocol=None sweep must never resolve a non-slot0 pool into the
    tick lane: ``protocols_for_chain`` sorts 'curve' ahead of the v3 forks, so
    an ungated sweep would return the curated Curve pool first for its
    curated pairs."""
    pool_registry = PoolReaderRegistry(
        rpc_call=lambda *a: b"\x00" * 32,
        token_resolver=_StubRegistryTokenResolver(),
    )
    est = _estimator(pool_registry=pool_registry)

    resolved = est._resolve_pool("USDC.e", "USDT", "arbitrum", None, None)

    curve_2pool = "0x7f90122BF0700F9E7e1F688fe926940E8839F353"
    assert resolved is None or resolved.lower() != curve_2pool.lower()


def test_favourable_quote_floored_at_zero_impact():
    """exec_price > 1 (got more than 1:1) reports zero impact, not negative."""
    registry = _FakeSwapQuoteRegistry(registered=True, result=_stable_curve_quote(5_001_000))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    estimate = est.estimate_slippage(**_CURVE).value
    assert estimate.price_impact_bps == 0
    assert estimate.effective_slippage_bps == 0


# ---------------------------------------------------------------------------
# 3. Fail-loud preserved
# ---------------------------------------------------------------------------


def test_quoter_failure_raises_not_fabricated_zero():
    """A registered-but-failing quoter must raise, not return zero slippage."""
    from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteUnavailable

    registry = _FakeSwapQuoteRegistry(registered=True, raises=SwapQuoteUnavailable("pool dead"))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)


def test_no_registered_quoter_for_protocol_raises_unavailable():
    """Registry wired but protocol has no quoter -> original 'no pool found'."""
    registry = _FakeSwapQuoteRegistry(registered=False)
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)
    assert registry.call_count == 0  # never consulted the quoter


def test_unresolvable_token_declines_fallback():
    """A resolver returning None for a token declines the fallback (no crash)."""

    class _NoneResolver:
        def resolve_for_swap(self, token, chain):  # noqa: ARG002
            return None

    registry = _FakeSwapQuoteRegistry(registered=True, result=_FakeQuoteResult(amount_out=4_995_000))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_NoneResolver(),
    )
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)
    assert registry.call_count == 0  # never reached the quoter


def test_non_stable_rate_declines_fallback():
    """A wildly off rate (non-stable pool) is not modelled with a 1.0 mid -> raises."""
    # 5 token_in -> 0.0001 token_out (rate 0.00002) is outside the stable band.
    registry = _FakeSwapQuoteRegistry(registered=True, result=_stable_curve_quote(100))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)


@pytest.mark.parametrize("protocol", ["uniswap_v4", "fluid", "aerodrome"])
def test_in_band_quote_without_safe_reference_fails_closed(protocol: str):
    """A near-parity execution rate is not proof that an arbitrary AMM is stable."""
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=4_995_000,
            metadata={"slippage_reference": SLIPPAGE_REFERENCE_UNSUPPORTED},
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )

    with pytest.raises(DataUnavailableError, match="supported slippage reference"):
        est.estimate_slippage(**(_CURVE | {"protocol": protocol}))


def test_missing_slippage_reference_fails_closed():
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(amount_out=4_995_000),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )

    with pytest.raises(DataUnavailableError, match="supported slippage reference"):
        est.estimate_slippage(**_CURVE)


# ---------------------------------------------------------------------------
# 4. Concentrated-liquidity quotes bypass tick loading
# ---------------------------------------------------------------------------


def test_v3_quote_bypasses_tick_depth_and_preserves_exact_route(monkeypatch):
    """A pinned V3 quote uses executable output without loading any ticks."""
    from datetime import UTC, datetime

    from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
    from almanak.framework.data.pools.reader import PoolPrice

    pool = "0x1111111111111111111111111111111111111111"
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=499_000_000_000_000,
            metadata={
                "fee_tier": 500,
                "pool_address": pool,
                "pool_key": 500,
                "pool_key_kind": "fee_tier",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
            },
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    monkeypatch.setattr(est, "_resolve_pool", lambda *a, **k: pool)
    monkeypatch.setattr(est, "_is_zero_for_one", lambda *a, **k: True)
    monkeypatch.setattr(
        est,
        "_read_pool_price",
        lambda *a, **k: DataEnvelope(
            value=PoolPrice(
                price=Decimal("0.0005"),
                tick=0,
                liquidity=10**18,
                fee_tier=500,
                block_number=1,
                timestamp=datetime.now(UTC),
                pool_address=pool,
                token0_decimals=6,
                token1_decimals=18,
            ),
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        ),
    )
    monkeypatch.setattr(
        est._liquidity_reader,
        "read_liquidity_depth",
        lambda **kwargs: pytest.fail(f"tick depth must not be read: {kwargs}"),
    )

    envelope = est.estimate_slippage(
        token_in="USDC",
        token_out="WETH",
        amount=Decimal("1"),
        chain="arbitrum",
        protocol="uniswap_v3",
        pool_address=pool,
        fee_tier=500,
    )

    assert registry.call_count == 1
    assert registry.last_request.pool_address == pool
    assert registry.last_request.fee_tier == 500
    assert envelope.value.expected_price == Decimal("0.000499")
    assert envelope.value.effective_slippage_bps == 20
    assert envelope.meta.source == "fake"


def test_v3_quote_metadata_pool_mismatch_fails_before_spot_read(monkeypatch):
    requested_pool = "0x1111111111111111111111111111111111111111"
    quoted_pool = "0x2222222222222222222222222222222222222222"
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=499_000_000_000_000,
            metadata={
                "pool_address": quoted_pool,
                "pool_key": 500,
                "pool_key_kind": "fee_tier",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
            },
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    monkeypatch.setattr(est, "_resolve_pool", lambda *args, **kwargs: requested_pool)
    monkeypatch.setattr(
        est,
        "_read_pool_price",
        lambda *args, **kwargs: pytest.fail("spot must not be read for a mismatched route"),
    )

    with pytest.raises(DataUnavailableError, match="metadata does not match"):
        est.estimate_slippage(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("1"),
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_address=requested_pool,
            fee_tier=500,
        )


def test_v3_quote_discriminator_mismatch_fails_before_pool_resolution(monkeypatch):
    pool = "0x1111111111111111111111111111111111111111"
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=499_000_000_000_000,
            metadata={
                "pool_address": pool,
                "pool_key": 3000,
                "pool_key_kind": "fee_tier",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
            },
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    monkeypatch.setattr(
        est,
        "_resolve_pool",
        lambda *args, **kwargs: pytest.fail("pool must not resolve for a mismatched discriminator"),
    )

    with pytest.raises(DataUnavailableError, match="does not match requested fee_tier 500"):
        est.estimate_slippage(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("1"),
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_address=pool,
            fee_tier=500,
        )


def test_v3_quote_discriminator_kind_mismatch_fails_closed(monkeypatch):
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=499_000_000_000_000,
            metadata={
                "pool_key": 500,
                "pool_key_kind": "tick_spacing",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
            },
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    monkeypatch.setattr(
        est,
        "_resolve_pool",
        lambda *args, **kwargs: pytest.fail("pool must not resolve for a mismatched discriminator kind"),
    )

    with pytest.raises(DataUnavailableError, match="expected 'fee_tier'"):
        est.estimate_slippage(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("1"),
            chain="arbitrum",
            protocol="uniswap_v3",
            fee_tier=500,
        )


def test_v3_native_input_uses_wrapped_address_and_reverse_spot(monkeypatch):
    from datetime import UTC, datetime

    from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
    from almanak.framework.data.pools.reader import PoolPrice

    pool = "0x1111111111111111111111111111111111111111"
    registry = _FakeSwapQuoteRegistry(
        registered=True,
        result=_FakeQuoteResult(
            amount_out=1_998_000_000,
            metadata={
                "pool_address": pool,
                "pool_key": 500,
                "pool_key_kind": "fee_tier",
                "slippage_reference": SLIPPAGE_REFERENCE_V3_SPOT,
            },
        ),
    )
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    resolved_pairs: list[tuple[str, str]] = []
    direction_pairs: list[tuple[str, str]] = []

    def resolve_pool(token_in, token_out, *args):
        resolved_pairs.append((token_in, token_out))
        return pool

    def is_zero_for_one(token_in, token_out, *args):
        direction_pairs.append((token_in, token_out))
        return False

    monkeypatch.setattr(est, "_resolve_pool", resolve_pool)
    monkeypatch.setattr(est, "_is_zero_for_one", is_zero_for_one)
    monkeypatch.setattr(
        est,
        "_read_pool_price",
        lambda *args, **kwargs: DataEnvelope(
            value=PoolPrice(
                price=Decimal("0.0005"),
                tick=0,
                liquidity=10**18,
                fee_tier=500,
                block_number=1,
                timestamp=datetime.now(UTC),
                pool_address=pool,
                token0_decimals=6,
                token1_decimals=18,
            ),
            meta=DataMeta(source="test", observed_at=datetime.now(UTC), finality="latest"),
            classification=DataClassification.EXECUTION_GRADE,
        ),
    )

    envelope = est.estimate_slippage(
        token_in="ETH",
        token_out="USDC",
        amount=Decimal("1"),
        chain="arbitrum",
        protocol="uniswap_v3",
        pool_address=pool,
        fee_tier=500,
    )

    executable_pair = (
        _StubTokenResolver._MAP["WETH"][0],
        _StubTokenResolver._MAP["USDC"][0],
    )
    assert resolved_pairs == [executable_pair]
    assert direction_pairs == [executable_pair]
    assert registry.last_request.token_in == executable_pair[0]
    assert envelope.value.expected_price == Decimal("1998")
    assert envelope.value.effective_slippage_bps == 10
