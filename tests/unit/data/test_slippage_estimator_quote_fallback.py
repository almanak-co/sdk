"""ALM-2896: SlippageEstimator AMM/stableswap fallback via connector swap quote.

A Curve 2crv LP strategy gates LP_OPEN on ``market.estimate_slippage(..., protocol="curve")``
and fails closed (permanent HOLD) when it raises. Curve has no V3 tick reader, so the
V3-only estimator could never produce an estimate. These tests pin:

1. The pre-fix failure on the no-quote-wiring path (regression guard).
2. The fix: a registered connector swap quoter yields a usable SlippageEstimate.
3. Fail-loud preserved: a failing/absent quoter does NOT fabricate zero slippage.
4. The V3 path is untouched (the quote fallback is not consulted when a V3 pool resolves).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

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
        "USDT": ("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", 6),
    }

    def resolve_for_swap(self, token: str, chain: str) -> _StubResolved:  # noqa: ARG002
        addr, dec = self._MAP[token.upper()]
        return _StubResolved(addr, dec)


class _FakeQuoteResult:
    def __init__(self, amount_out: int) -> None:
        self.amount_out = amount_out
        self.source = "fake"
        self.metadata: dict = {}


class _FakeSwapQuoteRegistry:
    """Minimal SWAP_QUOTE_REGISTRY double recording calls and returning a quote."""

    def __init__(self, *, registered: bool, result=None, raises: Exception | None = None) -> None:
        self._registered = registered
        self._result = result
        self._raises = raises
        self.call_count = 0

    def get(self, protocol: str):  # noqa: ARG002
        return object() if self._registered else None

    def quote_swap(self, ctx, request):  # noqa: ARG002
        self.call_count += 1
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


def _estimator(*, swap_quote_registry=None, quote_ctx=None, token_resolver=None):
    reader = LiquidityDepthReader(rpc_call=lambda *a: b"\x00" * 32)
    return SlippageEstimator(
        liquidity_reader=reader,
        pool_reader_registry=PoolReaderRegistry(rpc_call=lambda *a: b"\x00" * 32),
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
    registry = _FakeSwapQuoteRegistry(registered=True, result=_FakeQuoteResult(amount_out=4_995_000))
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


def test_favourable_quote_floored_at_zero_impact():
    """exec_price > 1 (got more than 1:1) reports zero impact, not negative."""
    registry = _FakeSwapQuoteRegistry(registered=True, result=_FakeQuoteResult(amount_out=5_001_000))
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
    registry = _FakeSwapQuoteRegistry(registered=True, result=_FakeQuoteResult(amount_out=100))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )
    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(**_CURVE)


# ---------------------------------------------------------------------------
# 4. V3 path untouched — the quote fallback is not consulted for V3 protocols
# ---------------------------------------------------------------------------


def test_v3_path_does_not_consult_quote_fallback(monkeypatch):
    """When a V3 pool resolves, the quote registry is never called."""
    registry = _FakeSwapQuoteRegistry(registered=True, result=_FakeQuoteResult(amount_out=4_995_000))
    est = _estimator(
        swap_quote_registry=registry,
        quote_ctx=object(),
        token_resolver=_StubTokenResolver(),
    )

    # Force _resolve_pool to return a pool so the V3 branch is taken; then make
    # _read_pool_price raise so we stay off the network but still prove the
    # quote fallback was never reached (call_count stays 0).
    monkeypatch.setattr(est, "_resolve_pool", lambda *a, **k: "0xpool")

    with pytest.raises(DataUnavailableError):
        est.estimate_slippage(
            token_in="USDC",
            token_out="WETH",
            amount=Decimal("1"),
            chain="arbitrum",
            protocol="uniswap_v3",
        )
    assert registry.call_count == 0
