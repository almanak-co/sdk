"""Tests for the runner teardown-path PT/YT price warming (VIB-5537).

``_warm_teardown_pt_yt_prices`` is the runner-driven sibling of the
``warm_and_validate_oracle`` pre-flight seam. The committed fix (``ef18b0f``)
warmed PT/YT prices only on the pre-flight path
(``oracle_warmup.warm_and_validate_oracle``) — but the production
``teardown request`` runner path calls ``_execute_intents`` via
``execute_and_verify`` and never reaches that function. A real-fork run proved
the runner path needs its own warming step before the VIB-2928 price guard, or a
Pendle PT close hard-stops.

This module pins the behaviour of the extracted runner-path helper directly,
driving it with a fake market that exposes the real ``pt_price`` contract
(``PtPriceData`` + ``ValueConfidence``):

(a) usable PT price (HIGH) -> merged into the runner price oracle (key upper-cased).
(b) ``pt_price`` returns None / UNAVAILABLE -> NOT merged (Empty != Zero) so the
    VIB-2928 guard still hard-stops.
(c) ``pt_price`` raises -> helper is best-effort and does not raise; oracle unchanged.
(d) ``teardown_market is None`` or ``price_oracle is None`` -> no-op, no crash.
"""

from decimal import Decimal

from almanak.framework.intents.vocabulary import Intent
from almanak.framework.market.models import PtPriceData
from almanak.framework.portfolio.models import ValueConfidence
from almanak.framework.runner._teardown_helpers import _warm_teardown_pt_yt_prices

_PT_SYMBOL = "PT-SUSDAI-15OCT2026"


class _FakeStrategy:
    """Minimal strategy stand-in: the helper only reads ``.chain``."""

    def __init__(self, chain: str | None = "arbitrum"):
        self.chain = chain


class _FakeMarketWithPt:
    """A MarketSnapshot-like fake exposing the ``pt_price`` gateway RPC.

    ``pt_results`` maps an upper-cased PT/YT symbol to the ``PtPriceData`` the
    gateway would return. An absent symbol yields the canonical Empty != Zero
    result (``price=None`` + ``UNAVAILABLE``), exactly as ``MarketSnapshot.pt_price``
    does for an unmeasured PT. The runner-path helper reads no price oracle off
    the market — it mutates the runner-supplied ``price_oracle`` dict — so this
    fake only needs ``pt_price``.
    """

    def __init__(self, pt_results: dict[str, PtPriceData], chain: str = "arbitrum"):
        self._pt_results = pt_results
        self.chain = chain
        self.pt_price_calls: list[tuple[str, str | None]] = []

    def pt_price(self, symbol: str, chain=None, maturity=None, *, quote: str = "USD") -> PtPriceData:
        self.pt_price_calls.append((symbol, chain))
        existing = self._pt_results.get(symbol.upper())
        if existing is not None:
            return existing
        return PtPriceData(symbol=symbol, chain=chain or "", price=None, confidence=ValueConfidence.UNAVAILABLE)


class _RaisingMarket:
    """A market whose ``pt_price`` raises — exercises best-effort semantics."""

    chain = "arbitrum"

    def pt_price(self, symbol: str, chain=None, maturity=None, *, quote: str = "USD") -> PtPriceData:
        raise RuntimeError("gateway GetPtPrice RPC exploded")


def _pt_swap_intent() -> Intent:
    return Intent.swap(from_token=_PT_SYMBOL, to_token="USDC", amount=Decimal("100"), chain="arbitrum")


# ---------------------------------------------------------------------------
# (a) usable PT price merged into the runner oracle
# ---------------------------------------------------------------------------


def test_runner_warm_merges_measured_high_pt_price():
    """VIB-5537 (a): a HIGH-confidence measured PT price is merged into the
    runner-supplied price oracle under the upper-cased symbol key."""
    market = _FakeMarketWithPt(
        {
            _PT_SYMBOL: PtPriceData(
                symbol=_PT_SYMBOL, chain="arbitrum", price=Decimal("0.97"), confidence=ValueConfidence.HIGH
            )
        }
    )
    price_oracle: dict[str, Decimal] = {"USDC": Decimal("1")}

    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [_pt_swap_intent()], price_oracle)

    # Measured price merged under the upper-cased symbol.
    assert price_oracle[_PT_SYMBOL] == Decimal("0.97")
    # Existing entries are untouched.
    assert price_oracle["USDC"] == Decimal("1")
    # Warmed via the dedicated pt_price RPC on the intent's chain.
    assert (_PT_SYMBOL, "arbitrum") in market.pt_price_calls


def test_runner_warm_uses_market_chain_when_strategy_chain_absent():
    """The helper falls back to the market chain when the strategy carries none."""
    market = _FakeMarketWithPt(
        {
            _PT_SYMBOL: PtPriceData(
                symbol=_PT_SYMBOL, chain="arbitrum", price=Decimal("0.97"), confidence=ValueConfidence.HIGH
            )
        },
        chain="arbitrum",
    )
    price_oracle: dict[str, Decimal] = {}

    _warm_teardown_pt_yt_prices(_FakeStrategy(chain=None), market, [_pt_swap_intent()], price_oracle)

    assert price_oracle[_PT_SYMBOL] == Decimal("0.97")


# ---------------------------------------------------------------------------
# (b) unavailable PT price NOT merged (Empty != Zero)
# ---------------------------------------------------------------------------


def test_runner_warm_unavailable_pt_not_merged():
    """VIB-5537 (b): an UNAVAILABLE / None PT price is never merged, so the
    runner oracle is unchanged and the VIB-2928 guard would still hard-stop."""
    # pt_results omits the symbol -> the fake returns price=None + UNAVAILABLE.
    market = _FakeMarketWithPt({})
    price_oracle: dict[str, Decimal] = {"USDC": Decimal("1")}

    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [_pt_swap_intent()], price_oracle)

    # Nothing fabricated; the PT stays absent.
    assert _PT_SYMBOL not in price_oracle
    assert price_oracle == {"USDC": Decimal("1")}
    # The RPC was attempted (best-effort), it just returned no usable price.
    assert (_PT_SYMBOL, "arbitrum") in market.pt_price_calls


def test_runner_warm_zero_pt_price_not_merged():
    """VIB-5537 (b/Empty != Zero): a zero PT price is unpriceable, never merged."""
    market = _FakeMarketWithPt(
        {
            _PT_SYMBOL: PtPriceData(
                symbol=_PT_SYMBOL, chain="arbitrum", price=Decimal("0"), confidence=ValueConfidence.HIGH
            )
        }
    )
    price_oracle: dict[str, Decimal] = {}

    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [_pt_swap_intent()], price_oracle)

    assert _PT_SYMBOL not in price_oracle
    assert price_oracle == {}


# ---------------------------------------------------------------------------
# (c) pt_price raises -> best-effort, helper does not raise, oracle unchanged
# ---------------------------------------------------------------------------


def test_runner_warm_pt_price_raises_is_best_effort():
    """VIB-5537 (c): when ``pt_price`` raises the helper must NOT propagate —
    teardown's first job is to remove on-chain risk; the guard fires loud
    downstream instead. The runner oracle is left unchanged."""
    market = _RaisingMarket()
    price_oracle: dict[str, Decimal] = {"USDC": Decimal("1")}

    # Must not raise.
    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [_pt_swap_intent()], price_oracle)

    # Oracle unchanged — no fabricated price entered.
    assert _PT_SYMBOL not in price_oracle
    assert price_oracle == {"USDC": Decimal("1")}


# ---------------------------------------------------------------------------
# (d) None market / None oracle -> no-op, no crash
# ---------------------------------------------------------------------------


def test_runner_warm_none_market_is_noop():
    """VIB-5537 (d): ``teardown_market is None`` -> no-op, no crash."""
    price_oracle: dict[str, Decimal] = {"USDC": Decimal("1")}
    _warm_teardown_pt_yt_prices(_FakeStrategy(), None, [_pt_swap_intent()], price_oracle)
    assert price_oracle == {"USDC": Decimal("1")}


def test_runner_warm_none_oracle_is_noop():
    """VIB-5537 (d): ``price_oracle is None`` -> no-op, no crash, no RPC."""
    market = _FakeMarketWithPt(
        {
            _PT_SYMBOL: PtPriceData(
                symbol=_PT_SYMBOL, chain="arbitrum", price=Decimal("0.97"), confidence=ValueConfidence.HIGH
            )
        }
    )
    # Must not raise even though there is a priceable PT in the plan.
    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [_pt_swap_intent()], None)
    # The helper short-circuits before touching the market.
    assert market.pt_price_calls == []


def test_runner_warm_regular_plan_does_not_call_pt_price():
    """A regular (non-PT) plan is a no-op: pt_price is never invoked and the
    oracle is unchanged — the common teardown case pays nothing."""
    market = _FakeMarketWithPt({})
    price_oracle: dict[str, Decimal] = {"WETH": Decimal("3400"), "USDC": Decimal("1")}
    intent = Intent.swap(from_token="WETH", to_token="USDC", amount="all", chain="arbitrum")

    _warm_teardown_pt_yt_prices(_FakeStrategy(), market, [intent], price_oracle)

    assert market.pt_price_calls == []
    assert price_oracle == {"WETH": Decimal("3400"), "USDC": Decimal("1")}
