"""VIB-5124 — producer-side by-address price backfill for coingecko_id-null legs.

``StrategyRunner._backfill_address_priced_legs`` runs a post-pass over the
assembled ledger price-oracle. For each receipt-derived token leg whose canonical
SYMBOL is missing from the oracle AND whose registry entry has
``coingecko_id is None`` (the precise condition that defeats by-symbol pricing),
it prices the leg BY ADDRESS — engaging the oracle's CoinGecko-contract /
DexScreener-by-address paths — and stores the result under the canonical SYMBOL
key the LP accounting consumer reads.

These tests prove:
- A coingecko_id-null token (SUSDAI) is resolved by address and keyed by symbol.
- A normal symbol-priceable token (coingecko_id present) is a no-op (no
  regression — the normal by-symbol path owns it).
- An already-present symbol is never overwritten.
- A price miss / non-positive price leaves the key absent (Empty≠Zero).
- The fix is primitive-agnostic: the same code path populates a lending/perp
  leg as long as the result exposes its token addresses (via the shared
  ``currency0``/``currency1`` receipt fields).
- The nested with_sources shape carries provenance.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

# Arbitrum addresses for the fixtures.
SUSDAI_ADDR = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
RESOLVER_PATH = "almanak.framework.data.tokens.get_token_resolver"


def _make_runner() -> StrategyRunner:
    config = RunnerConfig(
        default_interval_seconds=1,
        enable_state_persistence=False,
        enable_alerting=False,
        dry_run=False,
        decide_timeout_seconds=30.0,
    )
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=config,
    )


def _resolved(symbol: str, coingecko_id: str | None):
    """A minimal ResolvedToken stand-in carrying symbol + coingecko_id."""
    return SimpleNamespace(symbol=symbol, coingecko_id=coingecko_id)


def _market(chain: str = "arbitrum", prices: dict[str, Decimal] | None = None):
    """A market whose ``price(address)`` returns the configured per-address price.

    ``price_data(address).source`` is supplied so the nested-shape path can stamp
    provenance.
    """
    prices = prices or {}
    market = MagicMock()
    market.chain = chain

    def _price(token, chain=None):
        # ``_receipt_token_legs`` normalises addresses to lowercase before they
        # reach ``price()``, so match case-insensitively (the on-chain / resolver
        # casing contract). Keeps the fixture honest about the lowered address.
        for key, value in prices.items():
            if isinstance(token, str) and token.lower() == key.lower():
                return value
        raise ValueError(f"no price for {token}")

    market.price.side_effect = _price
    market.price_data.return_value = SimpleNamespace(source="coingecko_contract")
    return market


def _lp_result(currency0: str | None, currency1: str | None):
    """An ExecutionResult-like object exposing lp_open_data with currency addrs."""
    lp_open = SimpleNamespace(currency0=currency0, currency1=currency1)
    return SimpleNamespace(lp_open_data=lp_open, lp_close_data=None)


def _intent(chain: str = "arbitrum"):
    return SimpleNamespace(chain=chain)


# ---------------------------------------------------------------------------
# Headline fix: coingecko_id-null leg priced by address, keyed by symbol
# ---------------------------------------------------------------------------


def test_coingecko_null_leg_priced_by_address_keyed_by_symbol():
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("1.04")})
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=USDC_ADDR)

    def _resolve(token, chain, **kwargs):
        if token.lower() == SUSDAI_ADDR.lower():
            return _resolved("SUSDAI", None)  # coingecko_id null ⇒ must backfill
        if token.lower() == USDC_ADDR.lower():
            return _resolved("USDC", "usd-coin")  # has cg id ⇒ skipped
        raise AssertionError(token)

    resolver = MagicMock()
    resolver.resolve.side_effect = _resolve

    with patch(RESOLVER_PATH, return_value=resolver):
        # USDC already priced by the normal symbol path; SUSDAI missing.
        oracle = {"USDC": Decimal("1.00")}
        out = runner._backfill_address_priced_legs(market, oracle, _intent(), result, with_sources=False)

    assert out["SUSDAI"] == Decimal("1.04")  # priced by ADDRESS, keyed by SYMBOL
    assert out["USDC"] == Decimal("1.00")  # untouched
    # Only the null-cg token was priced by address; USDC's cg-id leg is skipped.
    # Address is lowercased at the shared ``_receipt_token_legs`` seam (lane symmetry).
    market.price.assert_called_once_with(SUSDAI_ADDR.lower(), chain="arbitrum")


def test_normal_symbol_token_is_a_noop():
    """A token WITH a coingecko_id is owned by the by-symbol path — the backfill
    must not touch it (no regression / no redundant by-address call)."""
    runner = _make_runner()
    market = _market(prices={USDC_ADDR: Decimal("1.00")})
    result = _lp_result(currency0=USDC_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.return_value = _resolved("USDC", "usd-coin")

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {}, _intent(), result, with_sources=False)

    assert out == {}  # nothing backfilled
    market.price.assert_not_called()


def test_existing_symbol_price_never_overwritten():
    """A higher-confidence cached symbol price must win — backfill only fills
    MISSING symbol keys."""
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("9.99")})  # would-be backfill value
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.return_value = _resolved("SUSDAI", None)

    with patch(RESOLVER_PATH, return_value=resolver):
        oracle = {"SUSDAI": Decimal("1.05")}  # already present
        out = runner._backfill_address_priced_legs(market, oracle, _intent(), result, with_sources=False)

    assert out["SUSDAI"] == Decimal("1.05")  # preserved
    market.price.assert_not_called()


def test_price_miss_leaves_key_absent_empty_not_zero():
    """A by-address price miss must leave the SYMBOL key ABSENT (Empty≠Zero) —
    never a fabricated $0."""
    runner = _make_runner()
    market = _market(prices={})  # price(address) raises for everything
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.return_value = _resolved("SUSDAI", None)

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {}, _intent(), result, with_sources=False)

    assert "SUSDAI" not in out


def test_non_positive_price_fails_closed():
    """A 0/negative by-address price is an oracle miss, not a value — fail closed."""
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("0")})
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.return_value = _resolved("SUSDAI", None)

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {}, _intent(), result, with_sources=False)

    assert "SUSDAI" not in out


def test_nested_with_sources_shape_carries_provenance():
    """The nested G12 shape must carry the real oracle source, not 'unknown'."""
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("1.04")})
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.return_value = _resolved("SUSDAI", None)

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {}, _intent(), result, with_sources=True)

    assert out["SUSDAI"]["price_usd"] == "1.04"
    assert out["SUSDAI"]["oracle_source"] == "coingecko_contract"
    assert out["SUSDAI"]["confidence"] == "HIGH"


# ---------------------------------------------------------------------------
# Primitive-agnostic: the fix lives at the shared assembly layer
# ---------------------------------------------------------------------------


def test_no_result_is_noop():
    """No receipt ⇒ nothing to backfill (failure path / non-LP intents)."""
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("1.04")})

    with patch(RESOLVER_PATH) as mock_resolver:
        out = runner._backfill_address_priced_legs(market, {"USDC": Decimal("1")}, _intent(), None, with_sources=False)

    assert out == {"USDC": Decimal("1")}
    mock_resolver.assert_not_called()


def test_lp_close_legs_also_backfilled():
    """LP_CLOSE returns both legs; the close-side receipt's currency addrs feed
    the same backfill (after the VIB-5032 full-drain close, the close needs the
    coingecko-null price too)."""
    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("1.04")})
    lp_close = SimpleNamespace(currency0=SUSDAI_ADDR, currency1=USDC_ADDR)
    result = SimpleNamespace(lp_open_data=None, lp_close_data=lp_close)

    def _resolve(token, chain, **kwargs):
        return _resolved("SUSDAI", None) if token.lower() == SUSDAI_ADDR.lower() else _resolved("USDC", "usd-coin")

    resolver = MagicMock()
    resolver.resolve.side_effect = _resolve

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {"USDC": Decimal("1.00")}, _intent(), result, with_sources=False)

    assert out["SUSDAI"] == Decimal("1.04")


def test_receipt_token_legs_extracts_addresses_only():
    """``_receipt_token_legs`` is the shared, primitive-agnostic discovery seam:
    it returns receipt token ADDRESSES (symbols are resolved downstream), so any
    primitive whose result exposes currency0/currency1 contributes legs."""
    # LP open — addresses are normalised to lowercase at this shared seam so
    # both lanes consume identically-cased addresses (VIB-5124 lane symmetry).
    legs = StrategyRunner._receipt_token_legs(_lp_result(SUSDAI_ADDR, USDC_ADDR))
    assert legs == [SUSDAI_ADDR.lower(), USDC_ADDR.lower()]
    # No receipt
    assert StrategyRunner._receipt_token_legs(None) == []
    # Non-address values filtered (e.g. a symbol accidentally stamped)
    assert StrategyRunner._receipt_token_legs(_lp_result("USDC", None)) == []
    # Dedupe identical addresses
    assert StrategyRunner._receipt_token_legs(_lp_result(SUSDAI_ADDR, SUSDAI_ADDR)) == [SUSDAI_ADDR.lower()]


def test_resolver_failure_is_skipped_not_raised():
    """A resolver miss on a leg must not raise — best-effort, skip that leg."""
    from almanak.framework.data.tokens import TokenNotFoundError

    runner = _make_runner()
    market = _market(prices={SUSDAI_ADDR: Decimal("1.04")})
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    resolver = MagicMock()
    resolver.resolve.side_effect = TokenNotFoundError(token=SUSDAI_ADDR, chain="arbitrum", reason="x")

    with patch(RESOLVER_PATH, return_value=resolver):
        out = runner._backfill_address_priced_legs(market, {}, _intent(), result, with_sources=False)

    assert out == {}


@pytest.mark.parametrize("missing_chain_obj", [SimpleNamespace(), SimpleNamespace(chain=None)])
def test_no_chain_is_noop(missing_chain_obj):
    """Without a chain we cannot price by address — no-op, no crash."""
    runner = _make_runner()
    market = MagicMock()
    market.chain = None  # market has no chain
    result = _lp_result(currency0=SUSDAI_ADDR, currency1=None)

    with patch(RESOLVER_PATH) as mock_resolver:
        out = runner._backfill_address_priced_legs(market, {}, missing_chain_obj, result, with_sources=False)

    assert out == {}
    mock_resolver.assert_not_called()
