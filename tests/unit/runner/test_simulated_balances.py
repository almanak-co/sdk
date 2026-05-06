"""Tests for dry-run simulated_balances injection in StrategyRunner.

VIB-2329: When running `--dry-run --no-gateway`, strategies that gate on token
balance always HOLD because the off-chain balance stack returns 0 for chains
where the wallet has no real funds. simulated_balances in config.json lets
strategy authors inject synthetic balances for testing strategy logic.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.market import MarketSnapshot, TokenBalance


# =============================================================================
# Helpers
# =============================================================================


def _make_runner(dry_run: bool = True) -> StrategyRunner:
    """Create a minimal StrategyRunner with dry_run configured."""
    runner = StrategyRunner.__new__(StrategyRunner)
    runner.config = MagicMock()
    runner.config.dry_run = dry_run
    return runner


def _make_snapshot() -> MarketSnapshot:
    """Create a minimal MarketSnapshot with no providers."""
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xdeadbeef",
        price_oracle=None,
        balance_provider=None,
    )


def _make_strategy(simulated_balances: dict | None = None) -> MagicMock:
    """Create a mock strategy with optional simulated_balances config."""
    strategy = MagicMock()

    def _get_config(key: str, default=None):
        if key == "simulated_balances":
            return simulated_balances
        return default

    strategy.get_config.side_effect = _get_config
    return strategy


# =============================================================================
# Tests
# =============================================================================


def test_inject_simulated_balances_sets_token_balance():
    """Balances from simulated_balances config are injected into the snapshot."""
    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy({"USDC": "10000", "WETH": "5"})

    runner._inject_simulated_balances(market, strategy)

    usdc = market.balance("USDC")
    assert usdc.balance == Decimal("10000")
    assert usdc.symbol == "USDC"

    weth = market.balance("WETH")
    assert weth.balance == Decimal("5")
    assert weth.symbol == "WETH"


def test_inject_simulated_balances_noop_when_no_config():
    """No injection occurs when simulated_balances is absent from config."""
    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy(simulated_balances=None)

    runner._inject_simulated_balances(market, strategy)

    # Without a balance provider or pre-populated balances, balance() raises ValueError
    with pytest.raises(ValueError, match="Cannot determine balance"):
        market.balance("USDC")


def test_inject_simulated_balances_noop_when_empty_dict():
    """No injection occurs when simulated_balances is an empty dict."""
    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy(simulated_balances={})

    runner._inject_simulated_balances(market, strategy)

    with pytest.raises(ValueError, match="Cannot determine balance"):
        market.balance("USDC")


def test_inject_simulated_balances_skips_invalid_amounts(caplog):
    """Invalid amount strings log a warning and are skipped."""
    import logging

    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy({"USDC": "not_a_number", "WETH": "5"})

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        runner._inject_simulated_balances(market, strategy)

    # USDC should NOT be set (invalid amount)
    with pytest.raises(ValueError, match="Cannot determine balance"):
        market.balance("USDC")

    # WETH should still be set
    assert market.balance("WETH").balance == Decimal("5")


def test_inject_simulated_balances_computes_balance_usd_via_price_oracle():
    """balance_usd is computed using the market price oracle when available."""
    runner = _make_runner()

    # Set up a snapshot with a price oracle that returns $2000 for WETH
    def mock_price_oracle(token: str, quote: str = "USD") -> Decimal:
        prices = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
        return prices[token]

    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xdeadbeef",
        price_oracle=mock_price_oracle,
        balance_provider=None,
    )
    strategy = _make_strategy({"WETH": "5"})

    runner._inject_simulated_balances(market, strategy)

    weth = market.balance("WETH")
    assert weth.balance == Decimal("5")
    assert weth.balance_usd == Decimal("10000")  # 5 * 2000


def test_inject_simulated_balances_fallback_balance_usd_zero_on_price_failure():
    """balance_usd falls back to 0 when price lookup fails."""
    runner = _make_runner()

    def failing_price_oracle(token: str, quote: str = "USD") -> Decimal:
        raise RuntimeError("Price unavailable")

    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xdeadbeef",
        price_oracle=failing_price_oracle,
        balance_provider=None,
    )
    strategy = _make_strategy({"WETH": "5"})

    runner._inject_simulated_balances(market, strategy)

    weth = market.balance("WETH")
    assert weth.balance == Decimal("5")
    assert weth.balance_usd == Decimal("0")  # Safe fallback


@pytest.mark.parametrize(
    "bad_amount",
    ["-10000", "-0.01", "0", "NaN", "Infinity", "-Infinity"],
)
def test_inject_simulated_balances_skips_invalid_values(bad_amount, caplog):
    """Non-positive and non-finite amounts are rejected with a warning."""
    import logging

    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy({"USDC": bad_amount, "WETH": "5"})

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        runner._inject_simulated_balances(market, strategy)

    # USDC with invalid amount must NOT be injected
    with pytest.raises(ValueError, match="Cannot determine balance"):
        market.balance("USDC")

    # WETH with positive finite amount must still be injected
    assert market.balance("WETH").balance == Decimal("5")


def test_inject_simulated_balances_multi_chain_injects_all_chains():
    """Balances are injected into every chain of a MultiChainMarketSnapshot.

    Also verifies that per-chain price() is used correctly for USD valuation.
    """
    from almanak.framework.market import MultiChainMarketSnapshot

    runner = _make_runner()
    strategy = _make_strategy({"USDC": "10000", "WETH": "5"})

    # Chain-aware price oracle: USDC=1 on both chains, WETH=2000 on arbitrum / 1900 on optimism
    def multi_price_oracle(token: str, quote: str, chain: str) -> Decimal:
        prices = {
            ("USDC", "USD", "arbitrum"): Decimal("1"),
            ("USDC", "USD", "optimism"): Decimal("1"),
            ("WETH", "USD", "arbitrum"): Decimal("2000"),
            ("WETH", "USD", "optimism"): Decimal("1900"),
        }
        return prices[(token, quote, chain)]

    market = MultiChainMarketSnapshot(
        chains=["arbitrum", "optimism"],
        wallet_address="0xdeadbeef",
        price_oracle=multi_price_oracle,
        balance_provider=None,
    )

    runner._inject_simulated_balances(market, strategy)

    arb_usdc = market.balance("USDC", chain="arbitrum")
    assert arb_usdc.balance == Decimal("10000")
    assert arb_usdc.symbol == "USDC"
    assert arb_usdc.balance_usd == Decimal("10000")  # 10000 * 1

    opt_usdc = market.balance("USDC", chain="optimism")
    assert opt_usdc.balance == Decimal("10000")
    assert opt_usdc.balance_usd == Decimal("10000")

    # Verify chain-aware pricing: WETH priced differently per chain
    arb_weth = market.balance("WETH", chain="arbitrum")
    assert arb_weth.balance == Decimal("5")
    assert arb_weth.balance_usd == Decimal("10000")  # 5 * 2000

    opt_weth = market.balance("WETH", chain="optimism")
    assert opt_weth.balance == Decimal("5")
    assert opt_weth.balance_usd == Decimal("9500")  # 5 * 1900


def test_inject_simulated_balances_skips_when_balance_provider_active():
    """Injection is skipped when the market snapshot has a real balance provider.

    MarketSnapshot.balance() prefers pre-populated balances over the provider,
    so injecting with an active gateway would silently override real on-chain data.
    """
    sentinel = TokenBalance(symbol="USDC", balance=Decimal("999"), balance_usd=Decimal("999"))

    def real_provider(token: str) -> TokenBalance:
        return sentinel

    runner = _make_runner()
    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xdeadbeef",
        price_oracle=None,
        balance_provider=real_provider,
    )
    strategy = _make_strategy({"USDC": "10000"})

    runner._inject_simulated_balances(market, strategy)

    # Pre-populated simulated balance must NOT override the real provider.
    # _balances dict should still be empty — provider is queried on balance() call.
    assert "USDC" not in market._balances


def test_inject_simulated_balances_non_dict_config_logs_warning(caplog):
    """A non-dict simulated_balances value logs a warning and is skipped."""
    import logging

    runner = _make_runner()
    market = _make_snapshot()
    strategy = _make_strategy(simulated_balances="USDC:10000")  # wrong type: string

    with caplog.at_level(logging.WARNING, logger="almanak.framework.runner.strategy_runner"):
        runner._inject_simulated_balances(market, strategy)

    assert any("must be a dict" in r.message for r in caplog.records)
    with pytest.raises(ValueError, match="Cannot determine balance"):
        market.balance("USDC")
