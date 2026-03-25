"""Teardown tests for PendleYTYieldStrategy (VIB-1850).

Validates that teardown uses sufficiently wide slippage for illiquid YT markets.
"""

from decimal import Decimal
from unittest.mock import patch

from strategies.demo.pendle_yt_yield import PendleYTYieldStrategy


def _make_strategy(**overrides) -> PendleYTYieldStrategy:
    with patch.object(PendleYTYieldStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = PendleYTYieldStrategy.__new__(PendleYTYieldStrategy)

    strategy._strategy_id = "test-pendle-yt"
    strategy._chain = "arbitrum"
    strategy._wallet_address = "0x" + "ab" * 20
    strategy.market = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
    strategy.market_name = "wstETH-25JUN2026"
    strategy.base_token = "WSTETH"
    strategy.base_token_symbol = "WSTETH"
    strategy.yt_token = "YT-wstETH-25JUN2026"
    strategy.yt_token_symbol = "YT-wstETH"
    strategy.max_slippage_bps = 200
    strategy.teardown_hard_slippage_bps = 1500
    strategy.teardown_soft_slippage_bps = 500
    strategy.stop_loss_pct = 50
    strategy._has_entered_position = True
    strategy._consecutive_holds = 0
    strategy._entry_value_usd = Decimal("10")

    for k, v in overrides.items():
        setattr(strategy, k, v)

    return strategy


def test_teardown_hard_uses_wide_slippage() -> None:
    """HARD teardown must use wide slippage for illiquid YT (VIB-1850)."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy()
    intents = strategy.generate_teardown_intents(TeardownMode.HARD)

    assert len(intents) == 1
    swap = intents[0]
    assert swap.from_token == "YT-wstETH-25JUN2026"
    assert swap.to_token == "WSTETH"
    # 15% slippage (1500bps) for HARD mode
    assert swap.max_slippage == Decimal("0.15")


def test_teardown_soft_uses_moderate_slippage() -> None:
    """SOFT teardown uses moderate slippage (5%)."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy()
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

    assert len(intents) == 1
    assert intents[0].max_slippage == Decimal("0.05")


def test_teardown_empty_when_no_position() -> None:
    """No teardown intents when no position exists."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy(_has_entered_position=False)
    intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
    assert intents == []


def test_teardown_slippage_configurable() -> None:
    """Teardown slippage can be overridden via config."""
    from almanak.framework.teardown import TeardownMode

    strategy = _make_strategy(teardown_hard_slippage_bps=2000)
    intents = strategy.generate_teardown_intents(TeardownMode.HARD)

    assert intents[0].max_slippage == Decimal("0.20")
