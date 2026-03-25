"""Tests for balancer_flash_arb EOA detection and swap fallback (VIB-1848)."""

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from strategies.demo.balancer_flash_arb.strategy import BalancerFlashArbStrategy


def _make_strategy(**overrides) -> BalancerFlashArbStrategy:
    with patch.object(BalancerFlashArbStrategy, "__init__", lambda self, *a, **kw: None):
        strategy = BalancerFlashArbStrategy.__new__(BalancerFlashArbStrategy)

    strategy._strategy_id = "test-balancer-flash"
    strategy._chain = "arbitrum"
    strategy._wallet_address = "0x" + "ab" * 20
    strategy.flash_loan_amount_usd = Decimal("1000")
    strategy.max_slippage_pct = 1.0
    strategy.base_token = "WETH"
    strategy.quote_token = "USDC"
    strategy.force_action = "flash_loan"
    strategy._trades_executed = 0
    strategy._fell_back_to_swap = False
    strategy._compiler = None
    strategy._current_intent = None

    for k, v in overrides.items():
        setattr(strategy, k, v)

    return strategy


def test_eoa_wallet_falls_back_to_swap() -> None:
    """EOA wallet with flash_loan config must fall back to swap (VIB-1848)."""
    strategy = _make_strategy(force_action="flash_loan")
    market = MagicMock()

    with patch.object(strategy, "_is_contract_wallet", return_value=False):
        intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "SWAP"
    assert intent.from_token == "USDC"
    assert intent.to_token == "WETH"
    assert strategy._fell_back_to_swap is True


def test_contract_wallet_uses_flash_loan() -> None:
    """Contract wallet with flash_loan config uses flash loan intent."""
    strategy = _make_strategy(force_action="flash_loan")
    market = MagicMock()

    with patch.object(strategy, "_is_contract_wallet", return_value=True):
        intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "FLASH_LOAN"


def test_swap_mode_always_works() -> None:
    """Swap mode works regardless of wallet type."""
    strategy = _make_strategy(force_action="swap")
    market = MagicMock()

    intent = strategy.decide(market)

    assert intent is not None
    assert intent.intent_type.value == "SWAP"


def test_is_contract_wallet_returns_false_without_gateway() -> None:
    """Without gateway, assumes EOA."""
    strategy = _make_strategy()
    assert strategy._is_contract_wallet() is False


def test_eoa_fallback_teardown_generates_intents() -> None:
    """Teardown must generate intents after EOA fallback swap (VIB-1848)."""
    strategy = _make_strategy(force_action="flash_loan", _trades_executed=1, _fell_back_to_swap=True)

    intents = strategy.generate_teardown_intents(mode=MagicMock())
    assert len(intents) == 1
    assert intents[0].intent_type.value == "SWAP"
    assert intents[0].from_token == "WETH"
    assert intents[0].to_token == "USDC"


def test_config_defaults_to_swap() -> None:
    """Default config should use swap mode (not flash_loan)."""
    import json

    config_path = Path(__file__).resolve().parents[1] / "config.json"
    with config_path.open() as f:
        config = json.load(f)
    assert config["force_action"] == "swap"
