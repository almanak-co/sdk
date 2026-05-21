from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.demo_strategies.uniswap_rsi.strategy import UniswapRSIStrategy
from almanak.framework.strategies import ConfigValidationError


class _RSI:
    def __init__(self, value: str) -> None:
        self.value = Decimal(value)


class _Balance:
    def __init__(self, balance: str, balance_usd: str) -> None:
        self.balance = Decimal(balance)
        self.balance_usd = Decimal(balance_usd)


class _Market:
    chain = "avalanche"

    def __init__(self, rsi: str = "20") -> None:
        self._rsi = rsi

    def price(self, token: str) -> Decimal:
        return Decimal("25")

    def rsi(self, token: str, period: int) -> _RSI:
        return _RSI(self._rsi)

    def balance(self, token: str) -> _Balance:
        if token == "USDC":
            return _Balance("100", "100")
        return _Balance("1", "25")


def _strategy(config: dict[str, object]) -> UniswapRSIStrategy:
    return UniswapRSIStrategy(
        config=config,
        chain=str(config["chain"]),
        wallet_address="0x" + "11" * 20,
    )


def test_traderjoe_avalanche_config_controls_swap_protocol() -> None:
    strategy = _strategy(
        {
            "chain": "avalanche",
            "protocol": "traderjoe_v2",
            "base_token": "WAVAX",
            "quote_token": "USDC",
            "trade_size_usd": 3,
            "rsi_period": 14,
            "rsi_oversold": 40,
            "rsi_overbought": 70,
            "max_slippage_bps": 100,
        }
    )

    intent = strategy.decide(_Market())

    assert intent is not None
    assert intent.protocol == "traderjoe_v2"
    assert intent.from_token == "USDC"
    assert intent.to_token == "WAVAX"


def test_rsi_strategy_buys_only_on_fresh_oversold_transition() -> None:
    strategy = _strategy(
        {
            "chain": "avalanche",
            "protocol": "traderjoe_v2",
            "base_token": "WAVAX",
            "quote_token": "USDC",
            "trade_size_usd": 3,
            "rsi_period": 14,
            "rsi_oversold": 40,
            "rsi_overbought": 70,
            "max_slippage_bps": 100,
        }
    )

    # First iteration: OVERSOLD signal, produce a swap intent. The latch is
    # NOT set yet — strategy must wait for the runner to report success.
    first = strategy.decide(_Market(rsi="20"))
    assert first is not None and first.intent_type.value == "SWAP"

    # Framework reports successful execution -> latch flips to OVERSOLD.
    strategy.on_intent_executed(first, True, object())

    # Second iteration still in OVERSOLD zone — should HOLD now.
    second = strategy.decide(_Market(rsi="21"))
    assert second is not None and second.intent_type.value == "HOLD"
    assert "remains oversold" in second.reason

    # Neutral RSI resets the latch to NEUTRAL.
    neutral = strategy.decide(_Market(rsi="50"))
    assert neutral is not None and neutral.intent_type.value == "HOLD"

    # Re-entering OVERSOLD after neutral fires another SWAP intent.
    third = strategy.decide(_Market(rsi="22"))
    assert third is not None and third.intent_type.value == "SWAP"


def test_rsi_signal_gate_only_latches_on_success() -> None:
    """A failed execution must NOT lock the strategy into the HOLD path.

    Regression guard for the bug Codex caught: marking ``_last_rsi_signal``
    in ``decide()`` before the framework confirms success caused transient
    RPC failures to suppress every subsequent retry.
    """
    strategy = _strategy(
        {
            "chain": "avalanche",
            "protocol": "traderjoe_v2",
            "base_token": "WAVAX",
            "quote_token": "USDC",
            "trade_size_usd": 3,
            "rsi_period": 14,
            "rsi_oversold": 40,
            "rsi_overbought": 70,
            "max_slippage_bps": 100,
        }
    )
    first = strategy.decide(_Market(rsi="20"))
    assert first is not None and first.intent_type.value == "SWAP"

    # Simulate transient framework failure (compile error, RPC drop, ...).
    strategy.on_intent_executed(first, False, object())

    # Latch must still be NEUTRAL — retry on the next tick.
    second = strategy.decide(_Market(rsi="20"))
    assert second is not None and second.intent_type.value == "SWAP"


def test_rsi_signal_gate_persists_across_restart() -> None:
    strategy = _strategy(
        {
            "chain": "avalanche",
            "protocol": "traderjoe_v2",
            "base_token": "WAVAX",
            "quote_token": "USDC",
            "trade_size_usd": 3,
            "rsi_period": 14,
            "rsi_oversold": 40,
            "rsi_overbought": 70,
            "max_slippage_bps": 100,
        }
    )
    first = strategy.decide(_Market(rsi="20"))
    assert first is not None and first.intent_type.value == "SWAP"
    # Successful swap latches OVERSOLD, which is what persists.
    strategy.on_intent_executed(first, True, object())

    restored = _strategy(
        {
            "chain": "avalanche",
            "protocol": "traderjoe_v2",
            "base_token": "WAVAX",
            "quote_token": "USDC",
            "trade_size_usd": 3,
            "rsi_period": 14,
            "rsi_oversold": 40,
            "rsi_overbought": 70,
            "max_slippage_bps": 100,
        }
    )
    restored.load_persistent_state(strategy.get_persistent_state())

    second = restored.decide(_Market(rsi="21"))

    assert second is not None and second.intent_type.value == "HOLD"
    assert "remains oversold" in second.reason


def test_invalid_protocol_chain_pair_fails_validation() -> None:
    with pytest.raises(ConfigValidationError, match="does not support chain"):
        _strategy(
            {
                "chain": "optimism",
                "protocol": "traderjoe_v2",
                "base_token": "WETH",
                "quote_token": "USDC",
                "trade_size_usd": 3,
                "rsi_period": 14,
                "rsi_oversold": 40,
                "rsi_overbought": 70,
                "max_slippage_bps": 100,
            }
        )
