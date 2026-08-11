"""Unit tests for the uniswap_rsi demo.

Snapshots come from ``almanak.framework.market.testing.seeded(...)``; time is
driven by advancing the snapshot ``timestamp`` between decide() calls.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.demo_strategies.uniswap_rsi.strategy import UniswapRSIStrategy
from almanak.framework.market.models import RSIData, TokenBalance
from almanak.framework.market.testing import seeded

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
COOLDOWN_S = 3600


@pytest.fixture
def config() -> dict:
    cfg = json.loads((Path(__file__).parent.parent / "config.json").read_text())
    cfg["trade_cooldown_seconds"] = COOLDOWN_S
    return cfg


@pytest.fixture
def strategy(config: dict) -> UniswapRSIStrategy:
    return UniswapRSIStrategy(
        config=config,
        chain=config["chain"],
        wallet_address="0x" + "1" * 40,
    )


def snap(ts: datetime, *, rsi: Decimal = Decimal("50")):
    """A funded market at ``ts`` with a configurable WETH RSI."""
    return seeded(
        chain="ethereum",
        prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
        balances={
            "USDC": TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")),
            "WETH": TokenBalance(symbol="WETH", balance=Decimal("1"), balance_usd=Decimal("2000")),
        },
        indicators={"WETH:rsi:14": RSIData(value=rsi)},
        timestamp=ts,
    )


class TestMarketClock:
    def test_decide_captures_snapshot_timestamp(self, strategy: UniswapRSIStrategy) -> None:
        strategy.decide(snap(T0))
        assert strategy._last_seen_market_ts == T0

    def test_confirmed_buy_stamps_market_clock(self, strategy: UniswapRSIStrategy) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("25")))
        assert intent.intent_type.value == "SWAP"

        strategy.on_intent_executed(intent, True, SimpleNamespace(swap_amounts=None))

        assert strategy._last_trade_at == T0

    def test_cooldown_measured_on_market_clock(self, strategy: UniswapRSIStrategy) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("25")))
        strategy.on_intent_executed(intent, True, SimpleNamespace(swap_amounts=None))

        strategy.decide(snap(T0 + timedelta(minutes=30)))
        assert strategy._cooldown_remaining_seconds() == pytest.approx(COOLDOWN_S - 1800)

        strategy.decide(snap(T0 + timedelta(hours=2)))
        assert strategy._cooldown_remaining_seconds() == 0.0

    def test_failed_fill_does_not_stamp_cooldown(self, strategy: UniswapRSIStrategy) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("25")))
        strategy.on_intent_executed(intent, False, SimpleNamespace(swap_amounts=None))
        assert strategy._last_trade_at is None


class TestPersistence:
    def test_trade_stamp_survives_restart(self, strategy: UniswapRSIStrategy, config: dict) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("25")))
        strategy.on_intent_executed(intent, True, SimpleNamespace(swap_amounts=None))

        state = strategy.get_persistent_state()
        fresh = UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        fresh.load_persistent_state(state)

        assert fresh._last_trade_at == T0


class TestPoolPinning:
    """swap_params exact-pool pinning via the optional ``swap_pool`` config."""

    POOL = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # mainnet USDC/WETH 0.05%

    def test_trading_swaps_carry_the_pin(self, config: dict) -> None:
        config["swap_pool"] = self.POOL
        strategy = UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        buy = strategy.decide(snap(T0, rsi=Decimal("25")))
        assert buy.intent_type.value == "SWAP"
        assert buy.swap_params == {"pool": self.POOL}

    def test_unset_pool_keeps_auto_routing(self, strategy: UniswapRSIStrategy) -> None:
        buy = strategy.decide(snap(T0, rsi=Decimal("25")))
        assert buy.intent_type.value == "SWAP"
        assert buy.swap_params is None

    def test_teardown_sweep_stays_unpinned(self, config: dict) -> None:
        config["swap_pool"] = self.POOL
        strategy = UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=snap(T0))
        swaps = [i for i in intents if i.intent_type.value == "SWAP"]
        assert swaps, "teardown should sweep the base token"
        assert all(i.swap_params is None for i in swaps)

    def test_malformed_pool_rejected_at_preflight(self, config: dict) -> None:
        from almanak.framework.strategies import ConfigValidationError

        config["swap_pool"] = "0x123"
        with pytest.raises(ConfigValidationError, match="swap_pool"):
            UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)

    def test_pool_pin_rejected_for_non_v3_fork(self, config: dict) -> None:
        from almanak.framework.strategies import ConfigValidationError

        config["swap_pool"] = self.POOL
        config["protocol"] = "traderjoe_v2"
        config["chain"] = "avalanche"
        with pytest.raises(ConfigValidationError, match="V3 fork"):
            UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)

    def test_sell_swaps_carry_the_pin(self, config: dict) -> None:
        config["swap_pool"] = self.POOL
        strategy = UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        sell = strategy.decide(snap(T0, rsi=Decimal("75")))
        assert sell.intent_type.value == "SWAP"
        assert sell.swap_params == {"pool": self.POOL}

    def test_non_hex_pool_of_correct_length_rejected_at_preflight(self, config: dict) -> None:
        from almanak.framework.strategies import ConfigValidationError

        config["swap_pool"] = "0x" + "g" * 40  # 42 chars, not hex
        with pytest.raises(ConfigValidationError, match="swap_pool"):
            UniswapRSIStrategy(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
