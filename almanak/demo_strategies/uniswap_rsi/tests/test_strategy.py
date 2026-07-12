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
