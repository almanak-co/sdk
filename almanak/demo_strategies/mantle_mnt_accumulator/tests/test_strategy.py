"""Unit tests for the mantle_mnt_accumulator demo.

Snapshots come from ``almanak.framework.market.testing.seeded(...)``; time is
driven by advancing the snapshot ``timestamp`` between decide() calls.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.demo_strategies.mantle_mnt_accumulator.strategy import MantleMntAccumulator
from almanak.framework.market.models import RSIData, TokenBalance
from almanak.framework.market.testing import seeded

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def config() -> dict:
    return json.loads((Path(__file__).parent.parent / "config.json").read_text())


@pytest.fixture
def strategy(config: dict) -> MantleMntAccumulator:
    return MantleMntAccumulator(
        config=config,
        chain=config["chain"],
        wallet_address="0x" + "1" * 40,
    )


def snap(ts: datetime, *, rsi: Decimal = Decimal("50")):
    """A funded market at ``ts`` with a configurable WMNT RSI."""
    return seeded(
        chain="mantle",
        prices={"WMNT": Decimal("0.6"), "USDT": Decimal("1")},
        balances={
            "USDT": TokenBalance(symbol="USDT", balance=Decimal("1000"), balance_usd=Decimal("1000")),
            "WMNT": TokenBalance(symbol="WMNT", balance=Decimal("50"), balance_usd=Decimal("30")),
        },
        indicators={"WMNT:rsi:14": RSIData(value=rsi)},
        timestamp=ts,
    )


class TestMarketClock:
    def test_decide_captures_snapshot_timestamp(self, strategy: MantleMntAccumulator) -> None:
        strategy.decide(snap(T0))
        assert strategy._last_seen_market_ts == T0

    def test_confirmed_fill_stamps_market_clock(self, strategy: MantleMntAccumulator) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("30")))
        assert intent.intent_type.value == "SWAP"

        strategy.on_intent_executed(intent, True, SimpleNamespace())

        assert strategy._last_trade_time == T0

    def test_cooldown_measured_on_market_clock(self, strategy: MantleMntAccumulator) -> None:
        """Cooldown is 5min (config.json): blocked at +2m, free at +10m."""
        intent = strategy.decide(snap(T0, rsi=Decimal("30")))
        strategy.on_intent_executed(intent, True, SimpleNamespace())

        held = strategy.decide(snap(T0 + timedelta(minutes=2), rsi=Decimal("30")))
        assert held.intent_type.value == "HOLD"
        assert "Cooldown active" in held.reason

        strategy.decide(snap(T0 + timedelta(minutes=10), rsi=Decimal("30")))
        assert strategy._cooldown_passed()

    def test_failed_fill_does_not_stamp_cooldown(self, strategy: MantleMntAccumulator) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("30")))
        strategy.on_intent_executed(intent, False, SimpleNamespace())
        assert strategy._last_trade_time is None


class TestPersistence:
    def test_trade_stamp_survives_restart(self, strategy: MantleMntAccumulator, config: dict) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("30")))
        strategy.on_intent_executed(intent, True, SimpleNamespace())

        state = strategy.get_persistent_state()
        fresh = MantleMntAccumulator(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        fresh.load_persistent_state(state)

        assert fresh._last_trade_time == T0


class TestFeeTierPinning:
    """swap_params tier pinning via the optional ``swap_fee_tier`` config."""

    def test_accumulation_swaps_carry_the_pin(self, config: dict) -> None:
        config["swap_fee_tier"] = 500
        strategy = MantleMntAccumulator(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        # NEUTRAL RSI with ample stables triggers the regular accumulation buy.
        intent = strategy.decide(snap(T0, rsi=Decimal("50")))
        assert intent.intent_type.value == "SWAP"
        assert intent.swap_params == {"fee_tier": 500}

    def test_unset_tier_keeps_auto_routing(self, strategy: MantleMntAccumulator) -> None:
        intent = strategy.decide(snap(T0, rsi=Decimal("50")))
        assert intent.intent_type.value == "SWAP"
        assert intent.swap_params is None

    def test_teardown_sweep_stays_unpinned(self, config: dict) -> None:
        from almanak.framework.teardown import TeardownMode

        config["swap_fee_tier"] = 500
        strategy = MantleMntAccumulator(config=config, chain=config["chain"], wallet_address="0x" + "1" * 40)
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT, market=snap(T0))
        swaps = [i for i in intents if i.intent_type.value == "SWAP"]
        assert swaps, "teardown should sweep the target token"
        assert all(i.swap_params is None for i in swaps)
