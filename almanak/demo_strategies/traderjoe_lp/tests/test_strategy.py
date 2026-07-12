"""Unit tests for the traderjoe_lp demo.

Snapshots come from ``almanak.framework.market.testing.seeded(...)``; time is
driven by advancing the snapshot ``timestamp`` between decide() calls.
"""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPConfig, TraderJoeLPStrategy
from almanak.framework.market.testing import seeded

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def config() -> TraderJoeLPConfig:
    """The demo's config.json, loaded into its typed config dataclass."""
    raw = json.loads((Path(__file__).parent.parent / "config.json").read_text())
    fields = TraderJoeLPConfig.__dataclass_fields__
    return TraderJoeLPConfig(**{k: v for k, v in raw.items() if k in fields})


@pytest.fixture
def strategy(config: TraderJoeLPConfig) -> TraderJoeLPStrategy:
    return TraderJoeLPStrategy(
        config=config,
        chain=config.chain,
        wallet_address="0x" + "1" * 40,
    )


def snap(ts: datetime):
    return seeded(
        chain="avalanche",
        prices={"WAVAX": Decimal("30"), "USDC": Decimal("1")},
        timestamp=ts,
    )


class TestMarketClock:
    def test_decide_captures_snapshot_timestamp(self, strategy: TraderJoeLPStrategy) -> None:
        strategy.decide(snap(T0))
        assert strategy._last_seen_market_ts == T0

    def test_rebalance_cooldown_measured_on_market_clock(self, strategy: TraderJoeLPStrategy) -> None:
        """Cooldown is 30min (config.json): blocked at T0+10m, free at T0+40m."""
        strategy._last_open_time = T0

        strategy.decide(snap(T0 + timedelta(minutes=10)))
        assert not strategy._rebalance_cooldown_passed()

        strategy.decide(snap(T0 + timedelta(minutes=40)))
        assert strategy._rebalance_cooldown_passed()

    def test_confirmed_open_stamps_market_clock(self, strategy: TraderJoeLPStrategy) -> None:
        strategy.decide(snap(T0))

        lp_open = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            range_lower=None,
            range_upper=None,
        )
        strategy.on_intent_executed(lp_open, True, SimpleNamespace(bin_ids=[1, 2, 3], extracted_data={}))

        assert strategy._last_open_time == T0

    def test_failed_open_does_not_stamp(self, strategy: TraderJoeLPStrategy) -> None:
        strategy.decide(snap(T0))
        lp_open = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
        strategy.on_intent_executed(lp_open, False, None)
        assert strategy._last_open_time is None

    def test_cooldown_open_when_no_position(self, strategy: TraderJoeLPStrategy) -> None:
        assert strategy._last_open_time is None
        assert strategy._rebalance_cooldown_passed()


class TestPersistence:
    def test_open_stamp_survives_restart(self, strategy: TraderJoeLPStrategy, config: TraderJoeLPConfig) -> None:
        strategy._last_open_time = T0

        state = strategy.get_persistent_state()
        fresh = TraderJoeLPStrategy(config=config, chain=config.chain, wallet_address="0x" + "1" * 40)
        fresh.load_persistent_state(state)

        assert fresh._last_open_time == T0
