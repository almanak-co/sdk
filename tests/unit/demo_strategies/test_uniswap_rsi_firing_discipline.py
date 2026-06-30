"""Firing-discipline tests for the RSI demo strategy.

Covers the D2 deliverable from ``docs/internal/accounting/AccountingStrats.md``:

* Re-arm hysteresis band: a bare tick past the threshold does NOT re-arm the
  latch — the documented buy-spree repro (RSI 29.9 / 30.1 / 29.8 firing a buy
  on every dip) holds instead of trading.
* Trade cooldown (opt-in): measured on the market snapshot's clock, blocks a
  confirmed-fill side from re-firing inside the window.
* Exposure cap: strategy-acquired inventory is tracked from fills and buys are
  blocked at ``max_position_usd``; sells reduce the tracked position.
* New state (cooldown stamp, position) survives a persistence round-trip.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

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


_T0 = datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC)


class _Market:
    chain = "arbitrum"

    def __init__(self, rsi: str = "20", at: datetime | None = None) -> None:
        self._rsi = rsi
        self.timestamp = at or _T0

    def price(self, token: str) -> Decimal:
        return Decimal("25")

    def rsi(self, token: str, period: int) -> _RSI:
        return _RSI(self._rsi)

    def balance(self, token: str) -> _Balance:
        if token == "USDC":
            return _Balance("1000", "1000")
        return _Balance("10", "250")


_BASE_CONFIG: dict[str, object] = {
    "chain": "arbitrum",
    "protocol": "uniswap_v3",
    "base_token": "WETH",
    "quote_token": "USDC",
    "trade_size_usd": 10,
    "rsi_oversold": 30,
    "rsi_overbought": 70,
}


def _strategy(**overrides: object) -> UniswapRSIStrategy:
    config = {**_BASE_CONFIG, **overrides}
    return UniswapRSIStrategy(
        config=config,
        chain="arbitrum",
        wallet_address="0x" + "11" * 20,
    )


def _confirm(strategy: UniswapRSIStrategy, intent, result: object | None = None) -> None:
    strategy.on_intent_executed(intent, True, result if result is not None else object())


# ---------------------------------------------------------------------------
# Re-arm hysteresis band
# ---------------------------------------------------------------------------


def test_threshold_noise_does_not_rearm_buy_side() -> None:
    """The spree repro: after a confirmed buy, RSI flickering just above the
    oversold threshold must NOT re-arm — only a recovery past threshold + band
    does."""
    strategy = _strategy(rsi_rearm_band=10)

    first = strategy.decide(_Market(rsi="20"))
    assert first.intent_type.value == "SWAP"
    _confirm(strategy, first)

    # Tick barely into neutral (31 < 30 + 10): latch must stay OVERSOLD.
    barely_neutral = strategy.decide(_Market(rsi="31"))
    assert barely_neutral.intent_type.value == "HOLD"

    # Dip back under the threshold: NOT a fresh transition -> no trade.
    dip = strategy.decide(_Market(rsi="29"))
    assert dip.intent_type.value == "HOLD", "threshold noise must not fire a second buy"

    # Recover past the re-arm level (45 >= 40): the buy side re-arms...
    rearmed = strategy.decide(_Market(rsi="45"))
    assert rearmed.intent_type.value == "HOLD"

    # ...so the next genuine oversold transition fires.
    second = strategy.decide(_Market(rsi="28"))
    assert second.intent_type.value == "SWAP"


def test_threshold_noise_does_not_rearm_sell_side() -> None:
    strategy = _strategy(rsi_rearm_band=10)

    sell = strategy.decide(_Market(rsi="75"))
    assert sell.intent_type.value == "SWAP"
    _confirm(strategy, sell)

    # 69 > 70 - 10: sell side not re-armed; spike back up must not re-fire.
    assert strategy.decide(_Market(rsi="69")).intent_type.value == "HOLD"
    assert strategy.decide(_Market(rsi="72")).intent_type.value == "HOLD"

    # Fall to the re-arm level (60 <= 60) and spike again: fires.
    assert strategy.decide(_Market(rsi="55")).intent_type.value == "HOLD"
    assert strategy.decide(_Market(rsi="73")).intent_type.value == "SWAP"


def test_rearm_band_zero_preserves_transition_behavior() -> None:
    """band=0 reproduces the legacy latch: any neutral tick re-arms."""
    strategy = _strategy(rsi_rearm_band=0)

    first = strategy.decide(_Market(rsi="20"))
    _confirm(strategy, first)

    assert strategy.decide(_Market(rsi="31")).intent_type.value == "HOLD"
    assert strategy.decide(_Market(rsi="29")).intent_type.value == "SWAP"


def test_rearm_band_too_wide_fails_validation() -> None:
    with pytest.raises(ConfigValidationError):
        _strategy(rsi_oversold=25, rsi_overbought=40, rsi_rearm_band=20)


# ---------------------------------------------------------------------------
# Trade cooldown (opt-in, snapshot-clock based)
# ---------------------------------------------------------------------------


def test_cooldown_blocks_within_window_and_releases_after() -> None:
    strategy = _strategy(rsi_rearm_band=0, trade_cooldown_seconds=300)

    first = strategy.decide(_Market(rsi="20", at=_T0))
    assert first.intent_type.value == "SWAP"
    _confirm(strategy, first)

    # Re-arm through neutral, then a fresh oversold INSIDE the window: blocked.
    strategy.decide(_Market(rsi="50", at=_T0 + timedelta(seconds=30)))
    blocked = strategy.decide(_Market(rsi="20", at=_T0 + timedelta(seconds=60)))
    assert blocked.intent_type.value == "HOLD"
    assert "cooldown" in blocked.reason

    # Same signal after the window expires: fires (the signal is not lost).
    released = strategy.decide(_Market(rsi="20", at=_T0 + timedelta(seconds=400)))
    assert released.intent_type.value == "SWAP"


def test_cooldown_disabled_by_default() -> None:
    strategy = _strategy(rsi_rearm_band=0)

    first = strategy.decide(_Market(rsi="20", at=_T0))
    _confirm(strategy, first)
    strategy.decide(_Market(rsi="50", at=_T0))
    immediate = strategy.decide(_Market(rsi="20", at=_T0))
    assert immediate.intent_type.value == "SWAP"


# ---------------------------------------------------------------------------
# Exposure cap on strategy-acquired inventory
# ---------------------------------------------------------------------------


def test_position_cap_blocks_buys_until_a_sell_reduces_inventory() -> None:
    # trade_size 10, cap 10: the first buy fits exactly, the second exceeds.
    strategy = _strategy(rsi_rearm_band=0, max_position_usd=10)

    first = strategy.decide(_Market(rsi="20"))
    assert first.intent_type.value == "SWAP"
    _confirm(strategy, first)  # no decoded amounts -> estimate 10/25 = 0.4 WETH

    assert strategy._position_base_amount == Decimal("0.4")

    strategy.decide(_Market(rsi="50"))  # re-arm
    capped = strategy.decide(_Market(rsi="20"))
    assert capped.intent_type.value == "HOLD"
    assert "position cap" in capped.reason

    # A confirmed sell reduces inventory and unblocks the buy side.
    sell = strategy.decide(_Market(rsi="75"))
    assert sell.intent_type.value == "SWAP"
    _confirm(strategy, sell)
    assert strategy._position_base_amount == Decimal("0")

    strategy.decide(_Market(rsi="50"))  # re-arm both sides
    next_buy = strategy.decide(_Market(rsi="20"))
    assert next_buy.intent_type.value == "SWAP"


def test_inventory_prefers_decoded_amounts_over_estimate() -> None:
    strategy = _strategy(rsi_rearm_band=0, max_position_usd=0)  # cap disabled

    buy = strategy.decide(_Market(rsi="20"))
    result = SimpleNamespace(swap_amounts=SimpleNamespace(amount_out_decimal=Decimal("0.37")))
    _confirm(strategy, buy, result)
    assert strategy._position_base_amount == Decimal("0.37")

    strategy.decide(_Market(rsi="50"))
    sell = strategy.decide(_Market(rsi="75"))
    sell_result = SimpleNamespace(swap_amounts=SimpleNamespace(amount_in_decimal=Decimal("0.17")))
    _confirm(strategy, sell, sell_result)
    assert strategy._position_base_amount == Decimal("0.2")


def test_cap_disabled_with_zero() -> None:
    strategy = _strategy(rsi_rearm_band=0, max_position_usd=0)
    for _ in range(5):
        buy = strategy.decide(_Market(rsi="20"))
        assert buy.intent_type.value == "SWAP"
        _confirm(strategy, buy)
        strategy.decide(_Market(rsi="50"))
    assert strategy._position_base_amount == Decimal("2.0")  # 5 x 0.4, uncapped


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def test_cooldown_and_position_survive_restart() -> None:
    strategy = _strategy(rsi_rearm_band=0, trade_cooldown_seconds=300)
    buy = strategy.decide(_Market(rsi="20", at=_T0))
    _confirm(strategy, buy)

    state = strategy.get_persistent_state()
    restored = _strategy(rsi_rearm_band=0, trade_cooldown_seconds=300)
    restored.load_persistent_state(state)

    assert restored._last_trade_at == _T0
    assert restored._position_base_amount == Decimal("0.4")
    assert restored._last_rsi_signal == "OVERSOLD"

    # The restored instance still honors the in-flight cooldown window.
    restored.decide(_Market(rsi="50", at=_T0 + timedelta(seconds=30)))
    blocked = restored.decide(_Market(rsi="20", at=_T0 + timedelta(seconds=31)))
    assert blocked.intent_type.value == "HOLD"
    assert "cooldown" in blocked.reason


# ---------------------------------------------------------------------------
# Review hardening (PR #2726 — CodeRabbit CLI findings)
# ---------------------------------------------------------------------------


def test_rearm_band_exactly_filling_neutral_zone_rejected() -> None:
    """At oversold + band == overbought, no RSI value in the exclusive
    neutral zone reaches the buy re-arm level — the buy side would latch
    permanently. Equality must fail validation."""
    with pytest.raises(ConfigValidationError):
        _strategy(rsi_oversold=30, rsi_overbought=70, rsi_rearm_band=40)


class _NoTimestampMarket(_Market):
    def __init__(self, rsi: str = "20") -> None:
        super().__init__(rsi)
        del self.timestamp


def test_capture_does_not_inject_wall_clock() -> None:
    """A market double without a timestamp must leave the captured snapshot
    clock as None — never bake wall time into state that backtests would
    later compare against simulated clocks."""
    strategy = _strategy(rsi_rearm_band=0)
    strategy.decide(_NoTimestampMarket(rsi="50"))
    assert strategy._last_seen_market_ts is None


def test_uncountable_buy_blocks_further_buys_until_counted_fill() -> None:
    strategy = _strategy(rsi_rearm_band=0, max_position_usd=100)

    first = strategy.decide(_Market(rsi="20"))
    assert first.intent_type.value == "SWAP"
    # Make the fill uncountable: no decoded amounts AND no price estimate.
    strategy._last_base_price = None
    _confirm(strategy, first)
    assert strategy._position_tracking_failed is True

    # Buy side is blocked even though the latch re-armed through neutral.
    strategy.decide(_Market(rsi="50"))
    blocked = strategy.decide(_Market(rsi="20"))
    assert blocked.intent_type.value == "HOLD"
    assert "tracking failed" in blocked.reason

    # Sells stay allowed (risk-reducing) and a counted fill clears the flag.
    sell = strategy.decide(_Market(rsi="75"))
    assert sell.intent_type.value == "SWAP"
    sell_result = SimpleNamespace(swap_amounts=SimpleNamespace(amount_in_decimal=Decimal("0.1")))
    _confirm(strategy, sell, sell_result)
    assert strategy._position_tracking_failed is False

    strategy.decide(_Market(rsi="50"))
    unblocked = strategy.decide(_Market(rsi="20"))
    assert unblocked.intent_type.value == "SWAP"


def test_tracking_failed_flag_survives_restart() -> None:
    strategy = _strategy(rsi_rearm_band=0, max_position_usd=100)
    strategy._position_tracking_failed = True
    state = strategy.get_persistent_state()

    restored = _strategy(rsi_rearm_band=0, max_position_usd=100)
    restored.load_persistent_state(state)
    assert restored._position_tracking_failed is True
