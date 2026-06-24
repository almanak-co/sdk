"""Unit tests for the traderjoe_lp rebalance hysteresis (deadband + cooldown).

A recentering LP with no hysteresis thrashes (close->reopen->close) when price
oscillates at the band edge. These tests drive decide() with a live position and
mocked prices/time to prove: in-range holds, a small overshoot within the
deadband holds, a move beyond the deadband rebalances, and the cooldown blocks a
rebalance of a freshly-opened position.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.demo_strategies.traderjoe_lp.strategy import TraderJoeLPConfig, TraderJoeLPStrategy

_WALLET = "0x" + "1" * 40


def _strategy(**cfg_over) -> TraderJoeLPStrategy:
    cfg = TraderJoeLPConfig(force_action="", **cfg_over)
    return TraderJoeLPStrategy(chain="avalanche", wallet_address=_WALLET, config=cfg)


def _market(wavax_price: float) -> MagicMock:
    m = MagicMock()
    m.price.side_effect = lambda t: Decimal(str(wavax_price)) if t == "WAVAX" else Decimal("1")
    return m


def _open_position(s: TraderJoeLPStrategy, center: float, opened_min_ago: float = 999) -> TraderJoeLPStrategy:
    """Simulate a live position centered at `center` (range ±range_width/2)."""
    center_d = Decimal(str(center))
    half = s.range_width_pct / Decimal("2")
    s._position_bin_ids = [100, 101, 102]
    s._range_lower = center_d * (Decimal("1") - half)
    s._range_upper = center_d * (Decimal("1") + half)
    s._last_open_time = datetime.now(UTC) - timedelta(minutes=opened_min_ago)
    return s


# Opened at 30 with range_width 0.10 -> range [28.5, 31.5], width 3,
# deadband buffer 0.5*3 = 1.5 -> rebalance band [27.0, 33.0].


def test_holds_in_range():
    s = _open_position(_strategy(), center=30)
    assert s.decide(_market(30)).intent_type.value == "HOLD"


def test_overshoot_within_deadband_holds():
    # 32 is outside the LP range (31.5) but inside the deadband (33.0) -> no churn.
    s = _open_position(_strategy(), center=30)
    intent = s.decide(_market(32))
    assert intent.intent_type.value == "HOLD"
    assert "deadband" in intent.reason


def test_beyond_deadband_rebalances():
    s = _open_position(_strategy(), center=30, opened_min_ago=999)  # cooldown long passed
    assert s.decide(_market(34)).intent_type.value == "LP_CLOSE"


def test_cooldown_blocks_fresh_rebalance():
    # Beyond the deadband, but the position was just opened -> cooldown holds it.
    s = _open_position(_strategy(rebalance_cooldown_minutes=30), center=30, opened_min_ago=0)
    intent = s.decide(_market(34))
    assert intent.intent_type.value == "HOLD"
    assert "cooldown" in intent.reason


def test_cooldown_elapsed_allows_rebalance():
    s = _open_position(_strategy(rebalance_cooldown_minutes=30), center=30, opened_min_ago=31)
    assert s.decide(_market(34)).intent_type.value == "LP_CLOSE"


def test_wider_buffer_widens_deadband():
    # buffer 1.0 -> band [28.5-3, 31.5+3] = [25.5, 34.5]; 34 now holds.
    s = _open_position(_strategy(rebalance_buffer_pct=Decimal("1.0")), center=30)
    assert s.decide(_market(34)).intent_type.value == "HOLD"


def test_open_sets_cooldown_clock_and_close_counts():
    s = _strategy()
    assert s._last_open_time is None and s._rebalance_count == 0
    open_intent = SimpleNamespace(
        intent_type=SimpleNamespace(value="LP_OPEN"), range_lower=Decimal("28.5"), range_upper=Decimal("31.5")
    )
    s.on_intent_executed(open_intent, True, SimpleNamespace(bin_ids=[100, 101], extracted_data={}))
    assert s._last_open_time is not None
    assert s._range_lower == Decimal("28.5") and s._range_upper == Decimal("31.5")

    close_intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_CLOSE"))
    s.on_intent_executed(close_intent, True, None)
    assert s._rebalance_count == 1
    assert s._position_bin_ids == [] and s._last_open_time is None


def test_cooldown_passes_when_no_open_time():
    # A restored position with no recorded open time must not be stranded forever.
    s = _open_position(_strategy(), center=30)
    s._last_open_time = None
    assert s.decide(_market(34)).intent_type.value == "LP_CLOSE"


def test_persists_and_restores_hysteresis_state():
    """rebalance_count and last_open_time survive a persist/restore round-trip, so a
    restored position keeps its cooldown clock instead of rebalancing immediately."""
    s = _open_position(_strategy(), center=30, opened_min_ago=5)
    s._rebalance_count = 3
    state = s.get_persistent_state()
    assert state["rebalance_count"] == 3
    assert "last_open_time" in state

    restored = _strategy()
    restored.load_persistent_state(state)
    assert restored._rebalance_count == 3
    assert restored._last_open_time == s._last_open_time
    # Restored 5 min ago with a 30 min cooldown -> still inside cooldown -> holds.
    assert restored.decide(_market(34)).intent_type.value == "HOLD"


def test_load_tolerates_malformed_hysteresis_state():
    """Malformed persisted values must warn-and-skip, not abort the whole restore."""
    s = _strategy()
    s.load_persistent_state({"rebalance_count": "not-an-int", "last_open_time": "not-a-date"})
    assert s._rebalance_count == 0  # left at default, not crashed
    assert s._last_open_time is None


def test_config_round_trips_hysteresis_fields():
    """The new hysteresis settings must survive __post_init__ coercion and to_dict()
    emission, so a config save/load doesn't silently drop them back to defaults."""
    cfg = TraderJoeLPConfig(
        force_action="",
        rebalance_buffer_pct="0.75",  # string inputs (e.g. from JSON) coerce in __post_init__
        rebalance_cooldown_minutes="45",
    )
    assert cfg.rebalance_buffer_pct == Decimal("0.75")
    assert cfg.rebalance_cooldown_minutes == 45

    d = cfg.to_dict()
    assert d["rebalance_buffer_pct"] == "0.75"
    assert d["rebalance_cooldown_minutes"] == 45

    restored = TraderJoeLPConfig(**d)
    assert restored.rebalance_buffer_pct == Decimal("0.75")
    assert restored.rebalance_cooldown_minutes == 45


def test_restore_normalizes_naive_open_time_to_utc():
    """A naive ISO timestamp must be coerced to aware UTC so the cooldown subtraction
    (datetime.now(UTC) - last_open_time) never raises naive/aware TypeError."""
    s = _strategy()
    s.load_persistent_state({"last_open_time": "2026-06-24T12:00:00"})  # no tzinfo
    assert s._last_open_time is not None
    # Aware *and specifically UTC* (zero offset), not just any timezone.
    assert s._last_open_time.utcoffset() == timedelta(0)
    # Subtraction against an aware now() must not raise.
    assert isinstance((datetime.now(UTC) - s._last_open_time), timedelta)
