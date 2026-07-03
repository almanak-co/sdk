"""Corrupt-blob robustness for ``load_persistent_state`` (VIB-5486 / TD-06c).

A restart restores strategy posture from a persisted JSON blob. If a counter or
Decimal/datetime field in that blob is corrupt (non-numeric string, wrong type),
an unguarded ``int()`` / ``Decimal()`` / ``datetime.fromisoformat()`` would raise
and crash restore entirely — worse than the teardown-blindness the persistence
fix closes, because the strategy never comes back up to tear down at all.

Gemini + CodeRabbit flagged the remaining bare ``int()`` conversions. These tests
prove ``load_persistent_state`` on a corrupt blob does NOT raise and falls back to
sane defaults (0 for the stat counters — Empty≠Zero is fine here: these are stats,
not money-path/identity values, and 0 is the correct restart default).
"""

from __future__ import annotations

from decimal import Decimal

_CORRUPT = {
    "has_position": True,
    "is_long": False,
    "executed": True,
    # Non-numeric / wrong-type values that would blow up a bare int()/Decimal()/fromisoformat().
    "position_opened_at": "not-a-date",
    "position_size_usd": "not-a-number",
    "trades_opened": "not-a-number",
    "trades_closed": [1, 2, 3],
    "positions_opened": "x",
    "positions_closed": {"nope": 1},
    "martingale_level": "x",
    "wins": "NaN-ish",
    "losses": object(),
    "first_entry_at": "garbage",
    "last_close_at": 12345,
    "entry_prices": ["bad", "1.5"],
    "entry_collaterals": [None],
    "local_top": "oops",
    "total_collateral_deployed": "oops",
    "total_position_size_usd": "oops",
}


def test_bb_perps_load_persistent_state_survives_corrupt_blob() -> None:
    from strategies.incubating.bb_perps.strategy import BBPerpsStrategy

    strat = BBPerpsStrategy.__new__(BBPerpsStrategy)
    # Must not raise.
    strat.load_persistent_state(dict(_CORRUPT))

    assert strat._trades_opened == 0
    assert strat._trades_closed == 0
    assert strat._position_size_usd == Decimal("0")
    assert strat._position_opened_at is None


def test_gmx_perps_load_persistent_state_survives_corrupt_blob() -> None:
    from strategies.incubating.gmx_perps.strategy import GMXPerpsStrategy

    strat = GMXPerpsStrategy.__new__(GMXPerpsStrategy)
    strat.load_persistent_state(dict(_CORRUPT))

    assert strat._positions_opened == 0
    assert strat._positions_closed == 0
    assert strat._position_size_usd == Decimal("0")
    assert strat._position_opened_at is None


def test_rsi_martingale_load_persistent_state_survives_corrupt_blob() -> None:
    from strategies.incubating.rsi_martingale_short.strategy import RSIMartingaleShortStrategy

    strat = RSIMartingaleShortStrategy.__new__(RSIMartingaleShortStrategy)
    strat.load_persistent_state(dict(_CORRUPT))

    assert strat._martingale_level == 0
    assert strat._trades_opened == 0
    assert strat._trades_closed == 0
    assert strat._wins == 0
    assert strat._losses == 0
    # Decimal + datetime helpers already guarded; confirm they fall back too.
    assert strat._local_top == Decimal("0")
    assert strat._total_collateral_deployed == Decimal("0")
    assert strat._total_position_size_usd == Decimal("0")
    assert strat._first_entry_at is None
    assert strat._last_close_at is None
