"""Caller and timestamp boundaries from the verified DualAggregator source."""

import pytest

from almanak.backtest.economic_oracles import DualState, Transmission

SECONDARY = "0x" + "11" * 20
PRIMARY = "0x" + "22" * 20


def state(**changes):
    return DualState(
        latest=3,
        secondary=1,
        is_latest_secondary=False,
        cutoff=30,
        max_sync_iterations=20,
        secondary_proxy=SECONDARY,
        rounds=tuple(
            Transmission(round_id=i, answer=i * 100, observed_at=t - 5, recorded_at=t)
            for i, t in ((1, 100), (2, 110), (3, 120))
        ),
    ).model_copy(update=changes)


def test_cutoff_is_strict_and_primary_does_not_use_secondary_delay():
    s = state()
    assert s.selected(SECONDARY, 130).round_id == 1
    assert s.selected(SECONDARY, 140).round_id == 1
    assert s.selected(SECONDARY, 141).round_id == 2
    assert s.selected(SECONDARY, 150).round_id == 2
    assert s.selected(SECONDARY, 151).round_id == 3
    assert s.selected(PRIMARY, 120).round_id == 3
    assert s.deadlines(120) == (121, 131, 141, 151)


def test_secondary_only_round_is_hidden_from_primary_in_its_block():
    s = state(secondary=3, is_latest_secondary=True)
    assert s.selected(PRIMARY, 120).round_id == 2
    assert s.selected(PRIMARY, 121).round_id == 3
    assert s.selected(SECONDARY, 120).round_id == 3
    assert state(secondary=3).selected(PRIMARY, 120).round_id == 3


def test_sync_search_bound_retains_secondary_and_missing_rounds_fail():
    s = state(max_sync_iterations=1)
    assert s.selected(SECONDARY, 145).round_id == 1
    assert s.selected(SECONDARY, 151).round_id == 3
    with pytest.raises(ValueError, match="transmission is missing"):
        s.model_copy(update={"rounds": s.rounds[:1]}).selected(SECONDARY, 151)
