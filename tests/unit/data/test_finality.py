"""Domain separation, parsing, and transition coverage for finality types."""

from __future__ import annotations

import pytest

from almanak.core.finality import (
    CANONICAL_CACHE_FINALITY_VALUES,
    CANONICAL_DATA_FINALITY_VALUES,
    CacheFinality,
    DataFinality,
    parse_cache_finality,
    parse_data_finality,
)


def test_data_finality_wire_vocabulary_is_stable() -> None:
    assert CANONICAL_DATA_FINALITY_VALUES == ("finalized", "safe", "latest", "off_chain")


def test_cache_finality_wire_vocabulary_is_stable() -> None:
    assert CANONICAL_CACHE_FINALITY_VALUES == ("provisional", "finalized")


@pytest.mark.parametrize("finality", list(DataFinality))
def test_data_finality_exact_historical_values_round_trip(finality: DataFinality) -> None:
    assert parse_data_finality(finality.value) is finality


@pytest.mark.parametrize("finality", list(CacheFinality))
def test_cache_finality_exact_historical_values_round_trip(finality: CacheFinality) -> None:
    assert parse_cache_finality(finality.value) is finality


@pytest.mark.parametrize("value", ["FINALIZED", " finalized", "finalized ", "unknown", ""])
def test_data_finality_unknown_or_normalized_values_fail(value: str) -> None:
    with pytest.raises(ValueError, match="Invalid finality"):
        parse_data_finality(value)


@pytest.mark.parametrize("value", ["FINALIZED", " provisional", "provisional ", "unknown", ""])
def test_cache_finality_unknown_or_normalized_values_fail(value: str) -> None:
    with pytest.raises(ValueError, match="Invalid finality_status"):
        parse_cache_finality(value)


def test_overlapping_finalized_value_does_not_cross_domains() -> None:
    with pytest.raises(TypeError, match="DataFinality, not CacheFinality"):
        parse_data_finality(CacheFinality.FINALIZED)
    with pytest.raises(TypeError, match="CacheFinality, not DataFinality"):
        parse_cache_finality(DataFinality.FINALIZED)


@pytest.mark.parametrize(
    ("current", "target", "allowed"),
    [
        (CacheFinality.PROVISIONAL, CacheFinality.PROVISIONAL, True),
        (CacheFinality.PROVISIONAL, CacheFinality.FINALIZED, True),
        (CacheFinality.FINALIZED, CacheFinality.FINALIZED, True),
        (CacheFinality.FINALIZED, CacheFinality.PROVISIONAL, False),
    ],
)
def test_cache_transition_matrix_is_exhaustive(
    current: CacheFinality,
    target: CacheFinality,
    allowed: bool,
) -> None:
    assert current.can_transition_to(target) is allowed
    if allowed:
        assert current.transition_to(target) is target
    else:
        with pytest.raises(ValueError, match="Invalid cache finality transition"):
            current.transition_to(target)


def test_data_finalized_dispatch_covers_every_member() -> None:
    assert {value: value.is_finalized for value in DataFinality} == {
        DataFinality.FINALIZED: True,
        DataFinality.SAFE: False,
        DataFinality.LATEST: False,
        DataFinality.OFF_CHAIN: False,
    }
