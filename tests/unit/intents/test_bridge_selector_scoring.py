"""Branch coverage for BridgeSelector score calculation.

Covers ``_calculate_overall_scores`` normalization (empty input, quoteless
scores, min-max scaling, degenerate ranges, liquidity ratio guards,
reliability lookup) and the per-priority weighting in
``_calculate_weighted_score``. Pure math — no bridges are queried.
"""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.intents.bridge_selector import (
    BridgeScore,
    BridgeSelector,
    SelectionPriority,
)


def _score(name="across", *, fee="10", time=60, input_amount="100", output_amount="99"):
    quote = SimpleNamespace(
        fee_amount=Decimal(fee),
        estimated_time_seconds=time,
        input_amount=Decimal(input_amount),
        output_amount=Decimal(output_amount),
    )
    return BridgeScore(bridge=SimpleNamespace(name=name), quote=quote, is_available=True)


def _unavailable(name="stargate"):
    return BridgeScore(
        bridge=SimpleNamespace(name=name), is_available=False, unavailable_reason="down"
    )


@pytest.fixture
def selector() -> BridgeSelector:
    return BridgeSelector(bridges=[], reliability_scores={"across": 0.9})


class TestCalculateOverallScores:
    def test_empty_scores_is_noop(self, selector):
        selector._calculate_overall_scores([], SelectionPriority.COST)

    def test_no_quotes_leaves_scores_untouched(self, selector):
        score = _unavailable()
        selector._calculate_overall_scores([score], SelectionPriority.COST)
        # BridgeScore defaults every component to the worst score (1.0);
        # with no quotes anywhere the early return leaves them as-is.
        assert score.overall_score == 1.0

    def test_min_max_normalization(self, selector):
        cheap = _score("across", fee="10", time=60)
        pricey = _score("hop", fee="30", time=300)
        selector._calculate_overall_scores([cheap, pricey], SelectionPriority.COST)
        assert cheap.cost_score == 0.0
        assert pricey.cost_score == 1.0
        assert cheap.speed_score == 0.0
        assert pricey.speed_score == 1.0
        assert pricey.overall_score > cheap.overall_score

    def test_quoteless_entry_skipped_during_scoring(self, selector):
        quoted, dead = _score(), _unavailable()
        selector._calculate_overall_scores([quoted, dead], SelectionPriority.COST)
        # The quoteless entry keeps its worst-case defaults.
        assert dead.overall_score == 1.0
        assert quoted.overall_score < 1.0

    def test_identical_quotes_degenerate_range(self, selector):
        first, second = _score("across"), _score("hop")
        selector._calculate_overall_scores([first, second], SelectionPriority.COST)
        # Equal fees/times: range falls back to 1.0 and both normalize to 0.
        assert first.cost_score == 0.0
        assert second.cost_score == 0.0

    def test_liquidity_from_output_ratio(self, selector):
        score = _score(input_amount="100", output_amount="95")
        selector._calculate_overall_scores([score], SelectionPriority.LIQUIDITY)
        assert score.liquidity_score == pytest.approx(0.05)

    def test_output_ratio_above_one_clamps_to_zero(self, selector):
        score = _score(input_amount="100", output_amount="105")
        selector._calculate_overall_scores([score], SelectionPriority.LIQUIDITY)
        assert score.liquidity_score == 0.0

    def test_zero_input_amount_worst_liquidity(self, selector):
        score = _score(input_amount="0")
        selector._calculate_overall_scores([score], SelectionPriority.LIQUIDITY)
        assert score.liquidity_score == 1.0

    def test_reliability_lookup_and_default(self, selector):
        known, unknown = _score("across"), _score("mystery-bridge")
        selector._calculate_overall_scores([known, unknown], SelectionPriority.RELIABILITY)
        assert known.reliability_score == pytest.approx(0.1)
        assert unknown.reliability_score == pytest.approx(0.5)


class TestCalculateWeightedScore:
    @pytest.mark.parametrize(
        ("priority", "dominant"),
        [
            (SelectionPriority.COST, "cost_score"),
            (SelectionPriority.SPEED, "speed_score"),
            (SelectionPriority.LIQUIDITY, "liquidity_score"),
            (SelectionPriority.RELIABILITY, "reliability_score"),
        ],
    )
    def test_priority_weights_dominant_component(self, selector, priority, dominant):
        score = _score()
        for field in ("cost_score", "speed_score", "liquidity_score", "reliability_score"):
            setattr(score, field, 0.0)
        setattr(score, dominant, 1.0)
        assert selector._calculate_weighted_score(score, priority) == pytest.approx(0.6)
