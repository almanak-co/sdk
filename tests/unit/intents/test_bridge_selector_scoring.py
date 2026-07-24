"""Branch coverage for BridgeSelector score calculation and selection.

Covers ``_calculate_overall_scores`` normalization (empty input, quoteless
scores, min-max scaling, degenerate ranges, liquidity ratio guards,
reliability lookup), the per-priority weighting in
``_calculate_weighted_score``, the end-to-end ``select_bridge`` flow
(priority parsing, availability filtering, ranking, reasoning), and
``select_bridge_with_fallback`` exclusion handling. Pure in-memory fakes —
no bridges are queried.
"""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.bridge_base import BridgeQuoteError
from almanak.framework.intents.bridge_selector import (
    BridgeScore,
    BridgeSelector,
    NoBridgeAvailableError,
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


def _quote(fee="10", time=60, *, expired=False):
    return SimpleNamespace(
        fee_amount=Decimal(fee),
        estimated_time_seconds=time,
        input_amount=Decimal("100"),
        output_amount=Decimal("99"),
        token="USDC",
        is_expired=expired,
    )


class _FakeBridge:
    """Minimal in-memory BridgeAdapter double."""

    def __init__(self, name, *, valid=True, reason=None, quote=None, error=None):
        self.name = name
        self._valid = valid
        self._reason = reason
        self._quote = quote
        self._error = error

    def validate_transfer(self, **kwargs):
        return (self._valid, self._reason)

    def get_quote(self, **kwargs):
        if self._error is not None:
            raise self._error
        return self._quote


def _select(selector, **overrides):
    kwargs = {
        "token": "USDC",
        "amount": Decimal("1000"),
        "from_chain": "arbitrum",
        "to_chain": "optimism",
    }
    kwargs.update(overrides)
    return selector.select_bridge(**kwargs)


class TestSelectBridge:
    def test_selects_cheapest_for_cost_priority(self):
        cheap = _FakeBridge("across", quote=_quote(fee="10", time=300))
        pricey = _FakeBridge("stargate", quote=_quote(fee="30", time=60))
        selector = BridgeSelector(bridges=[pricey, cheap])

        result = _select(selector, priority="cost")

        assert result.is_success
        assert result.bridge is cheap
        assert result.quote is cheap._quote
        assert len(result.scores) == 2
        assert "Selected across based on cost priority" in result.selection_reasoning
        assert "Fallback: stargate" in result.selection_reasoning
        assert "All bridges ranked" in result.selection_reasoning

    def test_speed_priority_selects_fastest(self):
        cheap_slow = _FakeBridge("across", quote=_quote(fee="10", time=300))
        pricey_fast = _FakeBridge("stargate", quote=_quote(fee="30", time=60))
        selector = BridgeSelector(bridges=[cheap_slow, pricey_fast])

        result = _select(selector, priority="speed")

        assert result.bridge is pricey_fast

    def test_unknown_priority_falls_back_to_default(self):
        cheap = _FakeBridge("across", quote=_quote(fee="10"))
        pricey = _FakeBridge("stargate", quote=_quote(fee="30"))
        selector = BridgeSelector(bridges=[pricey, cheap])

        result = _select(selector, priority="warp-speed")

        # Default priority is COST, so the cheapest bridge still wins.
        assert result.bridge is cheap

    def test_no_bridge_available_raises_with_reasons(self):
        down = _FakeBridge("across", valid=False, reason="route unsupported")
        selector = BridgeSelector(bridges=[down])

        with pytest.raises(NoBridgeAvailableError, match="across: route unsupported"):
            _select(selector)

    def test_quote_error_reason_included(self):
        broken = _FakeBridge("across", error=BridgeQuoteError("no liquidity"))
        selector = BridgeSelector(bridges=[broken])

        with pytest.raises(NoBridgeAvailableError, match="Quote error: no liquidity"):
            _select(selector)

    def test_expired_quote_excluded_from_selection(self):
        expired = _FakeBridge("across", quote=_quote(fee="1", expired=True))
        live = _FakeBridge("stargate", quote=_quote(fee="30"))
        selector = BridgeSelector(bridges=[expired, live])

        result = _select(selector)

        assert result.bridge is live
        expired_score = next(s for s in result.scores if s.bridge is expired)
        assert not expired_score.is_available
        assert expired_score.unavailable_reason == "Quote expired immediately"

    def test_single_available_bridge_has_no_fallback(self):
        only = _FakeBridge("across", quote=_quote())
        selector = BridgeSelector(bridges=[only])

        result = _select(selector)

        assert result.bridge is only
        assert "Fallback:" not in result.selection_reasoning
        assert "All bridges ranked" not in result.selection_reasoning


class TestSelectBridgeWithFallback:
    def _selector(self):
        cheap = _FakeBridge("across", quote=_quote(fee="10"))
        pricey = _FakeBridge("stargate", quote=_quote(fee="30"))
        return BridgeSelector(bridges=[cheap, pricey]), cheap, pricey

    def _select_with_fallback(self, selector, **overrides):
        kwargs = {
            "token": "USDC",
            "amount": Decimal("1000"),
            "from_chain": "arbitrum",
            "to_chain": "optimism",
        }
        kwargs.update(overrides)
        return selector.select_bridge_with_fallback(**kwargs)

    def test_no_exclusions_selects_best(self):
        selector, cheap, _ = self._selector()
        result = self._select_with_fallback(selector)
        assert result.bridge is cheap

    def test_excluded_bridge_not_selected(self):
        selector, _, pricey = self._selector()
        # Exclusion is case-insensitive.
        result = self._select_with_fallback(selector, excluded_bridges=["ACROSS"])
        assert result.bridge is pricey

    def test_excluding_all_bridges_raises(self):
        selector, _, _ = self._selector()
        with pytest.raises(NoBridgeAvailableError, match="after excluding"):
            self._select_with_fallback(selector, excluded_bridges=["across", "stargate"])

    def test_propagates_inner_no_bridge_error(self):
        down = _FakeBridge("across", valid=False, reason="paused")
        selector = BridgeSelector(bridges=[down])
        with pytest.raises(NoBridgeAvailableError, match="across: paused"):
            self._select_with_fallback(selector)
