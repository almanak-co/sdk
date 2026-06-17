from decimal import Decimal

import pytest

import almanak.framework.testing.ab_test as ab_test_module
from almanak.framework.testing.ab_test import (
    ABTestConfig,
    ABTestEventType,
    ABTestManager,
    ABTestStatus,
)


def _set_trade_samples(metrics, samples: list[Decimal]) -> None:
    metrics.trade_count = len(samples)
    metrics.metrics.total_trades = len(samples)
    metrics.metrics.net_pnl_usd = sum(samples, Decimal("0"))
    metrics.metrics.total_pnl_usd = metrics.metrics.net_pnl_usd
    metrics._pnl_sum = Decimal("0")
    metrics._pnl_sum_squares = Decimal("0")
    for sample in samples:
        metrics.record_trade_pnl(sample)


def _create_manager(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: ABTestConfig | None = None,
    on_comparison=None,
    on_test_end=None,
) -> tuple[ABTestManager, list]:
    events: list = []
    monkeypatch.setattr(ab_test_module, "add_event", events.append)

    manager = ABTestManager(
        deployment_id="deployment:test",
        on_comparison=on_comparison,
        on_test_end=on_test_end,
    )
    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        total_capital_usd=Decimal("1000"),
        config=config or ABTestConfig(min_sample_size=2),
    )
    assert result.success
    events.clear()
    return manager, events


def _set_variant_samples(
    manager: ABTestManager,
    variant_a: list[Decimal],
    variant_b: list[Decimal],
) -> None:
    assert manager.test is not None
    assert manager.test.variant_a_metrics is not None
    assert manager.test.variant_b_metrics is not None
    _set_trade_samples(manager.test.variant_a_metrics, variant_a)
    _set_trade_samples(manager.test.variant_b_metrics, variant_b)


def test_compare_without_test_returns_empty_comparison_without_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list = []
    callbacks: list = []
    monkeypatch.setattr(ab_test_module, "add_event", events.append)
    manager = ABTestManager(deployment_id="deployment:test", on_comparison=callbacks.append)

    comparison = manager.compare()

    assert comparison.variant_a_metrics.variant_id == ""
    assert comparison.variant_a_metrics.variant_name == "A"
    assert comparison.variant_a_metrics.capital_allocated_usd == Decimal("0")
    assert comparison.variant_a_metrics.is_control is True
    assert comparison.variant_b_metrics.variant_id == ""
    assert comparison.variant_b_metrics.variant_name == "B"
    assert comparison.variant_b_metrics.capital_allocated_usd == Decimal("0")
    assert comparison.variant_b_metrics.is_control is False
    assert comparison.recommendation_reason == "No test data available"
    assert comparison.has_sufficient_data is False
    assert events == []
    assert callbacks == []


def test_compare_insufficient_data_records_event_and_callback_order(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[tuple] = []
    manager: ABTestManager | None = None

    def capture_event(event) -> None:
        assert manager is not None
        assert manager.test is not None
        trace.append(
            (
                "event",
                event.details["ab_test_event_type"],
                len(manager.test.comparison_history),
                event.details.get("recommended_winner"),
            )
        )

    def on_comparison(test) -> None:
        trace.append(("callback", len(test.comparison_history), test.status.value))

    monkeypatch.setattr(ab_test_module, "add_event", capture_event)
    manager = ABTestManager(deployment_id="deployment:test", on_comparison=on_comparison)
    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        config=ABTestConfig(min_sample_size=3),
    )
    assert result.success
    trace.clear()
    _set_variant_samples(manager, [Decimal("10")], [Decimal("10"), Decimal("10")])

    comparison = manager.compare()

    assert comparison.has_sufficient_data is False
    assert comparison.pnl_statistical_result is None
    assert comparison.recommended_winner is None
    assert (
        comparison.recommendation_reason
        == "Insufficient data: A has 1 trades, B has 2 trades, minimum required is 3"
    )
    assert trace == [
        ("event", ABTestEventType.AB_TEST_COMPARISON_UPDATED.value, 1, None),
        ("callback", 1, ABTestStatus.RUNNING.value),
    ]


def test_compare_sufficient_data_without_significance_keeps_no_winner(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _create_manager(monkeypatch)
    _set_variant_samples(manager, [Decimal("10"), Decimal("10")], [Decimal("10"), Decimal("10")])

    comparison = manager.compare()

    assert comparison.has_sufficient_data is True
    assert comparison.pnl_statistical_result is not None
    assert comparison.pnl_statistical_result.is_significant is False
    assert comparison.recommended_winner is None
    assert comparison.recommendation_reason == "No significant difference detected (p=1.0000)"


@pytest.mark.parametrize(
    ("variant_a", "variant_b", "winner", "reason"),
    [
        (
            [Decimal("2"), Decimal("2")],
            [Decimal("8"), Decimal("8")],
            "variant_b",
            "Variant B significantly outperforms A (p=0.0000, effect size=6.000)",
        ),
        (
            [Decimal("8"), Decimal("8")],
            [Decimal("2"), Decimal("2")],
            "variant_a",
            "Variant A significantly outperforms B (p=0.0000, effect size=-6.000)",
        ),
    ],
)
def test_compare_selects_significant_winner(
    monkeypatch: pytest.MonkeyPatch,
    variant_a: list[Decimal],
    variant_b: list[Decimal],
    winner: str,
    reason: str,
) -> None:
    manager, _events = _create_manager(monkeypatch)
    _set_variant_samples(manager, variant_a, variant_b)

    comparison = manager.compare()

    assert comparison.has_sufficient_data is True
    assert comparison.recommended_winner == winner
    assert comparison.recommendation_reason == reason
    expected_improvement = Decimal("3.0") if winner == "variant_b" else Decimal("-0.75")
    assert comparison.relative_improvement == expected_improvement


def test_compare_keeps_relative_improvement_empty_when_control_mean_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _events = _create_manager(monkeypatch)
    _set_variant_samples(manager, [Decimal("0"), Decimal("0")], [Decimal("5"), Decimal("5")])

    comparison = manager.compare()

    assert comparison.mean_difference_usd == Decimal("5.0")
    assert comparison.relative_improvement is None
    assert comparison.recommended_winner == "variant_b"


def test_compare_auto_end_completes_after_nested_final_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[tuple] = []
    manager: ABTestManager | None = None

    def capture_event(event) -> None:
        assert manager is not None
        assert manager.test is not None
        trace.append(
            (
                "event",
                event.details["ab_test_event_type"],
                manager.test.status.value,
                len(manager.test.comparison_history),
            )
        )

    def on_comparison(test) -> None:
        trace.append(("comparison_callback", test.status.value, len(test.comparison_history)))

    def on_test_end(test) -> None:
        trace.append(("end_callback", test.status.value, len(test.comparison_history), test.winner))

    monkeypatch.setattr(ab_test_module, "add_event", capture_event)
    manager = ABTestManager(
        deployment_id="deployment:test",
        on_comparison=on_comparison,
        on_test_end=on_test_end,
    )
    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        config=ABTestConfig(min_sample_size=2, auto_end_on_significance=True),
    )
    assert result.success
    trace.clear()
    _set_variant_samples(manager, [Decimal("2"), Decimal("2")], [Decimal("8"), Decimal("8")])

    comparison = manager.compare()

    assert manager.test is not None
    assert comparison.recommended_winner == "variant_b"
    assert manager.test.status == ABTestStatus.COMPLETED
    assert manager.test.winner == "variant_b"
    assert len(manager.test.comparison_history) == 2
    assert trace == [
        ("event", ABTestEventType.AB_TEST_COMPARISON_UPDATED.value, ABTestStatus.RUNNING.value, 1),
        ("comparison_callback", ABTestStatus.RUNNING.value, 1),
        ("event", ABTestEventType.AB_TEST_COMPARISON_UPDATED.value, ABTestStatus.RUNNING.value, 2),
        ("comparison_callback", ABTestStatus.RUNNING.value, 2),
        ("event", ABTestEventType.AB_TEST_ENDED.value, ABTestStatus.COMPLETED.value, 2),
        ("event", ABTestEventType.AB_TEST_WINNER_SELECTED.value, ABTestStatus.COMPLETED.value, 2),
        ("end_callback", ABTestStatus.COMPLETED.value, 2, "variant_b"),
    ]


def test_compare_callback_error_is_logged_and_does_not_abort(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    def failing_callback(_test) -> None:
        raise RuntimeError("comparison hook exploded")

    manager, _events = _create_manager(monkeypatch, on_comparison=failing_callback)
    _set_variant_samples(manager, [Decimal("10")], [Decimal("10")])

    comparison = manager.compare()

    assert comparison.recommendation_reason.startswith("Insufficient data:")
    assert "Comparison callback failed: comparison hook exploded" in caplog.text
