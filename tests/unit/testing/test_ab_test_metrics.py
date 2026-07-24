"""Unit tests for ABTestManager.update_variant_metrics and cheap lifecycle paths.

Covers every branch of ``update_variant_metrics`` (guard clauses, variant
selection, each optional field write, event emission) plus the inexpensive
uncovered lifecycle surfaces reachable from the same fixtures: ``cancel_test``,
``end_test`` error/inconclusive paths, ``get_status``, and the manager
``to_dict``/``from_dict`` roundtrip. Timeline emission is stubbed at the module
seam so no file or gateway writes occur.
"""

from decimal import Decimal

import pytest

import almanak.framework.testing.ab_test as ab_test_module
from almanak.framework.testing.ab_test import (
    ABTestConfig,
    ABTestEventType,
    ABTestManager,
    ABTestStatus,
)


def _make_manager(
    monkeypatch: pytest.MonkeyPatch,
    *,
    config: ABTestConfig | None = None,
    create: bool = True,
) -> tuple[ABTestManager, list]:
    """Build a manager with timeline emission captured in-memory."""
    events: list = []
    monkeypatch.setattr(ab_test_module, "add_event", events.append)

    manager = ABTestManager(deployment_id="deployment:abtest", chain="testchain")
    if create:
        result = manager.create_ab_test(
            variant_a="baseline",
            variant_b="candidate",
            total_capital_usd=Decimal("1000"),
            config=config or ABTestConfig(min_sample_size=2),
        )
        assert result.success
        events.clear()
    return manager, events


# ---------------------------------------------------------------------------
# update_variant_metrics: guard clauses
# ---------------------------------------------------------------------------


def test_update_without_test_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch, create=False)

    assert manager.update_variant_metrics("a", pnl_usd=Decimal("10")) is False
    assert events == []


def test_update_on_non_running_test_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.cancel_test().success
    events.clear()

    assert manager.update_variant_metrics("a", pnl_usd=Decimal("10")) is False
    assert events == []


@pytest.mark.parametrize("variant", ["c", "", "ab", "variant_a"])
def test_update_invalid_variant_returns_false(monkeypatch: pytest.MonkeyPatch, variant: str) -> None:
    manager, events = _make_manager(monkeypatch)

    assert manager.update_variant_metrics(variant, trades=1) is False
    assert events == []


@pytest.mark.parametrize("variant", ["a", "b"])
def test_update_with_missing_variant_metrics_returns_false(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.test is not None
    if variant == "a":
        manager.test.variant_a_metrics = None
    else:
        manager.test.variant_b_metrics = None

    assert manager.update_variant_metrics(variant, trades=1) is False
    assert events == []


# ---------------------------------------------------------------------------
# update_variant_metrics: variant selection and field writes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variant", ["a", "A", "b", "B"])
def test_update_all_fields_writes_selected_variant(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.test is not None

    result = manager.update_variant_metrics(
        variant,
        pnl_usd=Decimal("42.5"),
        trades=7,
        errors=2,
        drawdown=Decimal("0.15"),
        sharpe=Decimal("1.8"),
        win_rate=Decimal("0.6"),
        trade_pnl=Decimal("3"),
    )

    assert result is True
    if variant.lower() == "a":
        updated = manager.test.variant_a_metrics
        untouched = manager.test.variant_b_metrics
    else:
        updated = manager.test.variant_b_metrics
        untouched = manager.test.variant_a_metrics
    assert updated is not None
    assert untouched is not None

    assert updated.metrics.net_pnl_usd == Decimal("42.5")
    assert updated.metrics.total_pnl_usd == Decimal("42.5")
    assert updated.trade_count == 7
    assert updated.metrics.total_trades == 7
    assert updated.error_count == 2
    assert updated.metrics.max_drawdown == Decimal("0.15")
    assert updated.metrics.sharpe_ratio == Decimal("1.8")
    assert updated.metrics.win_rate == Decimal("0.6")
    assert updated._pnl_sum == Decimal("3")
    assert updated._pnl_sum_squares == Decimal("9")

    # The other variant is untouched.
    assert untouched.trade_count == 0
    assert untouched.metrics.net_pnl_usd == Decimal("0")

    # One metrics-updated event, with the variant upper-cased.
    assert len(events) == 1
    event = events[0]
    assert event.description == f"Variant {variant.upper()} metrics updated"
    assert event.details["ab_test_event_type"] == ABTestEventType.AB_TEST_METRICS_UPDATED.value
    assert event.details["variant"] == variant.upper()
    assert event.details["pnl_usd"] == "42.5"
    assert event.details["trade_count"] == 7
    assert event.deployment_id == "deployment:abtest"
    assert event.chain == "testchain"


@pytest.mark.parametrize(
    ("kwargs", "getter", "expected"),
    [
        ({"pnl_usd": Decimal("12.5")}, lambda m: m.metrics.net_pnl_usd, Decimal("12.5")),
        ({"pnl_usd": Decimal("12.5")}, lambda m: m.metrics.total_pnl_usd, Decimal("12.5")),
        ({"trades": 9}, lambda m: m.trade_count, 9),
        ({"trades": 9}, lambda m: m.metrics.total_trades, 9),
        ({"errors": 4}, lambda m: m.error_count, 4),
        ({"drawdown": Decimal("0.25")}, lambda m: m.metrics.max_drawdown, Decimal("0.25")),
        ({"sharpe": Decimal("2.1")}, lambda m: m.metrics.sharpe_ratio, Decimal("2.1")),
        ({"win_rate": Decimal("0.75")}, lambda m: m.metrics.win_rate, Decimal("0.75")),
        ({"trade_pnl": Decimal("5")}, lambda m: m._pnl_sum, Decimal("5")),
        ({"trade_pnl": Decimal("5")}, lambda m: m._pnl_sum_squares, Decimal("25")),
    ],
)
def test_update_single_field_only_writes_that_field(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
    getter,
    expected,
) -> None:
    manager, _events = _make_manager(monkeypatch)
    assert manager.test is not None
    metrics = manager.test.variant_a_metrics
    assert metrics is not None

    assert manager.update_variant_metrics("a", **kwargs) is True
    assert getter(metrics) == expected


def test_update_with_no_fields_is_noop_but_emits_event(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.test is not None
    metrics = manager.test.variant_b_metrics
    assert metrics is not None

    assert manager.update_variant_metrics("b") is True

    assert metrics.trade_count == 0
    assert metrics.error_count == 0
    assert metrics.metrics.net_pnl_usd == Decimal("0")
    assert metrics.metrics.total_pnl_usd == Decimal("0")
    assert metrics.metrics.max_drawdown == Decimal("0")
    assert metrics.metrics.sharpe_ratio is None
    assert metrics.metrics.win_rate is None
    assert metrics._pnl_sum == Decimal("0")
    assert metrics._pnl_sum_squares == Decimal("0")
    assert len(events) == 1
    assert events[0].details["variant"] == "B"
    assert events[0].details["pnl_usd"] == "0"
    assert events[0].details["trade_count"] == 0


def test_update_trade_pnl_accumulates_variance_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    assert manager.test is not None
    metrics = manager.test.variant_a_metrics
    assert metrics is not None

    assert manager.update_variant_metrics("a", trades=1, trade_pnl=Decimal("2")) is True
    assert manager.update_variant_metrics("a", trades=2, trade_pnl=Decimal("4")) is True

    assert metrics._pnl_sum == Decimal("6")
    assert metrics._pnl_sum_squares == Decimal("20")
    # Welford variance for samples [2, 4]: mean 3, variance 2.
    assert metrics.pnl_variance == Decimal("2")


def test_update_with_emit_events_disabled_suppresses_timeline(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(
        monkeypatch,
        config=ABTestConfig(min_sample_size=2, emit_events=False),
    )

    assert manager.update_variant_metrics("a", pnl_usd=Decimal("1")) is True
    assert events == []


# ---------------------------------------------------------------------------
# Cheap lifecycle surfaces sharing the same fixture
# ---------------------------------------------------------------------------


def test_cancel_test_without_test_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch, create=False)

    result = manager.cancel_test()

    assert result.success is False
    assert result.error == "No test to cancel"


def test_cancel_running_test_marks_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch)

    result = manager.cancel_test()

    assert result.success is True
    assert result.winner is None
    assert result.final_comparison is not None
    assert manager.test is not None
    assert manager.test.status == ABTestStatus.CANCELLED
    assert manager.test.ended_at is not None
    event_types = [e.details["ab_test_event_type"] for e in events]
    assert ABTestEventType.AB_TEST_CANCELLED.value in event_types


def test_cancel_already_cancelled_test_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    assert manager.cancel_test().success

    result = manager.cancel_test()

    assert result.success is False
    assert result.error == "Cannot cancel test in status CANCELLED"


def test_end_test_without_test_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch, create=False)

    result = manager.end_test()

    assert result.success is False
    assert result.error == "No test to end"


def test_end_test_invalid_winner_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)

    result = manager.end_test(select_winner="variant_c")

    assert result.success is False
    assert result.error == "Invalid winner: variant_c, must be 'variant_a', 'variant_b', or None"
    assert manager.test is not None
    assert manager.test.status == ABTestStatus.RUNNING


def test_end_test_without_winner_and_insufficient_data_is_inconclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, events = _make_manager(monkeypatch)

    result = manager.end_test()

    assert result.success is True
    assert result.winner is None
    assert manager.test is not None
    assert manager.test.status == ABTestStatus.INCONCLUSIVE
    end_events = [e for e in events if e.details["ab_test_event_type"] == ABTestEventType.AB_TEST_ENDED.value]
    assert len(end_events) == 1
    assert "Inconclusive - insufficient data" in end_events[0].description


def test_end_test_without_winner_and_sufficient_data_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, events = _make_manager(monkeypatch)
    for variant in ("a", "b"):
        manager.update_variant_metrics(variant, trades=1, trade_pnl=Decimal("10"))
        manager.update_variant_metrics(variant, trades=2, trade_pnl=Decimal("10"))
    events.clear()

    result = manager.end_test()

    assert result.success is True
    assert result.winner is None
    assert result.final_comparison is not None
    assert result.final_comparison.has_sufficient_data is True
    assert manager.test is not None
    assert manager.test.status == ABTestStatus.COMPLETED
    end_events = [e for e in events if e.details["ab_test_event_type"] == ABTestEventType.AB_TEST_ENDED.value]
    assert len(end_events) == 1
    assert "Completed without winner selection" in end_events[0].description


def test_ending_test_blocks_further_metric_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    assert manager.end_test(select_winner="variant_b").success

    assert manager.update_variant_metrics("a", trades=1) is False


def test_get_status_without_test(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch, create=False)

    status = manager.get_status()

    assert status == {
        "has_active_test": False,
        "deployment_id": "deployment:abtest",
    }


def test_get_status_with_active_test(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    manager.update_variant_metrics("a", trades=3, pnl_usd=Decimal("15"))

    status = manager.get_status()

    assert status["has_active_test"] is True
    assert status["deployment_id"] == "deployment:abtest"
    assert status["status"] == ABTestStatus.RUNNING.value
    assert status["variant_a_id"] == "baseline"
    assert status["variant_b_id"] == "candidate"
    assert status["split_ratio"] == 0.5
    assert status["variant_a_metrics"]["trade_count"] == 3
    assert status["variant_a_metrics"]["metrics"]["net_pnl_usd"] == "15"
    assert status["comparison"] is not None
    assert status["winner"] is None


def test_manager_to_dict_from_dict_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    manager.update_variant_metrics("b", trades=5, pnl_usd=Decimal("25"), trade_pnl=Decimal("5"))

    restored = ABTestManager.from_dict(manager.to_dict())

    assert restored.deployment_id == "deployment:abtest"
    assert restored._chain == "testchain"
    assert restored.test is not None
    assert manager.test is not None
    assert restored.test.test_id == manager.test.test_id
    assert restored.test.status == ABTestStatus.RUNNING
    assert restored.test.variant_b_metrics is not None
    assert restored.test.variant_b_metrics.trade_count == 5
    assert restored.test.variant_b_metrics.metrics.net_pnl_usd == Decimal("25")
    assert restored.test.variant_b_metrics._pnl_sum == Decimal("5")
    assert restored.test.variant_b_metrics._pnl_sum_squares == Decimal("25")


def test_manager_from_dict_without_test_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ab_test_module, "add_event", lambda _event: None)

    restored = ABTestManager.from_dict({"deployment_id": "deployment:abtest"})

    assert restored.test is None
    assert restored._chain == "unknown"
