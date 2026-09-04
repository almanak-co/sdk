"""Characterization tests for A/B test creation, metrics, status, and persistence.

Timeline emission is stubbed at the module seam so no file or gateway writes
occur.
"""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import pytest

import almanak.framework.testing.ab_test as ab_test_module
from almanak.framework.testing.ab_test import (
    ABTest,
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


def _minimal_ab_test_payload() -> dict[str, object]:
    return {
        "test_id": "persisted-test-id",
        "deployment_id": "deployment:persisted",
        "variant_a_id": "baseline",
        "variant_b_id": "candidate",
        "status": ABTestStatus.PENDING.value,
    }


@pytest.mark.parametrize(
    ("variant_a", "variant_b", "error"),
    [
        ("", "candidate", "variant_a is required"),
        ("baseline", "", "variant_b is required"),
        ("same", "same", "variant_a and variant_b must be different"),
    ],
)
def test_create_rejects_invalid_variants_without_changing_state(
    monkeypatch: pytest.MonkeyPatch,
    variant_a: str,
    variant_b: str,
    error: str,
) -> None:
    manager, events = _make_manager(monkeypatch, create=False)

    result = manager.create_ab_test(variant_a=variant_a, variant_b=variant_b)

    assert result.success is False
    assert result.error == error
    assert manager.test is None
    assert events == []


def test_create_rejects_second_running_test_without_replacing_it(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch)
    original_test = manager.test

    result = manager.create_ab_test(variant_a="other-a", variant_b="other-b")

    assert result.success is False
    assert result.error == "An A/B test is already running. End it first before creating a new one."
    assert manager.test is original_test
    assert events == []


@pytest.mark.parametrize(
    "replaceable_status",
    [ABTestStatus.PENDING, ABTestStatus.COMPLETED, ABTestStatus.CANCELLED, ABTestStatus.INCONCLUSIVE],
)
def test_create_replaces_existing_non_running_test(
    monkeypatch: pytest.MonkeyPatch,
    replaceable_status: ABTestStatus,
) -> None:
    manager, events = _make_manager(monkeypatch)
    original_test = manager.test
    assert original_test is not None
    original_test.status = replaceable_status

    result = manager.create_ab_test(variant_a="other-a", variant_b="other-b")

    assert result.success is True
    assert result.test is manager.test
    assert result.test is not original_test
    assert result.test is not None
    assert result.test.deployment_id == "deployment:abtest"
    assert result.test.status == ABTestStatus.RUNNING
    assert len(events) == 1
    assert events[0].details["ab_test_event_type"] == ABTestEventType.AB_TEST_CREATED.value


@pytest.mark.parametrize("split_ratio", [0.0, 1.0])
def test_create_rejects_invalid_split_ratio(
    monkeypatch: pytest.MonkeyPatch,
    split_ratio: float,
) -> None:
    manager, events = _make_manager(monkeypatch, create=False)

    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        split_ratio=split_ratio,
    )

    assert result.success is False
    assert result.error == f"split_ratio must be between 0 and 1 (exclusive), got {split_ratio}"
    assert manager.test is None
    assert events == []


def test_create_uses_supplied_config_ratio_when_ratio_argument_is_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, events = _make_manager(monkeypatch, create=False)
    config = ABTestConfig(
        split_ratio=0.25,
        min_sample_size=7,
        confidence_level=0.9,
        auto_end_on_significance=True,
        max_duration_hours=12,
    )

    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        split_ratio=0.5,
        total_capital_usd=Decimal("1000"),
        config=config,
    )

    assert result.success is True
    assert result.test is not None
    assert result.test.config is config
    assert result.test.variant_a_capital_usd == Decimal("250.00")
    assert result.test.variant_b_capital_usd == Decimal("750.00")
    assert len(events) == 1
    assert events[0].description.endswith("split 25%/75%")
    assert events[0].details["split_ratio"] == 0.25


def test_create_non_default_ratio_overrides_config_and_preserves_other_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, events = _make_manager(monkeypatch, create=False)
    config = ABTestConfig(
        split_ratio=0.25,
        min_sample_size=7,
        confidence_level=0.9,
        emit_events=False,
        auto_end_on_significance=True,
        max_duration_hours=12,
    )

    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        split_ratio=0.6,
        total_capital_usd=Decimal("1000"),
        config=config,
    )

    assert result.success is True
    assert result.test is not None
    assert result.test.config == ABTestConfig(
        split_ratio=0.6,
        min_sample_size=7,
        confidence_level=0.9,
        emit_events=False,
        auto_end_on_significance=True,
        max_duration_hours=12,
    )
    assert result.test.config is not config
    assert result.test.variant_a_capital_usd == Decimal("600.0")
    assert result.test.variant_b_capital_usd == Decimal("400.0")
    assert events == []


def test_create_rejects_invalid_ratio_override_without_changing_state(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, events = _make_manager(monkeypatch, create=False)

    result = manager.create_ab_test(
        variant_a="baseline",
        variant_b="candidate",
        split_ratio=2.0,
        config=ABTestConfig(split_ratio=0.25),
    )

    assert result.success is False
    assert result.error == "split_ratio must be between 0 and 1 (exclusive), got 2.0"
    assert manager.test is None
    assert events == []


def test_create_initializes_identity_metrics_event_and_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list = []
    callbacks: list = []
    monkeypatch.setattr(ab_test_module, "add_event", events.append)
    manager = ABTestManager(
        deployment_id="deployment:stable",
        chain="arbitrum",
        on_test_start=callbacks.append,
    )

    result = manager.create_ab_test(
        variant_a="baseline-v1",
        variant_b="candidate-v2",
        total_capital_usd=Decimal("1000"),
    )

    assert result.success is True
    assert result.test is manager.test
    assert result.test is not None
    assert result.test_id == result.test.test_id
    assert result.test_id.startswith("abtest_deployment:stable_")
    assert result.test.deployment_id == "deployment:stable"
    assert result.test.status == ABTestStatus.RUNNING
    assert result.test.started_at is not None
    assert result.test.variant_a_metrics is not None
    assert result.test.variant_b_metrics is not None
    assert result.test.variant_a_metrics.measurement_start == result.test.started_at
    assert result.test.variant_b_metrics.measurement_start == result.test.started_at
    assert result.test.variant_a_metrics.capital_allocated_usd == Decimal("500.0")
    assert result.test.variant_b_metrics.capital_allocated_usd == Decimal("500.0")
    assert callbacks == [result.test]
    assert len(events) == 1
    assert events[0].deployment_id == "deployment:stable"
    assert events[0].chain == "arbitrum"
    assert events[0].details == {
        "ab_test_event_type": ABTestEventType.AB_TEST_CREATED.value,
        "test_id": result.test_id,
        "variant_a_id": "baseline-v1",
        "variant_b_id": "candidate-v2",
        "split_ratio": 0.5,
        "variant_a_capital_usd": "500.0",
        "variant_b_capital_usd": "500.0",
    }


def test_create_callback_error_is_logged_without_rolling_back_test(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(ab_test_module, "add_event", lambda _event: None)

    def failing_callback(_test) -> None:
        raise RuntimeError("start hook exploded")

    manager = ABTestManager(deployment_id="deployment:abtest", on_test_start=failing_callback)

    result = manager.create_ab_test(variant_a="baseline", variant_b="candidate")

    assert result.success is True
    assert manager.test is result.test
    assert manager.test is not None
    assert manager.test.status == ABTestStatus.RUNNING
    assert "Test start callback failed: start hook exploded" in caplog.text


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
def test_update_with_missing_variant_metrics_returns_false(monkeypatch: pytest.MonkeyPatch, variant: str) -> None:
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
def test_update_all_fields_writes_selected_variant(monkeypatch: pytest.MonkeyPatch, variant: str) -> None:
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


@pytest.mark.parametrize("missing_variant", ["a", "b"])
def test_get_status_with_incomplete_metrics_omits_comparison_without_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    missing_variant: str,
) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.test is not None
    if missing_variant == "a":
        manager.test.variant_a_metrics = None
    else:
        manager.test.variant_b_metrics = None

    status = manager.get_status()

    assert status["has_active_test"] is True
    assert status[f"variant_{missing_variant}_metrics"] is None
    assert status["comparison"] is None
    assert manager.test.comparison_history == []
    assert events == []


@pytest.mark.parametrize(
    "terminal_status",
    [ABTestStatus.COMPLETED, ABTestStatus.CANCELLED, ABTestStatus.INCONCLUSIVE],
)
def test_get_status_reports_terminal_test_as_active_and_records_comparison(
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: ABTestStatus,
) -> None:
    manager, events = _make_manager(monkeypatch)
    assert manager.test is not None
    manager.test.status = terminal_status

    status = manager.get_status()

    assert status["has_active_test"] is True
    assert status["status"] == terminal_status.value
    assert status["comparison"] is not None
    assert len(manager.test.comparison_history) == 1
    assert [event.details["ab_test_event_type"] for event in events] == [
        ABTestEventType.AB_TEST_COMPARISON_UPDATED.value
    ]


def test_get_status_can_auto_end_statistically_significant_running_test(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, events = _make_manager(
        monkeypatch,
        config=ABTestConfig(min_sample_size=2, auto_end_on_significance=True),
    )
    assert manager.test is not None
    assert manager.test.variant_a_metrics is not None
    assert manager.test.variant_b_metrics is not None
    metrics_a = manager.test.variant_a_metrics
    metrics_b = manager.test.variant_b_metrics
    metrics_a.trade_count = 2
    metrics_a.metrics.net_pnl_usd = Decimal("4")
    metrics_a.record_trade_pnl(Decimal("2"))
    metrics_a.record_trade_pnl(Decimal("2"))
    metrics_b.trade_count = 2
    metrics_b.metrics.net_pnl_usd = Decimal("16")
    metrics_b.record_trade_pnl(Decimal("8"))
    metrics_b.record_trade_pnl(Decimal("8"))

    status = manager.get_status()

    assert status["status"] == ABTestStatus.COMPLETED.value
    assert status["winner"] == "variant_b"
    assert manager.test.status == ABTestStatus.COMPLETED
    assert manager.test.winner == "variant_b"
    assert len(manager.test.comparison_history) == 2
    assert [event.details["ab_test_event_type"] for event in events] == [
        ABTestEventType.AB_TEST_COMPARISON_UPDATED.value,
        ABTestEventType.AB_TEST_COMPARISON_UPDATED.value,
        ABTestEventType.AB_TEST_ENDED.value,
        ABTestEventType.AB_TEST_WINNER_SELECTED.value,
    ]


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


def test_ab_test_from_legacy_minimal_dict_restores_defaults_and_exact_identity() -> None:
    before = datetime.now(UTC)

    restored = ABTest.from_dict(_minimal_ab_test_payload())

    after = datetime.now(UTC)
    assert restored.test_id == "persisted-test-id"
    assert restored.deployment_id == "deployment:persisted"
    assert restored.variant_a_id == "baseline"
    assert restored.variant_b_id == "candidate"
    assert restored.status == ABTestStatus.PENDING
    assert restored.config == ABTestConfig()
    assert restored.created_at is not None
    assert before <= restored.created_at <= after
    assert restored.started_at is None
    assert restored.ended_at is None
    assert restored.variant_a_metrics is None
    assert restored.variant_b_metrics is None
    assert restored.total_capital_usd == Decimal("0")
    assert restored.winner is None
    assert restored.comparison_history == []


@pytest.mark.parametrize("status", list(ABTestStatus))
def test_ab_test_from_dict_preserves_every_status(status: ABTestStatus) -> None:
    payload = _minimal_ab_test_payload()
    payload["status"] = status.value

    restored = ABTest.from_dict(payload)

    assert restored.status is status


def test_ab_test_from_dict_restores_complete_lifecycle_state(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, _events = _make_manager(monkeypatch)
    assert manager.test is not None
    manager.update_variant_metrics("a", trades=2, pnl_usd=Decimal("8"), trade_pnl=Decimal("4"))
    manager.update_variant_metrics("b", trades=2, pnl_usd=Decimal("10"), trade_pnl=Decimal("5"))
    assert manager.end_test(select_winner="variant_b").success
    original = manager.test

    restored = ABTest.from_dict(original.to_dict())

    assert restored.test_id == original.test_id
    assert restored.deployment_id == original.deployment_id
    assert restored.status == ABTestStatus.COMPLETED
    assert restored.config == original.config
    assert restored.created_at == original.created_at
    assert restored.started_at == original.started_at
    assert restored.ended_at == original.ended_at
    assert restored.total_capital_usd == original.total_capital_usd
    assert restored.winner == "variant_b"
    assert restored.comparison_history == original.comparison_history
    assert restored.variant_a_metrics is not None
    assert restored.variant_b_metrics is not None
    assert restored.variant_a_metrics.variant_id == "baseline"
    assert restored.variant_b_metrics.variant_id == "candidate"
    assert restored.variant_a_metrics._pnl_sum == Decimal("4")
    assert restored.variant_b_metrics._pnl_sum == Decimal("5")


@pytest.mark.parametrize(
    "required_field",
    ["test_id", "deployment_id", "variant_a_id", "variant_b_id", "status"],
)
def test_ab_test_from_dict_keeps_required_field_errors(required_field: str) -> None:
    payload = _minimal_ab_test_payload()
    del payload[required_field]

    with pytest.raises(KeyError) as exc_info:
        ABTest.from_dict(payload)

    assert exc_info.value.args == (required_field,)


def test_ab_test_from_dict_rejects_unknown_status() -> None:
    payload = _minimal_ab_test_payload()
    payload["status"] = "UNKNOWN"

    with pytest.raises(ValueError, match="'UNKNOWN' is not a valid ABTestStatus"):
        ABTest.from_dict(payload)


@pytest.mark.parametrize("timestamp_field", ["created_at", "started_at", "ended_at"])
def test_ab_test_from_dict_keeps_invalid_timestamp_errors(timestamp_field: str) -> None:
    payload = _minimal_ab_test_payload()
    payload[timestamp_field] = "not-an-iso-timestamp"

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        ABTest.from_dict(payload)


def test_ab_test_from_dict_keeps_invalid_config_error() -> None:
    payload = _minimal_ab_test_payload()
    payload["config"] = {"split_ratio": 0.0}

    with pytest.raises(ValueError, match="split_ratio must be between 0 and 1"):
        ABTest.from_dict(payload)


def test_ab_test_from_dict_keeps_invalid_capital_error() -> None:
    payload = _minimal_ab_test_payload()
    payload["total_capital_usd"] = "not-a-decimal"

    with pytest.raises(InvalidOperation):
        ABTest.from_dict(payload)


def test_manager_from_dict_without_test_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ab_test_module, "add_event", lambda _event: None)

    restored = ABTestManager.from_dict({"deployment_id": "deployment:abtest"})

    assert restored.test is None
    assert restored._chain == "unknown"
