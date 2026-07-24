"""Unit tests for EscalationPolicy (almanak/framework/alerting/escalation.py).

Primary target: full branch coverage of ``EscalationPolicy.check_escalation``:
- default current_time (None) path
- unknown alert_id short-circuit
- inactive-escalation short-circuit
- no-escalation-needed (target level <= current level)
- single-level and multi-level escalation with channel dedup across levels
- already-notified channel suppression at a level
- channels_notified bookkeeping (pre-existing vs fresh target-level entry)
- Level 4 auto-remediation vs emergency-pause selection

Also covers the surrounding surface (start/acknowledge/resolve, level and
channel helpers, process_escalation success/fallback/exception paths, the
sync wrapper, bulk check/process, state accessors, cleanup) since they are
cheap to hit from the same fixtures. No network, no real callbacks.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.alerting.alert_config import AlertChannel, AlertConfig
from almanak.framework.alerting.escalation import (
    ESCALATION_THRESHOLDS,
    EscalationLevel,
    EscalationPolicy,
    EscalationResult,
    EscalationState,
    EscalationStatus,
)
from almanak.framework.models.actions import AvailableAction, SuggestedAction
from almanak.framework.models.operator_card import (
    AutoRemediation,
    EventType,
    OperatorCard,
    PositionSummary,
    Severity,
)
from almanak.framework.models.stuck_reason import StuckReason

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _make_card(
    *,
    deployment_id: str = "strat-1",
    event_type: EventType = EventType.ERROR,
    severity: Severity = Severity.HIGH,
    reason: StuckReason = StuckReason.RPC_FAILURE,
    auto_remediation: AutoRemediation | None = None,
) -> OperatorCard:
    """Build a minimal OperatorCard for tests."""
    return OperatorCard(
        deployment_id=deployment_id,
        timestamp=T0,
        event_type=event_type,
        reason=reason,
        context={"err": "boom"},
        severity=severity,
        position_summary=PositionSummary(
            total_value_usd=Decimal("1000"),
            available_balance_usd=Decimal("100"),
        ),
        risk_description="Strategy cannot reach RPC",
        suggested_actions=[
            SuggestedAction(
                action=AvailableAction.PAUSE,
                description="Pause until RPC restored",
                priority=1,
                is_recommended=True,
            )
        ],
        available_actions=[AvailableAction.PAUSE, AvailableAction.RESUME],
        auto_remediation=auto_remediation,
    )


def _full_config() -> AlertConfig:
    """AlertConfig with every channel configured."""
    return AlertConfig(
        telegram_chat_id="123",
        slack_webhook="https://hooks.example/slack",
        email="ops@example.com",
        pagerduty_key="pd-key",
    )


def _policy(
    config: AlertConfig | None = None,
    **kwargs,
) -> EscalationPolicy:
    return EscalationPolicy(config=config or _full_config(), **kwargs)


def _started(policy: EscalationPolicy, card: OperatorCard) -> EscalationState:
    return policy.start_escalation(card, current_time=T0)


def _at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# check_escalation — short-circuit branches
# ---------------------------------------------------------------------------


class TestCheckEscalationShortCircuits:
    def test_unknown_alert_with_default_time(self):
        # current_time=None exercises the wall-clock default branch;
        # the unknown alert_id keeps the outcome deterministic.
        policy = _policy()
        result = policy.check_escalation("nope")
        assert result.escalated is False
        assert result.new_level is None
        assert result.channels_to_notify == []
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is False
        assert result.message == "Unknown alert: nope"

    def test_inactive_escalation_not_escalated(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert policy.acknowledge(state.alert_id, current_time=_at(10))
        result = policy.check_escalation(state.alert_id, current_time=_at(600))
        assert result.escalated is False
        assert result.message == "Alert is not active: status=ACKNOWLEDGED"

    @pytest.mark.parametrize("elapsed", [0, 150, 299])
    def test_below_level_2_threshold_no_escalation(self, elapsed):
        policy = _policy()
        state = _started(policy, _make_card())
        result = policy.check_escalation(state.alert_id, current_time=_at(elapsed))
        assert result.escalated is False
        assert result.message == "No escalation needed, current level=1"
        assert state.current_level == EscalationLevel.LEVEL_1

    def test_no_re_escalation_at_same_level(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert policy.check_escalation(state.alert_id, current_time=_at(300)).escalated
        result = policy.check_escalation(state.alert_id, current_time=_at(400))
        assert result.escalated is False
        assert result.message == "No escalation needed, current level=2"


# ---------------------------------------------------------------------------
# check_escalation — escalation paths
# ---------------------------------------------------------------------------


class TestCheckEscalationLevels:
    def test_level_2_escalation_full_config(self):
        policy = _policy()
        state = _started(policy, _make_card())
        result = policy.check_escalation(state.alert_id, current_time=_at(300))
        assert result.escalated is True
        assert result.new_level == EscalationLevel.LEVEL_2
        assert result.channels_to_notify == [
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
            AlertChannel.EMAIL,
        ]
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is False
        assert result.message == "Escalated from level 1 to 2 after 300 seconds"
        assert state.current_level == EscalationLevel.LEVEL_2
        assert state.last_escalation_at == _at(300)
        assert state.channels_notified[EscalationLevel.LEVEL_2] == [
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
            AlertChannel.EMAIL,
        ]

    def test_multi_level_jump_dedupes_channels_across_levels(self):
        # Jump 1 -> 3: levels 2 and 3 are both walked; channels repeated at
        # level 3 must not be listed twice, PagerDuty joins for HIGH severity.
        policy = _policy()
        state = _started(policy, _make_card(severity=Severity.HIGH))
        result = policy.check_escalation(state.alert_id, current_time=_at(900))
        assert result.escalated is True
        assert result.new_level == EscalationLevel.LEVEL_3
        assert result.channels_to_notify == [
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
            AlertChannel.EMAIL,
            AlertChannel.PAGERDUTY,
        ]
        # Bookkeeping records the batch at the target level only.
        assert state.channels_notified == {
            EscalationLevel.LEVEL_3: [
                AlertChannel.TELEGRAM,
                AlertChannel.SLACK,
                AlertChannel.EMAIL,
                AlertChannel.PAGERDUTY,
            ]
        }

    def test_pagerduty_excluded_for_low_severity_at_level_3(self):
        policy = _policy()
        state = _started(policy, _make_card(severity=Severity.LOW))
        result = policy.check_escalation(state.alert_id, current_time=_at(900))
        assert result.escalated is True
        assert AlertChannel.PAGERDUTY not in result.channels_to_notify

    def test_pagerduty_excluded_when_not_configured(self):
        config = AlertConfig(
            telegram_chat_id="123",
            slack_webhook="https://hooks.example/slack",
            email="ops@example.com",
            pagerduty_key=None,
        )
        policy = _policy(config)
        state = _started(policy, _make_card(severity=Severity.CRITICAL))
        result = policy.check_escalation(state.alert_id, current_time=_at(900))
        assert AlertChannel.PAGERDUTY not in result.channels_to_notify
        assert AlertChannel.EMAIL in result.channels_to_notify

    def test_unconfigured_regular_channels_filtered(self):
        policy = _policy(AlertConfig(telegram_chat_id="123"))
        state = _started(policy, _make_card())
        result = policy.check_escalation(state.alert_id, current_time=_at(300))
        assert result.channels_to_notify == [AlertChannel.TELEGRAM]

    def test_already_notified_channels_skipped(self):
        policy = _policy()
        state = _started(policy, _make_card())
        # Pre-seed: TELEGRAM already notified at level 2.
        state.channels_notified[EscalationLevel.LEVEL_2] = [AlertChannel.TELEGRAM]
        result = policy.check_escalation(state.alert_id, current_time=_at(300))
        assert result.escalated is True
        assert result.channels_to_notify == [AlertChannel.SLACK, AlertChannel.EMAIL]
        # Existing target-level entry is extended, not replaced.
        assert state.channels_notified[EscalationLevel.LEVEL_2] == [
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
            AlertChannel.EMAIL,
        ]


class TestCheckEscalationLevel4:
    def test_level_4_with_auto_remediation(self):
        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=True,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy(auto_remediation_callback=lambda dep, c: True)
        state = _started(policy, card)
        result = policy.check_escalation(state.alert_id, current_time=_at(1800))
        assert result.escalated is True
        assert result.new_level == EscalationLevel.LEVEL_4
        assert result.trigger_auto_remediation is True
        assert result.trigger_emergency_pause is False

    def test_level_4_without_auto_remediation_falls_back_to_pause(self):
        policy = _policy(auto_remediation_callback=lambda dep, c: True)
        state = _started(policy, _make_card(auto_remediation=None))
        result = policy.check_escalation(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is True

    def test_level_4_with_disabled_auto_remediation_falls_back_to_pause(self):
        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=False,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy(auto_remediation_callback=lambda dep, c: True)
        state = _started(policy, card)
        result = policy.check_escalation(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is True

    def test_level_4_remediation_card_but_no_callback_falls_back(self):
        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=True,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy()  # no callbacks wired
        state = _started(policy, card)
        result = policy.check_escalation(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is True


# ---------------------------------------------------------------------------
# Level / channel helpers and custom thresholds
# ---------------------------------------------------------------------------


class TestLevelHelpers:
    @pytest.mark.parametrize(
        ("elapsed", "expected"),
        [
            (0, EscalationLevel.LEVEL_1),
            (299, EscalationLevel.LEVEL_1),
            (300, EscalationLevel.LEVEL_2),
            (899, EscalationLevel.LEVEL_2),
            (900, EscalationLevel.LEVEL_3),
            (1799, EscalationLevel.LEVEL_3),
            (1800, EscalationLevel.LEVEL_4),
            (999999, EscalationLevel.LEVEL_4),
        ],
    )
    def test_get_level_for_time_default_thresholds(self, elapsed, expected):
        policy = _policy()
        assert policy._get_level_for_time(elapsed) == expected

    def test_get_level_for_time_negative_elapsed_falls_back_to_level_1(self):
        # No threshold matches a negative elapsed, exercising the fallback
        # return after the loop.
        policy = _policy()
        assert policy._get_level_for_time(-1.0) == EscalationLevel.LEVEL_1

    def test_custom_thresholds_respected(self):
        thresholds = {
            EscalationLevel.LEVEL_1: 0,
            EscalationLevel.LEVEL_2: 10,
            EscalationLevel.LEVEL_3: 20,
            EscalationLevel.LEVEL_4: 30,
        }
        policy = _policy(custom_thresholds=thresholds)
        assert policy.thresholds is thresholds
        assert policy._get_level_for_time(15) == EscalationLevel.LEVEL_2
        state = _started(policy, _make_card(auto_remediation=None))
        result = policy.check_escalation(state.alert_id, current_time=_at(30))
        assert result.new_level == EscalationLevel.LEVEL_4
        assert result.trigger_emergency_pause is True

    def test_default_thresholds_used_when_not_customized(self):
        assert _policy().thresholds is ESCALATION_THRESHOLDS

    def test_get_channels_for_level_1(self):
        policy = _policy()
        assert policy._get_channels_for_level(EscalationLevel.LEVEL_1, Severity.HIGH) == [
            AlertChannel.TELEGRAM,
            AlertChannel.SLACK,
        ]

    def test_pagerduty_guard_below_level_3(self, monkeypatch):
        # PAGERDUTY never appears in the default channel map below level 3;
        # inject it to document that the level guard alone filters it out.
        from almanak.framework.alerting import escalation as escalation_module

        monkeypatch.setitem(
            escalation_module.ESCALATION_CHANNELS,
            EscalationLevel.LEVEL_2,
            [AlertChannel.PAGERDUTY],
        )
        policy = _policy()
        assert policy._get_channels_for_level(EscalationLevel.LEVEL_2, Severity.CRITICAL) == []


# ---------------------------------------------------------------------------
# start_escalation / acknowledge / resolve
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_start_escalation_creates_state(self):
        policy = _policy()
        card = _make_card()
        state = _started(policy, card)
        assert state.deployment_id == "strat-1"
        assert state.alert_id == "strat-1:ERROR:RPC_FAILURE"
        assert state.card is card
        assert state.created_at == T0
        assert state.current_level == EscalationLevel.LEVEL_1
        assert state.status == EscalationStatus.ACTIVE
        assert state.last_escalation_at == T0
        assert state.channels_notified == {}

    def test_start_escalation_returns_existing_active(self):
        policy = _policy()
        first = _started(policy, _make_card())
        second = policy.start_escalation(_make_card(), current_time=_at(60))
        assert second is first
        assert len(policy.escalations) == 1

    def test_start_escalation_replaces_inactive(self):
        policy = _policy()
        first = _started(policy, _make_card())
        policy.resolve(first.alert_id)
        second = policy.start_escalation(_make_card(), current_time=_at(60))
        assert second is not first
        assert second.status == EscalationStatus.ACTIVE
        assert second.created_at == _at(60)

    def test_start_escalation_default_time_is_utc_now(self):
        policy = _policy()
        before = datetime.now(UTC)
        state = policy.start_escalation(_make_card())
        after = datetime.now(UTC)
        assert before <= state.created_at <= after

    def test_acknowledge_unknown_returns_false(self):
        assert _policy().acknowledge("nope") is False

    def test_acknowledge_active(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert policy.acknowledge(state.alert_id, acknowledged_by="alice", current_time=_at(30)) is True
        assert state.status == EscalationStatus.ACKNOWLEDGED
        assert state.acknowledged_at == _at(30)
        assert state.acknowledged_by == "alice"

    def test_acknowledge_inactive_is_noop_true(self):
        policy = _policy()
        state = _started(policy, _make_card())
        policy.resolve(state.alert_id)
        assert policy.acknowledge(state.alert_id, current_time=_at(30)) is True
        assert state.status == EscalationStatus.RESOLVED
        assert state.acknowledged_at is None

    def test_acknowledge_by_strategy_counts_only_matching_active(self):
        policy = _policy()
        a = _started(policy, _make_card(deployment_id="strat-1"))
        b = _started(policy, _make_card(deployment_id="strat-1", event_type=EventType.WARNING))
        other = _started(policy, _make_card(deployment_id="strat-2"))
        policy.resolve(b.alert_id)
        count = policy.acknowledge_by_strategy("strat-1", current_time=_at(30))
        assert count == 1
        assert a.status == EscalationStatus.ACKNOWLEDGED
        assert b.status == EscalationStatus.RESOLVED
        assert other.status == EscalationStatus.ACTIVE

    def test_acknowledge_by_strategy_default_time(self):
        policy = _policy()
        _started(policy, _make_card())
        assert policy.acknowledge_by_strategy("strat-1") == 1

    def test_acknowledge_by_strategy_counts_only_successful_acks(self):
        # Documents that the count only increments when acknowledge succeeds.
        policy = _policy()
        _started(policy, _make_card())
        policy.acknowledge = lambda alert_id, acknowledged_by="operator", current_time=None: False  # type: ignore[method-assign]
        assert policy.acknowledge_by_strategy("strat-1", current_time=_at(30)) == 0

    def test_resolve_unknown_returns_false(self):
        assert _policy().resolve("nope") is False

    def test_resolve_known(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert policy.resolve(state.alert_id) is True
        assert state.status == EscalationStatus.RESOLVED


# ---------------------------------------------------------------------------
# EscalationState helpers
# ---------------------------------------------------------------------------


class TestEscalationState:
    def test_time_since_created_explicit(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert state.time_since_created(_at(90)) == 90.0

    def test_post_init_preserves_provided_channels_notified(self):
        state = EscalationState(
            deployment_id="strat-1",
            alert_id="a",
            card=_make_card(),
            created_at=T0,
            channels_notified={EscalationLevel.LEVEL_1: [AlertChannel.TELEGRAM]},
        )
        assert state.channels_notified == {EscalationLevel.LEVEL_1: [AlertChannel.TELEGRAM]}

    def test_time_since_created_defaults_to_now(self):
        state = EscalationState(
            deployment_id="strat-1",
            alert_id="a",
            card=_make_card(),
            created_at=datetime.now(UTC) - timedelta(seconds=5),
        )
        assert state.time_since_created() >= 5.0

    def test_to_dict_round_trip_fields(self):
        policy = _policy()
        state = _started(policy, _make_card())
        d = state.to_dict()
        assert d["deployment_id"] == "strat-1"
        assert d["alert_id"] == state.alert_id
        assert d["created_at"] == T0.isoformat()
        assert d["current_level"] == 1
        assert d["status"] == "ACTIVE"
        assert d["acknowledged_at"] is None
        assert d["acknowledged_by"] is None
        assert d["last_escalation_at"] == T0.isoformat()
        assert d["channels_notified"] == {}
        assert d["card"]["deployment_id"] == "strat-1"

    def test_to_dict_after_acknowledged_escalation(self):
        policy = _policy()
        state = _started(policy, _make_card())
        policy.check_escalation(state.alert_id, current_time=_at(300))
        policy.acknowledge(state.alert_id, current_time=_at(400))
        d = state.to_dict()
        assert d["status"] == "ACKNOWLEDGED"
        assert d["acknowledged_at"] == _at(400).isoformat()
        assert d["channels_notified"] == {2: ["TELEGRAM", "SLACK", "EMAIL"]}


# ---------------------------------------------------------------------------
# process_escalation (async), sync wrapper, and bulk operations
# ---------------------------------------------------------------------------


class TestProcessEscalation:
    def test_not_escalated_short_circuit(self):
        policy = _policy()
        result = policy.process_escalation_sync("nope", current_time=_at(0))
        assert result.escalated is False

    def test_auto_remediation_success_sets_status(self):
        calls: list[tuple[str, OperatorCard]] = []

        def remediate(dep: str, card: OperatorCard) -> bool:
            calls.append((dep, card))
            return True

        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=True,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy(auto_remediation_callback=remediate)
        state = _started(policy, card)
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is True
        assert result.trigger_emergency_pause is False
        assert state.status == EscalationStatus.AUTO_REMEDIATED
        assert calls == [("strat-1", card)]

    def test_auto_remediation_failure_falls_back_to_pause(self):
        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=True,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy(
            auto_remediation_callback=lambda dep, c: False,
            emergency_pause_callback=lambda dep, c: True,
        )
        state = _started(policy, card)
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is True
        assert state.status == EscalationStatus.EMERGENCY_PAUSED

    def test_auto_remediation_exception_falls_back_to_pause(self):
        def boom(dep: str, card: OperatorCard) -> bool:
            raise RuntimeError("remediation exploded")

        card = _make_card(
            auto_remediation=AutoRemediation(
                enabled=True,
                action=AvailableAction.PAUSE,
                trigger_after_seconds=1800,
            )
        )
        policy = _policy(
            auto_remediation_callback=boom,
            emergency_pause_callback=lambda dep, c: True,
        )
        state = _started(policy, card)
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_auto_remediation is False
        assert result.trigger_emergency_pause is True
        assert state.status == EscalationStatus.EMERGENCY_PAUSED

    def test_emergency_pause_failure_leaves_status_active(self):
        policy = _policy(emergency_pause_callback=lambda dep, c: False)
        state = _started(policy, _make_card(auto_remediation=None))
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_emergency_pause is True
        assert state.status == EscalationStatus.ACTIVE

    def test_emergency_pause_exception_swallowed(self):
        def boom(dep: str, card: OperatorCard) -> bool:
            raise RuntimeError("pause exploded")

        policy = _policy(emergency_pause_callback=boom)
        state = _started(policy, _make_card(auto_remediation=None))
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_emergency_pause is True
        assert state.status == EscalationStatus.ACTIVE

    def test_emergency_pause_without_callback_is_noop(self):
        policy = _policy()
        state = _started(policy, _make_card(auto_remediation=None))
        result = policy.process_escalation_sync(state.alert_id, current_time=_at(1800))
        assert result.trigger_emergency_pause is True
        assert state.status == EscalationStatus.ACTIVE

    def test_process_escalation_default_time_unknown_alert(self):
        policy = _policy()
        result = asyncio.run(policy.process_escalation("nope"))
        assert result.escalated is False

    def test_process_escalation_defensive_missing_state_guard(self):
        # Documents the defensive branch: if check_escalation reports an
        # escalation but the state has vanished, the result is returned
        # unchanged and no callbacks fire.
        policy = _policy(auto_remediation_callback=lambda dep, c: True)
        stub = EscalationResult(escalated=True, trigger_auto_remediation=True)
        policy.check_escalation = lambda alert_id, current_time=None: stub  # type: ignore[method-assign]
        result = policy.process_escalation_sync("gone", current_time=_at(0))
        assert result is stub
        assert result.trigger_auto_remediation is True


class TestBulkOperations:
    def test_check_all_escalations_skips_inactive(self):
        policy = _policy()
        active = _started(policy, _make_card(deployment_id="strat-1"))
        resolved = _started(policy, _make_card(deployment_id="strat-2"))
        policy.resolve(resolved.alert_id)
        results = policy.check_all_escalations(current_time=_at(300))
        assert set(results) == {active.alert_id}
        assert results[active.alert_id].escalated is True

    def test_check_all_escalations_default_time_empty(self):
        assert _policy().check_all_escalations() == {}

    def test_process_all_escalations(self):
        policy = _policy(emergency_pause_callback=lambda dep, c: True)
        a = _started(policy, _make_card(deployment_id="strat-1", auto_remediation=None))
        b = _started(policy, _make_card(deployment_id="strat-2"))
        policy.acknowledge(b.alert_id, current_time=_at(10))
        results = asyncio.run(policy.process_all_escalations(current_time=_at(1800)))
        assert set(results) == {a.alert_id}
        assert results[a.alert_id].trigger_emergency_pause is True
        assert a.status == EscalationStatus.EMERGENCY_PAUSED

    def test_process_all_escalations_default_time_empty(self):
        assert asyncio.run(_policy().process_all_escalations()) == {}


# ---------------------------------------------------------------------------
# Accessors and cleanup
# ---------------------------------------------------------------------------


class TestAccessorsAndCleanup:
    def test_get_escalation_state(self):
        policy = _policy()
        state = _started(policy, _make_card())
        assert policy.get_escalation_state(state.alert_id) is state
        assert policy.get_escalation_state("nope") is None

    def test_get_active_escalations(self):
        policy = _policy()
        active = _started(policy, _make_card(deployment_id="strat-1"))
        resolved = _started(policy, _make_card(deployment_id="strat-2"))
        policy.resolve(resolved.alert_id)
        assert policy.get_active_escalations() == [active]

    def test_get_escalations_for_strategy(self):
        policy = _policy()
        a = _started(policy, _make_card(deployment_id="strat-1"))
        b = _started(policy, _make_card(deployment_id="strat-1", event_type=EventType.WARNING))
        _started(policy, _make_card(deployment_id="strat-2"))
        assert policy.get_escalations_for_strategy("strat-1") == [a, b]

    def test_clear_resolved_escalations(self):
        policy = _policy()
        old_resolved = _started(policy, _make_card(deployment_id="strat-1"))
        old_resolved.created_at = datetime(2020, 1, 1, tzinfo=UTC)
        policy.resolve(old_resolved.alert_id)

        young_resolved = _started(policy, _make_card(deployment_id="strat-2"))
        young_resolved.created_at = datetime.now(UTC)
        policy.resolve(young_resolved.alert_id)

        old_active = _started(policy, _make_card(deployment_id="strat-3"))
        old_active.created_at = datetime(2020, 1, 1, tzinfo=UTC)

        cleared = policy.clear_resolved_escalations(max_age_seconds=3600)
        assert cleared == 1
        assert old_resolved.alert_id not in policy.escalations
        assert young_resolved.alert_id in policy.escalations
        assert old_active.alert_id in policy.escalations

    def test_clear_resolved_escalations_nothing_to_clear(self):
        policy = _policy()
        _started(policy, _make_card())
        assert policy.clear_resolved_escalations() == 0
