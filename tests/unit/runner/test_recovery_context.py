"""Recovery retains original measurements instead of recapturing post-trade state."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.runner.reconciliation import BalanceSnapshot
from almanak.framework.runner.recovery_context import ExecutionRecoveryContext
from almanak.framework.runner.runner_models import ExecutionProgress


def capture(**kwargs):
    return ExecutionRecoveryContext.capture(
        **{
            "plan_hash": "a" * 64,
            "execution": ExecutionContext(
                deployment_id="deployment:test",
                intent_id="intent-original",
                chain="bsc",
                wallet_address="0x" + "11" * 20,
                correlation_id="correlation-original",
                cycle_id="cycle-original",
            ),
            "pre_snapshot": BalanceSnapshot(datetime.now(UTC), {"GOOGLB": Decimal("0"), "USDT": Decimal("10")}),
            "prices": {"GOOGLB": Decimal("338.161"), "USDT": Decimal("1")},
            "bundle_metadata": {"pool": "0x" + "22" * 20},
            **kwargs,
        }
    )


def test_original_measurements_survive_marker_round_trip_and_source_mutation():
    snapshot = BalanceSnapshot(datetime.now(UTC), {"GOOGLB": Decimal("0"), "USDT": Decimal("10")})
    prices = {"GOOGLB": Decimal("338.161")}
    metadata = {"nested": {"price": Decimal("338.161")}}
    context = capture(pre_snapshot=snapshot, prices=prices, bundle_metadata=metadata)
    marker = ExecutionProgress("exec", "deployment:test", "plan", 1, recovery_context=context)
    snapshot.balances["GOOGLB"] = Decimal("100")
    prices.clear()
    metadata["nested"]["price"] = Decimal("1000")

    restored = ExecutionProgress.from_dict(marker.to_dict()).recovery_context
    assert restored is not None
    assert restored.pre_snapshot.balances == {"GOOGLB": Decimal("0"), "USDT": Decimal("10")}
    assert restored.prices == {"GOOGLB": Decimal("338.161")}
    assert restored.bundle_metadata == {"nested": {"price": "338.161"}}
    assert restored.execution.intent_id == "intent-original"
    assert restored.execution.cycle_id == "cycle-original"
    assert restored.plan_hash == "a" * 64


@pytest.mark.parametrize("prices", [None, {}, {"TOKEN": Decimal("0")}])
def test_unmeasured_empty_and_zero_prices_remain_distinct(prices):
    context = capture(prices=prices, pre_snapshot=None)
    restored = ExecutionRecoveryContext.from_dict(context.to_dict())
    assert restored.prices == prices
    assert restored.pre_snapshot is None


@pytest.mark.parametrize("invalid", [Decimal("NaN"), Decimal("Infinity"), Decimal("-1"), True, "bad"])
def test_invalid_measurement_refuses_capture(invalid):
    with pytest.raises(ValueError):
        capture(prices={"TOKEN": invalid})


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"plan_hash": ""},
        {"execution": {}},
        {"bundle_metadata": []},
        {"pre_snapshot": {"timestamp": "2026-09-10T01:00:00", "balances": {}}},
        {"pre_snapshot": {"timestamp": "2026-09-10T01:00:00+00:00", "balances": None}},
    ],
)
def test_unknown_or_malformed_context_refuses_restoration(changes):
    payload = {**capture().to_dict(), **changes}
    with pytest.raises(ValueError):
        ExecutionRecoveryContext.from_dict(payload)


def test_legacy_marker_does_not_invent_recovery_context():
    marker = ExecutionProgress("exec", "deployment:test", "plan", 1)
    payload = marker.to_dict()
    payload.pop("recovery_context")
    assert ExecutionProgress.from_dict(payload).recovery_context is None


def test_strategy_checkpoint_is_detached_and_legacy_context_is_unmeasured():
    user = {"entered": False, "nested": {"amount": "1"}}
    framework = {"_almanak_test": {"position": "original"}}
    context = capture().with_strategy_checkpoint(user, framework)
    user["nested"]["amount"] = "99"
    framework.clear()
    restored = ExecutionRecoveryContext.from_dict(context.to_dict())
    assert restored.strategy_checkpoint["user_state"]["nested"]["amount"] == "1"
    assert restored.strategy_checkpoint["framework_state"] == {"_almanak_test": {"position": "original"}}
    legacy = context.to_dict()
    legacy.pop("strategy_checkpoint")
    assert ExecutionRecoveryContext.from_dict(legacy).strategy_checkpoint is None


@pytest.mark.parametrize("checkpoint", [{}, [], {"user_state": {}, "framework_state": None}])
def test_malformed_strategy_checkpoint_cannot_be_restored(checkpoint):
    payload = {**capture().to_dict(), "strategy_checkpoint": checkpoint}
    with pytest.raises(ValueError):
        ExecutionRecoveryContext.from_dict(payload)
