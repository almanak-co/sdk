"""Original pre-submission measurements retained for receipt recovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.runner.reconciliation import BalanceSnapshot
from almanak.framework.state.json_state import copy_json_state


def _decimal_measurements(value: Any) -> dict[str, Decimal] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("Recovery measurements must be an object")
    result: dict[str, Decimal] = {}
    for key, quantity in value.items():
        if (
            not isinstance(key, str)
            or not key
            or not isinstance(quantity, str | Decimal | int)
            or isinstance(quantity, bool)
        ):
            raise ValueError("Malformed recovery measurement")
        try:
            measured = Decimal(quantity)
        except InvalidOperation as exc:
            raise ValueError("Malformed recovery measurement") from exc
        if not measured.is_finite() or measured < 0:
            raise ValueError("Recovery measurement must be finite and nonnegative")
        result[key] = measured
    return result


@dataclass(frozen=True)
class ExecutionRecoveryContext:
    """Versioned pre-trade measurements; missing values remain unmeasured.

    This is a checkpoint of inputs, not an execution-completion attestation.
    Strategy and protocol-specific state must also be restored before callbacks.
    """

    plan_hash: str
    execution: ExecutionContext
    pre_snapshot: BalanceSnapshot | None
    prices: dict[str, Decimal] | None
    bundle_metadata: dict[str, Any] | None
    schema_version: int = 1
    strategy_checkpoint: dict[str, Any] | None = None
    failed_attempt_receipts: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "plan_hash": self.plan_hash,
            "execution": asdict(self.execution),
            "pre_snapshot": None
            if self.pre_snapshot is None
            else {
                "timestamp": self.pre_snapshot.timestamp.isoformat(),
                "balances": {key: str(value) for key, value in self.pre_snapshot.balances.items()},
            },
            "prices": None if self.prices is None else {key: str(value) for key, value in self.prices.items()},
            "bundle_metadata": copy_json_state(self.bundle_metadata),
            "strategy_checkpoint": copy_json_state(self.strategy_checkpoint),
            "failed_attempt_receipts": copy_json_state(self.failed_attempt_receipts),
        }

    def with_strategy_checkpoint(self, user_state: dict, framework_state: dict) -> ExecutionRecoveryContext:
        checkpoint = copy_json_state({"user_state": user_state, "framework_state": framework_state})
        return replace(self, strategy_checkpoint=_validate_checkpoint(checkpoint))

    @classmethod
    def capture(
        cls,
        *,
        plan_hash: str,
        execution: ExecutionContext,
        pre_snapshot: BalanceSnapshot | None,
        prices: dict | None,
        bundle_metadata: dict | None,
        failed_attempt_receipts: dict | None = None,
    ) -> ExecutionRecoveryContext:
        captured = cls(
            plan_hash=plan_hash,
            execution=execution,
            pre_snapshot=pre_snapshot,
            prices=_decimal_measurements(prices),
            bundle_metadata=bundle_metadata,
            failed_attempt_receipts=copy_json_state(failed_attempt_receipts),
        )
        return cls.from_dict(captured.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExecutionRecoveryContext:
        if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
            raise ValueError("Unsupported execution recovery context version")
        plan_hash = value.get("plan_hash")
        if (
            not isinstance(plan_hash, str)
            or len(plan_hash) != 64
            or any(c not in "0123456789abcdef" for c in plan_hash)
        ):
            raise ValueError("Recovery context has no original plan identity")
        raw_execution = value.get("execution")
        if not isinstance(raw_execution, dict):
            raise ValueError("Recovery execution identity missing")
        for key in ("deployment_id", "intent_id", "chain", "wallet_address", "correlation_id"):
            if not isinstance(raw_execution.get(key), str) or not raw_execution[key]:
                raise ValueError("Recovery execution identity incomplete")
        execution = ExecutionContext(**raw_execution)
        snapshot = _restore_snapshot(value.get("pre_snapshot"))
        metadata = value.get("bundle_metadata")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError("Recovery bundle metadata must be an object")
        return cls(
            plan_hash,
            execution,
            snapshot,
            _decimal_measurements(value.get("prices")),
            copy_json_state(metadata),
            strategy_checkpoint=_validate_checkpoint(value.get("strategy_checkpoint")),
            failed_attempt_receipts=copy_json_state(value.get("failed_attempt_receipts")),
        )


def _restore_snapshot(value: Any) -> BalanceSnapshot | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not isinstance(value.get("timestamp"), str):
        raise ValueError("Malformed recovery balance snapshot")
    timestamp = datetime.fromisoformat(value["timestamp"])
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Recovery balance timestamp must have a timezone")
    balances = _decimal_measurements(value.get("balances"))
    if balances is None:
        raise ValueError("Measured balance snapshot has no balances")
    return BalanceSnapshot(timestamp=timestamp, balances=balances)


def _validate_checkpoint(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if (
        not isinstance(value, dict)
        or set(value) != {"user_state", "framework_state"}
        or not isinstance(value["user_state"], dict)
        or not isinstance(value["framework_state"], dict)
    ):
        raise ValueError("Malformed strategy recovery checkpoint")
    return copy_json_state(value)
