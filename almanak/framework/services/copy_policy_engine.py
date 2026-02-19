"""Policy engine for deterministic copy-trading decisioning."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.services.copy_trading_models import (
    ActionPolicyConfig,
    CopyDecision,
    CopySignal,
    CopyTradingConfigV2,
    PerpPayload,
    SwapPayload,
)


@dataclass(frozen=True)
class PolicyCheckResult:
    """Outcome of a single policy check."""

    name: str
    passed: bool
    reason_code: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class CopyPolicyEngine:
    """Apply deterministic policy checks to copy signals before intent creation."""

    def __init__(
        self,
        config: CopyTradingConfigV2,
        reference_price_fn: Any = None,
    ) -> None:
        self._config = config
        self._reference_price_fn = reference_price_fn

    @property
    def config(self) -> CopyTradingConfigV2:
        """Get the active policy config."""
        return self._config

    def evaluate(
        self,
        signal: CopySignal,
        state: dict[str, Any] | None = None,
        current_time: int | None = None,
    ) -> CopyDecision:
        """Evaluate a signal and return execute/skip decision with detailed checks."""
        now = current_time if current_time is not None else int(time.time())
        mutable_state = self._normalize_state(state)
        self._rollover_day_if_needed(mutable_state, now)

        check_results: list[PolicyCheckResult] = [
            self._check_capabilities(signal),
            self._check_staleness(signal, now),
            self._check_leader_lag(signal),
            self._check_action_enabled(signal),
            self._check_protocol_allowlist(signal),
            self._check_token_allowlist(signal),
            self._check_notional_bounds(signal),
            self._check_daily_notional(signal, mutable_state),
            self._check_per_leader_cap(signal, mutable_state),
            self._check_price_deviation(signal),
        ]

        failed = next((c for c in check_results if not c.passed), None)
        projected_notional = self._derive_signal_notional_usd(signal)
        decision_id = str(uuid.uuid5(uuid.NAMESPACE_URL, signal.signal_id or signal.event_id))

        policy_results = {
            "checks": [
                {
                    "name": c.name,
                    "passed": c.passed,
                    "reason_code": c.reason_code,
                    "details": c.details,
                }
                for c in check_results
            ],
            "projected_notional_usd": str(projected_notional),
        }
        risk_snapshot = {
            "daily_notional_usd": str(mutable_state["daily_notional_usd"]),
            "leader_notional_usd": {k: str(v) for k, v in mutable_state["leader_notional_usd"].items()},
            "max_trade_usd": str(self._config.risk.max_trade_usd),
            "min_trade_usd": str(self._config.risk.min_trade_usd),
            "max_daily_notional_usd": str(self._config.risk.max_daily_notional_usd),
            "max_price_deviation_bps": self._config.risk.max_price_deviation_bps,
        }

        if failed is not None:
            return CopyDecision(
                signal=signal,
                action="skip",
                skip_reason=failed.reason_code,
                decision_id=decision_id,
                policy_results=policy_results,
                skip_reason_code=failed.reason_code,
                risk_snapshot=risk_snapshot,
            )

        return CopyDecision(
            signal=signal,
            action="execute",
            decision_id=decision_id,
            policy_results=policy_results,
            risk_snapshot=risk_snapshot,
        )

    def record_execution(
        self,
        signal: CopySignal,
        executed_usd: Decimal,
        state: dict[str, Any] | None = None,
        current_time: int | None = None,
    ) -> dict[str, Any]:
        """Update policy counters after a successful execution."""
        now = current_time if current_time is not None else int(time.time())
        mutable_state = self._normalize_state(state)
        self._rollover_day_if_needed(mutable_state, now)
        mutable_state["daily_notional_usd"] += executed_usd
        leader = signal.leader_address.lower()
        mutable_state["leader_notional_usd"][leader] = (
            mutable_state["leader_notional_usd"].get(leader, Decimal("0")) + executed_usd
        )
        return mutable_state

    def _normalize_state(self, state: dict[str, Any] | None) -> dict[str, Any]:
        if state is None:
            return {
                "date": datetime.now(UTC).strftime("%Y-%m-%d"),
                "daily_notional_usd": Decimal("0"),
                "leader_notional_usd": {},
            }
        leader_notional = state.get("leader_notional_usd", {})
        return {
            "date": state.get("date", datetime.now(UTC).strftime("%Y-%m-%d")),
            "daily_notional_usd": Decimal(str(state.get("daily_notional_usd", "0"))),
            "leader_notional_usd": {
                str(k).lower(): Decimal(str(v))
                for k, v in (leader_notional.items() if isinstance(leader_notional, dict) else [])
            },
        }

    def _rollover_day_if_needed(self, state: dict[str, Any], current_time: int) -> None:
        day = datetime.fromtimestamp(current_time, tz=UTC).strftime("%Y-%m-%d")
        if state["date"] != day:
            state["date"] = day
            state["daily_notional_usd"] = Decimal("0")
            state["leader_notional_usd"] = {}

    def _action_policy(self, action_type: str) -> ActionPolicyConfig:
        return self._config.action_policies.get(action_type.upper(), self._config.global_policy)

    def _derive_signal_notional_usd(self, signal: CopySignal) -> Decimal:
        """Compute signal notional in USD without double-counting swap legs."""
        if signal.amounts_usd:
            abs_values = [abs(v) for v in signal.amounts_usd.values()]
            if abs_values:
                return max(abs_values)

        if isinstance(signal.action_payload, PerpPayload) and signal.action_payload.size_usd is not None:
            return abs(signal.action_payload.size_usd)

        meta_notional = signal.metadata.get("notional_usd")
        if meta_notional is not None:
            return abs(Decimal(str(meta_notional)))

        return Decimal("0")

    def _check_capabilities(self, signal: CopySignal) -> PolicyCheckResult:
        flags = signal.capability_flags or {}
        if flags.get("chain_supported") is False:
            return PolicyCheckResult(
                name="capability_chain",
                passed=False,
                reason_code="unsupported_chain",
                details={"chain": signal.chain},
            )
        if flags.get("protocol_supported") is False:
            return PolicyCheckResult(
                name="capability_protocol",
                passed=False,
                reason_code="unsupported_protocol",
                details={"protocol": signal.protocol},
            )
        if flags.get("action_supported") is False:
            return PolicyCheckResult(
                name="capability_action",
                passed=False,
                reason_code="unsupported_action",
                details={"action_type": signal.action_type},
            )
        if flags.get("token_metadata_resolved") is False:
            return PolicyCheckResult(
                name="capability_tokens",
                passed=False,
                reason_code="missing_token_metadata",
                details={"tokens": signal.tokens},
            )
        return PolicyCheckResult(name="capability", passed=True)

    def _check_staleness(self, signal: CopySignal, now: int) -> PolicyCheckResult:
        age = max(signal.age_seconds, max(0, now - signal.timestamp))
        if age > self._config.monitoring.max_signal_age_seconds:
            return PolicyCheckResult(
                name="staleness",
                passed=False,
                reason_code="stale_signal",
                details={"age_seconds": age, "max_age_seconds": self._config.monitoring.max_signal_age_seconds},
            )
        return PolicyCheckResult(name="staleness", passed=True, details={"age_seconds": age})

    def _check_leader_lag(self, signal: CopySignal) -> PolicyCheckResult:
        lag = signal.metadata.get("leader_lag_blocks")
        if lag is None:
            return PolicyCheckResult(name="leader_lag", passed=True, details={"skipped": "lag_unknown"})

        lag_int = int(lag)
        if lag_int > self._config.monitoring.max_leader_lag_blocks:
            return PolicyCheckResult(
                name="leader_lag",
                passed=False,
                reason_code="leader_lag_exceeded",
                details={
                    "leader_lag_blocks": lag_int,
                    "max_lag_blocks": self._config.monitoring.max_leader_lag_blocks,
                },
            )
        return PolicyCheckResult(name="leader_lag", passed=True, details={"leader_lag_blocks": lag_int})

    def _check_action_enabled(self, signal: CopySignal) -> PolicyCheckResult:
        policy = self._action_policy(signal.action_type)
        if not policy.enabled:
            return PolicyCheckResult(
                name="action_enabled",
                passed=False,
                reason_code="action_disabled",
                details={"action_type": signal.action_type},
            )

        if policy.action_types:
            allowed = {x.upper() for x in policy.action_types}
            if signal.action_type.upper() not in allowed:
                return PolicyCheckResult(
                    name="action_enabled",
                    passed=False,
                    reason_code="action_not_allowlisted",
                    details={"action_type": signal.action_type, "allowlist": sorted(allowed)},
                )
        return PolicyCheckResult(name="action_enabled", passed=True)

    def _check_protocol_allowlist(self, signal: CopySignal) -> PolicyCheckResult:
        policy = self._action_policy(signal.action_type)
        if policy.protocols:
            allowlist = {x.lower() for x in policy.protocols}
            if signal.protocol.lower() not in allowlist:
                return PolicyCheckResult(
                    name="protocol_allowlist",
                    passed=False,
                    reason_code="protocol_not_allowlisted",
                    details={"protocol": signal.protocol, "allowlist": sorted(allowlist)},
                )
        return PolicyCheckResult(name="protocol_allowlist", passed=True)

    def _check_token_allowlist(self, signal: CopySignal) -> PolicyCheckResult:
        policy = self._action_policy(signal.action_type)
        if not policy.tokens:
            return PolicyCheckResult(name="token_allowlist", passed=True)

        allowlist = {token.upper() for token in policy.tokens}
        for token in signal.tokens:
            if token.upper() not in allowlist:
                return PolicyCheckResult(
                    name="token_allowlist",
                    passed=False,
                    reason_code="token_not_allowlisted",
                    details={"token": token, "allowlist": sorted(allowlist)},
                )
        return PolicyCheckResult(name="token_allowlist", passed=True)

    def _check_notional_bounds(self, signal: CopySignal) -> PolicyCheckResult:
        policy = self._action_policy(signal.action_type)
        projected = self._derive_signal_notional_usd(signal)

        if projected > self._config.risk.max_trade_usd:
            return PolicyCheckResult(
                name="notional_bounds",
                passed=False,
                reason_code="max_trade_exceeded",
                details={"projected_usd": str(projected), "max_trade_usd": str(self._config.risk.max_trade_usd)},
            )

        if projected < self._config.risk.min_trade_usd:
            return PolicyCheckResult(
                name="notional_bounds",
                passed=False,
                reason_code="below_min_trade",
                details={"projected_usd": str(projected), "min_trade_usd": str(self._config.risk.min_trade_usd)},
            )

        if policy.min_usd_value is not None and projected < policy.min_usd_value:
            return PolicyCheckResult(
                name="notional_bounds",
                passed=False,
                reason_code="below_min_policy_notional",
                details={"projected_usd": str(projected), "policy_min_usd": str(policy.min_usd_value)},
            )

        if policy.max_usd_value is not None and projected > policy.max_usd_value:
            return PolicyCheckResult(
                name="notional_bounds",
                passed=False,
                reason_code="above_max_policy_notional",
                details={"projected_usd": str(projected), "policy_max_usd": str(policy.max_usd_value)},
            )

        return PolicyCheckResult(name="notional_bounds", passed=True, details={"projected_usd": str(projected)})

    def _check_daily_notional(self, signal: CopySignal, state: dict[str, Any]) -> PolicyCheckResult:
        projected = self._derive_signal_notional_usd(signal)
        if state["daily_notional_usd"] + projected > self._config.risk.max_daily_notional_usd:
            return PolicyCheckResult(
                name="daily_notional",
                passed=False,
                reason_code="daily_notional_cap_reached",
                details={
                    "current_daily_usd": str(state["daily_notional_usd"]),
                    "projected_usd": str(projected),
                    "max_daily_usd": str(self._config.risk.max_daily_notional_usd),
                },
            )
        return PolicyCheckResult(name="daily_notional", passed=True)

    def _check_per_leader_cap(self, signal: CopySignal, state: dict[str, Any]) -> PolicyCheckResult:
        cap = self._config.get_leader_cap(signal.leader_address)
        if cap is None:
            return PolicyCheckResult(name="leader_cap", passed=True)

        projected = self._derive_signal_notional_usd(signal)
        current = state["leader_notional_usd"].get(signal.leader_address.lower(), Decimal("0"))
        if current + projected > cap:
            return PolicyCheckResult(
                name="leader_cap",
                passed=False,
                reason_code="leader_notional_cap_reached",
                details={
                    "leader": signal.leader_address,
                    "current_usd": str(current),
                    "projected_usd": str(projected),
                    "cap_usd": str(cap),
                },
            )
        return PolicyCheckResult(name="leader_cap", passed=True)

    def _check_price_deviation(self, signal: CopySignal) -> PolicyCheckResult:
        if self._reference_price_fn is None:
            return PolicyCheckResult(name="price_deviation", passed=True, details={"skipped": "no_reference_price_fn"})

        if not isinstance(signal.action_payload, SwapPayload):
            return PolicyCheckResult(name="price_deviation", passed=True, details={"skipped": "non_swap_signal"})

        if signal.action_payload.effective_price is None or signal.action_payload.effective_price <= 0:
            return PolicyCheckResult(name="price_deviation", passed=True, details={"skipped": "no_effective_price"})

        try:
            token_in_price = self._reference_price_fn(signal.action_payload.token_in, signal.chain)
            token_out_price = self._reference_price_fn(signal.action_payload.token_out, signal.chain)
            if token_in_price is None or token_out_price is None or token_in_price <= 0 or token_out_price <= 0:
                return PolicyCheckResult(name="price_deviation", passed=True, details={"skipped": "no_reference_price"})

            token_in_price_dec = Decimal(str(token_in_price))
            token_out_price_dec = Decimal(str(token_out_price))
            reference_ratio = token_out_price_dec / token_in_price_dec
            deviation = abs(signal.action_payload.effective_price - reference_ratio) / reference_ratio
            deviation_bps = int(deviation * Decimal("10000"))
            if deviation_bps > self._config.risk.max_price_deviation_bps:
                return PolicyCheckResult(
                    name="price_deviation",
                    passed=False,
                    reason_code="price_deviation_exceeded",
                    details={"deviation_bps": deviation_bps, "max_bps": self._config.risk.max_price_deviation_bps},
                )
            return PolicyCheckResult(name="price_deviation", passed=True, details={"deviation_bps": deviation_bps})
        except Exception as exc:
            return PolicyCheckResult(
                name="price_deviation",
                passed=False,
                reason_code="price_deviation_check_failed",
                details={"error": str(exc)},
            )
