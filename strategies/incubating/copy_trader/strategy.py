"""Copy Trader Demo Strategy with policy + intent-builder copy pipeline."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.services.copy_circuit_breaker import CopyCircuitBreaker
from almanak.framework.services.copy_intent_builder import CopyIntentBuilder
from almanak.framework.services.copy_ledger import CopyLedger
from almanak.framework.services.copy_performance_tracker import CopyPerformanceTracker
from almanak.framework.services.copy_policy_engine import CopyPolicyEngine
from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import (
    CopyDecision,
    CopyExecutionRecord,
    CopySignal,
    CopyTradingConfigV2,
)
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy

logger = logging.getLogger(__name__)


def _get_config(config: Any, key: str, default: Any) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


@almanak_strategy(
    name="demo_copy_trader",
    description="Monitors leader wallets and mirrors copy signals with deterministic policy checks",
    version="0.2.0",
    author="Almanak",
    tags=["copy-trading", "demo", "policy-engine", "multi-intent"],
    supported_chains=["arbitrum"],
    supported_protocols=["uniswap_v3", "aave_v3", "morpho_blue", "gmx_v2"],
    intent_types=["SWAP", "LP_OPEN", "LP_CLOSE", "SUPPLY", "WITHDRAW", "BORROW", "REPAY", "PERP_OPEN", "PERP_CLOSE", "HOLD"],
)
class CopyTraderStrategy(IntentStrategy):
    """Copy trading strategy with institutional policy, sizing, and audit hooks."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        ct_raw = _get_config(self.config, "copy_trading", {})
        if not isinstance(ct_raw, dict):
            ct_raw = {}

        try:
            self._ct_config = CopyTradingConfigV2.from_config(ct_raw)
        except Exception as exc:
            logger.warning("Invalid copy_trading config, using safe defaults: %s", exc)
            self._ct_config = CopyTradingConfigV2()

        sizing_cfg = CopySizingConfig.from_config(
            self._ct_config.sizing.model_dump(mode="python"),
            self._ct_config.risk.model_dump(mode="python"),
        )
        self._sizer = CopySizer(config=sizing_cfg)

        self._policy = CopyPolicyEngine(
            config=self._ct_config,
            reference_price_fn=self._reference_price,
        )
        self._builder = CopyIntentBuilder(config=self._ct_config, sizer=self._sizer)
        self._circuit = CopyCircuitBreaker.from_copy_config(self._ct_config)

        ledger_block = ct_raw.get("ledger", {}) if isinstance(ct_raw.get("ledger"), dict) else {}
        self._ledger = CopyLedger(ledger_block.get("db_path", "./almanak_copy_ledger.db"))

        self._copy_mode = str(self._ct_config.execution_policy.copy_mode)
        self._submission_mode = self._ct_config.execution_policy.submission_mode
        self._dry_run = bool(_get_config(self.config, "dry_run", False) or self._copy_mode in {"shadow", "replay"})

        self._global_policy = self._ct_config.global_policy
        self._signals_seen = 0
        self._signals_copied = 0
        self._policy_state: dict[str, Any] | None = None
        self._pending_by_intent_id: dict[str, tuple[CopySignal, CopyDecision]] = {}
        self._tracker = CopyPerformanceTracker()

        logger.info(
            "CopyTraderStrategy initialized: mode=%s, submission_mode=%s, strict=%s",
            self._copy_mode,
            self._submission_mode,
            self._ct_config.execution_policy.strict,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            signals = market.wallet_activity(
                action_types=self._global_policy.action_types or None,
                protocols=self._global_policy.protocols or None,
                min_usd_value=self._global_policy.min_usd_value,
            )

            if self._global_policy.tokens:
                allow_tokens = {t.upper() for t in self._global_policy.tokens}
                signals = [s for s in signals if all(tok.upper() in allow_tokens for tok in s.tokens)]

            if not signals:
                return Intent.hold(reason="No new leader activity")

            can_execute, reason = self._circuit.can_execute()
            if not can_execute:
                self._emit_timeline(
                    TimelineEventType.COPY_CIRCUIT_BREAKER,
                    f"Copy circuit breaker active: {reason}",
                    details={"reason_code": reason},
                )
                return Intent.hold(reason=f"Circuit breaker: {reason}")

            provider = getattr(self, "_wallet_activity_provider", None)

            for signal in sorted(signals, key=lambda s: (s.leader_block or s.block_number, s.signal_id or s.event_id)):
                self._signals_seen += 1
                self._ledger.record_signal(signal)

                decision = self._policy.evaluate(signal, state=self._policy_state)
                self._ledger.record_decision(decision)

                if decision.action != "execute":
                    code = decision.skip_reason_code or decision.skip_reason or "policy_blocked"
                    self._tracker.record_skip(code)
                    self._emit_timeline(
                        TimelineEventType.COPY_POLICY_BLOCKED,
                        f"Copy blocked: {code}",
                        chain=signal.chain,
                        details={"signal_id": signal.signal_id, "reason_code": code},
                    )
                    self._consume_signal(provider, signal.event_id)
                    continue

                self._emit_timeline(
                    TimelineEventType.COPY_DECISION_MADE,
                    f"Copy decision approved: {signal.action_type} on {signal.protocol}",
                    chain=signal.chain,
                    details={"signal_id": signal.signal_id, "decision_id": decision.decision_id},
                )

                build_result = self._builder.build(signal)
                if build_result.intent is None:
                    code = build_result.reason_code or "intent_build_failed"
                    self._tracker.record_skip(code)
                    self._consume_signal(provider, signal.event_id)
                    continue

                if self._dry_run:
                    self._signals_copied += 1
                    self._ledger.record_execution(
                        CopyExecutionRecord(
                            event_id=signal.event_id,
                            signal_id=signal.signal_id,
                            status="skipped",
                            skip_reason="shadow_or_dry_run",
                            submission_mode=self._submission_mode,
                            timestamp=int(time.time()),
                            status_code="shadow_mode",
                        )
                    )
                    self._consume_signal(provider, signal.event_id)
                    continue

                intent = build_result.intent
                if hasattr(intent, "intent_id"):
                    self._pending_by_intent_id[intent.intent_id] = (signal, decision)

                self._signals_copied += 1
                self._emit_timeline(
                    TimelineEventType.COPY_INTENT_CREATED,
                    f"Copy intent created: {signal.action_type} on {signal.protocol}",
                    chain=signal.chain,
                    details={
                        "signal_id": signal.signal_id,
                        "decision_id": decision.decision_id,
                        "intent_type": intent.intent_type.value if hasattr(intent, "intent_type") else "unknown",
                    },
                )
                return intent

            return Intent.hold(reason="No actionable copy signals")

        except Exception as exc:
            logger.exception("Error in copy decide(): %s", exc)
            return Intent.hold(reason=f"Error: {exc}")

    def on_copy_execution_result(self, intent: Any, success: bool, result: Any) -> None:
        """Runner hook: maintain ledger + quality metrics for copy executions."""
        intent_id = getattr(intent, "intent_id", None)
        signal, decision = self._pending_by_intent_id.pop(intent_id, (None, None))

        if signal is None:
            return

        now = int(time.time())
        lag_ms = max(0, (now - signal.detected_at) * 1000)
        deviation_bps = self._extract_price_deviation_bps(decision)

        record = CopyExecutionRecord(
            event_id=signal.event_id,
            signal_id=signal.signal_id,
            intent_id=intent_id,
            intent_ids=[intent_id] if intent_id else None,
            status="executed" if success else "failed",
            tx_hashes=self._extract_tx_hashes(result),
            submission_mode=self._submission_mode,
            leader_follower_lag_ms=lag_ms,
            price_deviation_bps=deviation_bps,
            timestamp=now,
            status_code="ok" if success else "execution_failed",
        )
        self._ledger.record_execution(record)

        if success:
            executed_usd = self._extract_intent_notional(intent, signal)
            self._policy_state = self._policy.record_execution(signal, executed_usd, state=self._policy_state)
            self._sizer.record_execution(executed_usd)
            if signal.action_type.upper() in {"SWAP", "LP_CLOSE", "PERP_CLOSE", "WITHDRAW", "REPAY"}:
                self._sizer.record_close()
            self._tracker.record_execution(executed_usd)
            self._emit_timeline(
                TimelineEventType.COPY_EXECUTION_RESULT,
                f"Copy execution success: {signal.action_type}",
                chain=signal.chain,
                details={"signal_id": signal.signal_id, "lag_ms": lag_ms},
            )
        else:
            self._tracker.record_skip("execution_failed")
            self._emit_timeline(
                TimelineEventType.COPY_EXECUTION_RESULT,
                f"Copy execution failed: {signal.action_type}",
                chain=signal.chain,
                details={"signal_id": signal.signal_id, "error": str(result)},
            )

        allowed, breaker_reason = self._circuit.record(record)
        self._emit_timeline(
            TimelineEventType.COPY_EXECUTION_QUALITY,
            f"Copy execution quality: lag={lag_ms}ms, deviation={deviation_bps}",
            chain=signal.chain,
            details={
                "signal_id": signal.signal_id,
                "leader_follower_lag_ms": lag_ms,
                "price_deviation_bps": deviation_bps,
                "breaker_allowed": allowed,
            },
        )
        if not allowed:
            self._emit_timeline(
                TimelineEventType.COPY_CIRCUIT_BREAKER,
                f"Copy circuit breaker triggered: {breaker_reason}",
                chain=signal.chain,
                details={"reason_code": breaker_reason},
            )

        self._consume_signal(getattr(self, "_wallet_activity_provider", None), signal.event_id)

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Legacy execution callback retained for compatibility."""
        return

    def _reference_price(self, token: str, chain: str) -> Decimal | None:
        price_fn = getattr(self, "_price_oracle", None)
        if not callable(price_fn):
            return None
        try:
            value = price_fn(token, "USD")
            if value is None:
                return None
            return Decimal(str(value))
        except Exception:
            return None

    @staticmethod
    def _extract_tx_hashes(result: Any) -> list[str] | None:
        tx_results = getattr(result, "transaction_results", None)
        if not tx_results:
            return None
        hashes = [getattr(r, "tx_hash", None) for r in tx_results]
        return [h for h in hashes if h]

    @staticmethod
    def _extract_price_deviation_bps(decision: CopyDecision | None) -> int | None:
        if decision is None:
            return None
        for check in decision.policy_results.get("checks", []):
            if check.get("name") == "price_deviation":
                details = check.get("details", {})
                if "deviation_bps" in details:
                    return int(details["deviation_bps"])
        return None

    @staticmethod
    def _extract_intent_notional(intent: Any, signal: CopySignal) -> Decimal:
        amount_usd = getattr(intent, "amount_usd", None)
        if amount_usd is not None:
            return Decimal(str(amount_usd)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        notional = signal.metadata.get("notional_usd")
        if notional is not None:
            return Decimal(str(notional)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        if signal.amounts_usd:
            return max((abs(v) for v in signal.amounts_usd.values()), default=Decimal("0")).quantize(
                Decimal("0.01"), rounding=ROUND_DOWN
            )

        return Decimal("0")

    @staticmethod
    def _consume_signal(provider: Any, event_id: str) -> None:
        if provider is None:
            return
        try:
            provider.consume_signals([event_id])
        except Exception:
            logger.debug("Failed to consume signal %s", event_id, exc_info=True)

    def _emit_timeline(
        self,
        event_type: TimelineEventType,
        description: str,
        chain: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=event_type,
                    description=description,
                    strategy_id=getattr(self, "strategy_id", ""),
                    chain=chain or getattr(self, "chain", ""),
                    details=details or {},
                )
            )
        except Exception:
            logger.debug("Failed to emit copy timeline event", exc_info=True)

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_copy_trader",
            "chain": self.chain,
            "copy_mode": self._copy_mode,
            "submission_mode": str(self._submission_mode),
            "signals_seen": self._signals_seen,
            "signals_copied": self._signals_copied,
            "daily_notional": str(self._sizer._daily_notional),
            "open_positions": self._sizer._open_positions,
            "performance": self._tracker.get_metrics(),
        }
