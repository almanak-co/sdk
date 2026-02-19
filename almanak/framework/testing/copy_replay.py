"""Deterministic copy-trading replay harness."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.framework.services.copy_intent_builder import CopyIntentBuilder
from almanak.framework.services.copy_ledger import CopyLedger
from almanak.framework.services.copy_policy_engine import CopyPolicyEngine
from almanak.framework.services.copy_trading_models import (
    CopySignal,
    CopyTradingConfigV2,
    LendingPayload,
    LPPayload,
    PerpPayload,
    SwapPayload,
)


class CopyReplayRunner:
    """Replay normalized copy signals through policy and intent mapping."""

    def __init__(
        self,
        config: CopyTradingConfigV2,
        policy_engine: CopyPolicyEngine | None = None,
        intent_builder: CopyIntentBuilder | None = None,
        ledger: CopyLedger | None = None,
    ) -> None:
        self._config = config
        self._policy = policy_engine or CopyPolicyEngine(config)
        self._builder = intent_builder or CopyIntentBuilder(config)
        self._ledger = ledger

    def run(self, replay_file: str | Path, shadow: bool = True) -> dict[str, Any]:
        """Run deterministic replay from a JSON/JSONL signal file."""
        signals = self.load_signals(replay_file)

        decisions = 0
        approved = 0
        mapped_intents = 0
        blocked: dict[str, int] = {}

        policy_state: dict[str, Any] | None = None
        intent_types: list[str] = []

        for signal in sorted(signals, key=lambda s: (s.leader_block or s.block_number, s.signal_id or s.event_id)):
            if self._ledger is not None and self._ledger.has_seen_signal(signal.signal_id or signal.event_id):
                blocked["duplicate_signal"] = blocked.get("duplicate_signal", 0) + 1
                continue

            if self._ledger is not None:
                self._ledger.record_signal(signal)

            evaluation_time = signal.detected_at if signal.detected_at > 0 else signal.timestamp + 1
            decision = self._policy.evaluate(signal, state=policy_state, current_time=evaluation_time)
            decisions += 1

            if self._ledger is not None:
                self._ledger.record_decision(decision)

            if decision.action != "execute":
                code = decision.skip_reason_code or decision.skip_reason or "policy_blocked"
                blocked[code] = blocked.get(code, 0) + 1
                continue

            approved += 1

            build = self._builder.build(signal)
            if build.intent is None:
                code = build.reason_code or "intent_build_blocked"
                blocked[code] = blocked.get(code, 0) + 1
                continue

            mapped_intents += 1
            if hasattr(build.intent, "intent_type"):
                intent_types.append(build.intent.intent_type.value)

            if not shadow:
                notional = Decimal(str(signal.metadata.get("notional_usd", "0")))
                policy_state = self._policy.record_execution(signal, notional, state=policy_state)

        return {
            "signals_loaded": len(signals),
            "decisions_made": decisions,
            "approved": approved,
            "mapped_intents": mapped_intents,
            "blocked_by_reason": blocked,
            "intent_types": sorted(intent_types),
            "shadow_mode": shadow,
        }

    def load_signals(self, replay_file: str | Path) -> list[CopySignal]:
        """Load replay signals from JSON array or JSONL."""
        path = Path(replay_file)
        if not path.exists():
            raise FileNotFoundError(f"Replay file not found: {path}")

        raw = path.read_text().strip()
        if not raw:
            return []

        signals: list[CopySignal] = []

        if raw.startswith("["):
            rows = json.loads(raw)
            if not isinstance(rows, list):
                raise ValueError("Replay JSON must be a list of signal objects")
            for row in rows:
                signals.append(self._parse_signal(row))
            return signals

        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            signals.append(self._parse_signal(json.loads(line)))

        return signals

    def export_signal_template(self, signal: CopySignal) -> dict[str, Any]:
        """Serialize a runtime signal to replay-fixture shape."""
        payload = signal.action_payload
        if is_dataclass(payload):
            payload_obj: Any = asdict(payload)
        else:
            payload_obj = payload

        return {
            "event_id": signal.event_id,
            "signal_id": signal.signal_id,
            "action_type": signal.action_type,
            "protocol": signal.protocol,
            "chain": signal.chain,
            "tokens": signal.tokens,
            "amounts": {k: str(v) for k, v in signal.amounts.items()},
            "amounts_usd": {k: str(v) for k, v in signal.amounts_usd.items()},
            "metadata": signal.metadata,
            "leader_address": signal.leader_address,
            "block_number": signal.block_number,
            "timestamp": signal.timestamp,
            "leader_tx_hash": signal.leader_tx_hash,
            "leader_block": signal.leader_block,
            "detected_at": signal.detected_at,
            "age_seconds": signal.age_seconds,
            "action_payload": payload_obj,
            "capability_flags": signal.capability_flags,
        }

    def _parse_signal(self, row: dict[str, Any]) -> CopySignal:
        action_type = str(row.get("action_type", "")).upper()
        payload = self._parse_payload(action_type, row.get("action_payload"))

        return CopySignal(
            event_id=row["event_id"],
            signal_id=row.get("signal_id"),
            action_type=action_type,
            protocol=str(row.get("protocol", "unknown")),
            chain=str(row.get("chain", "arbitrum")),
            tokens=[str(t) for t in row.get("tokens", [])],
            amounts={k: Decimal(str(v)) for k, v in row.get("amounts", {}).items()},
            amounts_usd={k: Decimal(str(v)) for k, v in row.get("amounts_usd", {}).items()},
            metadata=row.get("metadata", {}),
            leader_address=str(row.get("leader_address", "")),
            block_number=int(row["block_number"]) if row.get("block_number") is not None else 0,
            timestamp=int(row["timestamp"]) if row.get("timestamp") is not None else 0,
            leader_tx_hash=row.get("leader_tx_hash"),
            leader_block=int(row["leader_block"]) if row.get("leader_block") is not None else None,
            detected_at=int(row["detected_at"]) if row.get("detected_at") is not None else 0,
            age_seconds=int(row["age_seconds"]) if row.get("age_seconds") is not None else 0,
            action_payload=payload,
            capability_flags=row.get("capability_flags", {}),
        )

    def _parse_payload(self, action_type: str, payload: Any) -> Any:
        if not isinstance(payload, dict):
            return payload

        if action_type == "SWAP":
            return SwapPayload(
                token_in=str(payload.get("token_in", "")),
                token_out=str(payload.get("token_out", "")),
                amount_in=Decimal(str(payload.get("amount_in", "0"))),
                amount_out=Decimal(str(payload.get("amount_out", "0"))),
                effective_price=Decimal(str(payload["effective_price"])) if payload.get("effective_price") else None,
                slippage_bps=int(payload["slippage_bps"]) if payload.get("slippage_bps") is not None else None,
            )

        if action_type in {"LP_OPEN", "LP_CLOSE"}:
            return LPPayload(
                pool=payload.get("pool"),
                position_id=str(payload["position_id"]) if payload.get("position_id") is not None else None,
                amount0=Decimal(str(payload["amount0"])) if payload.get("amount0") is not None else None,
                amount1=Decimal(str(payload["amount1"])) if payload.get("amount1") is not None else None,
                range_lower=Decimal(str(payload["range_lower"])) if payload.get("range_lower") is not None else None,
                range_upper=Decimal(str(payload["range_upper"])) if payload.get("range_upper") is not None else None,
                close_fraction=(
                    Decimal(str(payload["close_fraction"])) if payload.get("close_fraction") is not None else None
                ),
            )

        if action_type in {"SUPPLY", "WITHDRAW", "BORROW", "REPAY"}:
            return LendingPayload(
                token=payload.get("token"),
                amount=Decimal(str(payload["amount"])) if payload.get("amount") is not None else None,
                collateral_token=payload.get("collateral_token"),
                borrow_token=payload.get("borrow_token"),
                market_id=payload.get("market_id"),
                use_as_collateral=payload.get("use_as_collateral"),
            )

        if action_type in {"PERP_OPEN", "PERP_CLOSE"}:
            return PerpPayload(
                market=payload.get("market"),
                collateral_token=payload.get("collateral_token"),
                collateral_amount=(
                    Decimal(str(payload["collateral_amount"])) if payload.get("collateral_amount") is not None else None
                ),
                size_usd=Decimal(str(payload["size_usd"])) if payload.get("size_usd") is not None else None,
                is_long=payload.get("is_long"),
                leverage=Decimal(str(payload["leverage"])) if payload.get("leverage") is not None else None,
                position_id=str(payload["position_id"]) if payload.get("position_id") is not None else None,
            )

        return payload
