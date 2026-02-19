"""Copy-trading reporting and go-live gate evaluation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from almanak.framework.services.copy_ledger import CopyLedger


@dataclass(frozen=True)
class CopyGoLiveGates:
    """Quantitative production gates for copy-trading rollout."""

    decode_success_rate_min: float = 99.0
    false_positive_rate_max: float = 0.5
    execution_success_rate_min: float = 97.0
    revert_rate_max: float = 1.5
    unexplained_skip_rate_max: float = 0.5
    audit_trace_completeness_min: float = 100.0


class CopyReportGenerator:
    """Generate operational copy-trading reports from the local ledger."""

    def __init__(self, ledger: CopyLedger, gates: CopyGoLiveGates | None = None) -> None:
        self._ledger = ledger
        self._gates = gates or CopyGoLiveGates()

    def generate(self, since_seconds: int | None = None) -> dict[str, Any]:
        """Generate report and gate verdicts.

        Args:
            since_seconds: Optional trailing window in seconds.
        """
        since_ts = int(time.time()) - since_seconds if since_seconds is not None else None

        summary = self._ledger.get_summary(since_ts=since_ts)
        decisions = self._ledger.get_recent_decisions(limit=5000)

        approved = [d for d in decisions if d["action"] == "execute"]
        skipped = [d for d in decisions if d["action"] == "skip"]
        unexplained_skips = [d for d in skipped if not d.get("skip_reason_code")]

        executions = self._load_execution_rows(since_ts)
        executed_success = len([e for e in executions if e["status"] == "executed"])
        executed_failed = len([e for e in executions if e["status"] == "failed"])

        decision_count = max(len(decisions), 1)
        execution_total = max(executed_success + executed_failed, 1)

        decode_success_rate = (len(decisions) / max(summary["signals"], 1)) * 100.0
        false_positive_rate = (len(skipped) / decision_count) * 100.0
        execution_success_rate = (executed_success / execution_total) * 100.0
        revert_rate = (executed_failed / execution_total) * 100.0
        unexplained_skip_rate = (len(unexplained_skips) / decision_count) * 100.0

        # Trace completeness: decision has signal + every execution references signal
        execution_with_signal = len([e for e in executions if e.get("signal_id")])
        trace_num = len([d for d in decisions if d.get("signal_id")]) + execution_with_signal
        trace_den = len(decisions) + max(len(executions), 1)
        audit_trace_completeness = (trace_num / trace_den) * 100.0

        metrics = {
            "decode_success_rate": decode_success_rate,
            "false_positive_rate": false_positive_rate,
            "execution_success_rate": execution_success_rate,
            "revert_rate": revert_rate,
            "unexplained_skip_rate": unexplained_skip_rate,
            "audit_trace_completeness": audit_trace_completeness,
            "decision_count": len(decisions),
            "approved_count": len(approved),
            "execution_count": len(executions),
        }

        gates = {
            "decode_success_rate": metrics["decode_success_rate"] >= self._gates.decode_success_rate_min,
            "false_positive_rate": metrics["false_positive_rate"] <= self._gates.false_positive_rate_max,
            "execution_success_rate": metrics["execution_success_rate"] >= self._gates.execution_success_rate_min,
            "revert_rate": metrics["revert_rate"] <= self._gates.revert_rate_max,
            "unexplained_skip_rate": metrics["unexplained_skip_rate"] <= self._gates.unexplained_skip_rate_max,
            "audit_trace_completeness": (
                metrics["audit_trace_completeness"] >= self._gates.audit_trace_completeness_min
            ),
        }

        return {
            "window_since_ts": since_ts,
            "summary": summary,
            "metrics": metrics,
            "go_live_gates": gates,
            "all_gates_pass": all(gates.values()),
        }

    def _load_execution_rows(self, since_ts: int | None) -> list[dict[str, Any]]:
        return self._ledger.get_execution_rows(since_ts=since_ts)
