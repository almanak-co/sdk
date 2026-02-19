"""Copy trading performance tracker.

Tracks execution metrics for copy trading: total copies, volume,
average copy latency in blocks, and skip reasons breakdown.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal


@dataclass
class CopyPerformanceTracker:
    """Tracks performance metrics for copy trading operations."""

    _total_copies: int = field(default=0, init=False)
    _total_volume_usd: Decimal = field(default=Decimal("0"), init=False)
    _total_skips: int = field(default=0, init=False)
    _skip_reasons: Counter = field(default_factory=Counter, init=False)
    _latency_sum_blocks: int = field(default=0, init=False)
    _latency_count: int = field(default=0, init=False)
    _start_time: float = field(default_factory=time.time, init=False)

    def record_execution(self, usd_amount: Decimal, latency_blocks: int = 0) -> None:
        """Record a successful copy trade execution."""
        self._total_copies += 1
        self._total_volume_usd += usd_amount
        if latency_blocks > 0:
            self._latency_sum_blocks += latency_blocks
            self._latency_count += 1

    def record_skip(self, reason: str) -> None:
        """Record a skipped signal with the reason."""
        self._total_skips += 1
        self._skip_reasons[reason] += 1

    def get_metrics(self) -> dict:
        """Return current performance metrics."""
        avg_latency = self._latency_sum_blocks / self._latency_count if self._latency_count > 0 else 0.0
        uptime_seconds = time.time() - self._start_time

        return {
            "total_copies": self._total_copies,
            "total_volume_usd": str(self._total_volume_usd),
            "total_skips": self._total_skips,
            "skip_reasons": dict(self._skip_reasons),
            "avg_latency_blocks": round(avg_latency, 2),
            "uptime_seconds": round(uptime_seconds, 0),
        }
