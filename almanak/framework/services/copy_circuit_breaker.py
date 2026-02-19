"""Copy-trading circuit breakers for execution quality and safety."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.services.copy_trading_models import CopyExecutionRecord, CopyTradingConfigV2


@dataclass(frozen=True)
class CopyCircuitBreakerConfig:
    """Thresholds for tripping copy-trading safety breakers."""

    window_size: int = 50
    min_samples: int = 10
    max_revert_rate: Decimal = Decimal("0.015")
    max_avg_lag_ms: int = 30_000
    max_price_deviation_bps: int = 200


@dataclass
class CopyCircuitState:
    """Current circuit breaker state."""

    tripped: bool = False
    reason_code: str | None = None
    tripped_at: datetime | None = None


@dataclass
class CopyCircuitBreaker:
    """Sliding-window circuit breaker for copy execution quality."""

    config: CopyCircuitBreakerConfig = field(default_factory=CopyCircuitBreakerConfig)
    _history: deque[CopyExecutionRecord] = field(default_factory=deque, init=False, repr=False)
    _state: CopyCircuitState = field(default_factory=CopyCircuitState, init=False, repr=False)

    @classmethod
    def from_copy_config(cls, config: CopyTradingConfigV2) -> CopyCircuitBreaker:
        """Build with conservative defaults derived from copy config."""
        cb = CopyCircuitBreakerConfig(
            max_price_deviation_bps=max(config.risk.max_price_deviation_bps, 1),
        )
        return cls(config=cb)

    @property
    def state(self) -> CopyCircuitState:
        """Read current breaker state."""
        return CopyCircuitState(
            tripped=self._state.tripped,
            reason_code=self._state.reason_code,
            tripped_at=self._state.tripped_at,
        )

    def reset(self) -> None:
        """Reset breaker and clear history."""
        self._history.clear()
        self._state = CopyCircuitState()

    def can_execute(self) -> tuple[bool, str | None]:
        """Return whether copy execution is currently permitted."""
        if self._state.tripped:
            return False, self._state.reason_code or "circuit_breaker_tripped"
        return True, None

    def record(self, execution: CopyExecutionRecord) -> tuple[bool, str | None]:
        """Record an execution sample and re-evaluate breaker state."""
        self._history.append(execution)
        while len(self._history) > self.config.window_size:
            self._history.popleft()

        self._evaluate()
        return self.can_execute()

    def _evaluate(self) -> None:
        if len(self._history) < self.config.min_samples:
            return

        failures = [r for r in self._history if r.status in {"failed"}]
        revert_rate = Decimal(len(failures)) / Decimal(len(self._history))
        if revert_rate > self.config.max_revert_rate:
            self._trip("copy_revert_rate_breach")
            return

        lag_samples = [r.leader_follower_lag_ms for r in self._history if r.leader_follower_lag_ms is not None]
        if lag_samples:
            avg_lag = sum(lag_samples) / len(lag_samples)
            if avg_lag > self.config.max_avg_lag_ms:
                self._trip("copy_lag_breach")
                return

        deviation_samples = [r.price_deviation_bps for r in self._history if r.price_deviation_bps is not None]
        if deviation_samples:
            if max(deviation_samples) > self.config.max_price_deviation_bps:
                self._trip("copy_price_deviation_breach")
                return

    def _trip(self, reason_code: str) -> None:
        if self._state.tripped:
            return
        self._state.tripped = True
        self._state.reason_code = reason_code
        self._state.tripped_at = datetime.now(UTC)
