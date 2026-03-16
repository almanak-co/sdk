"""LP Performance Tracker — impermanent loss, fee tracking, gas costs, HODL benchmark.

Provides ``LPPerformanceTracker`` for strategy authors who need to evaluate
LP position performance beyond simple USD value deltas.

Usage::

    from almanak.framework.backtesting.lp_performance import LPPerformanceTracker

    tracker = LPPerformanceTracker(benchmark="hodl")
    tracker.record_snapshot(
        position_value_usd=Decimal("10500"),
        token0_amount=Decimal("2.5"),
        token1_amount=Decimal("5000"),
        fees_delta_usd=Decimal("12.50"),
        gas_delta_usd=Decimal("0.85"),
        token0_price=Decimal("2100"),
        token1_price=Decimal("1.0"),
    )
    report = tracker.summary()
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

BenchmarkType = Literal["hodl", "usd", "token0", "token1"]

_ZERO = Decimal("0")


@dataclass
class LPSnapshot:
    """A single point-in-time snapshot of an LP position."""

    timestamp: datetime
    position_value_usd: Decimal
    token0_amount: Decimal
    token1_amount: Decimal
    fees_delta_usd: Decimal
    gas_delta_usd: Decimal
    token0_price: Decimal
    token1_price: Decimal


@dataclass
class LPPerformanceReport:
    """Summary report of LP position performance."""

    # Core PnL
    total_pnl_usd: Decimal
    net_pnl_usd: Decimal  # total_pnl - gas_spent

    # Impermanent loss
    il_usd: Decimal
    il_pct: Decimal

    # Components
    fees_earned_usd: Decimal
    gas_spent_usd: Decimal

    # Benchmark comparison
    benchmark: str
    hodl_value_usd: Decimal
    vs_hodl_usd: Decimal
    vs_hodl_pct: Decimal

    # Time info
    duration_hours: Decimal
    num_snapshots: int

    # Risk (annualized Sharpe from snapshot returns including fees/gas, risk-free = 0)
    sharpe_ratio: float | None

    def to_dict(self) -> dict[str, Any]:
        """JSON-serializable dict representation."""
        return {
            "total_pnl_usd": str(self.total_pnl_usd),
            "net_pnl_usd": str(self.net_pnl_usd),
            "il_usd": str(self.il_usd),
            "il_pct": str(self.il_pct),
            "fees_earned_usd": str(self.fees_earned_usd),
            "gas_spent_usd": str(self.gas_spent_usd),
            "benchmark": self.benchmark,
            "hodl_value_usd": str(self.hodl_value_usd),
            "vs_hodl_usd": str(self.vs_hodl_usd),
            "vs_hodl_pct": str(self.vs_hodl_pct),
            "duration_hours": str(self.duration_hours),
            "num_snapshots": self.num_snapshots,
            "sharpe_ratio": self.sharpe_ratio,
        }


@dataclass
class LPPerformanceTracker:
    """Tracks LP position performance with IL, fees, gas, and benchmark comparison.

    Args:
        benchmark: Comparison benchmark — ``"hodl"`` (default), ``"usd"``,
            ``"token0"``, or ``"token1"``.

    Snapshot fields ``fees_delta_usd`` and ``gas_delta_usd`` are **per-snapshot
    deltas** (fees earned / gas spent since the previous snapshot), NOT cumulative
    totals. The tracker sums them internally.
    """

    benchmark: BenchmarkType = "hodl"
    _snapshots: list[LPSnapshot] = field(default_factory=list, repr=False)

    def record_snapshot(
        self,
        position_value_usd: Decimal,
        token0_amount: Decimal,
        token1_amount: Decimal,
        fees_delta_usd: Decimal = _ZERO,
        gas_delta_usd: Decimal = _ZERO,
        token0_price: Decimal = _ZERO,
        token1_price: Decimal = _ZERO,
        timestamp: datetime | None = None,
    ) -> None:
        """Record a point-in-time LP position snapshot.

        Args:
            fees_delta_usd: Fees earned since the previous snapshot (delta, not cumulative).
            gas_delta_usd: Gas spent since the previous snapshot (delta, not cumulative).
        """
        self._snapshots.append(
            LPSnapshot(
                timestamp=timestamp or datetime.now(UTC),
                position_value_usd=Decimal(str(position_value_usd)),
                token0_amount=Decimal(str(token0_amount)),
                token1_amount=Decimal(str(token1_amount)),
                fees_delta_usd=Decimal(str(fees_delta_usd)),
                gas_delta_usd=Decimal(str(gas_delta_usd)),
                token0_price=Decimal(str(token0_price)),
                token1_price=Decimal(str(token1_price)),
            )
        )

    def reset(self) -> None:
        """Clear all recorded snapshots."""
        self._snapshots.clear()

    @property
    def snapshots(self) -> list[LPSnapshot]:
        """Read-only access to recorded snapshots."""
        return list(self._snapshots)

    def summary(self) -> LPPerformanceReport:
        """Compute the LP performance report from recorded snapshots.

        Raises:
            ValueError: If fewer than 2 snapshots have been recorded.
            ValueError: If the initial position value is not positive.
            ValueError: If required token prices are zero for the selected benchmark.
        """
        if len(self._snapshots) < 2:
            raise ValueError("At least 2 snapshots are required to compute a summary")

        first = self._snapshots[0]
        last = self._snapshots[-1]

        initial_value = first.position_value_usd
        if initial_value <= 0:
            raise ValueError("Initial position value must be positive")

        # Validate prices required by the benchmark mode
        self._validate_benchmark_prices(first, last)

        # --- Core PnL ---
        total_fees = sum((s.fees_delta_usd for s in self._snapshots), _ZERO)
        total_gas = sum((s.gas_delta_usd for s in self._snapshots), _ZERO)

        # Position value change (last value vs first, excluding fees/gas already counted)
        value_delta = last.position_value_usd - first.position_value_usd
        total_pnl = value_delta + total_fees
        net_pnl = total_pnl - total_gas

        # --- Impermanent Loss (always vs true HODL: initial tokens at current prices) ---
        # IL is independent of the selected benchmark mode so the headline
        # metric always reflects the real cost of providing liquidity.
        true_hodl = first.token0_amount * last.token0_price + first.token1_amount * last.token1_price
        il_usd = true_hodl - last.position_value_usd
        il_pct = il_usd / initial_value * Decimal("100")

        # --- Benchmark comparison ---
        hodl_value = self._compute_hodl_value(first, last)
        vs_hodl_usd = net_pnl - (hodl_value - initial_value)
        vs_hodl_pct = vs_hodl_usd / initial_value * Decimal("100")

        # --- Duration ---
        duration_secs = (last.timestamp - first.timestamp).total_seconds()
        duration_hours = Decimal(str(duration_secs)) / Decimal("3600")

        # --- Sharpe ratio ---
        sharpe = self._compute_sharpe()

        return LPPerformanceReport(
            total_pnl_usd=total_pnl,
            net_pnl_usd=net_pnl,
            il_usd=il_usd,
            il_pct=il_pct,
            fees_earned_usd=total_fees,
            gas_spent_usd=total_gas,
            benchmark=self.benchmark,
            hodl_value_usd=hodl_value,
            vs_hodl_usd=vs_hodl_usd,
            vs_hodl_pct=vs_hodl_pct,
            duration_hours=duration_hours,
            num_snapshots=len(self._snapshots),
            sharpe_ratio=sharpe,
        )

    def _validate_benchmark_prices(self, first: LPSnapshot, last: LPSnapshot) -> None:
        """Raise ValueError if required token prices are zero for the benchmark mode.

        IL is always computed against true HODL (initial tokens at current prices),
        so the last snapshot must always have positive prices for both tokens.
        Additional prices may be required depending on the benchmark mode.
        """
        # IL always needs last-snapshot prices for both tokens
        if last.token0_price <= 0 or last.token1_price <= 0:
            raise ValueError(
                f"Positive token prices required in the last snapshot for IL calculation "
                f"(token0_price={last.token0_price}, token1_price={last.token1_price})"
            )
        # Benchmark-specific additional requirements
        if self.benchmark == "token0":
            if first.token0_price <= 0:
                raise ValueError(
                    f"token0 benchmark requires positive token0_price in the first snapshot (got {first.token0_price})"
                )
        elif self.benchmark == "token1":
            if first.token1_price <= 0:
                raise ValueError(
                    f"token1 benchmark requires positive token1_price in the first snapshot (got {first.token1_price})"
                )

    def _compute_hodl_value(self, first: LPSnapshot, last: LPSnapshot) -> Decimal:
        """Compute the HODL benchmark value based on the configured benchmark type."""
        if self.benchmark == "hodl":
            # Hold initial token amounts at current prices
            return first.token0_amount * last.token0_price + first.token1_amount * last.token1_price
        elif self.benchmark == "usd":
            # Just hold USD (no change from initial)
            return first.position_value_usd
        elif self.benchmark == "token0":
            # Convert everything to token0 at start, value at current price
            total_token0 = first.position_value_usd / first.token0_price
            return total_token0 * last.token0_price
        elif self.benchmark == "token1":
            # Convert everything to token1 at start, value at current price
            total_token1 = first.position_value_usd / first.token1_price
            return total_token1 * last.token1_price
        raise ValueError(f"Unknown benchmark type: {self.benchmark}")

    def _compute_sharpe(self) -> float | None:
        """Compute annualized Sharpe ratio from snapshot-to-snapshot returns.

        Returns include fees earned and gas spent for each period, so the Sharpe
        reflects the full LP economics (not just position value changes).

        Annualization factor is derived from actual timestamp intervals, not
        assumed to be daily.

        Returns None if there are fewer than 3 snapshots or zero std dev.
        """
        if len(self._snapshots) < 3:
            return None

        returns: list[float] = []
        # Track cumulative fees/gas so each period's return is measured off
        # the correct economic capital base (position value + accrued net fees).
        cumulative_net_fees = 0.0
        for i in range(1, len(self._snapshots)):
            prev = self._snapshots[i - 1]
            curr = self._snapshots[i]
            prev_total = float(prev.position_value_usd) + cumulative_net_fees
            period_net_fee = float(curr.fees_delta_usd - curr.gas_delta_usd)
            curr_total = float(curr.position_value_usd) + cumulative_net_fees + period_net_fee
            if prev_total > 0:
                returns.append((curr_total - prev_total) / prev_total)
            cumulative_net_fees += period_net_fee

        if len(returns) < 2:
            return None

        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / (len(returns) - 1)
        std_ret = math.sqrt(variance)

        if std_ret == 0:
            return None

        # Annualize based on actual snapshot frequency
        total_secs = (self._snapshots[-1].timestamp - self._snapshots[0].timestamp).total_seconds()
        num_periods = len(self._snapshots) - 1
        avg_period_secs = total_secs / num_periods if num_periods > 0 else 0
        if avg_period_secs <= 0:
            return None
        periods_per_year = (365.25 * 24 * 3600) / avg_period_secs
        annualization_factor = math.sqrt(periods_per_year)
        return round((mean_ret / std_ret) * annualization_factor, 4)
