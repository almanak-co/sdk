"""Shared mock strategy used by CLI backtest / sweep / optimize commands.

Issue #1701: three near-duplicate copies lived inline in
``almanak/framework/cli/backtest/sweep.py`` (`MockSweepStrategy`,
`MockOptimizeStrategy`, `MockWorkerStrategy`). They differed only in
`deployment_id` strings and had identical no-op `decide()` implementations.

Consolidating them into a single ``MockBacktestStrategy`` with a
configurable ``deployment_id`` keeps the fallback behaviour identical
while eliminating the drift risk inherent in three copies: any future
protocol addition (new ABC method, new decide signature) needs to land
in exactly one place.

This mock is ONLY used when the strategy registry is empty — it is a
demo / tutorial affordance, never a production path. Real strategies
must pass ``validate_strategy_is_registered`` and resolve via
``get_strategy``.
"""

from __future__ import annotations

from typing import Any


class MockBacktestStrategy:
    """Minimal no-op strategy used as a fallback in sweep / optimize flows.

    The class exposes the small surface that the PnL backtester needs:
    - ``deployment_id`` attribute (configurable per-instance via
      ``__init__`` so a worker subprocess, a sweep run, and an optimize
      run can distinguish their outputs),
    - a ``decide(market)`` method that always returns ``None`` (i.e.
      the strategy never produces an intent — the resulting equity
      curve is a flat line at the initial capital).

    Consolidates the three inline classes previously in sweep.py:
    ``MockSweepStrategy``, ``MockOptimizeStrategy``, ``MockWorkerStrategy``.
    All three were behaviourally identical; the only delta was the
    ``deployment_id`` string ("mock-sweep" / "mock-optimize" /
    "mock-worker"). Callers now pass ``deployment_id`` explicitly to
    preserve those exact ids in observable output.
    """

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        *,
        deployment_id: str = "mock-backtest",
    ) -> None:
        self.config: dict[str, Any] = config if config is not None else {}
        self.deployment_id: str = deployment_id

    def decide(self, market: Any) -> dict[str, Any] | None:
        """No-op decision — always returns ``None`` (no intent)."""
        return None


def make_mock_strategy_class(deployment_id: str) -> type[MockBacktestStrategy]:
    """Build a ``MockBacktestStrategy`` subclass bound to ``deployment_id``.

    Sweep / optimize / worker code paths instantiate the strategy via
    ``strategy_class(config)`` — they do not control construction kwargs
    once the class leaves the CLI layer. Returning a subclass with a
    per-id default keeps that contract while pinning the identifier.
    """

    class _Bound(MockBacktestStrategy):
        def __init__(self, config: dict[str, Any] | None = None) -> None:
            super().__init__(config, deployment_id=deployment_id)

    # Class-level attribute so callers can inspect the bound id before
    # instantiating the class (e.g. for logging / registry lookups).
    # Instance-level ``self.deployment_id`` is still set in __init__.
    _Bound.deployment_id = deployment_id
    _Bound.__name__ = f"MockBacktestStrategy_{deployment_id.replace('-', '_')}"
    _Bound.__qualname__ = _Bound.__name__
    return _Bound


__all__ = [
    "MockBacktestStrategy",
    "make_mock_strategy_class",
]
