"""Internal-only construction/execution seams for persistent-fork unit tests.

Production ``PaperTrader`` construction and execution must always reject
persistent forks until fork-bound gateway oracle support exists. A small set of
unit tests still exercise isolated retained implementation details (time
advance, position reconciliation, and loop ordering). Those tests use these
explicit seams instead of weakening either production boundary.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig
from almanak.framework.backtesting.paper.engine import PaperTrader


def construct_unsupported_persistent_trader_for_test(
    *,
    config: PaperTraderConfig,
    **kwargs: Any,
) -> PaperTrader:
    """Construct a persistent trader only for isolated internal unit coverage."""
    assert config.fork_lifecycle == ForkLifecycle.PERSISTENT
    assert config.reset_fork_every_tick is False

    with patch("almanak.framework.backtesting.paper.engine.validate_fork_lifecycle"):
        return PaperTrader(config=config, **kwargs)


async def run_unsupported_persistent_trader_for_test(
    trader: PaperTrader,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run retained persistent behavior only within an explicit test seam."""
    assert trader.config.fork_lifecycle == ForkLifecycle.PERSISTENT
    assert trader.config.reset_fork_every_tick is False

    with patch("almanak.framework.backtesting.paper.engine.validate_fork_lifecycle"):
        return await trader.run(*args, **kwargs)
