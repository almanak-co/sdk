"""Strategy helpers express HyperCore actions from retained observations.

One surface serves fork runs and backtests, as GMX's does: the exported types are the
fork run's (`state.py`), and a backtest's (`economic_state.py`) carry the same names a
strategy reads. A decision sees each allocation's `effective_at`, `withdrawable` and
`owned` account (`activated`, `leverage`, `holding` with its `szi` and `entry_px`) in
both modes. An outcome is the intent's `ActionResult`, or in a fork run an
`EffectsResult` for fills, funding and liquidations the venue reports, which a backtest
shows only in the next observation.
"""

from decimal import Decimal

from almanak.connectors.hyperliquid.models import (
    ClosePosition,
    Deposit,
    IncreasePosition,
    SetLeverage,
    Withdraw,
)
from almanak.connectors.hyperliquid.positions import text
from almanak.connectors.hyperliquid.state import (
    ActionResult,
    Decision,
    DecisionInput,
    EffectsResult,
    OutcomeInput,
)
from almanak.models import AssetAmount

__all__ = [
    "ActionResult",
    "Actions",
    "Decision",
    "DecisionInput",
    "EffectsResult",
    "OutcomeInput",
]


class Actions:
    def __init__(self, context):
        self.context = context

    def deposit(self, name, amount: AssetAmount | None = None):
        """Move `amount` USDC (the allocation's capital by default) to the HyperCore account.

        The account's first deposit pays HyperCore's 1 USDC activation fee.
        """
        observed = self.context.require_observation(name)
        value = self.context.config.allocation(name).capital if amount is None else amount
        if value.asset != observed.target.margin_asset:
            raise ValueError("a HyperCore deposit is USDC on eip155:999")
        return Deposit(target=observed.target, amount=value.amount)

    def set_leverage(self, name):
        """Set the allocation's configured cross leverage; only while flat."""
        observed = self.context.require_observation(name)
        return SetLeverage(
            target=observed.target, leverage=self.context.config.allocation(name).leverage
        )

    def open(self, name, *, side, size):
        """An `Ioc` order for `size` coins on `side` (`long` or `short`), its limit the
        allocation's slippage from the reference price: the book on a fork, the latest
        trade close in a backtest."""
        observed = self.context.require_observation(name)
        return IncreasePosition(target=observed.target, side=side, size=text(Decimal(str(size))))

    def close(self, name):
        """A reduce-only `Ioc` order for the whole position."""
        observed = self.context.require_observation(name)
        return ClosePosition(target=observed.target)

    def withdraw(self, name, amount: AssetAmount | None = None):
        """Move `amount` USDC (everything withdrawable by default) back to HyperEVM."""
        observed = self.context.require_observation(name)
        if amount is not None and amount.asset != observed.target.margin_asset:
            raise ValueError("a HyperCore withdrawal is USDC on eip155:999")
        return Withdraw(target=observed.target, amount=None if amount is None else amount.amount)
