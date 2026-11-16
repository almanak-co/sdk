"""Exchange author actions consume retained observations through concrete intent constructors."""

from collections.abc import Callable
from types import ModuleType
from typing import ClassVar

from almanak.connectors.dex_residual import counter_settled


class Actions:
    authoring: ClassVar[ModuleType]
    collect_intent: ClassVar[Callable]
    revoke_intent: ClassVar[Callable]
    exit_intent: ClassVar[Callable]

    def __init__(self, context):
        self.context = context

    def swap(self, name, *, exit=False, rebalance=False):
        """Swap along an observed exact-size quote, with the minimum output less `slippage_bps`.

        Follows `entry_quote` by default, `exit_quote` with `exit=True` and the banded
        `rebalance_quote` with `rebalance=True`; raises when that quote is absent at this block.
        """
        if exit and rebalance:
            raise ValueError("a swap follows either the exit quote or the rebalance quote")
        observed = self.context.require_observation(name)
        if rebalance:
            quote = getattr(observed, "rebalance_quote", None)
        else:
            quote = observed.exit_quote if exit else observed.entry_quote
        return self.authoring.swap(self.context.config.allocation(name), observed, quote)

    def open_liquidity(self, name, *, fraction_bps=10000):
        """Open a position from up to `fraction_bps` of owned inventory in both assets.

        Concentrated pools: a banded allocation centres the range on the observed tick, a fixed
        one uses its range. Classic Aerodrome stable/volatile pools have no range.
        """
        observed = self.context.require_observation(name)
        return self.authoring.open_liquidity(
            self.context.config.allocation(name), observed, fraction_bps=fraction_bps
        )

    def increase_liquidity(self, name, *, fraction_bps=10000):
        """Add up to `fraction_bps` of owned inventory to the owned position."""
        observed = self.context.require_observation(name)
        return self.authoring.increase_liquidity(
            self.context.config.allocation(name), observed, fraction_bps=fraction_bps
        )

    def decrease_liquidity(self, name, liquidity):
        """Remove `liquidity` units from the owned position, keeping it open.

        Concentrated pools also collect fees; classic Aerodrome burns shares for principal only,
        so claim its fees with `collect`. Minimums are observed principal less `slippage_bps`.
        """
        observed = self.context.require_observation(name)
        return self.authoring.decrease_liquidity(
            self.context.config.allocation(name), observed, liquidity
        )

    def close_liquidity(self, name):
        """Remove all owned liquidity into owned inventory, with principal less `slippage_bps`.

        Concentrated pools also collect fees; classic Aerodrome burns shares for principal only,
        so claim its fees with `collect` (or `close_liquidity` again once no shares remain).
        """
        observed = self.context.require_observation(name)
        return self.authoring.close_liquidity(self.context.config.allocation(name), observed)

    def collect(self, name):
        """Collect accrued fees from the owned position without changing its liquidity."""
        observed = self.context.require_observation(name)
        allocation = self.context.config.allocation(name)
        return self.collect_intent(
            target=observed.target,
            deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
        )

    def revoke(self, name):
        """Zero the allocation's remaining token allowances (spending authority)."""
        observed = self.context.require_observation(name)
        return self.revoke_intent(target=observed.target)

    def exit(self, name):
        """Full exit/teardown: close owned liquidity, swap counter inventory to funding, revoke.

        Exit converts residual inventory to the configured funding asset and clears remaining
        allowances in one runtime-driven sequence; returns None when nothing remains to exit.
        Measured dust whose exact-size exit quote returns zero output may stay owned after it.
        """
        observed = self.context.require_observation(name)
        owned = observed.owned
        # Counter inventory measured here as residual dust has no exit swap to perform.
        obligations = (
            owned.position is not None
            or not counter_settled(owned, observed.exit_residual)
            or observed.needs_cleanup
        )
        return self.exit_intent(target=observed.target) if obligations else None
