"""Public exchange actions retain the selected allocation and observation evidence."""

from almanak.author_capabilities import declared
from almanak.connectors.dex_residual import refuse_unmeasured_entry
from almanak.connectors.uniswap_v4.authoring import close_liquidity, open_liquidity, swap
from almanak.connectors.uniswap_v4.models import Target
from almanak.connectors.uniswap_v4.state import Decision, DecisionInput, OutcomeInput

__all__ = [
    "Actions",
    "Decision",
    "DecisionInput",
    "OutcomeInput",
    "Target",
    "close_liquidity",
    "declared",
    "open_liquidity",
    "swap",
]


class Actions:
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
        allocation = self.context.config.allocation(name)
        if rebalance:
            quote = observed.rebalance_quote
        else:
            quote = observed.exit_quote if exit else observed.entry_quote
            if not exit:
                refuse_unmeasured_entry(
                    observed, "swap requires its explicit exact-size quote at the decision block"
                )
        return swap(allocation, observed, quote)

    def open_liquidity(self, name):
        """Open a position using all owned inventory of both currencies as maximums.

        A banded allocation centres the range on the observed tick; a fixed one uses its range.
        """
        observed = self.context.require_observation(name)
        return open_liquidity(self.context.config.allocation(name), observed)

    def close_liquidity(self, name):
        """Remove all owned liquidity into owned inventory.

        Minimums are the observed principal less the allocation's `slippage_bps`.
        """
        observed = self.context.require_observation(name)
        return close_liquidity(self.context.config.allocation(name), observed)

    def exit(self, name):
        """Full exit/teardown: close owned liquidity, swap counter inventory to funding, revoke.

        Exit converts residual inventory to the configured funding asset and clears remaining
        allowances in one runtime-driven sequence; returns None when nothing remains to exit.
        Measured dust whose exact-size exit quote returns zero output may stay owned after it.
        """
        from almanak.connectors.uniswap_v4.exit import obligations
        from almanak.dex_intents import ExitExchange

        observed = self.context.require_observation(name)
        return (
            ExitExchange[Target](target=observed.target)
            if obligations(observed) == "remaining"
            else None
        )

    def collect(self, name):
        """Collect accrued fees from the owned position without changing its liquidity."""
        from almanak.dex_intents import CollectLiquidityFees

        observed = self.context.require_observation(name)
        allocation = self.context.config.allocation(name)
        return CollectLiquidityFees[Target](
            target=observed.target,
            deadline=int(observed.effective_at.timestamp()) + allocation.deadline_seconds,
        )
