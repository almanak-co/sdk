"""Exchange author actions consume retained observations through concrete intent constructors."""

from almanak.connectors.curve import authoring
from almanak.connectors.curve.models import Target
from almanak.connectors.curve.state import Decision, DecisionInput, OutcomeInput, exit_complete
from almanak.dex_intents import ExitExchange, RevokeExchange

__all__ = ["Actions", "Decision", "DecisionInput", "OutcomeInput", "Target"]


class Actions:
    def __init__(self, context):
        self.context = context

    def swap(self, name, *, exit=False):
        """Swap along `entry_quote`, or the first of `exit_quotes` with `exit=True`.

        The minimum output is the quote less `slippage_bps`; raises when that quote is absent.
        """
        observed = self.context.require_observation(name)
        return authoring.swap(
            self.context.config.allocation(name),
            observed,
            (observed.exit_quotes[0] if observed.exit_quotes else None)
            if exit
            else observed.entry_quote,
        )

    def open_liquidity(self, name, *, fraction_bps=10000):
        """Deposit up to `fraction_bps` of owned inventory of every pool coin for LP shares.

        Refuses when shares are already owned; use `increase_liquidity` then.
        """
        observed = self.context.require_observation(name)
        return authoring.open_liquidity(
            self.context.config.allocation(name), observed, fraction_bps=fraction_bps
        )

    def increase_liquidity(self, name, *, fraction_bps=10000):
        """Deposit up to `fraction_bps` of owned inventory into the owned LP shares."""
        observed = self.context.require_observation(name)
        return authoring.increase_liquidity(
            self.context.config.allocation(name), observed, fraction_bps=fraction_bps
        )

    def decrease_liquidity(self, name, liquidity):
        """Burn `liquidity` LP shares, keeping some owned, with slippage-bounded minimums.

        Withdraws in `single_coin_exit` when configured, otherwise proportionally.
        """
        observed = self.context.require_observation(name)
        return authoring.decrease_liquidity(
            self.context.config.allocation(name), observed, liquidity
        )

    def close_liquidity(self, name):
        """Burn all owned LP shares with slippage-bounded minimums.

        Withdraws in `single_coin_exit` when configured, otherwise proportionally.
        """
        observed = self.context.require_observation(name)
        return authoring.close_liquidity(self.context.config.allocation(name), observed)

    def revoke(self, name):
        """Zero the allocation's remaining token allowances (spending authority)."""
        observed = self.context.require_observation(name)
        return RevokeExchange[Target](target=observed.target)

    def exit(self, name):
        """Full exit/teardown: burn owned LP shares, swap non-funding coins to funding, revoke.

        Exit converts residual inventory to the configured funding asset and clears remaining
        allowances in one runtime-driven sequence; returns None when nothing remains to exit.
        Measured dust whose exact-size exit quote returns zero output may stay owned after it.
        """
        observed = self.context.require_observation(name)
        return None if exit_complete(observed) else ExitExchange[Target](target=observed.target)
