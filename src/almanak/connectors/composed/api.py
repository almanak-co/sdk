"""Named author actions route through validated market targets in one decision context."""

from almanak.connectors.composed.models import component_for_allocation
from almanak.connectors.composition import connector_for


class Actions:
    def __init__(self, context):
        self.context = context

    def component(self, name):
        """The `ctx.actions` of the component connector that owns allocation `name`.

        Call it for anything a wrapper here does not forward, e.g.
        `ctx.actions.component(name).open_liquidity(name)` on uniswap_v4, or GMX `increase`.
        """
        config = component_for_allocation(self.context.config, name)
        context = self.context.model_copy(update={"config": config})
        return connector_for(config).codec.actions(context)

    def supply(self, name, amount):
        """Route `supply` to the component connector that owns allocation `name`.

        Lending components: aave_v3, aave_v4, sparklend, compound_v3, morpho_blue.
        """
        return self.component(name).supply(name, amount)

    def borrow(self, name, amount):
        """Route `borrow` to the component connector that owns allocation `name`.

        Lending components: aave_v3, aave_v4, sparklend, compound_v3, morpho_blue.
        """
        return self.component(name).borrow(name, amount)

    def withdraw(self, name):
        """Route `withdraw` to the component connector that owns allocation `name`.

        Lending components: aave_v3, aave_v4, sparklend, compound_v3, morpho_blue.
        """
        return self.component(name).withdraw(name)

    def withdraw_amount(self, name, amount):
        """Route `withdraw_amount` to the component connector that owns allocation `name`.

        Lending components: aave_v3, sparklend, compound_v3, morpho_blue; aave_v4 has none.
        """
        return self.component(name).withdraw_amount(name, amount)

    def repay(self, name):
        """Route `repay` to the component connector that owns allocation `name`.

        Lending components: aave_v3, aave_v4, sparklend, compound_v3, morpho_blue.
        """
        return self.component(name).repay(name)

    def repay_amount(self, name, amount):
        """Route `repay_amount` to the component connector that owns allocation `name`.

        Lending components: aave_v3, sparklend, compound_v3, morpho_blue; aave_v4 has none.
        """
        return self.component(name).repay_amount(name, amount)

    def set_collateral(self, name, enabled):
        """Route `set_collateral` to the component connector that owns allocation `name`.

        Components: aave_v3 and sparklend.
        """
        return self.component(name).set_collateral(name, enabled)

    def bridge(self, name, amount):
        """Route `bridge` to the component connector that owns allocation `name`.

        Component: across. The amount leaves owned cash of the source account when the
        deposit is mined, and is owned cash of the destination account only once its
        arrival is measured there.
        """
        return self.component(name).bridge(name, amount)

    def swap(self, name, *, exit=False):
        """Route `swap` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream, curve and
        uniswap_v4, forwarding only `exit`; call `component(name)` for `rebalance=True` and GMX
        swaps.
        """
        return self.component(name).swap(name, exit=exit)

    def open_liquidity(self, name, *, fraction_bps=10000):
        """Route `open_liquidity` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream and curve.
        uniswap_v4 takes no `fraction_bps` and GMX takes maximums: call them through
        `component(name)`.
        """
        return self.component(name).open_liquidity(name, fraction_bps=fraction_bps)

    def increase_liquidity(self, name, *, fraction_bps=10000):
        """Route `increase_liquidity` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream and curve. GMX takes
        maximums: call it through `component(name)`; uniswap_v4 has none.
        """
        return self.component(name).increase_liquidity(name, fraction_bps=fraction_bps)

    def decrease_liquidity(self, name, liquidity):
        """Route `decrease_liquidity` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream and curve. GMX takes
        `shares` and minimums: call it through `component(name)`.
        """
        return self.component(name).decrease_liquidity(name, liquidity)

    def close_liquidity(self, name):
        """Route `close_liquidity` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream, curve and
        uniswap_v4. GMX takes minimums: call it through `component(name)`.
        """
        return self.component(name).close_liquidity(name)

    def collect(self, name):
        """Route `collect` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream and uniswap_v4.
        """
        return self.component(name).collect(name)

    def revoke(self, name):
        """Route `revoke` to the component connector that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream, curve, gmx_v2
        and across.
        """
        return self.component(name).revoke(name)

    def exit(self, name):
        """Route `exit` (full teardown) to the component that owns allocation `name`.

        Components: uniswap_v3, pancakeswap_v3, aerodrome, aerodrome_slipstream, curve, uniswap_v4
        and gmx_v2; lending components have none. Returns None when that component has nothing left
        to exit.
        """
        return self.component(name).exit(name)
