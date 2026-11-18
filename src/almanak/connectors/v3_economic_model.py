"""Concentrated-pool hooks for the shared economic orchestration driver."""

from almanak.connectors.dex_economic_model import EconomicModel as ExchangeModel
from almanak.connectors.dex_economic_model import remaining as remaining
from almanak.connectors.v3_economic_validation import validate
from almanak.connectors.v3_pool_authority import admit
from almanak.connectors.v3_pool_observations import quotes_for
from almanak.connectors.v3_pool_positions import Allowance
from almanak.connectors.v3_pool_state import exit_complete


def allowances(target):
    return tuple(
        Allowance(asset=asset, spender=spender, amount=0)
        for asset in target.assets
        for spender in (target.contracts.router, target.contracts.position_manager)
    )


class EconomicModel(ExchangeModel):
    admit = staticmethod(admit)
    validate = staticmethod(validate)
    quotes_for = staticmethod(quotes_for)
    exit_complete = staticmethod(exit_complete)
    allowances = staticmethod(allowances)
    delta_field = "liquidity_delta"

    def snapshot_quotes(self, record, payload, time, *, entry=True):
        # The pool sample also prices the rebalance swap a band allocation may request.
        quote = self.snapshot_quote(record, payload, time)
        return self.quotes_for(self.config, payload.owned, quote, record.payload.pool, entry=entry)

    def simulate(self, intent, payload, record, pending):
        return self.simulation.simulate(
            self.config,
            intent,
            payload,
            record,
            pending.eligible_at,
            pending.identity,
            growth=self.band_growth,
        )

    def band_growth(self, record, target, selected):
        return self.book.band_bounds(target, record, selected)
