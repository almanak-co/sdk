"""Concrete connector binds the shared durable exchange economic driver."""

from almanak.connectors import dex_economic_model as family
from almanak.connectors.aerodrome import (
    authoring,
    economic_data,
    economic_parameters,
    economic_simulation,
    economic_state,
)
from almanak.connectors.aerodrome.authority import admit, validate_capabilities
from almanak.connectors.aerodrome.economic_validation import validate
from almanak.connectors.aerodrome.models import Target
from almanak.connectors.aerodrome.observations import quotes_for
from almanak.connectors.aerodrome.positions import Allowance
from almanak.connectors.aerodrome.state import Decision, exit_complete
from almanak.connectors.dex_economic_model import remaining as remaining
from almanak.dex_intents import RevokeExchange
from almanak.effects import Effects


def allowances(target):
    return tuple(
        Allowance(asset=asset, spender=target.contracts.router, amount=0)
        for asset in (*target.assets, target.pool.shares_asset)
    )


class EconomicModel(family.EconomicModel):
    admit = staticmethod(admit)
    validate = staticmethod(validate)
    quotes_for = staticmethod(quotes_for)
    exit_complete = staticmethod(exit_complete)
    allowances = staticmethod(allowances)
    delta_field = "shares_delta"
    data = economic_data
    state_types = economic_state
    parameters = economic_parameters
    simulation = economic_simulation
    authoring = authoring
    decision_type = Decision
    revoke_type = RevokeExchange[Target]
    effects_type = Effects[economic_state.EconomicPosition]
    validate_capabilities = staticmethod(validate_capabilities)

    def snapshot_quote(self, record, payload, time):
        return lambda amount: self.simulation.measured_quote_at(
            record, payload.target, amount, time
        )

    def snapshot(self, state, payload, time, *, quotes=True, entry=True):
        from almanak.configuration.metadata import verify

        observed = super().snapshot(state, payload, time, quotes=quotes, entry=entry)
        verify(self.config, observed.pool.shares)
        return observed
