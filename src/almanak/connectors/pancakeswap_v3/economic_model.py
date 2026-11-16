"""Concrete connector binds the shared durable V3 economic driver."""

from almanak.connectors import v3_economic_model as family
from almanak.connectors.pancakeswap_v3 import (
    authoring,
    economic_data,
    economic_parameters,
    economic_simulation,
    economic_state,
)
from almanak.connectors.pancakeswap_v3.authority import validate_capabilities
from almanak.connectors.pancakeswap_v3.models import Target
from almanak.connectors.pancakeswap_v3.state import Decision
from almanak.connectors.v3_economic_model import remaining as remaining
from almanak.dex_intents import RevokeExchange
from almanak.effects import Effects


class EconomicModel(family.EconomicModel):
    data = economic_data
    state_types = economic_state
    parameters = economic_parameters
    simulation = economic_simulation
    authoring = authoring
    decision_type = Decision
    revoke_type = RevokeExchange[Target]
    effects_type = Effects[economic_state.EconomicPosition]
    validate_capabilities = staticmethod(validate_capabilities)
