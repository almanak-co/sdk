"""Bind hypothetical inventory to concrete connector parameter and state types."""

from almanak.connectors.aerodrome.economic_data import CONNECTOR
from almanak.connectors.aerodrome.economic_state import EconomicPosition, State
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.observations import initial_owned
from almanak.connectors.dex_economic_parameters import ParametersBinding
from almanak.connectors.dex_economic_parameters import payer_wallet as payer_wallet
from almanak.connectors.dex_economic_parameters import wallet as wallet
from almanak.effects import Effects, Portfolio, Position, PositionChange

_binding = ParametersBinding(
    connector=CONNECTOR,
    parameters_type=Parameters,
    position_type=EconomicPosition,
    state_type=State,
    initial_owned=initial_owned,
    position_record_type=Position[EconomicPosition],
    portfolio_type=Portfolio[EconomicPosition],
    effects_type=Effects[EconomicPosition],
    change_type=PositionChange[EconomicPosition],
)
parameters = _binding.parameters
position_key = _binding.position_key
initial = _binding.initial
transition_effects = _binding.transition_effects
