"""Concrete V3 position ownership and measured quote sources."""

from almanak.connectors import v3_pool_positions as family
from almanak.connectors.aerodrome_slipstream.models import Target
from almanak.connectors.v3_pool_positions import Allowance as Allowance
from almanak.connectors.v3_pool_positions import Position as Position
from almanak.connectors.v3_pool_positions import SimulatedPosition as SimulatedPosition


class Ownership(family.Ownership[Target]):
    pass


class Quote(family.Quote[Target]):
    pass
