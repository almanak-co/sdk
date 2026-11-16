"""Concrete V3 operation constructors bind shared economic planning."""

from almanak.connectors.pancakeswap_v3 import operations
from almanak.connectors.pancakeswap_v3.authority import admit
from almanak.connectors.v3_pool_planning import Planner
from almanak.connectors.v3_pool_planning import sqrt_bounds as sqrt_bounds

_planner = Planner(operations=operations, admit=admit)
operation = _planner.operation
