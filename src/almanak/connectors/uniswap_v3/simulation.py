"""Router-specific swap returns and shared V3 position-manager result validation."""

from almanak.connectors import v3_pool_simulation as family
from almanak.connectors.v3_pool_simulation import decoded as decoded
from almanak.connectors.v3_pool_simulation import multicall as multicall
from almanak.connectors.v3_pool_simulation import validate_addition as validate_addition
from almanak.connectors.v3_pool_simulation import validate_removal as validate_removal


def swap_result(raw):
    return decoded(multicall(raw, 1)[0], ("uint256",))


def validate_simulation(operation, result, before):
    return family.validate_simulation(operation, result, before, swap_result)
