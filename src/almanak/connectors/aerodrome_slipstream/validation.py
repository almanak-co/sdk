"""Shared gateway invariants with the concrete connector's economic planner."""

from almanak.connectors import v3_pool_validation as family
from almanak.connectors.aerodrome_slipstream.planning import operation as planned_operation
from almanak.connectors.v3_pool_validation import deadline as deadline
from almanak.connectors.v3_pool_validation import decision_observation as decision_observation
from almanak.connectors.v3_pool_validation import increases_exposure as increases_exposure
from almanak.connectors.v3_pool_validation import price_movement as price_movement
from almanak.connectors.v3_pool_validation import removal_limits as removal_limits
from almanak.connectors.v3_pool_validation import requested as requested
from almanak.connectors.v3_pool_validation import swap_limits as swap_limits
from almanak.connectors.v3_pool_validation import validate_current as validate_current
from almanak.connectors.v3_pool_validation import validate_entry as validate_entry
from almanak.connectors.v3_pool_validation import validate_inventory as validate_inventory
from almanak.connectors.v3_pool_validation import validate_owned as validate_owned


def validate_operation(backend, state, operation, before):
    return family.validate_operation(backend, state, operation, before, planned_operation)
