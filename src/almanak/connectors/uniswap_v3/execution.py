"""Concrete V3 protocol readers and codecs use the shared gateway execution path."""

from almanak.connectors.uniswap_v3 import abi
from almanak.connectors.uniswap_v3.deployments import verify_periphery, verify_pool
from almanak.connectors.uniswap_v3.observations import snapshot
from almanak.connectors.uniswap_v3.planning import operation
from almanak.connectors.uniswap_v3.reconciliation import reconcile
from almanak.connectors.uniswap_v3.simulation import validate_simulation
from almanak.connectors.uniswap_v3.validation import (
    increases_exposure,
    validate_current,
    validate_operation,
)
from almanak.connectors.v3_pool_execution import Execution
from almanak.connectors.v3_pool_execution import preflight_position as preflight_position
from almanak.connectors.v3_pool_execution import (
    validate_preflight_position as validate_preflight_position,
)


class UniswapExecution(Execution):
    snapshot = staticmethod(snapshot)
    operation = staticmethod(operation)
    validate_operation = staticmethod(validate_operation)
    validate_current = staticmethod(validate_current)
    validate_simulation = staticmethod(validate_simulation)
    reconcile = staticmethod(reconcile)
    increases_exposure = staticmethod(increases_exposure)
    encode_call = staticmethod(abi.call)
    verify_pool = staticmethod(verify_pool)
    verify_periphery = staticmethod(verify_periphery)
