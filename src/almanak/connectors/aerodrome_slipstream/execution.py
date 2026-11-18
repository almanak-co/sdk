"""Concrete V3 protocol readers and codecs use the shared gateway execution path."""

from almanak.connectors.aerodrome_slipstream import abi
from almanak.connectors.aerodrome_slipstream.deployments import verify_periphery, verify_pool
from almanak.connectors.aerodrome_slipstream.observations import snapshot
from almanak.connectors.aerodrome_slipstream.planning import operation
from almanak.connectors.aerodrome_slipstream.reconciliation import reconcile
from almanak.connectors.aerodrome_slipstream.simulation import validate_simulation
from almanak.connectors.aerodrome_slipstream.validation import (
    increases_exposure,
    validate_current,
    validate_operation,
)
from almanak.connectors.v3_pool_execution import Execution
from almanak.connectors.v3_pool_execution import preflight_position as preflight_position
from almanak.connectors.v3_pool_execution import (
    validate_preflight_position as validate_preflight_position,
)


class SlipstreamExecution(Execution):
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
