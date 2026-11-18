"""Concrete checkpoint types retain shared V3 ownership and teardown rules."""

from almanak.connectors.aerodrome_slipstream.observations import initial_owned
from almanak.connectors.aerodrome_slipstream.state import Checkpoint, Work
from almanak.connectors.v3_pool_lifecycle import Lifecycle
from almanak.connectors.v3_pool_lifecycle import commit_owned as commit_owned
from almanak.connectors.v3_pool_lifecycle import has_obligations as has_obligations
from almanak.connectors.v3_pool_lifecycle import increases_exposure as increases_exposure
from almanak.connectors.v3_pool_lifecycle import validate_closure as validate_closure
from almanak.connectors.v3_pool_lifecycle import validate_initial as validate_initial
from almanak.connectors.v3_pool_lifecycle import validate_observed as validate_observed
from almanak.connectors.v3_pool_lifecycle import (
    validate_protocol_closure as validate_protocol_closure,
)


class SlipstreamLifecycle(Lifecycle):
    checkpoint_type = Checkpoint
    work_type = Work
    initial_owned = staticmethod(initial_owned)
