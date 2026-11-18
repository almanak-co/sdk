"""Protocol-specific identities for bounded prepared pool samples."""

from almanak.backtest.dataset import DataKey, DataRequirement, Digest, InputRecord
from almanak.connectors.aerodrome_slipstream.domain import PoolObservation
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.v3_prepared_data import BoundedPoolSample
from almanak.connectors.v3_prepared_data import TickState as TickState
from almanak.models import Block


class PoolSample(BoundedPoolSample[PoolObservation]):
    pass


UNIT = "aerodrome_slipstream_integer_pool"
MODEL = "slipstream-economic-integer-fixed-growth-v1"
REVISION = "slipstream-pool-samples-v1"
CONNECTOR = ConnectorId("aerodrome_slipstream")


class PreparedSample(PoolSample):
    """Published samples bind the retained historical continuity evidence."""

    baseline_digest: Digest


Record = InputRecord[PreparedSample, Block]


def key(target):
    return DataKey(
        connector=CONNECTOR, chain=target.fund.chain, source=target.allocation, product="pool_state"
    )


def requirement(target, settings):
    return DataRequirement(
        key=key(target),
        start=settings.schedule.start,
        end=settings.schedule.deadline,
        semantics="sampled",
        sample_seconds=settings.data.sample_seconds,
        max_age_seconds=settings.data.max_age_seconds,
        unit=UNIT,
        visibility="end_of_block",
        payload_revision=REVISION,
    )
