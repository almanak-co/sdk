"""Prepared records retain explicit invariant, reserves, supply, fee indices and code evidence."""

from pydantic import model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, InputRecord
from almanak.connectors.aerodrome.domain import PoolObservation
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Block, Model

UNIT = "aerodrome_integer_pool"
MODEL = "aerodrome-economic-integer-fixed-indices-v1"
REVISION = "aerodrome-pool-samples-v1"
CONNECTOR = ConnectorId("aerodrome")


class PoolSample(Model):
    pool: PoolObservation
    contracts: tuple[ContractIdentity, ...]

    @model_validator(mode="after")
    def supported(self):
        if min(self.pool.reserve0, self.pool.reserve1, self.pool.total_supply) <= 0:
            raise ValueError("prepared Aerodrome models require an existing funded pool")
        if (self.pool.balance0, self.pool.balance1) != (
            self.pool.reserve0,
            self.pool.reserve1,
        ) or self.pool.deposited_shares:
            raise ValueError(
                "prepared Aerodrome models require synchronized balances and no deposited LP shares"
            )
        if len({c.address for c in self.contracts}) != len(self.contracts):
            raise ValueError("prepared contract identities must be unique")
        return self


Record = InputRecord[PoolSample, Block]


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
