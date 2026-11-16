"""Prepared pool state retains ordered units, parameter history and executable identities."""

from pydantic import model_validator

from almanak.backtest.dataset import DataKey, DataRequirement, Digest, InputRecord
from almanak.backtest.history_logs import HistoricalLog
from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.domain import CurvePool, PoolObservation
from almanak.connectors.curve.pool_report import FUNDED_POOL, funded
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Block, Model

UNIT = "curve_stableswap_integer_pool"
MODEL = "curve-stableswap-fixed-external-unit-balances-v1"
REVISION = "curve-stableswap-samples-and-parameters-v1"
CRYPTO_MODEL = "curve-ng-fixed-external-unit-balances-v1"
CRYPTO_REVISION = "curve-ng-samples-parameters-and-profit-v1"
CONNECTOR = ConnectorId("curve")


class ParameterPath(Model):
    source: CurvePool
    first: Block
    first_next: Block
    last: Block
    last_next: Block
    events: tuple[HistoricalLog, ...]
    inputs: tuple[Digest, ...]

    @model_validator(mode="after")
    def scope(self):
        if (
            any(
                block.chain != self.source.chain
                for block in (self.first, self.first_next, self.last, self.last_next)
            )
            or self.last.number < self.first.number
        ):
            raise ValueError("Curve parameter evidence requires its ordered chain scope")
        for current, following in ((self.first, self.first_next), (self.last, self.last_next)):
            if following.number != current.number + 1 or following.timestamp <= current.timestamp:
                raise ValueError("Curve parameter path lacks a canonical time-floor boundary")
        orders = tuple(event.order for event in self.events)
        if orders != tuple(sorted(set(orders))):
            raise ValueError("Curve parameter events require unique canonical ordering")
        for event in self.events:
            if (
                event.address
                not in (
                    (self.source.pool,)
                    if self.source.family == "stableswap_ng"
                    else (self.source.pool, self.source.factory)
                )
                or event.block.chain != self.source.chain
                or not self.first.number < event.block.number <= self.last.number
            ):
                raise ValueError("Curve parameter event lies outside its pool or time window")
        return self


class PoolSample(Model):
    pool: PoolObservation | CryptoObservation | TriCryptoObservation
    contracts: tuple[ContractIdentity, ...]
    parameter_path: Digest
    successor: Block

    @model_validator(mode="after")
    def supported(self):
        if not funded(self.pool.balances, self.pool.total_supply):
            raise ValueError(f"prepared {FUNDED_POOL}")
        if len({contract.address for contract in self.contracts}) != len(self.contracts):
            raise ValueError("prepared Curve executable identities must be unique")
        if (
            self.successor.chain != self.pool.block.chain
            or self.successor.number != self.pool.block.number + 1
            or self.successor.timestamp <= self.pool.block.timestamp
        ):
            raise ValueError("prepared Curve sample requires its canonical time-floor successor")
        return self


Record = InputRecord[PoolSample, Block]


def model_identity(config):
    if all(allocation.pool.family == "stableswap_ng" for allocation in config.allocations):
        return MODEL, REVISION
    return CRYPTO_MODEL, CRYPTO_REVISION


def unit(source):
    return (
        UNIT
        if source.family == "stableswap_ng"
        else (
            "curve_twocrypto_integer_pool"
            if source.family == "twocrypto_ng"
            else "curve_tricrypto_integer_pool"
        )
    )


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
        unit=unit(target.pool),
        visibility="end_of_block",
        payload_revision=REVISION if target.pool.family == "stableswap_ng" else CRYPTO_REVISION,
    )
