"""Bounded pool samples shared by independently verified V3 connectors."""

from pydantic import model_validator

from almanak.connectors.concentrated_pool import Tick, TickIndex, Word, validate_coverage
from almanak.connectors.v3_pool_domain import ConcentratedObservation, Uint256
from almanak.models import Model


class TickState(Tick):
    outside0: Uint256
    outside1: Uint256


class BoundedPoolSample[ObservationT: ConcentratedObservation](Model):
    pool: ObservationT
    lower: TickIndex
    upper: TickIndex
    words: tuple[Word, ...]
    ticks: tuple[TickState, ...]
    global_growth: tuple[Uint256, Uint256]

    @model_validator(mode="after")
    def coverage(self):
        validate_coverage(self.pool, self.lower, self.upper, self.words, self.ticks)
        if not self.pool.is_unlocked:
            raise ValueError("prepared V3 state must be observed outside pool execution")
        return self
