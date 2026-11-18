"""Validated connector-specific historical wire types share one durable envelope."""

from typing import Any, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import Digest
from almanak.backtest.types import Capability
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection


class Manifest[Parameters, Settings, Authority, Baseline, State](Model):
    schema_version: int = 1
    sdk_digest: str
    config: Parameters
    settings: Settings
    strategy: StrategyDefinition
    capabilities: tuple[Capability, ...]
    authority: Authority
    baselines: tuple[Baseline, ...]
    initial: State
    interval: IntervalSelection
    cadence_fidelity: Literal["configured", "reduced"]
    # Public roots freeze the resolved market facts the parameters were built from.
    resolution_facts: Digest | None = Field(default=None, exclude_if=lambda v: v is None)
    # Unlike reusable market facts, this binds the exact invocation/operator/account result.
    resolution_identity: Digest | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def cadence(self):
        value: Any = self
        if (
            value.config.decision_trigger != self.interval.effective
            or value.initial.interval != self.interval
            or value.initial.schedule != value.settings.schedule
            or self.cadence_fidelity != self.interval.report()["cadence_fidelity"]
        ):
            raise ValueError("manifest cadence differs from the effective run configuration")
        return self
