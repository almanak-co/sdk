"""Frozen model, implementation, settings and dataset identity for economic runs."""

from pathlib import Path
from typing import Any, Literal

from pydantic import Field, model_validator

from almanak.backtest.dataset import Digest
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection


class Envelope[Config, Settings, State](Model):
    schema_version: Literal[1] = 1
    model: str
    sdk_digest: Digest
    dataset: Path
    dataset_sha256: Digest
    config: Config
    settings: Settings
    strategy: StrategyDefinition
    initial: State
    interval: IntervalSelection
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
        ):
            raise ValueError("economic manifest differs from frozen scheduling")
        return self
