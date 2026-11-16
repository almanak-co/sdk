"""Product sampling policies without protocol-specific rate or block fields."""

from pydantic import model_validator

from almanak.backtest.dataset import DataKey
from almanak.models import Model
from almanak.triggers import PositiveSeconds


class Sampling(Model):
    sample_seconds: PositiveSeconds
    max_age_seconds: PositiveSeconds

    @model_validator(mode="after")
    def coverage(self):
        if self.max_age_seconds < self.sample_seconds:
            raise ValueError("maximum age must cover the sampling interval")
        return self


class SamplingOverride(Sampling):
    key: DataKey
