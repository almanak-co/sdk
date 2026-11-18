"""Explicit origin anchor selections shared by read contexts and operator fixtures."""

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.chains import ChainReference, CheckpointReference, validate_reference
from almanak.models import Chain, Model, UInt


class AnchorSelection(Model):
    chain: ChainId
    height: UInt | None = None
    reference: ChainReference | None = None
    at: UInt | None = Field(default=None, exclude_if=lambda value: value is None)
    fresh: bool = False

    @model_validator(mode="after")
    def choice(self):
        fixed = self.height is not None or self.reference is not None
        if (fixed, self.at is not None, self.fresh).count(True) != 1:
            raise ValueError(
                "select a fixed anchor, the last block at a time, "
                "or explicitly request a fresh anchor"
            )
        if self.reference is not None:
            validate_reference(Chain(chain_id=self.chain, environment="mainnet"), self.reference)
        return self

    def require(self, checkpoint: CheckpointReference):
        if checkpoint.chain != Chain(chain_id=self.chain, environment="mainnet"):
            raise ValueError("resolution anchor belongs to another chain or environment")
        if self.height is not None and self.height != checkpoint.height:
            raise ValueError("resolution anchor differs from the selected height")
        if self.reference is not None and self.reference != checkpoint.reference:
            raise ValueError("resolution anchor differs from the selected reference")
        if self.at is not None and checkpoint.timestamp > self.at:
            raise ValueError("resolution anchor is later than the selected time")
