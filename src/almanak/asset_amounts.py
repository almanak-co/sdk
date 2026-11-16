"""Ordered economic quantities retain each asset's identity and chain scope."""

from typing import Annotated

from pydantic import ConfigDict, Field, RootModel, model_validator

from almanak.models import AssetAmount


class AssetAmounts(RootModel[Annotated[tuple[AssetAmount, ...], Field(min_length=2)]]):
    model_config = ConfigDict(frozen=True)

    @model_validator(mode="after")
    def unique_scope(self):
        assets = tuple(value.asset for value in self.root)
        if len(set(assets)) != len(assets):
            raise ValueError("ordered quantities cannot repeat an asset")
        if len({asset.chain for asset in assets}) != 1:
            raise ValueError("one pool's quantities must belong to the same CAIP chain")
        return self

    def in_order(self, assets):
        if tuple(value.asset for value in self.root) != tuple(assets):
            raise ValueError("liquidity amounts must follow the target's currency order")

    def require_inventory(self):
        if not any(value.amount for value in self.root):
            raise ValueError("liquidity addition requires positive inventory")
