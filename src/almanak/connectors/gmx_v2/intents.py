"""Bounded economic position requests; connector planning owns approvals and keeper requests."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.exchange_domain import ExchangeIntent
from almanak.models import AssetAmount, Model, UInt

UsdE30 = Annotated[UInt, Field(lt=2**256)]


class PositionIntent(Model):
    target: Target


class IncreasePosition(PositionIntent):
    kind: Literal["increase_position"] = "increase_position"
    collateral: AssetAmount
    size_delta_usd_e30: UsdE30

    @model_validator(mode="after")
    def amounts(self):
        if self.collateral.asset != self.target.collateral_asset:
            raise ValueError("GMX increase collateral differs from its target asset")
        if not self.collateral.amount and not self.size_delta_usd_e30:
            raise ValueError("GMX increase requires collateral or notional change")
        return self


class DecreasePosition(PositionIntent):
    kind: Literal["decrease_position"] = "decrease_position"
    collateral_withdrawal: AssetAmount
    size_delta_usd_e30: UsdE30

    @model_validator(mode="after")
    def amounts(self):
        if self.collateral_withdrawal.asset != self.target.collateral_asset:
            raise ValueError("GMX withdrawal differs from its target collateral asset")
        if not self.collateral_withdrawal.amount and not self.size_delta_usd_e30:
            raise ValueError("GMX decrease requires collateral or notional change")
        return self


class ClosePosition(PositionIntent):
    kind: Literal["close_position"] = "close_position"


class RevokePosition(PositionIntent):
    kind: Literal["revoke_position"] = "revoke_position"


Intent = Annotated[
    IncreasePosition | DecreasePosition | ClosePosition | RevokePosition | ExchangeIntent,
    Field(discriminator="kind"),
]
