"""Economic bridge requests between two named fund accounts on different chains."""

from abc import ABC, abstractmethod
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.models import Account, AssetAmount, Model

Name = Annotated[str, Field(min_length=1, pattern=r"^\S+$")]


class BridgeTarget(Model, ABC):
    """`fund` is the source: a bridge is planned, signed and routed on the chain it leaves."""

    allocation: Name
    account: Name
    fund: Account
    destination_account: Name
    destination_fund: Account

    @property
    @abstractmethod
    def asset(self) -> AssetType:
        """Asset sent from the source fund, from the connector's complete route identity."""

    @property
    @abstractmethod
    def destination_asset(self) -> AssetType:
        """Asset the destination fund receives, from the same route identity."""

    @model_validator(mode="after")
    def route_scope(self) -> Self:
        if self.asset.chain != self.fund.chain.chain_id:
            raise ValueError("bridge target asset has a different fund chain")
        if self.destination_asset.chain != self.destination_fund.chain.chain_id:
            raise ValueError("bridge target destination asset has a different fund chain")
        if self.fund.chain.chain_id == self.destination_fund.chain.chain_id:
            raise ValueError("a bridge requires two different chains")
        if self.account == self.destination_account:
            raise ValueError("a bridge requires two different named accounts")
        return self


class BridgeIntent[Target: BridgeTarget](Model):
    kind: Literal["bridge"] = "bridge"
    target: Target
    # Sent from the source fund. The received amount is measured on arrival, never stated.
    amount: AssetAmount

    @model_validator(mode="after")
    def concrete_target(self) -> Self:
        # A generic bound or a base target class can discard connector identity.
        if type(self.target) is not type(self).model_fields["target"].annotation:
            raise ValueError("bridge intents require their exact concrete target type")
        return self

    @model_validator(mode="after")
    def quantities(self) -> Self:
        if not 0 < self.amount.amount < 2**256:
            raise ValueError("bridge requires a positive uint256 asset amount")
        if self.amount.asset != self.target.asset:
            raise ValueError("bridge amount must identify the target's sent asset")
        return self
