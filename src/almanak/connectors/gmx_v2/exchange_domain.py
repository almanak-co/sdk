"""GMX exchange targets identify market currencies without perpetual position fields."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.domain import Contracts, Market
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    DexTarget,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.models import Address


class ExchangeContracts(Contracts):
    swap_order_executor: Address
    swap_handler: Address
    deposit_handler: Address
    deposit_vault: Address
    withdrawal_handler: Address
    withdrawal_vault: Address


class ExchangeMarket(Market):
    contracts: ExchangeContracts


class ExchangeTarget(DexTarget):
    kind: Literal["gmx_exchange"] = "gmx_exchange"
    market: ExchangeMarket
    funding_asset: AssetType

    @property
    def assets(self):
        return self.market.long_asset, self.market.short_asset

    @property
    def accounting_assets(self):
        from almanak.gateway.evm import native_asset

        return tuple(
            dict.fromkeys(
                (
                    *self.assets,
                    self.market.wrapped_native,
                    self.market.share_asset,
                    native_asset(self.fund.chain.chain_id),
                )
            )
        )

    @property
    def liquidity_scope(self):
        """Allocation names cannot split one fund's fungible GM token balance."""
        return self.fund, self.market.share_asset

    @model_validator(mode="after")
    def scope(self):
        if self.fund.chain != self.market.chain:
            raise ValueError("GMX exchange target must retain its exact fund environment")
        if self.funding_asset not in self.assets:
            raise ValueError(
                "GMX exchange funding asset must identify a configured market currency"
            )
        return self


Swap = SwapExactInput[ExchangeTarget]
Open = OpenLiquidity[ExchangeTarget]
Increase = IncreaseLiquidity[ExchangeTarget]
Decrease = DecreaseLiquidity[ExchangeTarget]
Close = CloseLiquidity[ExchangeTarget]
Revoke = RevokeExchange[ExchangeTarget]
ExchangeIntent = Annotated[
    Swap | Open | Increase | Decrease | Close | Revoke, Field(discriminator="kind")
]
