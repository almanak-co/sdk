"""GM liquidity amounts retain fungible share identity separately from market currencies."""

from typing import Literal

from pydantic import model_validator

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_models import Bound, ExchangeAllocation
from almanak.models import AssetAmount, Model


def validate_shares(market, *values):
    if any(value.asset != market.share_asset or not 0 < value.amount < 2**128 for value in values):
        raise ValueError("GM liquidity amounts require positive bounded configured GM-share units")


class LiquidityAllocation(ExchangeAllocation):
    kind: Literal["gmx_liquidity"] = "gmx_liquidity"
    funding_asset: AssetType
    entry_amounts: AssetAmounts
    additional_amounts: AssetAmounts
    minimum_market_tokens: AssetAmount
    partial_withdrawal: AssetAmount
    withdrawal_minimums: AssetAmounts

    @model_validator(mode="after")
    def liquidity_scope(self):
        assets = self.market.long_asset, self.market.short_asset
        if self.funding_asset not in assets:
            raise ValueError("GM funding identity must identify a configured market currency")
        for values in (self.entry_amounts, self.additional_amounts, self.withdrawal_minimums):
            values.in_order(assets)
            if not any(value.amount for value in values.root):
                raise ValueError(
                    "GM liquidity reference requires positive inputs and output bounds"
                )
        validate_shares(self.market, self.minimum_market_tokens, self.partial_withdrawal)
        return self


class LiquidityCapability(Model):
    target: ExchangeTarget
    minimum_market_tokens: AssetAmount
    minimum_outputs: AssetAmounts
    maximum_execution_fee: Bound

    @model_validator(mode="after")
    def scope(self):
        self.minimum_outputs.in_order(self.target.assets)
        validate_shares(self.target.market, self.minimum_market_tokens)
        if not self.maximum_execution_fee:
            raise ValueError("GM keeper fee requires a positive wrapped-native bound")
        return self
