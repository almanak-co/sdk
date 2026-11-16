"""Modeled transfers retain physical market balances and independent claim obligations."""

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.models import Block, Model, UInt


class TokenSolvency(Model):
    asset: AssetType
    balance: UInt
    minimum_balance: UInt
    collateral_amount: UInt
    claimable_funding_amount: UInt


class MarketSolvency(Model):
    market: Market
    block: Block
    long: TokenSolvency
    short: TokenSolvency

    @model_validator(mode="after")
    def scope(self):
        if (
            self.market.chain != self.block.chain
            or self.long.asset != self.market.long_asset
            or self.short.asset != self.market.short_asset
        ):
            raise ValueError("GMX solvency requires its canonical market and exact token units")
        return self


def validate_token_transfer(token, delta):
    balance = token.balance + delta
    # Pool, impact and fee accounting move by the same net transfer. Funding and
    # position collateral remain independent lower bounds on physical inventory.
    required = max(
        token.minimum_balance + delta, token.collateral_amount, token.claimable_funding_amount
    )
    if balance < required:
        raise SwapConstraint("GMX modeled swap leaves insufficient physical market token balance")


def validate_swap_solvency(spot, solvency, calculation):
    if solvency.market != spot.market or solvency.block != spot.block:
        raise ValueError("GMX solvency differs from its exact swap state")
    for token in (solvency.long, solvency.short):
        delta = (
            calculation.amount_in
            if token.asset == calculation.asset_in
            else -calculation.amount_out
        )
        validate_token_transfer(token, delta)
