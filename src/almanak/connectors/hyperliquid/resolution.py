"""Resolve an authored HyperCore perpetual market for backtests (blueprint 30, section 12).

A market is a coin of the first perp dex. Its facts are HyperCore's, not EVM state: the
asset index, size decimals, maximum leverage and margin tiers come from `/info`
`metaAndAssetCtxs`, retained as resolution evidence beside the chain's reads. `/info` has
no as-of query, so the facts are the venue's when resolution reads them, not at the
window start; the asset index never changes once assigned. The margin asset is USDC on
HyperEVM, pinned by CoreDepositWallet's `token()` at the anchor block, the only way USDC
enters HyperCore.
"""

import hashlib
import json
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.chains import HYPEREVM
from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET, USDC
from almanak.connectors.hyperliquid.markets import find_market, perp_markets
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import erc20_address
from almanak.gateway.hypercore_info import Coin, MetaAndAssetCtxs
from almanak.gateway.hypercore_shapes import RESPONSES
from almanak.models import Asset, Block, Model

USDC_DECIMALS = 6


class MarketAssertions(Model):
    """Facts the author expects; resolution refuses a market that differs."""

    asset_index: Annotated[int, Field(strict=True, ge=0)] | None = None
    size_decimals: Annotated[int, Field(strict=True, ge=0)] | None = None
    max_leverage: Annotated[int, Field(strict=True, ge=1)] | None = None

    def verify(self, market: "PerpMarket"):
        observed = {
            "asset_index": market.asset,
            "size_decimals": market.size_decimals,
            "max_leverage": market.max_leverage,
        }
        for field, expected in self.model_dump(exclude_none=True).items():
            if expected != observed[field]:
                raise ValueError(f"HyperCore market assertion {field} differs from its meta")


class MarketSelection(Model):
    connector: Literal["hyperliquid"]
    chain: ChainId
    # The coin as `meta` names it, such as `BTC`.
    market: Coin
    assertions: MarketAssertions | None = None

    @model_validator(mode="after")
    def scope(self):
        if self.chain != HYPEREVM.chain:
            raise ValueError("a HyperCore market is a venue of eip155:999")
        return self


class PerpMarket(Model):
    """The resolved facts of one first-dex perpetual."""

    coin: Coin
    asset: Annotated[int, Field(strict=True, ge=0, lt=10_000)]
    size_decimals: Annotated[int, Field(strict=True, ge=0, le=6)]
    max_leverage: Annotated[int, Field(strict=True, ge=1)]
    # The first tier only: notional at or above its upper bound is refused (section 12).
    tier_bound: Annotated[str, Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]+)?$")] | None

    @property
    def maintenance_rate(self) -> Decimal:
        """Maintenance margin per unit of notional: half the initial margin at maximum."""
        return Decimal(1) / (2 * self.max_leverage)


class ResolvedMarket(Model):
    resolver: Literal["hyperliquid-perp-market-resolution-v1"] = (
        "hyperliquid-perp-market-resolution-v1"
    )
    selection: MarketSelection
    block: Block
    market: PerpMarket
    usdc: Asset

    @model_validator(mode="after")
    def scope(self):
        if self.selection.chain != self.block.chain.chain_id or self.usdc.chain != self.block.chain:
            raise ValueError("resolved HyperCore market has mixed chain or anchor scope")
        if self.market.coin.casefold() != self.selection.market.casefold():
            raise ValueError("resolved HyperCore market differs from its authored coin")
        if (self.usdc.identifier, self.usdc.decimals) != (USDC, USDC_DECIMALS):
            raise ValueError("resolved HyperCore margin asset is not HyperEVM USDC")
        if self.selection.assertions is not None:
            self.selection.assertions.verify(self.market)
        return self

    @property
    def digest(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def assets(self) -> tuple[Asset, ...]:
        return (self.usdc,)

    def summary(self) -> dict:
        """Resolved identities for previews; evidence bodies never appear."""
        return {
            "selection": self.selection.model_dump(mode="json"),
            "coin": self.market.coin,
            "asset_index": self.market.asset,
            "size_decimals": self.market.size_decimals,
            "max_leverage": self.market.max_leverage,
            "margin_asset": str(self.usdc.identifier),
            "block": self.block.number,
        }


def resolve_market(reader, selection: MarketSelection, block: Block) -> ResolvedMarket:
    if selection.chain != block.chain.chain_id or reader.chain != block.chain:
        raise DataError("chain_mismatch", "HyperCore resolution requires its chain and anchor")
    if reader.pin_block(block.number) != block:
        raise DataError("block_mismatch", "HyperCore resolution anchor is no longer canonical")
    (token,) = reader.call(CORE_DEPOSIT_WALLET, "token()", block, outputs=("address",))
    if token.lower() != erc20_address(USDC, block.chain.chain_id):
        raise DataError("identity", "CoreDepositWallet holds another token than pinned USDC")
    usdc = reader.asset(USDC, block)
    # JSON mode keeps strict semantics: JSON arrays validate as typed tuples.
    answer = RESPONSES["metaAndAssetCtxs"].validate_json(
        json.dumps(reader.hypercore(MetaAndAssetCtxs()))
    )
    market = find_market(perp_markets(answer), selection.market)
    return ResolvedMarket(selection=selection, block=block, market=perp_market(market), usdc=usdc)


def perp_market(market) -> PerpMarket:
    meta = market.meta
    if meta.isDelisted or meta.onlyIsolated or meta.marginMode is not None:
        raise DataError("unsupported", f"HyperCore market {meta.name} admits no cross position")
    tiers = market.margin_tiers
    if tiers is not None and (
        Decimal(tiers[0].lowerBound) != 0 or tiers[0].maxLeverage != meta.maxLeverage
    ):
        raise DataError("inconsistent", "HyperCore margin table differs from its market maximum")
    return PerpMarket(
        coin=meta.name,
        asset=market.asset,
        size_decimals=meta.szDecimals,
        max_leverage=meta.maxLeverage,
        tier_bound=tiers[1].lowerBound if tiers is not None and len(tiers) > 1 else None,
    )
