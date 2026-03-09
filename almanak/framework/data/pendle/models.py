"""Data models for Pendle data layer.

These dataclasses represent pricing, market, and asset data from
the Pendle REST API and on-chain RouterStatic contract.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class PendleAsset:
    """A Pendle asset (PT, YT, SY, or LP token).

    Attributes:
        address: Token contract address
        symbol: Human-readable symbol (e.g., "PT-sUSDe-29MAY2025")
        decimals: Token decimals
        chain_id: Chain ID (1=Ethereum, 42161=Arbitrum)
        asset_type: One of "PT", "YT", "SY", "LP", "UNDERLYING"
        underlying_address: Address of the underlying yield-bearing asset
        expiry: Unix timestamp of maturity (0 if not applicable)
    """

    address: str
    symbol: str
    decimals: int
    chain_id: int
    asset_type: str = ""
    underlying_address: str = ""
    expiry: int = 0

    def is_expired(self, current_timestamp: int) -> bool:
        """Check if the asset has expired."""
        if self.expiry == 0:
            return False
        return current_timestamp >= self.expiry

    def to_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "symbol": self.symbol,
            "decimals": self.decimals,
            "chain_id": self.chain_id,
            "asset_type": self.asset_type,
            "underlying_address": self.underlying_address,
            "expiry": self.expiry,
        }


@dataclass
class PendleMarketData:
    """Market data for a Pendle market.

    Attributes:
        market_address: Market contract address
        chain_id: Chain ID
        pt_address: PT token address
        yt_address: YT token address
        sy_address: SY token address
        underlying_address: Underlying asset address
        expiry: Market expiry timestamp
        implied_apy: Current implied APY (e.g., 0.05 = 5%)
        underlying_apy: Underlying protocol's APY
        pt_price_in_asset: PT price denominated in the underlying asset
        yt_price_in_asset: YT price denominated in the underlying asset
        liquidity_usd: Total market liquidity in USD
        volume_24h_usd: 24h trading volume in USD
        pt_discount: PT discount from par (e.g., 0.03 = 3% discount)
        is_expired: Whether the market has expired
    """

    market_address: str
    chain_id: int
    pt_address: str = ""
    yt_address: str = ""
    sy_address: str = ""
    underlying_address: str = ""
    expiry: int = 0
    implied_apy: Decimal = Decimal("0")
    underlying_apy: Decimal = Decimal("0")
    pt_price_in_asset: Decimal = Decimal("0")
    yt_price_in_asset: Decimal = Decimal("0")
    liquidity_usd: Decimal = Decimal("0")
    volume_24h_usd: Decimal = Decimal("0")
    pt_discount: Decimal = Decimal("0")
    is_expired: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_address": self.market_address,
            "chain_id": self.chain_id,
            "pt_address": self.pt_address,
            "yt_address": self.yt_address,
            "sy_address": self.sy_address,
            "underlying_address": self.underlying_address,
            "expiry": self.expiry,
            "implied_apy": str(self.implied_apy),
            "underlying_apy": str(self.underlying_apy),
            "pt_price_in_asset": str(self.pt_price_in_asset),
            "yt_price_in_asset": str(self.yt_price_in_asset),
            "liquidity_usd": str(self.liquidity_usd),
            "volume_24h_usd": str(self.volume_24h_usd),
            "pt_discount": str(self.pt_discount),
            "is_expired": self.is_expired,
        }


@dataclass
class PendleSwapQuote:
    """Quote for a Pendle swap operation.

    Attributes:
        market_address: Market contract address
        token_in: Input token address
        token_out: Output token address
        amount_in: Input amount in wei
        amount_out: Estimated output amount in wei
        price_impact_bps: Price impact in basis points
        exchange_rate: Effective exchange rate (amount_out / amount_in adjusted for decimals)
        source: Where the quote came from ("api", "on_chain", "estimate")
    """

    market_address: str
    token_in: str
    token_out: str
    amount_in: int
    amount_out: int
    price_impact_bps: int = 0
    exchange_rate: Decimal = Decimal("0")
    source: str = "estimate"
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_address": self.market_address,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "price_impact_bps": self.price_impact_bps,
            "exchange_rate": str(self.exchange_rate),
            "source": self.source,
            "warnings": self.warnings,
        }
