"""Strategy-side protocol metadata provider for Pendle."""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.protocol_metadata_registry import (
    MarketMintMetadata,
    ProtocolMarketMetadata,
    ProtocolMetadataCapability,
    ProtocolMetadataConnector,
    ProtocolTokenMetadata,
)


class PendleProtocolMetadataConnector(ProtocolMetadataConnector, ProtocolMetadataCapability):
    """Pendle PT/YT token and market metadata."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def synthetic_tokens(self) -> tuple[ProtocolTokenMetadata, ...]:
        """Return static Pendle PT/YT token metadata."""
        from almanak.connectors.pendle.sdk import PT_TOKEN_INFO, YT_TOKEN_INFO

        tokens: list[ProtocolTokenMetadata] = []
        for family, token_map in (("PT", PT_TOKEN_INFO), ("YT", YT_TOKEN_INFO)):
            for chain, chain_tokens in token_map.items():
                for symbol, (address, decimals) in chain_tokens.items():
                    tokens.append(
                        ProtocolTokenMetadata(
                            protocol=str(self.protocol),
                            chain=chain,
                            symbol=symbol,
                            address=address,
                            decimals=decimals,
                            family=family,
                        )
                    )
        return tuple(tokens)

    def market_tokens(self) -> tuple[ProtocolMarketMetadata, ...]:
        """Return static Pendle PT/YT token -> market metadata."""
        from almanak.connectors.pendle.sdk import MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN

        markets: list[ProtocolMarketMetadata] = []
        for family, market_map in (("PT", MARKET_BY_PT_TOKEN), ("YT", MARKET_BY_YT_TOKEN)):
            for chain, chain_markets in market_map.items():
                for token_symbol, market_address in chain_markets.items():
                    markets.append(
                        ProtocolMarketMetadata(
                            protocol=str(self.protocol),
                            chain=chain,
                            token_symbol=token_symbol,
                            market_address=market_address,
                            family=family,
                        )
                    )
        return tuple(markets)

    def market_mint_tokens(self) -> tuple[MarketMintMetadata, ...]:
        """Return static Pendle market -> SY mint token metadata."""
        from almanak.connectors.pendle.sdk import MARKET_TOKEN_MINT_SY

        return tuple(
            MarketMintMetadata(
                protocol=str(self.protocol),
                chain=chain,
                market_address=market_address,
                mint_token_address=mint_token,
            )
            for chain, chain_markets in MARKET_TOKEN_MINT_SY.items()
            for market_address, mint_token in chain_markets.items()
        )


__all__ = ["PendleProtocolMetadataConnector"]
