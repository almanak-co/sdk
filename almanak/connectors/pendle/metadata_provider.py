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
        """Return static Pendle PT/YT/LP token metadata.

        LP tokens (VIB-5487, BUG B): a Pendle market contract address IS its own
        fungible LP token. A strategy that holds LP has that market address in
        its tracked-token set, so every portfolio/teardown snapshot reads its
        balance — resolving the market address as a token. Without a static
        entry, resolution falls to the gateway ``GetTokenMetadata`` fallback,
        which times out (30s × 3 ≈ 90-180s per snapshot) on the market contract.
        Registering the market address here (18 decimals — a Pendle protocol
        invariant, on-chain-verified) makes it resolve from the static index like
        PT/YT, with no gateway round-trip. Derived from the SAME
        ``MARKET_BY_*_TOKEN`` maps as ``market_tokens`` so the LP registry can
        never drift from the market registry.
        """
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
        tokens.extend(self._lp_token_metadata())
        return tuple(tokens)

    def _lp_token_metadata(self) -> list[ProtocolTokenMetadata]:
        """Derive fungible LP-token metadata for every known Pendle market.

        A market's LP token = the market contract address, always 18 decimals
        (``PENDLE_LP_TOKEN_DECIMALS``). The synthetic symbol mirrors the on-chain
        LP symbol convention (``PLP-<underlying>-<maturity>``, e.g. the live
        market's on-chain ``PLP-sUSDai-15OCT2026``) by swapping the PT-/YT- prefix
        for ``PLP-`` — distinct from PT-/YT- so it never collides with a PT/YT
        entry. Deduped by (chain, address) since the market maps carry
        case-variant keys and both PT and YT point at the same market.
        """
        from almanak.connectors.pendle.sdk import (
            MARKET_BY_PT_TOKEN,
            MARKET_BY_YT_TOKEN,
            PENDLE_LP_TOKEN_DECIMALS,
        )

        tokens: list[ProtocolTokenMetadata] = []
        seen: set[tuple[str, str]] = set()
        for token_map in (MARKET_BY_PT_TOKEN, MARKET_BY_YT_TOKEN):
            for chain, chain_markets in token_map.items():
                for token_symbol, market_address in chain_markets.items():
                    key = (chain.lower(), market_address.lower())
                    if key in seen:
                        continue
                    seen.add(key)
                    lp_symbol = token_symbol.replace("PT-", "PLP-", 1).replace("YT-", "PLP-", 1)
                    tokens.append(
                        ProtocolTokenMetadata(
                            protocol=str(self.protocol),
                            chain=chain,
                            symbol=lp_symbol,
                            address=market_address,
                            decimals=PENDLE_LP_TOKEN_DECIMALS,
                            family="LP",
                        )
                    )
        return tokens

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
