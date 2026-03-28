"""Framework-owned position discovery service.

Proactively discovers on-chain positions without relying on
strategy.get_open_positions(). Uses existing LP, lending, and perps
readers to scan for active positions based on strategy configuration.

This decouples portfolio valuation from strategy cooperation:
strategies that don't implement get_open_positions() (or implement it
poorly) still get accurate position tracking.

Discovery strategies by protocol:
- **Aave V3 lending**: Scan tracked tokens via getUserReserveData.
  Any non-zero aToken balance or debt is reported.
- **LP (Uniswap V3 / forks)**: LP positions require a token ID that
  can't be enumerated cheaply. Discovery accepts explicit token IDs
  from strategy state or prior execution results.
- **GMX V2 perps**: Query all open positions for the wallet via
  Reader contract (with REST API fallback).
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.lending_position_reader import (
    LendingPositionOnChain,
    LendingPositionReader,
)
from almanak.framework.valuation.lp_position_reader import LPPositionReader
from almanak.framework.valuation.perps_position_reader import PerpsPositionReader

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveryConfig:
    """What the discovery service should look for.

    Built from strategy metadata + runtime config.

    Attributes:
        chain: Blockchain identifier (e.g. "arbitrum").
        wallet_address: Wallet to scan for positions.
        protocols: Protocols the strategy uses (e.g. ["aave_v3", "uniswap_v3"]).
        tracked_tokens: Token symbols the strategy tracks (used as lending scan list).
        lp_token_ids: Known LP NFT token IDs (from state or execution results).
        lp_protocol: Protocol for LP positions (default "uniswap_v3").
    """

    chain: str
    wallet_address: str
    protocols: list[str] = field(default_factory=list)
    tracked_tokens: list[str] = field(default_factory=list)
    lp_token_ids: list[int] = field(default_factory=list)
    lp_protocol: str = "uniswap_v3"


@dataclass
class DiscoveryResult:
    """Result of a position discovery scan.

    Attributes:
        positions: Discovered active positions.
        scanned_at: When the scan ran.
        lending_assets_scanned: Number of lending assets checked.
        lp_ids_scanned: Number of LP token IDs checked.
        errors: Non-fatal errors encountered during scanning.
    """

    positions: list[PositionInfo] = field(default_factory=list)
    scanned_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    lending_assets_scanned: int = 0
    lp_ids_scanned: int = 0
    perps_scanned: bool = False
    errors: list[str] = field(default_factory=list)

    @property
    def has_positions(self) -> bool:
        return len(self.positions) > 0


class PositionDiscoveryService:
    """Discovers on-chain positions without strategy cooperation.

    Uses the existing LP and lending readers from the valuation module.
    The service is stateless — call discover() with a config each time.

    Usage:
        service = PositionDiscoveryService(gateway_client=client)
        config = DiscoveryConfig(
            chain="arbitrum",
            wallet_address="0x...",
            protocols=["aave_v3"],
            tracked_tokens=["WETH", "USDC"],
        )
        result = service.discover(config)
        for pos in result.positions:
            print(f"{pos.position_type}: {pos.protocol} ${pos.value_usd}")
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)

    def set_gateway_client(self, gateway_client: object | None) -> None:
        """Update the gateway client (called when connection is established)."""
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)

    def discover(self, config: DiscoveryConfig) -> DiscoveryResult:
        """Scan for on-chain positions based on the discovery config.

        Never raises — returns partial results with errors logged.

        Args:
            config: What to look for (chain, wallet, protocols, tokens).

        Returns:
            DiscoveryResult with any active positions found.
        """
        result = DiscoveryResult()

        # Discover lending positions (Aave V3)
        if _has_lending_protocol(config.protocols):
            self._discover_lending(config, result)

        # Discover LP positions (Uniswap V3 / forks)
        if config.lp_token_ids and _has_lp_protocol(config.protocols):
            self._discover_lp(config, result)

        # Discover perpetual positions (GMX V2)
        if _has_perps_protocol(config.protocols):
            self._discover_perps(config, result)

        if result.has_positions:
            logger.info(
                "Position discovery found %d positions on %s (lending_scanned=%d, lp_scanned=%d, perps_scanned=%s)",
                len(result.positions),
                config.chain,
                result.lending_assets_scanned,
                result.lp_ids_scanned,
                result.perps_scanned,
            )

        return result

    def _discover_lending(self, config: DiscoveryConfig, result: DiscoveryResult) -> None:
        """Scan tracked tokens for Aave V3 lending positions."""
        if not config.tracked_tokens:
            return

        token_addresses = self._resolve_token_addresses(config.tracked_tokens, config.chain)
        if not token_addresses:
            return

        for symbol, address in token_addresses.items():
            result.lending_assets_scanned += 1
            try:
                on_chain = self._lending_reader.read_position(
                    chain=config.chain,
                    asset_address=address,
                    wallet_address=config.wallet_address,
                )
                if on_chain is None or not on_chain.is_active:
                    continue

                positions = _lending_to_position_infos(
                    on_chain,
                    symbol,
                    config.chain,
                    config.wallet_address,
                )
                result.positions.extend(positions)

            except Exception as e:
                error_msg = f"Lending discovery failed for {symbol} on {config.chain}: {e}"
                logger.debug(error_msg, exc_info=True)
                result.errors.append(error_msg)

    def _discover_lp(self, config: DiscoveryConfig, result: DiscoveryResult) -> None:
        """Scan known LP token IDs for active positions."""
        for token_id in config.lp_token_ids:
            result.lp_ids_scanned += 1
            try:
                on_chain = self._lp_reader.read_position(
                    chain=config.chain,
                    token_id=token_id,
                    protocol=config.lp_protocol,
                )
                if on_chain is None:
                    continue

                # Position with zero liquidity AND zero fees = closed
                if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
                    continue

                result.positions.append(
                    PositionInfo(
                        position_type=PositionType.LP,
                        position_id=str(token_id),
                        chain=config.chain,
                        protocol=config.lp_protocol,
                        value_usd=Decimal("0"),  # Repriced by portfolio_valuer
                        details={
                            "token_id": token_id,
                            "token0_address": on_chain.token0,
                            "token1_address": on_chain.token1,
                            "liquidity": str(on_chain.liquidity),
                        },
                    )
                )

            except Exception as e:
                error_msg = f"LP discovery failed for token_id={token_id} on {config.chain}: {e}"
                logger.debug(error_msg, exc_info=True)
                result.errors.append(error_msg)

    def _discover_perps(self, config: DiscoveryConfig, result: DiscoveryResult) -> None:
        """Scan for GMX V2 perpetual positions."""
        result.perps_scanned = True
        try:
            positions = self._perps_reader.read_positions(
                chain=config.chain,
                wallet_address=config.wallet_address,
            )
            for pos in positions:
                side = "long" if pos.is_long else "short"
                result.positions.append(
                    PositionInfo(
                        position_type=PositionType.PERP,
                        position_id=pos.position_key,
                        chain=config.chain,
                        protocol="gmx_v2",
                        value_usd=Decimal("0"),  # Repriced by portfolio_valuer
                        details={
                            "market": pos.market,
                            "collateral_token": pos.collateral_token,
                            "is_long": pos.is_long,
                            "size_in_usd": str(pos.size_in_usd),
                            "size_in_tokens": str(pos.size_in_tokens),
                            "collateral_amount": str(pos.collateral_amount),
                            "wallet_address": config.wallet_address,
                            "side": side,
                        },
                    )
                )
        except Exception as e:
            error_msg = f"Perps discovery failed on {config.chain}: {e}"
            logger.debug(error_msg, exc_info=True)
            result.errors.append(error_msg)

    @staticmethod
    def _resolve_token_addresses(symbols: list[str], chain: str) -> dict[str, str]:
        """Resolve token symbols to addresses for on-chain queries.

        Returns {symbol: address} for tokens that resolved successfully.
        """
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
        except Exception:
            logger.debug("TokenResolver unavailable for position discovery")
            return {}

        addresses: dict[str, str] = {}
        for symbol in symbols:
            try:
                resolved = resolver.resolve(symbol, chain)
                if resolved and resolved.address:
                    addresses[symbol] = resolved.address
            except Exception:
                logger.debug("Could not resolve %s on %s for discovery", symbol, chain)

        return addresses


def _has_lending_protocol(protocols: list[str]) -> bool:
    """Check if any protocol in the list is an Aave V3-compatible lending protocol.

    Only includes Aave V3 (and its alias "aave") because the discovery path
    uses the Aave V3 PoolDataProvider. Spark and Compound V3 have different
    contracts and would need their own readers.
    """
    lending_protocols = {"aave_v3", "aave"}
    return bool({p.lower() for p in protocols} & lending_protocols)


def _has_lp_protocol(protocols: list[str]) -> bool:
    """Check if any protocol in the list is an LP protocol."""
    lp_protocols = {
        "uniswap_v3",
        "sushiswap_v3",
        "pancakeswap_v3",
        "aerodrome",
        "velodrome",
    }
    return bool({p.lower() for p in protocols} & lp_protocols)


def _has_perps_protocol(protocols: list[str]) -> bool:
    """Check if any protocol in the list is a GMX V2 perpetuals protocol."""
    perps_protocols = {"gmx_v2", "gmx"}
    return bool({p.lower() for p in protocols} & perps_protocols)


def _lending_to_position_infos(
    on_chain: LendingPositionOnChain,
    symbol: str,
    chain: str,
    wallet_address: str = "",
) -> list[PositionInfo]:
    """Convert on-chain lending data to PositionInfo entries.

    Creates separate SUPPLY and BORROW positions as appropriate.
    value_usd is set to 0 here — the portfolio_valuer handles repricing.
    wallet_address is included so the repricing path can re-query on-chain.
    """
    positions: list[PositionInfo] = []

    if on_chain.current_atoken_balance > 0:
        positions.append(
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id=f"aave-supply-{symbol}-{chain}",
                chain=chain,
                protocol="aave_v3",
                value_usd=Decimal("0"),  # Repriced by portfolio_valuer
                details={
                    "asset": symbol,
                    "asset_address": on_chain.asset_address,
                    "wallet_address": wallet_address,
                    "atoken_balance_raw": str(on_chain.current_atoken_balance),
                    "collateral_enabled": on_chain.usage_as_collateral_enabled,
                },
            )
        )

    if on_chain.total_debt > 0:
        positions.append(
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id=f"aave-borrow-{symbol}-{chain}",
                chain=chain,
                protocol="aave_v3",
                value_usd=Decimal("0"),  # Repriced (negative) by portfolio_valuer
                details={
                    "asset": symbol,
                    "asset_address": on_chain.asset_address,
                    "wallet_address": wallet_address,
                    "stable_debt_raw": str(on_chain.current_stable_debt),
                    "variable_debt_raw": str(on_chain.current_variable_debt),
                },
            )
        )

    return positions
