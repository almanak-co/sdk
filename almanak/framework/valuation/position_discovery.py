"""Framework-owned position discovery service.

Proactively discovers on-chain positions without relying on
strategy.get_open_positions(). Uses existing LP, lending, and perps
readers to scan for active positions based on strategy configuration.

This decouples portfolio valuation from strategy cooperation:
strategies that don't implement get_open_positions() (or implement it
poorly) still get accurate position tracking.

Discovery strategies by protocol:
- **Aave-fork lending (Aave V3 / Spark / …)**: Scan tracked
  tokens via getUserReserveData across every declared, connector-supported
  lending protocol, each routed to its OWN per-chain data provider. Any
  non-zero aToken balance or debt is reported.
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

from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry
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
        """Scan tracked tokens for lending positions across every declared,
        connector-supported lending protocol.

        A strategy may use more than one Aave-fork market (Aave V3, Spark,
        …); each lives at a DIFFERENT per-chain
        ``pool_data_provider``. We fan out across the intersection of the
        strategy's declared protocols and the registry's connector-owned
        lending reads, threading the resolved ``protocol`` into every
        ``read_position`` so each reserve is queried against — and the emitted
        position stamped with — its OWN protocol instead of silently defaulting
        every position to Aave V3.
        """
        if not config.tracked_tokens:
            return

        protocols = _lending_protocols_to_scan(config.protocols)
        if not protocols:
            return

        token_addresses = self._resolve_token_addresses(config.tracked_tokens, config.chain)
        if not token_addresses:
            return

        for protocol in protocols:
            self._scan_lending_protocol(config, protocol, token_addresses, result)

    def _scan_lending_protocol(
        self,
        config: DiscoveryConfig,
        protocol: str,
        token_addresses: dict[str, str],
        result: DiscoveryResult,
    ) -> None:
        """Scan every tracked token for one lending ``protocol``.

        Each reserve read routes through ``read_position(..., protocol=...)`` so
        the gateway ``eth_call`` hits ``protocol``'s own data provider, and every
        emitted position carries ``protocol`` so the valuation repricing path
        re-queries the same contract.
        """
        for symbol, address in token_addresses.items():
            result.lending_assets_scanned += 1
            try:
                on_chain = self._lending_reader.read_position(
                    chain=config.chain,
                    asset_address=address,
                    wallet_address=config.wallet_address,
                    protocol=protocol,
                )
                if on_chain is None or not on_chain.is_active:
                    continue

                positions = _lending_to_position_infos(
                    on_chain,
                    symbol,
                    config.chain,
                    config.wallet_address,
                    protocol=protocol,
                )
                result.positions.extend(positions)

            except Exception as e:
                error_msg = f"Lending discovery failed for {symbol} ({protocol}) on {config.chain}: {e}"
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
        """Scan for perpetual positions across every connector-owned perp venue.

        Iterates :meth:`PerpsReadRegistry.supported_protocols` so discovery names
        no venue of its own — adding a perp connector extends discovery with no
        framework edit. Each venue's read is routed with its OWN ``protocol`` so
        the emitted positions carry the venue that produced them (and the
        valuation repricing path re-queries the same venue).

        Distinguishes "not deployed on this chain" from "deployed but the read
        failed" via a :meth:`PerpsReadRegistry.resolve_plan` probe, so a genuine
        gateway/RPC/decode failure on a DEPLOYED venue is surfaced instead of
        being swallowed:

        - ``resolve_plan is None`` — the venue's reader/data-store address is not
          in ``AddressRegistry`` for ``config.chain`` (e.g. a BSC-only venue
          while scanning Arbitrum). It is provably not deployed here, so it has
          no positions: skip SILENTLY (no error), exactly as lending does for an
          unresolved reserve.
        - plan resolved, read raises — recorded as an error (the per-protocol
          ``except`` below), exactly as lending does.
        - plan resolved, ``ok=False`` — the venue IS deployed but the read
          failed (a real gateway/RPC/decode failure); recorded as an error
          rather than skipped silently.
        - plan resolved, ``ok=True`` with no active positions — a measured empty
          book (nothing emitted, no error).

        A probe that itself raises is treated as "venue resolves — proceed to
        the real read": ``discover()`` must never raise, and we must never
        silently drop a potentially-deployed venue on a probe quirk; the real
        ``read_positions`` call below then surfaces any genuine failure.
        """
        result.perps_scanned = True
        for protocol in PerpsReadRegistry.supported_protocols():
            probe = PerpsPositionQuery(chain=config.chain, wallet_address=config.wallet_address)
            try:
                is_deployed = PerpsReadRegistry.resolve_plan(protocol, probe) is not None
            except Exception:
                # Probe could not be built (e.g. an ABI-encode quirk on this
                # wallet). The venue's address resolved past the not-deployed
                # gate, so treat it as deployed and fall through to the real
                # read rather than silently dropping it.
                is_deployed = True
            if not is_deployed:
                # Venue not deployed on this chain (no address) — provably no
                # positions; skip silently. Distinguishes not-deployed from a
                # genuine read failure.
                continue
            try:
                read_result = self._perps_reader.read_positions(
                    chain=config.chain,
                    wallet_address=config.wallet_address,
                    protocol=protocol,
                )
            except Exception as e:
                error_msg = f"Perps discovery failed for {protocol} on {config.chain}: {e}"
                logger.debug(error_msg, exc_info=True)
                result.errors.append(error_msg)
                continue

            if not read_result.ok:
                # Plan resolved (venue IS deployed) but the read failed — a real
                # gateway/RPC/decode failure; surface it instead of skipping
                # silently.
                error_msg = f"Perps read failed for {protocol} on {config.chain}"
                logger.debug(error_msg)
                result.errors.append(error_msg)
                continue

            for pos in read_result.positions:
                side = "long" if pos.is_long else "short"
                result.positions.append(
                    PositionInfo(
                        position_type=PositionType.PERP,
                        position_id=pos.position_key,
                        chain=config.chain,
                        protocol=protocol,
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


def _lending_protocols_to_scan(protocols: list[str]) -> list[str]:
    """Lending protocols to scan during discovery, in deterministic registry order.

    The intersection of the strategy's declared ``protocols`` and the registry's
    connector-owned lending reads (Aave V3 / Spark / …). The
    registry owns the canonical key and resolves aliases (e.g. ``"aave"`` ->
    ``"aave_v3"``), so discovery names no protocol of its own — a strategy that
    declares Spark gets its reserves queried against Spark's data provider
    instead of silently defaulting to Aave V3.
    """
    declared: set[str] = set()
    for p in protocols:
        canonical = LendingReadRegistry.canonical(p)
        if canonical is not None:
            declared.add(canonical)
    return [p for p in LendingReadRegistry.supported_protocols() if p in declared]


def _has_lending_protocol(protocols: list[str]) -> bool:
    """Return True when any declared protocol has a connector-owned lending read.

    Generalised beyond Aave V3: any Aave-fork the registry knows about (Spark,
    …) now gates discovery on. Protocols without a connector-owned
    single-reserve read (e.g. Compound V3, Morpho) are still excluded — they are
    discovered through their own connector paths.
    """
    return bool(_lending_protocols_to_scan(protocols))


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
    """Check if any protocol in the list is a perpetuals protocol.

    Reads the same connector-declared perp membership as the synthetic
    permission-discovery path (``_PERP_PROTOCOLS`` in
    ``almanak/framework/permissions/synthetic_intents.py``, itself derived from
    each connector's ``PermissionHints.synthetic_discovery_intents``), so that
    every perp venue whose synthetic intents are generated for permission
    discovery is also discovered/valued by the position-discovery flow.

    The ``"gmx"`` alias is added on top of the derived set: position-discovery
    callers may pass the historical short protocol name, but ``gmx_v2`` is the
    canonical connector slug that declares perp participation (so the derived
    set carries ``gmx_v2``, not ``gmx``).
    """
    from almanak.framework.permissions.synthetic_intents import _PERP_PROTOCOLS

    perps_protocols = _PERP_PROTOCOLS | {"gmx"}
    return bool({p.lower() for p in protocols} & perps_protocols)


def _lending_to_position_infos(
    on_chain: LendingPositionOnChain,
    symbol: str,
    chain: str,
    wallet_address: str = "",
    *,
    protocol: str,
) -> list[PositionInfo]:
    """Convert on-chain lending data to PositionInfo entries.

    Creates separate SUPPLY and BORROW positions as appropriate. ``protocol`` is
    stamped on each PositionInfo (and its id) so the valuation repricing path
    re-queries the SAME protocol's data provider — a Spark position must
    never be re-priced against Aave V3's contract — and so two reserves of the
    same token across different lending markets stay distinct.
    value_usd is set to 0 here — the portfolio_valuer handles repricing.
    wallet_address is included so the repricing path can re-query on-chain.
    """
    positions: list[PositionInfo] = []

    if on_chain.current_atoken_balance > 0:
        positions.append(
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id=f"{protocol}-supply-{symbol}-{chain}",
                chain=chain,
                protocol=protocol,
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
                position_id=f"{protocol}-borrow-{symbol}-{chain}",
                chain=chain,
                protocol=protocol,
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
