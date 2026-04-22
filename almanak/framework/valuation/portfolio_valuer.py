"""Portfolio valuation orchestrator.

Produces PortfolioSnapshot by querying the gateway (via MarketSnapshot)
for wallet balances and token prices, using the PositionDiscoveryService
to proactively find on-chain positions (LP, lending, perps), with
strategy.get_open_positions() as a supplementary source.

This is the single source of truth for portfolio valuation at runtime.
The framework owns both discovery and math.
"""

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionInfo
from almanak.framework.valuation.lending_position_reader import LendingPositionReader
from almanak.framework.valuation.lending_valuer import value_lending_position
from almanak.framework.valuation.lp_position_reader import LPPositionReader
from almanak.framework.valuation.lp_valuer import value_lp_position
from almanak.framework.valuation.perps_position_reader import PerpsPositionReader
from almanak.framework.valuation.perps_valuer import value_perps_position
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    PositionDiscoveryService,
)
from almanak.framework.valuation.spot_valuer import total_value, value_tokens
from almanak.framework.valuation.vault_position_reader import VaultPositionReader

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownPositionSummary

logger = logging.getLogger(__name__)

FRAMEWORK_EXTERNAL_AGREEMENT_THRESHOLD = Decimal("0.10")
FRAMEWORK_EXTERNAL_DIVERGENCE_THRESHOLD = Decimal("0.20")


@runtime_checkable
class MarketDataSource(Protocol):
    """Minimal interface for fetching prices and balances.

    Satisfied by both the strategy-facing MarketSnapshot and
    the data-layer MarketSnapshot.
    """

    def price(self, token: str, quote: str = "USD") -> Decimal: ...
    def balance(self, token: str) -> Any: ...


@runtime_checkable
class StrategyLike(Protocol):
    """Minimal strategy interface for PortfolioValuer."""

    @property
    def strategy_id(self) -> str: ...

    @property
    def chain(self) -> str: ...

    @property
    def wallet_address(self) -> str: ...

    def _get_tracked_tokens(self) -> list[str]: ...


class PortfolioValuer:
    """Framework-owned portfolio valuation engine.

    Replaces strategy-level get_portfolio_snapshot() as the primary
    valuation path. Strategies still implement get_open_positions()
    for position discovery (LP, lending, perps), but the valuer
    owns the math and re-prices via gateway data.

    Usage:
        valuer = PortfolioValuer()
        snapshot = valuer.value(strategy, market)

        # With gateway client for on-chain LP re-pricing:
        valuer = PortfolioValuer(gateway_client=client)
    """

    def __init__(self, gateway_client: object | None = None) -> None:
        """Initialize the valuer.

        Args:
            gateway_client: Optional GatewayClient for on-chain LP position
                queries. If None, LP positions use strategy-reported values.
                Can also be set later via set_gateway_client().
        """
        self._gateway_client = gateway_client
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)
        self._vault_reader = VaultPositionReader(gateway_client)
        self._discovery = PositionDiscoveryService(gateway_client)

    def set_gateway_client(self, gateway_client: object | None) -> None:
        """Update the gateway client for on-chain queries.

        Called by StrategyRunner once the gateway connection is established.
        """
        self._gateway_client = gateway_client
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._perps_reader = PerpsPositionReader.from_gateway_client(gateway_client)
        self._vault_reader.set_gateway_client(gateway_client)
        self._discovery.set_gateway_client(gateway_client)

    def value(
        self,
        strategy: StrategyLike,
        market: MarketDataSource,
        iteration_number: int = 0,
    ) -> PortfolioSnapshot:
        """Produce a PortfolioSnapshot with real USD values.

        Never raises -- returns UNAVAILABLE confidence on total failure.
        This guarantees gap-free time series for PnL charts.

        Args:
            strategy: Strategy instance for position discovery and config
            market: MarketSnapshot for price/balance queries
            iteration_number: Current strategy iteration count

        Returns:
            PortfolioSnapshot with real values and appropriate ValueConfidence
        """
        now = datetime.now(UTC)
        strategy_id = ""
        chain = ""

        try:
            strategy_id = strategy.strategy_id
            chain = strategy.chain

            # Step 1: Discover tracked tokens from strategy config
            tracked_tokens = strategy._get_tracked_tokens()

            # Step 2: Fetch wallet balances and prices via gateway
            balances: dict[str, Decimal] = {}
            prices: dict[str, Decimal] = {}
            wallet_data_incomplete = False

            for token in tracked_tokens:
                try:
                    balance_result = market.balance(token)
                    # MarketSnapshot.balance() returns TokenBalance or Decimal
                    if hasattr(balance_result, "balance"):
                        bal = balance_result.balance
                    else:
                        bal = Decimal(str(balance_result))
                    if bal > 0:
                        balances[token] = bal
                except Exception:
                    wallet_data_incomplete = True
                    logger.debug("Could not fetch balance for %s", token)

                try:
                    price = market.price(token)
                    prices[token] = Decimal(str(price))
                except Exception:
                    if token in balances:
                        wallet_data_incomplete = True
                    logger.debug("Could not fetch price for %s", token)

            # Check for tokens with positive balance but missing/non-positive price
            for token in balances:
                token_price = prices.get(token)
                if token_price is None or token_price <= 0:
                    wallet_data_incomplete = True

            # Step 3: Apply spot valuation math (pure, deterministic)
            wallet_balances = value_tokens(balances, prices)
            wallet_value = total_value(wallet_balances)

            # Step 4: Get non-wallet positions (LP, lending, perps) if available
            positions, position_value, positions_unavailable = self._get_positions(strategy, market, prices)

            # Step 5: Determine confidence level
            has_any_value = bool(wallet_balances or positions)
            if not has_any_value and (positions_unavailable or wallet_data_incomplete):
                confidence = ValueConfidence.UNAVAILABLE
            elif positions_unavailable or wallet_data_incomplete:
                confidence = ValueConfidence.ESTIMATED
            else:
                confidence = ValueConfidence.HIGH

            # Step 6: Build audit-safe token price map (chain:address keyed)
            token_price_records = self._build_token_price_records(chain, prices, tracked_tokens)

            framework_snapshot = PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy_id,
                total_value_usd=wallet_value + position_value,
                available_cash_usd=wallet_value,
                value_confidence=confidence,
                positions=positions,
                wallet_balances=wallet_balances,
                token_prices=token_price_records,
                chain=chain,
                iteration_number=iteration_number,
            )
            # Reconciliation is advisory — never let it downgrade the framework snapshot.
            try:
                return self._reconcile_with_external(strategy, framework_snapshot)
            except Exception as recon_err:
                logger.warning("External reconciliation failed (returning framework snapshot): %s", recon_err)
                return framework_snapshot

        except Exception as e:
            # Failure contract: NEVER skip a snapshot. Persist with UNAVAILABLE.
            logger.warning("Portfolio valuation failed: %s", e)
            return PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy_id,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=chain,
                iteration_number=iteration_number,
            )

    def _reconcile_with_external(
        self,
        strategy: StrategyLike,
        framework_snapshot: PortfolioSnapshot,
    ) -> PortfolioSnapshot:
        """Reconcile framework valuation with external wallet portfolio data."""
        external = self._fetch_external_portfolio(strategy)
        if external is None:
            return framework_snapshot

        external_total = external["total_value_usd"]
        framework_total = framework_snapshot.total_value_usd

        metadata = {
            "valuation_source": "framework",
            "external_provider": external["provider"],
            "framework_total_value_usd": str(framework_total),
            "external_total_value_usd": str(external_total),
            "reconciliation_status": "framework_only",
            "external_cache_hit": external["cache_hit"],
            "external_timestamp": external["timestamp"].isoformat(),
            "external_positions_count": len(external["positions"]),
        }

        if external_total <= 0:
            metadata["reconciliation_status"] = "external_non_positive"
            framework_snapshot.snapshot_metadata = metadata
            return framework_snapshot

        divergence_ratio = self._calculate_divergence_ratio(framework_total, external_total)
        metadata["divergence_ratio"] = str(divergence_ratio)

        # External only replaces framework when framework reports zero.
        # When both are positive, framework's on-chain queries are authoritative;
        # external data is advisory metadata for operator dashboards.
        if framework_total <= 0 and external_total > 0:
            metadata["valuation_source"] = "reconciled_external"
            metadata["reconciliation_status"] = "external_won_zero_framework"
            logger.warning(
                "External portfolio valuation replaced zero framework value for %s on %s: framework=$%s external=$%s",
                framework_snapshot.strategy_id,
                framework_snapshot.chain,
                framework_total,
                external_total,
            )
            return self._build_external_reconciled_snapshot(framework_snapshot, external, metadata)

        if framework_total > 0 and divergence_ratio <= FRAMEWORK_EXTERNAL_AGREEMENT_THRESHOLD:
            metadata["reconciliation_status"] = "framework_won_close_agreement"
        elif divergence_ratio > FRAMEWORK_EXTERNAL_DIVERGENCE_THRESHOLD:
            metadata["reconciliation_status"] = "framework_won_large_divergence"
            logger.warning(
                "Large divergence between framework and external for %s on %s: framework=$%s external=$%s divergence=%s",
                framework_snapshot.strategy_id,
                framework_snapshot.chain,
                framework_total,
                external_total,
                divergence_ratio,
            )
        else:
            metadata["reconciliation_status"] = "framework_won_moderate_divergence"

        framework_snapshot.snapshot_metadata = metadata
        return framework_snapshot

    def _fetch_external_portfolio(self, strategy: StrategyLike) -> dict[str, Any] | None:
        """Fetch external wallet portfolio data through the gateway integration RPC."""
        gateway_client = self._gateway_client
        if gateway_client is None:
            return None

        wallet_address = getattr(strategy, "wallet_address", "")
        chain = getattr(strategy, "chain", "")
        if not wallet_address or not chain:
            return None

        try:
            from almanak.gateway.proto import gateway_pb2

            response = gateway_client.integration.GetWalletPortfolio(  # type: ignore[attr-defined]
                gateway_pb2.WalletPortfolioRequest(wallet_address=wallet_address, chain=chain)
            )
        except Exception as e:
            logger.debug("External portfolio RPC failed for %s on %s: %s", wallet_address, chain, e)
            return None

        if not response.success:
            logger.debug(
                "External portfolio unavailable for %s on %s: %s",
                wallet_address,
                chain,
                response.error or "unknown error",
            )
            return None

        try:
            total_value_usd = Decimal(response.total_value_usd or "0")
        except Exception:
            logger.debug("Invalid total_value_usd from external portfolio: %r", response.total_value_usd)
            return None

        return {
            "provider": response.provider or "unknown",
            "total_value_usd": total_value_usd,
            "cache_hit": bool(response.cache_hit),
            "timestamp": datetime.fromtimestamp(response.timestamp, tz=UTC)
            if response.timestamp
            else datetime.now(UTC),
            "positions": [self._external_position_to_value(position, chain) for position in response.positions],
        }

    def _build_external_reconciled_snapshot(
        self,
        framework_snapshot: PortfolioSnapshot,
        external: dict[str, Any],
        metadata: dict[str, Any],
    ) -> PortfolioSnapshot:
        """Build a reconciled snapshot where the external total wins."""
        external_positions = external["positions"]
        merged_positions = self._merge_external_positions(framework_snapshot.positions, external_positions)
        external_total = external["total_value_usd"]

        # Derive cash as remainder to ensure consistency: total = positions + cash.
        # If positions already exceed the external total, cash is zero.
        pos_total = sum((p.value_usd for p in merged_positions), Decimal("0"))
        available_cash_usd = max(Decimal("0"), external_total - pos_total)

        return PortfolioSnapshot(
            timestamp=framework_snapshot.timestamp,
            strategy_id=framework_snapshot.strategy_id,
            total_value_usd=external_total,
            available_cash_usd=available_cash_usd,
            value_confidence=ValueConfidence.ESTIMATED,
            error=framework_snapshot.error,
            positions=merged_positions,
            wallet_balances=framework_snapshot.wallet_balances,
            chain=framework_snapshot.chain,
            iteration_number=framework_snapshot.iteration_number,
            snapshot_metadata=metadata,
        )

    def _external_position_to_value(self, position: Any, chain: str) -> PositionValue:
        """Convert external portfolio data into a PositionValue."""
        details = self._decode_external_details(position.raw_details_json)
        pool_address = getattr(position, "pool_address", "") or ""
        if pool_address:
            details.setdefault("pool_address", pool_address)
        details.setdefault("position_id", getattr(position, "position_id", ""))
        details.setdefault("source", "external_portfolio_api")

        return PositionValue(
            position_type=self._map_external_position_type(getattr(position, "position_type", "")),
            protocol=getattr(position, "protocol", "unknown") or "unknown",
            chain=chain,
            value_usd=Decimal(getattr(position, "value_usd", "0") or "0"),
            label=getattr(position, "label", "") or getattr(position, "protocol", "external"),
            tokens=list(getattr(position, "token_symbols", []) or []),
            details=details,
        )

    def _merge_external_positions(
        self,
        framework_positions: list[PositionValue],
        external_positions: list[PositionValue],
    ) -> list[PositionValue]:
        """Merge framework and external positions, preserving framework detail when possible."""
        merged = list(framework_positions)

        for external_position in external_positions:
            match_index = next(
                (index for index, existing in enumerate(merged) if self._positions_match(existing, external_position)),
                None,
            )
            if match_index is None:
                merged.append(external_position)
                continue

            existing = merged[match_index]
            merged[match_index] = PositionValue(
                position_type=existing.position_type or external_position.position_type,
                protocol=existing.protocol or external_position.protocol,
                chain=existing.chain or external_position.chain,
                value_usd=external_position.value_usd,
                label=existing.label or external_position.label,
                tokens=existing.tokens or external_position.tokens,
                details={**external_position.details, **existing.details},
            )

        return merged

    @staticmethod
    def _positions_match(existing: PositionValue, external_position: PositionValue) -> bool:
        """Determine whether framework and external positions refer to the same exposure."""
        if existing.protocol.lower() != external_position.protocol.lower():
            return False

        existing_pool = str(existing.details.get("pool_address") or existing.details.get("pool") or "").lower()
        external_pool = str(external_position.details.get("pool_address") or "").lower()
        if existing_pool and external_pool:
            return existing_pool == external_pool

        return existing.label == external_position.label and set(existing.tokens) == set(external_position.tokens)

    @staticmethod
    def _decode_external_details(raw_details_json: str) -> dict[str, Any]:
        """Decode external raw details JSON defensively."""
        if not raw_details_json:
            return {}
        try:
            payload = json.loads(raw_details_json)
            return payload if isinstance(payload, dict) else {}
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _map_external_position_type(position_type: str) -> Any:
        """Map external provider position types onto Almanak teardown position types."""
        from almanak.framework.teardown.models import PositionType

        normalized = position_type.strip().lower()
        if "perp" in normalized or "future" in normalized:
            return PositionType.PERP
        if "borrow" in normalized or "debt" in normalized or "loan" in normalized:
            return PositionType.BORROW
        if "supply" in normalized or "deposit" in normalized or "lend" in normalized:
            return PositionType.SUPPLY
        if "vault" in normalized or "yield" in normalized or "earn" in normalized:
            return PositionType.VAULT
        if "stake" in normalized or "farm" in normalized:
            return PositionType.STAKE
        if "predict" in normalized:
            return PositionType.PREDICTION
        if "cex" in normalized:
            return PositionType.CEX
        if "lp" in normalized or "liquidity" in normalized or "pool" in normalized:
            return PositionType.LP
        return PositionType.TOKEN

    @staticmethod
    def _calculate_divergence_ratio(framework_total: Decimal, external_total: Decimal) -> Decimal:
        """Return the absolute divergence ratio between framework and external totals."""
        baseline = max(abs(framework_total), abs(external_total))
        if baseline <= 0:
            return Decimal("0")
        return abs(framework_total - external_total) / baseline

    @staticmethod
    def _build_token_price_records(
        chain: str,
        prices: dict[str, Decimal],
        tracked_tokens: list[str],
    ) -> dict[str, dict]:
        """Build an audit-safe token price map keyed by chain:address.

        Each entry contains the USD price, display symbol, and decimals so
        historical snapshots can be re-verified without re-querying oracles.
        """
        token_price_records: dict[str, dict] = {}
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
        except Exception:
            resolver = None

        for token in tracked_tokens:
            price = prices.get(token)
            if price is None or price <= 0:
                continue
            try:
                if resolver:
                    resolved = resolver.resolve(token, chain)
                    address = resolved.address if resolved else token
                    decimals = resolved.decimals if resolved else None
                else:
                    address = token
                    decimals = None
                key = f"{chain}:{address.lower()}" if address.startswith("0x") else f"{chain}:{token}"
                token_price_records[key] = {
                    "price_usd": str(price),
                    "symbol": token,
                    "decimals": decimals,
                }
            except Exception:
                # Best-effort: fall back to symbol-only key
                token_price_records[f"{chain}:{token}"] = {
                    "price_usd": str(price),
                    "symbol": token,
                    "decimals": None,
                }
        return token_price_records

    def _get_positions(
        self,
        strategy: StrategyLike,
        market: MarketDataSource,
        prices: dict[str, Decimal],
    ) -> tuple[list[PositionValue], Decimal, bool]:
        """Discover and value non-wallet positions.

        Two-source strategy:
        1. **Discovery** (primary): Framework-owned PositionDiscoveryService
           scans on-chain for lending/LP positions using strategy config.
        2. **Strategy** (supplementary): strategy.get_open_positions() provides
           position types that discovery can't detect (perps, stakes, etc.)
           and LP token IDs that discovery uses for scanning.

        Positions from both sources are deduplicated by position_id.
        All positions are re-priced with on-chain data when possible.

        Returns:
            (positions, total_position_value, positions_unavailable)
        """
        all_position_infos: dict[str, PositionInfo] = {}
        strategy_failed = False

        # Source 1: Strategy-reported positions (get_open_positions)
        strategy_positions, strategy_failed = self._get_strategy_positions(strategy)
        for p in strategy_positions:
            all_position_infos[p.position_id] = p

        # Source 2: Framework discovery (on-chain scanning)
        discovery_had_errors = False
        discovery_config = self._build_discovery_config(strategy, strategy_positions)
        if discovery_config:
            discovered = self._discovery.discover(discovery_config)
            if discovered.errors:
                discovery_had_errors = True
            # Track which (protocol, position_type) combos the strategy reported.
            # For perps, strategy-reported positions take priority since
            # discovery and strategy use different ID formats.
            from almanak.framework.teardown.models import PositionType as _PT

            strategy_protocol_types = {(sp.protocol, sp.position_type) for sp in strategy_positions}

            for p in discovered.positions:
                # Skip discovered perps if strategy already reported perps
                # for the same protocol (avoids double-counting from ID mismatch)
                if p.position_type == _PT.PERP and (p.protocol, _PT.PERP) in strategy_protocol_types:
                    continue

                if p.position_id not in all_position_infos:
                    all_position_infos[p.position_id] = p
                else:
                    # Discovery found the same position — merge:
                    # strategy has domain knowledge (position_type, value hint),
                    # discovery has fresh on-chain details (asset_address, wallet etc.)
                    existing = all_position_infos[p.position_id]
                    merged_details = {**existing.details, **p.details}
                    all_position_infos[p.position_id] = PositionInfo(
                        position_type=existing.position_type,  # Strategy knows best
                        position_id=p.position_id,
                        chain=p.chain,
                        protocol=p.protocol,
                        value_usd=existing.value_usd,  # Keep strategy value as hint
                        details=merged_details,
                    )

        positions_incomplete = strategy_failed or discovery_had_errors
        if not all_position_infos:
            return [], Decimal("0"), positions_incomplete

        # Re-price all positions and enrich details with valuer breakdown
        positions: list[PositionValue] = []
        for p in all_position_infos.values():
            value_usd, enriched_details = self._reprice_position_enriched(p, strategy.chain, market)

            # Merge enriched valuer details into position details
            merged_details = {**p.details, **enriched_details}

            positions.append(
                PositionValue(
                    position_type=p.position_type,
                    protocol=p.protocol,
                    chain=p.chain,
                    value_usd=value_usd,
                    label=f"{p.protocol} {p.position_type.value}",
                    tokens=p.details.get("tokens", []),
                    details=merged_details,
                )
            )

        position_value = sum((p.value_usd for p in positions), Decimal("0"))
        # Signal incomplete if strategy failed OR discovery had errors.
        # Even if some positions were found, we may be missing others.
        return positions, position_value, positions_incomplete

    def _get_strategy_positions(self, strategy: StrategyLike) -> tuple[list["PositionInfo"], bool]:
        """Get positions from strategy.get_open_positions(), gracefully.

        Returns:
            (positions, failed) — failed is True if get_open_positions raised.
        """
        if not hasattr(strategy, "get_open_positions"):
            return [], False
        try:
            summary: TeardownPositionSummary = strategy.get_open_positions()
            if summary and summary.positions:
                return list(summary.positions), False
            return [], False
        except Exception as e:
            logger.warning("Failed to get open positions: %s", e)
            return [], True

    def _build_discovery_config(
        self,
        strategy: StrategyLike,
        strategy_positions: list["PositionInfo"],
    ) -> DiscoveryConfig | None:
        """Build discovery config from strategy metadata.

        Returns None if we don't have enough information to discover anything.
        """
        try:
            chain = strategy.chain
            wallet = strategy.wallet_address
            if not chain or not wallet:
                return None

            # Get protocols from strategy metadata
            protocols: list[str] = []
            metadata = getattr(strategy, "STRATEGY_METADATA", None)
            if metadata and hasattr(metadata, "supported_protocols"):
                protocols = list(metadata.supported_protocols)

            # Get tracked tokens
            tracked_tokens: list[str] = []
            try:
                tracked_tokens = strategy._get_tracked_tokens()
            except Exception:
                pass

            # Extract LP token IDs from strategy-reported positions
            lp_token_ids: list[int] = []
            lp_protocol = "uniswap_v3"
            for p in strategy_positions:
                from almanak.framework.teardown.models import PositionType as PT

                if p.position_type == PT.LP:
                    token_id = self._extract_token_id(p)
                    if token_id is not None:
                        lp_token_ids.append(token_id)
                    if p.protocol:
                        lp_protocol = p.protocol

            if not protocols and not tracked_tokens:
                return None

            return DiscoveryConfig(
                chain=chain,
                wallet_address=wallet,
                protocols=protocols,
                tracked_tokens=tracked_tokens,
                lp_token_ids=lp_token_ids,
                lp_protocol=lp_protocol,
            )
        except Exception:
            logger.debug("Could not build discovery config", exc_info=True)
            return None

    def _reprice_position(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal:
        """Re-price a single position using on-chain data when possible.

        For LP positions: query on-chain V3 data and re-calculate value.
        For SUPPLY/BORROW: query on-chain Aave data and re-calculate value.
        For VAULT: query ERC-4626 share balance + convertToAssets via the
            vault adapter registry.
        For other types: pass through strategy-reported value.

        Falls back to strategy-reported value_usd on any failure.
        """
        from almanak.framework.teardown.models import PositionType

        if position.position_type == PositionType.LP:
            repriced = self._reprice_lp_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            repriced = self._reprice_lending_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            # Normalize fallback: BORROW should reduce portfolio (negative),
            # matching the on-chain path which returns -debt_value_usd.
            if position.position_type == PositionType.BORROW and position.value_usd > 0:
                return -position.value_usd
            return position.value_usd

        if position.position_type == PositionType.PERP:
            repriced = self._reprice_perps_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        if position.position_type == PositionType.VAULT:
            repriced = self._reprice_vault_on_chain(position, chain, market)
            if repriced is not None:
                return repriced
            return position.value_usd

        return position.value_usd

    def _reprice_position_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]]:
        """Re-price a position and return enriched details for persistence.

        Returns:
            (value_usd, enriched_details) where enriched_details contains
            the full valuer breakdown (amounts, ticks, health factor, etc.)
        """
        from almanak.framework.teardown.models import PositionType

        if position.position_type == PositionType.LP:
            result = self._reprice_lp_on_chain_enriched(position, chain, market)
            if result is not None:
                return result
            return position.value_usd, {}

        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            result = self._reprice_lending_on_chain_enriched(position, chain, market)
            if result is not None:
                return result
            if position.position_type == PositionType.BORROW and position.value_usd > 0:
                return -position.value_usd, {}
            return position.value_usd, {}

        if position.position_type == PositionType.PERP:
            result = self._reprice_perps_on_chain_enriched(position, chain, market)
            if result is not None:
                return result
            return position.value_usd, {}

        if position.position_type == PositionType.VAULT:
            result = self._reprice_vault_on_chain_enriched(position, chain, market)
            if result is not None:
                return result
            return position.value_usd, {}

        return position.value_usd, {}

    def _reprice_lp_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price LP and return enriched details for snapshot persistence."""
        try:
            token_id = self._extract_token_id(position)
            if token_id is None:
                return None

            on_chain = self._lp_reader.read_position(chain=chain, token_id=token_id, protocol=position.protocol)
            if on_chain is None:
                return None

            if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
                return Decimal("0"), {"position_id": str(token_id), "liquidity": "0"}

            token0_symbol = self._resolve_token_symbol(on_chain.token0, position, "token0")
            token1_symbol = self._resolve_token_symbol(on_chain.token1, position, "token1")
            if not token0_symbol or not token1_symbol:
                return None

            try:
                token0_price = Decimal(str(market.price(token0_symbol)))
                token1_price = Decimal(str(market.price(token1_symbol)))
            except Exception:
                return None

            if token0_price <= 0 or token1_price <= 0:
                return None

            token0_decimals = self._get_token_decimals(token0_symbol, chain)
            token1_decimals = self._get_token_decimals(token1_symbol, chain)
            if token0_decimals is None or token1_decimals is None:
                return None

            pool_address = position.details.get("pool") or position.details.get("pool_address")
            current_tick: int | None = None
            sqrt_price_x96: int | None = None
            if pool_address:
                slot0 = self._lp_reader.read_pool_slot0(chain, pool_address)
                if slot0:
                    current_tick = slot0.tick
                    sqrt_price_x96 = slot0.sqrt_price_x96

            if current_tick is None:
                current_tick = self._price_ratio_to_tick(token0_price, token1_price, token0_decimals, token1_decimals)

            lp_value = value_lp_position(
                liquidity=on_chain.liquidity,
                tick_lower=on_chain.tick_lower,
                tick_upper=on_chain.tick_upper,
                current_tick=current_tick,
                token0_price_usd=token0_price,
                token1_price_usd=token1_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sqrt_price_x96=sqrt_price_x96,
            )

            fees_usd = Decimal("0")
            fees0_human = Decimal("0")
            fees1_human = Decimal("0")
            if on_chain.tokens_owed0 > 0:
                fees0_human = Decimal(on_chain.tokens_owed0) / Decimal(10**token0_decimals)
                fees_usd += fees0_human * token0_price
            if on_chain.tokens_owed1 > 0:
                fees1_human = Decimal(on_chain.tokens_owed1) / Decimal(10**token1_decimals)
                fees_usd += fees1_human * token1_price

            total = lp_value.value_usd + fees_usd

            enriched = {
                "position_id": str(token_id),
                "amount0": str(lp_value.amount0),
                "amount1": str(lp_value.amount1),
                "token0_value_usd": str(lp_value.token0_value_usd),
                "token1_value_usd": str(lp_value.token1_value_usd),
                "in_range": lp_value.in_range,
                "tick_lower": on_chain.tick_lower,
                "tick_upper": on_chain.tick_upper,
                "liquidity": str(on_chain.liquidity),
                "fees0": str(fees0_human),
                "fees1": str(fees1_human),
                "fees_usd": str(fees_usd),
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "valuation_source": "on_chain",
            }

            return total, enriched

        except Exception:
            logger.debug("LP enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_lending_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price lending position and return enriched details."""
        from almanak.framework.teardown.models import PositionType

        try:
            asset_address = self._extract_asset_address(position)
            if not asset_address:
                asset_symbol = position.details.get("asset")
                if asset_symbol:
                    try:
                        from almanak.framework.data.tokens import get_token_resolver

                        resolved = get_token_resolver().resolve(asset_symbol, chain)
                        if resolved and resolved.address:
                            asset_address = resolved.address
                    except Exception:
                        pass

            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not asset_address or not wallet_address:
                return None

            on_chain = self._lending_reader.read_position(
                chain=chain, asset_address=asset_address, wallet_address=wallet_address
            )
            if on_chain is None:
                return None

            if not on_chain.is_active:
                return Decimal("0"), {"valuation_source": "on_chain", "is_active": False}

            token_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not token_symbol:
                token_symbol = position.details.get("asset")
            if not token_symbol:
                return None

            try:
                token_price = Decimal(str(market.price(token_symbol)))
            except Exception:
                return None

            if token_price <= 0:
                return None

            token_decimals = self._get_token_decimals(token_symbol, chain)
            if token_decimals is None:
                return None

            valued = value_lending_position(
                atoken_balance=on_chain.current_atoken_balance,
                stable_debt=on_chain.current_stable_debt,
                variable_debt=on_chain.current_variable_debt,
                token_price_usd=token_price,
                token_decimals=token_decimals,
                collateral_enabled=on_chain.usage_as_collateral_enabled,
                asset=token_symbol,
            )

            if position.position_type == PositionType.BORROW:
                result_value = -valued.debt_value_usd
            else:
                result_value = valued.net_value_usd

            enriched = {
                "supply_balance": str(valued.supply_balance),
                "supply_value_usd": str(valued.supply_value_usd),
                "stable_debt_balance": str(valued.stable_debt_balance),
                "variable_debt_balance": str(valued.variable_debt_balance),
                "debt_value_usd": str(valued.debt_value_usd),
                "net_value_usd": str(valued.net_value_usd),
                "collateral_enabled": valued.collateral_enabled,
                "health_factor": str(on_chain.health_factor) if hasattr(on_chain, "health_factor") else None,
                "valuation_source": "on_chain",
            }

            return result_value, enriched

        except Exception:
            logger.debug("Lending enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_perps_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price perps position and return enriched details."""
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not wallet_address:
                return None

            on_chain_positions = self._perps_reader.read_positions(chain, wallet_address)
            if not on_chain_positions:
                return None

            market_address = position.details.get("market", "").lower()
            if "is_long" not in position.details:
                return None
            is_long = position.details["is_long"]
            collateral_token = position.details.get("collateral_token", "").lower()

            matched = None
            for ocp in on_chain_positions:
                if ocp.market.lower() == market_address and ocp.is_long == is_long:
                    if collateral_token and ocp.collateral_token.lower() != collateral_token:
                        continue
                    matched = ocp
                    break

            if matched is None:
                return None

            index_token_symbol = self._resolve_perps_index_token(matched.market, chain)
            if not index_token_symbol:
                return None

            try:
                mark_price = Decimal(str(market.price(index_token_symbol)))
            except Exception:
                return None
            if mark_price <= 0:
                return None

            collateral_symbol = self._resolve_token_symbol(matched.collateral_token, position, "collateral_token")
            if not collateral_symbol:
                return None

            try:
                collateral_price = Decimal(str(market.price(collateral_symbol)))
            except Exception:
                return None
            if collateral_price <= 0:
                return None

            collateral_decimals = self._get_token_decimals(collateral_symbol, chain)
            index_decimals = self._get_perps_index_decimals(matched.market, chain)
            if collateral_decimals is None or index_decimals is None:
                return None

            valued = value_perps_position(
                size_in_usd=matched.size_in_usd,
                size_in_tokens=matched.size_in_tokens,
                collateral_amount=matched.collateral_amount,
                is_long=matched.is_long,
                mark_price_usd=mark_price,
                collateral_token_price_usd=collateral_price,
                collateral_token_decimals=collateral_decimals,
                index_token_decimals=index_decimals,
                market=matched.market,
            )

            enriched = {
                "market": valued.market,
                "is_long": valued.is_long,
                "size_usd": str(valued.size_usd),
                "collateral_value_usd": str(valued.collateral_value_usd),
                "entry_price_usd": str(valued.entry_price_usd),
                "mark_price_usd": str(valued.mark_price_usd),
                "unrealized_pnl_usd": str(valued.unrealized_pnl_usd),
                "pending_fees_usd": str(valued.pending_fees_usd),
                "leverage": str(valued.leverage),
                "valuation_source": "on_chain",
            }

            return valued.net_value_usd, enriched

        except Exception:
            logger.debug("Perps enriched re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_lp_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price an LP position using on-chain V3 math.

        Queries the NonfungiblePositionManager for full position data
        (tick range, liquidity), then calculates token amounts and
        prices them with live market data.

        Returns:
            USD value if successful, None to signal fallback needed.
        """
        try:
            # Need a numeric token ID for on-chain query
            token_id = self._extract_token_id(position)
            if token_id is None:
                return None

            # Query on-chain position details
            on_chain = self._lp_reader.read_position(
                chain=chain,
                token_id=token_id,
                protocol=position.protocol,
            )
            if on_chain is None:
                return None

            # Position with zero liquidity AND zero fees = truly empty
            if on_chain.liquidity == 0 and on_chain.tokens_owed0 == 0 and on_chain.tokens_owed1 == 0:
                return Decimal("0")

            # Resolve token symbols for pricing
            token0_symbol = self._resolve_token_symbol(on_chain.token0, position, "token0")
            token1_symbol = self._resolve_token_symbol(on_chain.token1, position, "token1")
            if not token0_symbol or not token1_symbol:
                return None

            # Get live prices
            try:
                token0_price = Decimal(str(market.price(token0_symbol)))
                token1_price = Decimal(str(market.price(token1_symbol)))
            except Exception:
                logger.debug("Could not get prices for LP tokens %s/%s", token0_symbol, token1_symbol)
                return None

            if token0_price <= 0 or token1_price <= 0:
                return None

            # Get token decimals -- abort if unknown (never guess)
            token0_decimals = self._get_token_decimals(token0_symbol, chain)
            token1_decimals = self._get_token_decimals(token1_symbol, chain)
            if token0_decimals is None or token1_decimals is None:
                logger.debug("Unknown decimals for LP tokens %s/%s, falling back", token0_symbol, token1_symbol)
                return None

            # Query pool slot0 for exact price and current tick
            pool_address = position.details.get("pool") or position.details.get("pool_address")
            current_tick: int | None = None
            sqrt_price_x96: int | None = None
            if pool_address:
                slot0 = self._lp_reader.read_pool_slot0(chain, pool_address)
                if slot0:
                    current_tick = slot0.tick
                    sqrt_price_x96 = slot0.sqrt_price_x96

            if current_tick is None:
                # Derive approximate tick from price ratio
                current_tick = self._price_ratio_to_tick(token0_price, token1_price, token0_decimals, token1_decimals)

            # Calculate position value using V3 math
            # Prefer exact sqrtPriceX96 for mid-tick precision in narrow ranges
            lp_value = value_lp_position(
                liquidity=on_chain.liquidity,
                tick_lower=on_chain.tick_lower,
                tick_upper=on_chain.tick_upper,
                current_tick=current_tick,
                token0_price_usd=token0_price,
                token1_price_usd=token1_price,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                sqrt_price_x96=sqrt_price_x96,
            )

            # Add uncollected fees
            fees_usd = Decimal("0")
            if on_chain.tokens_owed0 > 0:
                fees_usd += Decimal(on_chain.tokens_owed0) / Decimal(10**token0_decimals) * token0_price
            if on_chain.tokens_owed1 > 0:
                fees_usd += Decimal(on_chain.tokens_owed1) / Decimal(10**token1_decimals) * token1_price

            total = lp_value.value_usd + fees_usd

            logger.debug(
                "LP re-priced: position=%s value=$%s (lp=$%s fees=$%s) in_range=%s",
                position.position_id,
                total,
                lp_value.value_usd,
                fees_usd,
                lp_value.in_range,
            )

            return total

        except Exception:
            logger.debug("LP on-chain re-pricing failed for %s", position.position_id, exc_info=True)
            return None

    def _reprice_lending_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price a lending position using on-chain Aave V3 data.

        Queries getUserReserveData for the position's asset and calculates
        supply value and/or debt value using live prices.

        For SUPPLY positions: returns supply_value - debt_value (net).
        For BORROW positions: returns negative debt_value_usd so it
        reduces the portfolio total when summed.

        Returns:
            USD value if successful, None to signal fallback needed.
        """
        from almanak.framework.teardown.models import PositionType

        try:
            # Need asset address and wallet address
            asset_address = self._extract_asset_address(position)

            # Fallback: resolve asset address from symbol via TokenResolver
            if not asset_address:
                asset_symbol = position.details.get("asset")
                if asset_symbol:
                    try:
                        from almanak.framework.data.tokens import get_token_resolver

                        resolved = get_token_resolver().resolve(asset_symbol, chain)
                        if resolved and resolved.address:
                            asset_address = resolved.address
                    except Exception:
                        pass

            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not asset_address or not wallet_address:
                return None

            # Query on-chain position
            on_chain = self._lending_reader.read_position(
                chain=chain,
                asset_address=asset_address,
                wallet_address=wallet_address,
            )
            if on_chain is None:
                return None

            # No supply and no debt = truly empty
            if not on_chain.is_active:
                return Decimal("0")

            # Resolve token symbol for pricing
            token_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not token_symbol:
                # Try the asset field directly
                token_symbol = position.details.get("asset")
            if not token_symbol:
                return None

            # Get live price
            try:
                token_price = Decimal(str(market.price(token_symbol)))
            except Exception:
                logger.debug("Could not get price for lending token %s", token_symbol)
                return None

            if token_price <= 0:
                return None

            # Get token decimals
            token_decimals = self._get_token_decimals(token_symbol, chain)
            if token_decimals is None:
                logger.debug("Unknown decimals for lending token %s, falling back", token_symbol)
                return None

            # Calculate value
            valued = value_lending_position(
                atoken_balance=on_chain.current_atoken_balance,
                stable_debt=on_chain.current_stable_debt,
                variable_debt=on_chain.current_variable_debt,
                token_price_usd=token_price,
                token_decimals=token_decimals,
                collateral_enabled=on_chain.usage_as_collateral_enabled,
                asset=token_symbol,
            )

            # For SUPPLY positions: return net value (supply - debt).
            # For BORROW positions: return negative debt value so it
            # reduces the portfolio total when summed in _get_positions.
            if position.position_type == PositionType.BORROW:
                result = -valued.debt_value_usd
            else:
                result = valued.net_value_usd

            logger.debug(
                "Lending re-priced: position=%s type=%s value=$%s (supply=$%s debt=$%s) collateral=%s",
                position.position_id,
                position.position_type.value,
                result,
                valued.supply_value_usd,
                valued.debt_value_usd,
                valued.collateral_enabled,
            )

            return result

        except Exception:
            logger.debug(
                "Lending on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

    def _reprice_perps_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price a GMX V2 perp position using on-chain data.

        Queries the wallet's open GMX V2 positions, matches by market address
        and direction (long/short), then computes mark-to-market value using
        the perps_valuer pure math.

        Returns:
            Net USD value (collateral + unrealized PnL - fees) if successful,
            None to signal fallback needed.
        """
        try:
            # Need wallet address and market info from position details
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            if not wallet_address:
                return None

            # Query all positions for this wallet
            on_chain_positions = self._perps_reader.read_positions(chain, wallet_address)
            if not on_chain_positions:
                return None

            # Match position by market address and direction
            market_address = position.details.get("market", "").lower()
            if "is_long" not in position.details:
                # Direction is money-critical — never assume long/short
                return None
            is_long = position.details["is_long"]
            collateral_token = position.details.get("collateral_token", "").lower()

            matched = None
            for ocp in on_chain_positions:
                if ocp.market.lower() == market_address and ocp.is_long == is_long:
                    # If collateral token specified, match it too
                    if collateral_token and ocp.collateral_token.lower() != collateral_token:
                        continue
                    matched = ocp
                    break

            if matched is None:
                logger.debug(
                    "No matching GMX V2 position found for %s (market=%s, is_long=%s)",
                    position.position_id,
                    market_address,
                    is_long,
                )
                return None

            # Resolve index token price (mark price)
            index_token_symbol = self._resolve_perps_index_token(matched.market, chain)
            if not index_token_symbol:
                return None

            try:
                mark_price = Decimal(str(market.price(index_token_symbol)))
            except Exception:
                logger.debug("Could not get mark price for %s", index_token_symbol)
                return None

            if mark_price <= 0:
                return None

            # Resolve collateral token price
            collateral_symbol = self._resolve_token_symbol(matched.collateral_token, position, "collateral_token")
            if not collateral_symbol:
                return None

            try:
                collateral_price = Decimal(str(market.price(collateral_symbol)))
            except Exception:
                logger.debug("Could not get collateral price for %s", collateral_symbol)
                return None

            if collateral_price <= 0:
                return None

            # Get token decimals
            collateral_decimals = self._get_token_decimals(collateral_symbol, chain)
            index_decimals = self._get_perps_index_decimals(matched.market, chain)
            if collateral_decimals is None or index_decimals is None:
                return None

            # Compute mark-to-market value.
            # Note: pending funding/borrowing fees are NOT included yet —
            # computing them requires cumulative rate data from DataStore.
            # Net value is therefore an upper bound (fees would reduce it).
            valued = value_perps_position(
                size_in_usd=matched.size_in_usd,
                size_in_tokens=matched.size_in_tokens,
                collateral_amount=matched.collateral_amount,
                is_long=matched.is_long,
                mark_price_usd=mark_price,
                collateral_token_price_usd=collateral_price,
                collateral_token_decimals=collateral_decimals,
                index_token_decimals=index_decimals,
                market=matched.market,
            )

            logger.debug(
                "Perps re-priced: position=%s value=$%s (size=$%s pnl=$%s fees=$%s leverage=%sx)",
                position.position_id,
                valued.net_value_usd,
                valued.size_usd,
                valued.unrealized_pnl_usd,
                valued.pending_fees_usd,
                valued.leverage,
            )

            return valued.net_value_usd

        except Exception:
            logger.debug(
                "Perps on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

    def _reprice_vault_on_chain(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> Decimal | None:
        """Attempt to re-price an ERC-4626 vault position using on-chain data.

        Reads share balance via the vault registry and converts to underlying
        asset amount using the vault's PPFS / convertToAssets. Closes the
        silent zero-valuation gap that today affects MetaMorpho positions.

        Returns USD value if successful, None to signal fallback needed.
        """
        result = self._reprice_vault_on_chain_enriched(position, chain, market)
        if result is None:
            return None
        return result[0]

    def _reprice_vault_on_chain_enriched(
        self,
        position: "PositionInfo",
        chain: str,
        market: MarketDataSource,
    ) -> tuple[Decimal, dict[str, Any]] | None:
        """Re-price a vault position and return enriched details for snapshots."""
        try:
            wallet_address = (
                position.details.get("wallet")
                or position.details.get("wallet_address")
                or position.details.get("owner")
            )
            vault_address = position.details.get("vault_address") or position.details.get("vault")
            protocol = position.protocol
            if not wallet_address or not vault_address or not protocol:
                return None

            on_chain = self._vault_reader.read_position(
                protocol=protocol,
                chain=chain,
                vault_address=vault_address,
                wallet_address=wallet_address,
            )
            if on_chain is None:
                return None

            if not on_chain.is_active:
                return Decimal("0"), {
                    "vault_address": vault_address,
                    "shares_wei": "0",
                    "asset_amount_wei": "0",
                }

            # Resolve underlying asset symbol for pricing
            asset_symbol = self._resolve_token_symbol(on_chain.asset_address, position, "asset")
            if not asset_symbol:
                asset_symbol = position.details.get("asset")
            if not asset_symbol:
                logger.debug(
                    "Vault re-pricing: cannot resolve asset symbol for %s (asset=%s)",
                    position.position_id,
                    on_chain.asset_address,
                )
                return None

            try:
                asset_price = Decimal(str(market.price(asset_symbol)))
            except Exception:
                logger.debug("Could not get price for vault asset %s", asset_symbol)
                return None

            if asset_price <= 0:
                return None

            asset_decimals = on_chain.asset_decimals
            if asset_decimals <= 0:
                # Defensive: fall back to token resolver if the on-chain decimals() read returned 0.
                resolved = self._get_token_decimals(asset_symbol, chain)
                if resolved is None:
                    return None
                asset_decimals = resolved

            asset_amount = Decimal(on_chain.asset_amount_wei) / Decimal(10**asset_decimals)
            value_usd = asset_amount * asset_price

            details: dict[str, Any] = {
                "vault_address": vault_address,
                "asset_address": on_chain.asset_address,
                "asset_symbol": asset_symbol,
                "shares_wei": str(on_chain.shares_wei),
                "asset_amount_wei": str(on_chain.asset_amount_wei),
                "asset_amount": str(asset_amount),
                "asset_price_usd": str(asset_price),
            }

            logger.debug(
                "Vault re-priced: position=%s protocol=%s value=$%s (shares=%s assets=%s %s)",
                position.position_id,
                protocol,
                value_usd,
                on_chain.shares_wei,
                asset_amount,
                asset_symbol,
            )

            return value_usd, details

        except Exception:
            logger.debug(
                "Vault on-chain re-pricing failed for %s",
                position.position_id,
                exc_info=True,
            )
            return None

    @staticmethod
    def _resolve_perps_index_token(market_address: str, chain: str) -> str | None:
        """Map a GMX V2 market address to its index token symbol.

        Uses the market address tables from the GMX V2 adapter.
        """
        try:
            from almanak.framework.connectors.gmx_v2.adapter import GMX_V2_MARKETS

            markets = GMX_V2_MARKETS.get(chain, {})
            addr_lower = market_address.lower()
            for name, addr in markets.items():
                if addr.lower() == addr_lower:
                    # name is like "ETH/USD" — extract index token
                    return name.split("/")[0]
        except ImportError:
            pass
        return None

    @staticmethod
    def _get_perps_index_decimals(market_address: str, chain: str) -> int | None:
        """Get the index token decimals for a GMX V2 market.

        Uses the decimal table from the GMX V2 adapter.
        Case-insensitive lookup to handle both checksummed and lowercased addresses.
        """
        try:
            from almanak.framework.connectors.gmx_v2.adapter import _GMX_V2_INDEX_TOKEN_DECIMALS

            chain_decimals = _GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {})
            addr_lower = market_address.lower()
            for addr, decimals in chain_decimals.items():
                if addr.lower() == addr_lower:
                    return decimals
        except ImportError:
            pass
        return None

    @staticmethod
    def _extract_asset_address(position: "PositionInfo") -> str | None:
        """Extract the underlying asset address from position details."""
        for key in ("asset_address", "assetAddress", "token_address", "underlying"):
            val = position.details.get(key)
            if val and isinstance(val, str) and len(val) >= 40:
                return val
        return None

    @staticmethod
    def _extract_token_id(position: "PositionInfo") -> int | None:
        """Extract numeric NFT token ID from position data."""
        pid = position.position_id
        if not pid:
            return None

        # Try direct numeric parse
        try:
            token_id = int(pid)
            if token_id >= 0:
                return token_id
        except (ValueError, TypeError):
            pass

        # Check details dict
        for key in ("token_id", "tokenId", "nft_id"):
            val = position.details.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass

        return None

    @staticmethod
    def _resolve_token_symbol(
        token_address: str,
        position: "PositionInfo",
        field_name: str,
    ) -> str | None:
        """Resolve a token address to a symbol.

        Prefers the authoritative on-chain address via TokenResolver,
        then falls back to strategy-reported metadata.
        """
        # Primary: resolve from on-chain address (authoritative)
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            resolved = resolver.resolve(token_address, position.chain)
            if resolved and resolved.symbol:
                return resolved.symbol
        except Exception:
            pass

        # Fallback: strategy-reported metadata
        symbol = position.details.get(field_name)
        if symbol:
            return symbol

        # Fallback: tokens list (LP-specific — only valid for token0/token1)
        if field_name in ("token0", "token1"):
            tokens = position.details.get("tokens", [])
            idx = 0 if field_name == "token0" else 1
            if len(tokens) > idx:
                return tokens[idx]

        return None

    @staticmethod
    def _get_token_decimals(symbol: str, chain: str) -> int | None:
        """Get token decimals. Returns None if unknown (never defaults to 18).

        Per codebase rules: "NEVER default to 18 decimals -- always raise
        TokenNotFoundError if decimals unknown."
        """
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            return resolver.get_decimals(chain, symbol)
        except Exception:
            return None

    @staticmethod
    def _price_ratio_to_tick(
        token0_price: Decimal,
        token1_price: Decimal,
        token0_decimals: int,
        token1_decimals: int,
    ) -> int:
        """Derive approximate V3 tick from USD prices and decimals.

        V3 price = token1_amount / token0_amount (in wei terms).
        tick = log(price) / log(1.0001)
        """
        import math

        if token0_price <= 0 or token1_price <= 0:
            return 0

        # V3 price is token1/token0 in wei terms
        # price = (token0_price / token1_price) * (10^token1_decimals / 10^token0_decimals)
        price_ratio = float(token0_price / token1_price) * (10**token1_decimals / 10**token0_decimals)

        if price_ratio <= 0:
            return 0

        # tick = log(price) / log(1.0001)
        tick = math.log(price_ratio) / math.log(1.0001)
        return int(tick)
