"""Portfolio valuation orchestrator.

Produces PortfolioSnapshot by querying the gateway (via MarketSnapshot)
for wallet balances and token prices, using the PositionDiscoveryService
to proactively find on-chain positions (LP, lending, perps), with
strategy.get_open_positions() as a supplementary source.

This is the single source of truth for portfolio valuation at runtime.
The framework owns both discovery and math.
"""

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
from almanak.framework.valuation.position_discovery import (
    DiscoveryConfig,
    PositionDiscoveryService,
)
from almanak.framework.valuation.spot_valuer import total_value, value_tokens

if TYPE_CHECKING:
    from almanak.framework.teardown.models import TeardownPositionSummary

logger = logging.getLogger(__name__)


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
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
        self._discovery = PositionDiscoveryService(gateway_client)

    def set_gateway_client(self, gateway_client: object | None) -> None:
        """Update the gateway client for on-chain queries.

        Called by StrategyRunner once the gateway connection is established.
        """
        self._lp_reader = LPPositionReader(gateway_client)
        self._lending_reader = LendingPositionReader(gateway_client)
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

            return PortfolioSnapshot(
                timestamp=now,
                strategy_id=strategy_id,
                total_value_usd=wallet_value + position_value,
                available_cash_usd=wallet_value,
                value_confidence=confidence,
                positions=positions,
                wallet_balances=wallet_balances,
                chain=chain,
                iteration_number=iteration_number,
            )

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
            for p in discovered.positions:
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

        # Re-price all positions
        positions: list[PositionValue] = []
        for p in all_position_infos.values():
            value_usd = self._reprice_position(p, strategy.chain, market)

            positions.append(
                PositionValue(
                    position_type=p.position_type,
                    protocol=p.protocol,
                    chain=p.chain,
                    value_usd=value_usd,
                    label=f"{p.protocol} {p.position_type.value}",
                    tokens=p.details.get("tokens", []),
                    details=p.details,
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

        return position.value_usd

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
