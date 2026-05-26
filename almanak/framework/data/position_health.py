"""Position health monitoring for lending protocols.

Provides health factor calculations and deleverage trigger detection
for Aave V3, Morpho Blue and Compound V3 positions, with special support
for PT-collateralized positions on Pendle.

Key Classes:
    PositionHealth: Health factor and risk metrics for a lending position
    PTPositionHealth: Extended health data for PT-collateral positions
    DeleverageTrigger: Warning/critical thresholds for automated deleverage
    HealthFactorProvider: Protocol for per-protocol health-factor adapters
    PositionHealthProvider: Reads on-chain position data and computes health

Module-level API:
    get_health_factor(chain, protocol, wallet, market, ...) -> Decimal
        Unified dispatch that returns just the health factor for a lending
        position. Uses each protocol's canonical on-chain source.
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PositionHealth:
    """Health factor and risk metrics for a lending position.

    Attributes:
        health_factor: Ratio of (collateral * LLTV) / debt.
            > 1.0 = healthy, < 1.0 = liquidatable, Infinity = no debt.
        collateral_value_usd: USD value of deposited collateral.
        debt_value_usd: USD value of outstanding debt.
        lltv: Liquidation loan-to-value ratio of the market.
        max_borrow_usd: Additional USD borrowable before liquidation.
        protocol: Protocol name ("morpho_blue", "aave_v3").
        market_id: Protocol-specific market identifier.
    """

    health_factor: Decimal
    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    lltv: Decimal
    max_borrow_usd: Decimal = Decimal("0")
    protocol: str = ""
    market_id: str = ""

    @property
    def is_healthy(self) -> bool:
        """Position is above liquidation threshold."""
        return self.health_factor >= Decimal("1.0")

    @property
    def is_warning(self) -> bool:
        """Position is below the safe threshold (< 1.5) but still healthy."""
        return Decimal("1.0") <= self.health_factor < Decimal("1.5")

    @property
    def is_critical(self) -> bool:
        """Position is very close to liquidation (< 1.1)."""
        return self.health_factor < Decimal("1.1")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "health_factor": str(self.health_factor),
            "collateral_value_usd": str(self.collateral_value_usd),
            "debt_value_usd": str(self.debt_value_usd),
            "lltv": str(self.lltv),
            "max_borrow_usd": str(self.max_borrow_usd),
            "is_healthy": self.is_healthy,
            "is_warning": self.is_warning,
            "is_critical": self.is_critical,
            "protocol": self.protocol,
            "market_id": self.market_id,
        }


@dataclass
class PTPositionHealth(PositionHealth):
    """Extended health data for PT-collateral positions.

    Adds Pendle-specific metrics: implied APY at liquidation point,
    PT discount, and maturity risk indicators.
    """

    implied_apy: Decimal = Decimal("0")
    pt_discount_pct: Decimal = Decimal("0")
    days_to_maturity: int = 0
    pendle_market: str = ""

    @property
    def maturity_risk(self) -> str:
        """Assess maturity-related risk."""
        if self.days_to_maturity < 0:
            return "unknown"
        elif self.days_to_maturity == 0:
            return "expired"
        elif self.days_to_maturity <= 7:
            return "imminent"
        elif self.days_to_maturity <= 30:
            return "near"
        return "safe"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        base = super().to_dict()
        base.update(
            {
                "implied_apy": str(self.implied_apy),
                "pt_discount_pct": str(self.pt_discount_pct),
                "days_to_maturity": self.days_to_maturity,
                "pendle_market": self.pendle_market,
                "maturity_risk": self.maturity_risk,
            }
        )
        return base


@dataclass
class DeleverageTrigger:
    """Thresholds that determine when to deleverage a position.

    Used by strategies to automate position management:
    - warning_hf: Log a warning and potentially start preparing unwind
    - critical_hf: Trigger immediate deleverage to safe_target_hf
    """

    warning_hf: Decimal = Decimal("1.5")
    critical_hf: Decimal = Decimal("1.2")
    safe_target_hf: Decimal = Decimal("2.0")
    max_leverage: Decimal = Decimal("10")

    def should_deleverage(self, health: PositionHealth) -> bool:
        """Check if health factor is below critical threshold."""
        return health.health_factor < self.critical_hf

    def should_warn(self, health: PositionHealth) -> bool:
        """Check if health factor is in warning zone."""
        return health.health_factor < self.warning_hf


# =============================================================================
# Position Health Provider
# =============================================================================


@dataclass
class _CachedHealth:
    """Internal cache entry for position health data."""

    health: PositionHealth
    block_number: int = 0


class PositionHealthProvider:
    """Reads on-chain position data and computes health factors.

    Supports Morpho Blue and Aave V3 protocols. For Morpho positions
    with PT collateral, can also compute PT-specific risk metrics.

    Usage:
        provider = PositionHealthProvider(rpc_url="https://...", chain="ethereum")
        health = provider.get_health("morpho_blue", market_id, wallet_address)
        print(f"Health factor: {health.health_factor}")
    """

    def __init__(
        self,
        rpc_url: str = "",
        chain: str = "ethereum",
        price_oracle: Any = None,
        gateway_client: "GatewayClient | None" = None,
    ):
        self._rpc_url = rpc_url
        self._chain = chain
        self._price_oracle = price_oracle
        self._gateway_client = gateway_client
        self._cache: dict[str, _CachedHealth] = {}

    def get_health(
        self,
        protocol: str,
        market_id: str,
        user_address: str,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> PositionHealth:
        """Get health factor for a lending position.

        Args:
            protocol: "morpho_blue", "aave_v3", or "compound_v3"
            market_id: Protocol-specific market identifier. For Aave V3 this
                is ignored / informational (one pool per chain). For Morpho
                Blue this is the bytes32 market id. For Compound V3 this
                is the market key (e.g. "usdc", "weth") used to look up the
                Comet contract.
            user_address: Wallet address holding the position
            collateral_price_usd: Optional override for collateral price
            debt_price_usd: Optional override for debt token price

        Returns:
            PositionHealth with computed health factor

        Raises:
            ValueError: If protocol is unsupported
        """
        protocol_lower = _normalize_protocol(protocol)
        if protocol_lower == "morpho_blue":
            return self._get_morpho_health(market_id, user_address, collateral_price_usd, debt_price_usd)
        elif protocol_lower == "aave_v3":
            return self._get_aave_health(market_id, user_address)
        elif protocol_lower == "compound_v3":
            return self._get_compound_health(market_id, user_address)
        else:
            raise ValueError(f"Unsupported protocol for health monitoring: {protocol}")

    def get_pt_position_health(
        self,
        morpho_market_id: str,
        pendle_market_address: str,
        user_address: str,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> PTPositionHealth:
        """Get extended health data for a PT-collateral position on Morpho.

        Combines Morpho position data with Pendle market data for
        comprehensive risk assessment.

        Args:
            morpho_market_id: Morpho Blue market ID
            pendle_market_address: Pendle market address for the PT
            user_address: Wallet address
            collateral_price_usd: Override for PT collateral price
            debt_price_usd: Override for debt token price

        Returns:
            PTPositionHealth with Morpho + Pendle risk metrics
        """
        # Get base Morpho health
        base_health = self._get_morpho_health(morpho_market_id, user_address, collateral_price_usd, debt_price_usd)

        # Get Pendle-specific data
        implied_apy = Decimal("0")
        pt_discount_pct = Decimal("0")
        days_to_maturity = 0

        try:
            from almanak.framework.data.pendle.on_chain_reader import PendleOnChainReader

            if self._gateway_client is not None:
                reader = PendleOnChainReader(gateway_client=self._gateway_client, chain=self._chain)
            else:
                reader = PendleOnChainReader(rpc_url=self._rpc_url, chain=self._chain)
            implied_apy = reader.get_implied_apy(pendle_market_address)

            # Check if market is expired
            if reader.is_market_expired(pendle_market_address):
                days_to_maturity = 0
            else:
                # Estimate days from PT discount (PT trades below 1:1 before maturity)
                pt_rate = reader.get_pt_to_asset_rate(pendle_market_address)
                if pt_rate < Decimal("1"):
                    pt_discount_pct = (Decimal("1") - pt_rate) * Decimal("100")
                    # Rough estimate: if APY is known, days ~ discount / (APY / 365)
                    if implied_apy > 0:
                        daily_rate = implied_apy / Decimal("365")
                        if daily_rate > 0:
                            days_to_maturity = int(pt_discount_pct / Decimal("100") / daily_rate)

        except Exception as e:
            logger.warning(f"Failed to get Pendle data for PT health: {e}")

        return PTPositionHealth(
            health_factor=base_health.health_factor,
            collateral_value_usd=base_health.collateral_value_usd,
            debt_value_usd=base_health.debt_value_usd,
            lltv=base_health.lltv,
            max_borrow_usd=base_health.max_borrow_usd,
            protocol=base_health.protocol,
            market_id=base_health.market_id,
            implied_apy=implied_apy,
            pt_discount_pct=pt_discount_pct,
            days_to_maturity=days_to_maturity,
            pendle_market=pendle_market_address,
        )

    def _get_morpho_health(
        self,
        market_id: str,
        user_address: str,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> PositionHealth:
        """Compute health factor from Morpho Blue on-chain data."""
        try:
            from almanak.connectors.morpho_blue.sdk import MorphoBlueSDK

            if self._gateway_client is not None and not self._gateway_client.is_connected:
                raise ValueError(
                    f"GatewayClient is not connected; cannot fetch Morpho Blue health for market {market_id[:10]}..."
                )
            sdk = MorphoBlueSDK(
                rpc_url=self._rpc_url,
                chain=self._chain,
                gateway_client=self._gateway_client,
            )
            position = sdk.get_position(market_id, user_address)
            market_params = sdk.get_market_params(market_id)

            # Extract position values
            collateral = Decimal(str(position.collateral))
            borrow_shares = Decimal(str(position.borrow_shares))

            # Get market state for share-to-amount conversion
            market_state = sdk.get_market_state(market_id)
            if market_state.total_borrow_shares > 0:
                debt_amount = (
                    borrow_shares
                    * Decimal(str(market_state.total_borrow_assets))
                    / Decimal(str(market_state.total_borrow_shares))
                )
            else:
                debt_amount = Decimal("0")

            lltv = Decimal(str(market_params.lltv)) / Decimal("1e18")

            # For cross-asset markets, prices are required to avoid silent miscalculation
            collateral_token = market_params.collateral_token.lower()
            loan_token = market_params.loan_token.lower()

            if collateral_token != loan_token:
                if collateral_price_usd is None or debt_price_usd is None:
                    raise ValueError(
                        f"Price overrides required for cross-asset Morpho market {market_id}. "
                        f"Collateral and debt tokens differ -- cannot default to 1:1."
                    )
                col_price = collateral_price_usd
                d_price = debt_price_usd
            else:
                # Same-asset market: 1:1 is safe
                col_price = collateral_price_usd if collateral_price_usd is not None else Decimal("1")
                d_price = debt_price_usd if debt_price_usd is not None else Decimal("1")

            collateral_value = collateral * col_price
            debt_value = debt_amount * d_price

            if debt_value == 0:
                health_factor = Decimal("Infinity")
            elif collateral_value == 0:
                health_factor = Decimal("0")
            else:
                health_factor = (collateral_value * lltv) / debt_value

            max_borrow = collateral_value * lltv - debt_value
            if max_borrow < 0:
                max_borrow = Decimal("0")

            return PositionHealth(
                health_factor=health_factor,
                collateral_value_usd=collateral_value,
                debt_value_usd=debt_value,
                lltv=lltv,
                max_borrow_usd=max_borrow,
                protocol="morpho_blue",
                market_id=market_id,
            )

        except Exception as e:
            logger.error(f"Failed to get Morpho health for market {market_id[:10]}...: {e}")
            raise

    def _get_aave_health(
        self,
        market_id: str,
        user_address: str,
    ) -> PositionHealth:
        """Compute health factor from Aave V3 on-chain data.

        Aave V3's LendingPool.getUserAccountData() returns healthFactor directly.
        """
        try:
            from web3 import Web3

            if self._gateway_client is not None:
                from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

                if not self._gateway_client.is_connected:
                    raise ValueError(f"GatewayClient is not connected; cannot fetch Aave V3 health on {self._chain}.")
                w3 = Web3(GatewayWeb3Provider(self._gateway_client, chain=self._chain))
            else:
                w3 = Web3(Web3.HTTPProvider(self._rpc_url))

            # Aave V3 Pool addresses by chain
            aave_pool_addresses = {
                "ethereum": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
                "arbitrum": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
                "optimism": "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
                "base": "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5",
            }

            pool_address = aave_pool_addresses.get(self._chain)
            if not pool_address:
                raise ValueError(f"Aave V3 not configured for chain: {self._chain}")

            # Minimal ABI for getUserAccountData
            abi = [
                {
                    "name": "getUserAccountData",
                    "type": "function",
                    "inputs": [{"name": "user", "type": "address"}],
                    "outputs": [
                        {"name": "totalCollateralBase", "type": "uint256"},
                        {"name": "totalDebtBase", "type": "uint256"},
                        {"name": "availableBorrowsBase", "type": "uint256"},
                        {"name": "currentLiquidationThreshold", "type": "uint256"},
                        {"name": "ltv", "type": "uint256"},
                        {"name": "healthFactor", "type": "uint256"},
                    ],
                }
            ]

            pool = w3.eth.contract(address=w3.to_checksum_address(pool_address), abi=abi)
            result = pool.functions.getUserAccountData(w3.to_checksum_address(user_address)).call()

            # Aave returns values in base currency units (USD with 8 decimals)
            collateral_value = Decimal(str(result[0])) / Decimal("1e8")
            debt_value = Decimal(str(result[1])) / Decimal("1e8")
            available_borrow = Decimal(str(result[2])) / Decimal("1e8")
            liq_threshold = Decimal(str(result[3])) / Decimal("10000")  # basis points
            health_factor_raw = Decimal(str(result[5])) / Decimal("1e18")

            if debt_value == 0:
                health_factor_raw = Decimal("Infinity")

            return PositionHealth(
                health_factor=health_factor_raw,
                collateral_value_usd=collateral_value,
                debt_value_usd=debt_value,
                lltv=liq_threshold,
                max_borrow_usd=available_borrow,
                protocol="aave_v3",
                market_id=market_id,
            )

        except Exception as e:
            logger.error(f"Failed to get Aave health: {e}")
            raise

    # Base tokens that are USD-pegged stablecoins. These are the *only* symbols
    # for which it is safe to assume price == $1 when no external price oracle
    # is provided. Anything else (WETH, AERO, WBTC, etc.) MUST have an
    # explicit ``price_oracle`` or the HF computation fails closed.
    _STABLE_BASE_SYMBOLS: frozenset[str] = frozenset(
        {"USDC", "USDT", "USDS", "USDC.E", "DAI", "FRAX", "LUSD", "USDBC", "SUSDS"}
    )

    def _resolve_base_decimals(self, symbol: str, address: str) -> int:
        """Resolve base-token decimals via the unified TokenResolver.

        Raises on failure. NEVER guesses -- a wrong decimals value silently
        mis-scales debt by orders of magnitude (e.g. WETH=18 vs USDC=6 is a
        1e12 miscalculation). Per CLAUDE.md: "NEVER default to 18 decimals
        - always raise TokenNotFoundError if decimals unknown."
        """
        from almanak.framework.data.tokens import TokenNotFoundError, get_token_resolver

        try:
            resolver = get_token_resolver()
            token = resolver.resolve(symbol, self._chain)
            return int(token.decimals)
        except TokenNotFoundError:
            raise
        except Exception as e:
            raise TokenNotFoundError(
                token=symbol,
                chain=self._chain,
                reason=(
                    f"Could not resolve decimals for Compound V3 base token "
                    f"{symbol!r} ({address}) on {self._chain}: {e}. "
                    f"Refusing to guess -- a wrong decimals value silently mis-scales debt."
                ),
            ) from e

    def _resolve_base_price(self, symbol: str) -> Decimal:
        """Resolve USD price for a Compound V3 base token.

        Price-source protocol, in order:
          1. ``price_oracle`` supports ``.get_aggregated_price(symbol, quote)``
             (async PriceOracle Protocol) -> await it.
          2. ``price_oracle`` is a plain callable ``symbol -> number`` -> call it.
          3. ``price_oracle`` is None AND ``symbol`` is a known USD stablecoin
             -> return ``Decimal("1")``.
          4. Otherwise -> raise. Silent ``base_price = 1`` on non-stable bases
             would inflate reported HF by orders of magnitude.
        """
        oracle = self._price_oracle
        if oracle is not None:
            # Case 1: PriceOracle Protocol (async).
            get_price = getattr(oracle, "get_aggregated_price", None)
            if get_price is not None and callable(get_price):
                try:
                    coro = get_price(symbol, "USD")
                    # May be a coroutine -> run to completion.
                    import asyncio
                    import inspect

                    if inspect.iscoroutine(coro):
                        try:
                            # Detect a running loop; if we're inside one, dispatch to a thread.
                            asyncio.get_running_loop()
                            import concurrent.futures

                            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                                result = ex.submit(asyncio.run, coro).result()
                        except RuntimeError:
                            # No running loop -> safe to use asyncio.run directly.
                            result = asyncio.run(coro)
                    else:
                        result = coro
                    price = getattr(result, "price", result)
                    return Decimal(str(price))
                except Exception as e:
                    logger.warning(f"Compound V3 PriceOracle({symbol}) failed: {e}")
            # Case 2: plain callable.
            elif callable(oracle):
                try:
                    return Decimal(str(oracle(symbol)))
                except Exception as e:
                    logger.warning(f"Compound V3 price_oracle({symbol}) failed: {e}")

        # Case 3: stablecoin 1:1 fallback.
        if symbol.upper() in self._STABLE_BASE_SYMBOLS:
            return Decimal("1")

        # Case 4: fail closed -- refuse to silently mis-price non-stable bases.
        raise ValueError(
            f"Compound V3 base token {symbol!r} is not a recognized USD stablecoin "
            f"and no working price_oracle was provided. Pass an async PriceOracle "
            f"(with .get_aggregated_price) or a callable(symbol)->Decimal to "
            f"PositionHealthProvider / MarketSnapshot.position_health() to avoid "
            f"inflating reported health factor."
        )

    def _get_compound_health(
        self,
        market_id: str,
        user_address: str,
    ) -> PositionHealth:
        """Compute health factor from Compound V3 (Comet) on-chain data.

        Compound V3 exposes ``isLiquidatable(account)`` and
        ``getBorrowCollateralFactor(asset)`` / ``getLiquidateCollateralFactor(asset)``
        on the Comet contract. We use the adapter's canonical calculation
        (liquidation_threshold_usd / borrow_value_usd) rather than reinventing
        the formula so single-source-of-truth is preserved.

        Args:
            market_id: Comet market key (e.g. "usdc", "weth"). Must be a key
                in ``COMPOUND_V3_COMET_ADDRESSES[chain]``.
            user_address: Wallet to inspect.
        """
        try:
            from web3 import Web3

            from almanak.connectors.compound_v3.adapter import (
                COMPOUND_V3_COMET_ADDRESSES,
                COMPOUND_V3_MARKETS,
            )

            chain_markets = COMPOUND_V3_COMET_ADDRESSES.get(self._chain, {})
            if not chain_markets:
                raise ValueError(f"Compound V3 not configured for chain: {self._chain}")

            market_key = market_id.lower() if market_id else ""
            comet_address = chain_markets.get(market_key)
            if comet_address is None:
                raise ValueError(
                    f"Compound V3 market '{market_id}' not found on {self._chain}. "
                    f"Available: {sorted(chain_markets.keys())}"
                )

            if self._gateway_client is not None:
                from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

                if not self._gateway_client.is_connected:
                    raise ValueError(
                        f"GatewayClient is not connected; cannot fetch Compound V3 health on {self._chain}."
                    )
                w3 = Web3(GatewayWeb3Provider(self._gateway_client, chain=self._chain))
            else:
                w3 = Web3(Web3.HTTPProvider(self._rpc_url))

            # Minimal Comet ABI for HF computation.
            abi = [
                {
                    "name": "isLiquidatable",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "account", "type": "address"}],
                    "outputs": [{"name": "", "type": "bool"}],
                },
                {
                    "name": "borrowBalanceOf",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "account", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
                {
                    "name": "collateralBalanceOf",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [
                        {"name": "account", "type": "address"},
                        {"name": "asset", "type": "address"},
                    ],
                    "outputs": [{"name": "", "type": "uint128"}],
                },
                {
                    "name": "getAssetInfoByAddress",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "asset", "type": "address"}],
                    "outputs": [
                        {
                            "components": [
                                {"name": "offset", "type": "uint8"},
                                {"name": "asset", "type": "address"},
                                {"name": "priceFeed", "type": "address"},
                                {"name": "scale", "type": "uint64"},
                                {"name": "borrowCollateralFactor", "type": "uint64"},
                                {"name": "liquidateCollateralFactor", "type": "uint64"},
                                {"name": "liquidationFactor", "type": "uint64"},
                                {"name": "supplyCap", "type": "uint128"},
                            ],
                            "name": "",
                            "type": "tuple",
                        }
                    ],
                },
                {
                    "name": "getPrice",
                    "type": "function",
                    "stateMutability": "view",
                    "inputs": [{"name": "priceFeed", "type": "address"}],
                    "outputs": [{"name": "", "type": "uint256"}],
                },
            ]

            comet = w3.eth.contract(address=w3.to_checksum_address(comet_address), abi=abi)
            user = w3.to_checksum_address(user_address)

            market_config = COMPOUND_V3_MARKETS.get(self._chain, {}).get(market_key, {})
            collaterals = market_config.get("collaterals", {})
            base_token_address = market_config.get("base_token_address")

            # 1e18 scale per Comet's priceScale; values are USD-denominated with 8 decimals
            PRICE_SCALE = Decimal("1e8")

            collateral_value_usd = Decimal("0")
            liquidation_threshold_usd = Decimal("0")

            for _sym, cinfo in collaterals.items():
                addr = cinfo.get("address")
                if not addr:
                    continue
                bal_raw = comet.functions.collateralBalanceOf(user, w3.to_checksum_address(addr)).call()
                if bal_raw == 0:
                    continue
                asset_info = comet.functions.getAssetInfoByAddress(w3.to_checksum_address(addr)).call()
                # Tuple layout follows the ABI above.
                _, _, price_feed, scale, _borrow_cf, liquidate_cf, _liq_factor, _supply_cap = asset_info
                price_raw = comet.functions.getPrice(price_feed).call()

                # Convert balance to human units using scale (token decimals) and price to USD.
                bal = Decimal(str(bal_raw)) / Decimal(str(scale))
                price = Decimal(str(price_raw)) / PRICE_SCALE
                value = bal * price
                # Compound V3 collateral factors are uint64 1e18-scaled.
                liq_cf = Decimal(str(liquidate_cf)) / Decimal("1e18")

                collateral_value_usd += value
                liquidation_threshold_usd += value * liq_cf

            borrow_raw = comet.functions.borrowBalanceOf(user).call()
            # Base-token price for borrow value. Compound V3 exposes the base-token price
            # through ``getPrice(baseTokenPriceFeed)`` but that feed address is not part of
            # the minimal ABI here. USD stablecoin bases (USDC / USDT / USDS / USDC.e / DAI)
            # fall back to 1:1 safely; non-stable bases (WETH, AERO) MUST be priced via
            # ``price_oracle`` -- we refuse to silently assume $1 and inflate HF.
            if base_token_address is not None and borrow_raw > 0:
                base_symbol = market_config.get("base_token", "USDC")
                base_price = self._resolve_base_price(base_symbol)
                base_decimals = self._resolve_base_decimals(base_symbol, base_token_address)
                borrow_amount = Decimal(str(borrow_raw)) / (Decimal("10") ** base_decimals)
                borrow_value_usd = borrow_amount * base_price
            else:
                borrow_value_usd = Decimal("0")

            if borrow_value_usd > 0:
                health_factor = liquidation_threshold_usd / borrow_value_usd
            else:
                health_factor = Decimal("Infinity")

            max_borrow_usd = liquidation_threshold_usd - borrow_value_usd
            if max_borrow_usd < 0:
                max_borrow_usd = Decimal("0")

            # Compound V3 doesn't expose a single LLTV -- use the weighted liq-threshold share.
            lltv = liquidation_threshold_usd / collateral_value_usd if collateral_value_usd > 0 else Decimal("0")

            return PositionHealth(
                health_factor=health_factor,
                collateral_value_usd=collateral_value_usd,
                debt_value_usd=borrow_value_usd,
                lltv=lltv,
                max_borrow_usd=max_borrow_usd,
                protocol="compound_v3",
                market_id=market_id,
            )

        except Exception as e:
            logger.error(f"Failed to get Compound V3 health for market {market_id}: {e}")
            raise


# =============================================================================
# Unified Health Factor API
# =============================================================================


def _normalize_protocol(protocol: str) -> str:
    """Normalize common protocol name aliases."""
    if protocol is None:
        return ""
    p = protocol.lower().strip()
    aliases = {
        "aave": "aave_v3",
        "aavev3": "aave_v3",
        "aave-v3": "aave_v3",
        "morpho": "morpho_blue",
        "morphoblue": "morpho_blue",
        "morpho-blue": "morpho_blue",
        "compound": "compound_v3",
        "compoundv3": "compound_v3",
        "compound-v3": "compound_v3",
        "comet": "compound_v3",
    }
    return aliases.get(p, p)


@runtime_checkable
class HealthFactorProvider(Protocol):
    """Protocol for per-protocol health-factor adapters.

    An implementation returns the canonical on-chain health factor for a
    user's position on a specific market. The returned ``Decimal`` follows
    the SDK convention:

        * ``>= 1.0``  = healthy
        * ``< 1.0``   = liquidatable
        * ``Infinity`` = no outstanding debt

    Implementations MUST use each protocol's canonical source (no invented
    formulas). See ``PositionHealthProvider`` for the built-in adapters.
    """

    def get_health_factor(self, wallet: str, market: str) -> Decimal:  # noqa: D401 - Protocol
        """Return the live health factor for ``wallet`` on ``market``."""
        ...


# Registry of provider factories keyed by normalized protocol name.
# Strategies can register custom providers without forking the SDK.
_HF_FACTORIES: dict[str, Any] = {}


def register_health_factor_provider(protocol: str, factory: Any) -> None:
    """Register a ``HealthFactorProvider`` factory for a protocol.

    The factory is called as ``factory(chain=..., rpc_url=..., gateway_client=...,
    price_oracle=...)`` and must return an object implementing
    :class:`HealthFactorProvider`. Factories are looked up by the normalized
    protocol name (see :func:`_normalize_protocol`).

    Args:
        protocol: Protocol name (e.g. "aave_v3", "morpho_blue", "compound_v3").
        factory: Callable returning a :class:`HealthFactorProvider`.
    """
    _HF_FACTORIES[_normalize_protocol(protocol)] = factory


class _PositionHealthProviderAdapter:
    """Wraps :class:`PositionHealthProvider` so it implements the Protocol.

    Bound to a specific protocol so ``get_health_factor(wallet, market)`` is
    unambiguous at call-sites.
    """

    def __init__(self, inner: PositionHealthProvider, protocol: str):
        self._inner = inner
        self._protocol = _normalize_protocol(protocol)

    def get_health_factor(self, wallet: str, market: str) -> Decimal:
        health = self._inner.get_health(
            protocol=self._protocol,
            market_id=market,
            user_address=wallet,
        )
        return health.health_factor


def get_health_factor(
    chain: str,
    protocol: str,
    wallet: str,
    market: str,
    *,
    rpc_url: str = "",
    gateway_client: "GatewayClient | None" = None,
    price_oracle: Any = None,
) -> Decimal:
    """Return the health factor for a lending position.

    Unified dispatch across Aave V3, Morpho Blue, and Compound V3. Each
    protocol uses its canonical on-chain source:

        * **Aave V3**: ``Pool.getUserAccountData().healthFactor``
        * **Morpho Blue**: ``(collateral * LLTV) / debt`` from on-chain position
        * **Compound V3**: ``liquidation_threshold_usd / borrow_value_usd``

    Args:
        chain: Chain name (e.g. "ethereum", "arbitrum").
        protocol: Lending protocol name ("aave_v3", "morpho_blue",
            "compound_v3", or any alias handled by
            :func:`_normalize_protocol`).
        wallet: Wallet address to inspect.
        market: Protocol-specific market identifier. For Aave V3 this is
            informational. For Morpho Blue this is the bytes32 market id.
            For Compound V3 this is the Comet market key (e.g. "usdc").
        rpc_url: HTTP RPC endpoint (when not using gateway).
        gateway_client: Optional gateway client (preferred in production).
        price_oracle: Optional callable ``symbol -> Decimal`` used by the
            Compound V3 path for base-token pricing.

    Returns:
        ``Decimal`` health factor (``Infinity`` if no debt).

    Raises:
        ValueError: If the protocol is unsupported or no registered factory
            can handle it.
    """
    protocol_n = _normalize_protocol(protocol)
    factory = _HF_FACTORIES.get(protocol_n)
    if factory is not None:
        provider = factory(
            chain=chain,
            rpc_url=rpc_url,
            gateway_client=gateway_client,
            price_oracle=price_oracle,
        )
        return provider.get_health_factor(wallet, market)

    # Default: use the built-in PositionHealthProvider.
    inner = PositionHealthProvider(
        rpc_url=rpc_url,
        chain=chain,
        price_oracle=price_oracle,
        gateway_client=gateway_client,
    )
    adapter = _PositionHealthProviderAdapter(inner, protocol_n)
    return adapter.get_health_factor(wallet, market)


__all__ = [
    "DeleverageTrigger",
    "HealthFactorProvider",
    "PTPositionHealth",
    "PositionHealth",
    "PositionHealthProvider",
    "get_health_factor",
    "register_health_factor_provider",
]
