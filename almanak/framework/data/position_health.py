"""Position health monitoring for lending protocols.

Provides health factor calculations and deleverage trigger detection
for Morpho Blue and Aave V3 positions, with special support for
PT-collateralized positions on Pendle.

Key Classes:
    PositionHealth: Health factor and risk metrics for a lending position
    PTPositionHealth: Extended health data for PT-collateral positions
    DeleverageTrigger: Warning/critical thresholds for automated deleverage
    PositionHealthProvider: Reads on-chain position data and computes health
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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
            protocol: "morpho_blue" or "aave_v3"
            market_id: Protocol-specific market identifier
            user_address: Wallet address holding the position
            collateral_price_usd: Optional override for collateral price
            debt_price_usd: Optional override for debt token price

        Returns:
            PositionHealth with computed health factor

        Raises:
            ValueError: If protocol is unsupported
        """
        protocol_lower = protocol.lower()
        if protocol_lower == "morpho_blue":
            return self._get_morpho_health(market_id, user_address, collateral_price_usd, debt_price_usd)
        elif protocol_lower == "aave_v3":
            return self._get_aave_health(market_id, user_address)
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
            from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

            sdk = MorphoBlueSDK(rpc_url=self._rpc_url, chain=self._chain)
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


__all__ = [
    "DeleverageTrigger",
    "PTPositionHealth",
    "PositionHealth",
    "PositionHealthProvider",
]
