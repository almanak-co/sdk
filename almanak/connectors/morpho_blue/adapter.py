"""Morpho Blue Adapter.

This module provides an adapter for interacting with Morpho Blue lending protocol,
supporting supply, borrow, repay, and withdraw operations.

Morpho Blue is a permissionless lending protocol that allows:
- Creating isolated lending markets
- Supplying assets to earn yield
- Borrowing against collateral
- Flexible market parameters (LTV, oracles, interest rate models)

Supported chains:
- Ethereum
- Base
- Arbitrum
- Polygon
- Monad

Example:
    from almanak.connectors.morpho_blue import MorphoBlueAdapter, MorphoBlueConfig

    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = MorphoBlueAdapter(config)

    # Supply collateral
    result = adapter.supply_collateral(
        market_id="0x...",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        market_id="0x...",
        amount=Decimal("500"),
    )
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import IntEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError
from almanak.framework.deployment.mode import is_hosted

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

from .addresses import MORPHO_BLUE as _MORPHO_BLUE_REGISTRY
from .addresses import MORPHO_MARKETS

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Morpho Blue contract addresses per chain (derived from centralized registry)
MORPHO_BLUE_ADDRESSES: dict[str, str] = {chain: addrs["morpho"] for chain, addrs in _MORPHO_BLUE_REGISTRY.items()}

# Bundler addresses per chain (for batched operations, derived from centralized registry).
# ``bundler`` is optional: it is not used by any supply/borrow/repay/withdraw path, so a
# chain whose Bundler3 deployment is unresolved (e.g. robinhood) may omit the key entirely.
# ``.get(config.chain)`` on the consumer side already tolerates the resulting absence/None.
MORPHO_BUNDLER_ADDRESSES: dict[str, str] = {
    chain: bundler for chain, addrs in _MORPHO_BLUE_REGISTRY.items() if (bundler := addrs.get("bundler")) is not None
}

# Morpho Blue function selectors
# Note: MarketParams struct is encoded as tuple (address,address,address,address,uint256)
MORPHO_SUPPLY_SELECTOR = "0xa99aad89"  # supply((address,address,address,address,uint256),uint256,uint256,address,bytes)
MORPHO_WITHDRAW_SELECTOR = "0x5c2bea49"  # withdraw(MarketParams,uint256,uint256,address,address)
MORPHO_BORROW_SELECTOR = "0x50d8cd4b"  # borrow(MarketParams,uint256,uint256,address,address)
MORPHO_REPAY_SELECTOR = "0x20b76e81"  # repay(MarketParams,uint256,uint256,address,bytes)
MORPHO_SUPPLY_COLLATERAL_SELECTOR = "0x238d6579"  # supplyCollateral(MarketParams,uint256,address,bytes)
MORPHO_WITHDRAW_COLLATERAL_SELECTOR = "0x8720316d"  # withdrawCollateral(MarketParams,uint256,address,address)
MORPHO_LIQUIDATE_SELECTOR = "0xd8eabcb8"  # liquidate(MarketParams,address,uint256,uint256,bytes)
MORPHO_FLASH_LOAN_SELECTOR = "0xe0232b42"  # flashLoan(address,uint256,bytes)
MORPHO_SET_AUTHORIZATION_SELECTOR = "0xeecea000"  # setAuthorization(address,bool)
MORPHO_ACCRUE_INTEREST_SELECTOR = "0x151c1ade"  # accrueInterest(MarketParams)

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Max values
MAX_UINT256 = 2**256 - 1

# Gas estimates for Morpho Blue operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 180000,
    "withdraw": 180000,
    "borrow": 250000,
    "repay": 180000,
    "supply_collateral": 180000,
    "withdraw_collateral": 180000,
    "liquidate": 350000,
    "flash_loan": 400000,
    "set_authorization": 50000,
    "accrue_interest": 100000,
    "approve": 46000,
}

# Pre-configured Morpho Blue markets (market_id -> market info). The literal now
# lives in ``addresses.py`` beside the other Morpho address tables (VIB-4929
# PR-3a); it is re-exported here (see the ``from .addresses import MORPHO_MARKETS``
# above) so the ~30 call sites importing
# ``almanak.connectors.morpho_blue.adapter.MORPHO_MARKETS`` keep resolving.


# =============================================================================
# Enums
# =============================================================================


class MorphoBlueInterestRateMode(IntEnum):
    """Morpho Blue uses a single interest rate model per market.

    Unlike Aave, Morpho Blue markets have a single adaptive interest rate
    determined by the IRM (Interest Rate Model) contract.
    """

    VARIABLE = 0  # All rates are variable in Morpho Blue


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class MorphoBlueConfig:
    """Configuration for Morpho Blue adapter.

    Attributes:
        chain: Blockchain network (ethereum, base, arbitrum, monad)
        wallet_address: User wallet address
        default_slippage_bps: Default slippage tolerance in basis points
        rpc_url: Optional RPC URL. If not provided, uses ALCHEMY_API_KEY.
        price_provider: Optional dict mapping token symbols to USD prices.
            Required for health factor calculations in production.
        allow_placeholder_prices: If True, allows using placeholder prices
            for testing. DO NOT use in production.
        enable_sdk: If True, initializes the SDK for on-chain reads.
            Requires RPC access.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50  # 0.5%
    rpc_url: str | None = None  # DEPRECATED — use gateway_client
    price_provider: dict[str, Decimal] | None = None
    allow_placeholder_prices: bool = False
    enable_sdk: bool = True  # Enable SDK by default for production use
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(MORPHO_BLUE_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")


@dataclass
class MorphoBlueMarketParams:
    """Market parameters for Morpho Blue.

    In Morpho Blue, each market is uniquely identified by these 5 parameters.
    The market_id is derived as: keccak256(abi.encode(loan_token, collateral_token, oracle, irm, lltv))

    Attributes:
        loan_token: Address of the asset being borrowed
        collateral_token: Address of the collateral asset
        oracle: Address of the price oracle
        irm: Address of the interest rate model
        lltv: Liquidation LTV (in 1e18 scale, e.g., 860000000000000000 = 86%)
    """

    loan_token: str
    collateral_token: str
    oracle: str
    irm: str
    lltv: int

    def to_tuple(self) -> tuple[str, str, str, str, int]:
        """Convert to tuple for ABI encoding."""
        return (self.loan_token, self.collateral_token, self.oracle, self.irm, self.lltv)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "loan_token": self.loan_token,
            "collateral_token": self.collateral_token,
            "oracle": self.oracle,
            "irm": self.irm,
            "lltv": self.lltv,
            "lltv_percent": self.lltv / 1e16,  # Convert to percentage
        }


@dataclass
class MorphoBlueMarketState:
    """State of a Morpho Blue market.

    Attributes:
        market_id: Unique identifier for the market
        total_supply_assets: Total assets supplied to the market
        total_supply_shares: Total supply shares
        total_borrow_assets: Total assets borrowed from the market
        total_borrow_shares: Total borrow shares
        last_update: Timestamp of last interest accrual
        fee: Protocol fee (in 1e18 scale)
    """

    market_id: str
    total_supply_assets: Decimal = Decimal("0")
    total_supply_shares: Decimal = Decimal("0")
    total_borrow_assets: Decimal = Decimal("0")
    total_borrow_shares: Decimal = Decimal("0")
    last_update: int = 0
    fee: Decimal = Decimal("0")

    @property
    def utilization(self) -> Decimal:
        """Calculate market utilization rate."""
        if self.total_supply_assets == 0:
            return Decimal("0")
        return self.total_borrow_assets / self.total_supply_assets

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "total_supply_assets": str(self.total_supply_assets),
            "total_supply_shares": str(self.total_supply_shares),
            "total_borrow_assets": str(self.total_borrow_assets),
            "total_borrow_shares": str(self.total_borrow_shares),
            "last_update": self.last_update,
            "fee": str(self.fee),
            "utilization": str(self.utilization),
        }


@dataclass
class MorphoBluePosition:
    """User position in a Morpho Blue market.

    Attributes:
        market_id: Market identifier
        supply_shares: User's supply shares
        borrow_shares: User's borrow shares
        collateral: User's collateral amount
    """

    market_id: str
    supply_shares: Decimal = Decimal("0")
    borrow_shares: Decimal = Decimal("0")
    collateral: Decimal = Decimal("0")

    @property
    def has_supply(self) -> bool:
        """Check if user has supply in this market."""
        return self.supply_shares > 0

    @property
    def has_borrow(self) -> bool:
        """Check if user has borrow in this market."""
        return self.borrow_shares > 0

    @property
    def has_collateral(self) -> bool:
        """Check if user has collateral in this market."""
        return self.collateral > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "supply_shares": str(self.supply_shares),
            "borrow_shares": str(self.borrow_shares),
            "collateral": str(self.collateral),
            "has_supply": self.has_supply,
            "has_borrow": self.has_borrow,
            "has_collateral": self.has_collateral,
        }


@dataclass
class MorphoBlueHealthFactor:
    """Health factor calculation for a Morpho Blue position.

    Attributes:
        collateral_value_usd: Value of collateral in USD
        debt_value_usd: Value of debt in USD
        lltv: Liquidation LTV of the market
        health_factor: Calculated health factor
        max_borrow_usd: Maximum borrowable amount
    """

    collateral_value_usd: Decimal
    debt_value_usd: Decimal
    lltv: Decimal
    health_factor: Decimal
    max_borrow_usd: Decimal = Decimal("0")

    @property
    def is_healthy(self) -> bool:
        """Check if position is healthy (HF >= 1)."""
        return self.health_factor >= Decimal("1.0")

    @property
    def liquidation_threshold_usd(self) -> Decimal:
        """Get the USD debt level at which liquidation would occur."""
        return self.collateral_value_usd * self.lltv

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "collateral_value_usd": str(self.collateral_value_usd),
            "debt_value_usd": str(self.debt_value_usd),
            "lltv": str(self.lltv),
            "health_factor": str(self.health_factor),
            "is_healthy": self.is_healthy,
            "max_borrow_usd": str(self.max_borrow_usd),
            "liquidation_threshold_usd": str(self.liquidation_threshold_usd),
        }


@dataclass
class TransactionResult:
    """Result of a transaction build operation.

    Attributes:
        success: Whether operation succeeded
        tx_data: Transaction data (to, value, data)
        gas_estimate: Estimated gas
        description: Human-readable description
        error: Error message if failed
    """

    success: bool
    tx_data: dict[str, Any] | None = None
    gas_estimate: int = 0
    description: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "tx_data": self.tx_data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "error": self.error,
        }


# =============================================================================
# Type Aliases
# =============================================================================

PriceOracle = Callable[[str], Decimal]


# =============================================================================
# Adapter
# =============================================================================


class MorphoBlueAdapter:
    """Adapter for Morpho Blue lending protocol.

    This adapter provides methods for interacting with Morpho Blue:
    - Supply/withdraw assets (lending)
    - Supply/withdraw collateral
    - Borrow/repay assets
    - Health factor calculations
    - On-chain position and market state reading (via SDK)

    Example:
        # Production usage with real prices
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
            price_provider={"USDC": Decimal("1"), "wstETH": Decimal("3500")},
        )
        adapter = MorphoBlueAdapter(config)

        # Read on-chain position
        position = adapter.get_position_on_chain(market_id)
        print(f"Collateral: {position.collateral}")

        # Supply collateral
        result = adapter.supply_collateral(
            market_id="0x...",
            amount=Decimal("1.0"),
        )

        # For testing only (with placeholder prices)
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
            allow_placeholder_prices=True,
            enable_sdk=False,  # Disable SDK for unit tests
        )
        adapter = MorphoBlueAdapter(config)
    """

    def __init__(
        self,
        config: MorphoBlueConfig,
        price_oracle: PriceOracle | None = None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            price_oracle: Optional price oracle callback. If not provided,
                uses config.price_provider dict or placeholder prices.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        self.morpho_address = MORPHO_BLUE_ADDRESSES[config.chain]
        self.bundler_address = MORPHO_BUNDLER_ADDRESSES.get(config.chain)

        # Markets for this chain
        self.markets = MORPHO_MARKETS.get(config.chain, {})

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # SDK for on-chain reads (optional)
        self._sdk: Any = None  # Lazy initialization
        self._sdk_enabled = config.enable_sdk

        # Price oracle setup.
        # `_using_placeholder_prices`: the placeholder oracle (1.0 fallback) is wired.
        # `_placeholder_prices_authorized`: the placeholder was an EXPLICIT opt-in
        #   (`allow_placeholder_prices=True`). When False, the placeholder is an
        #   *unauthorized silent fallback* and MUST fail loud if a price is actually
        #   consumed on the hosted perimeter — returning a fabricated 1.0 for a real
        #   valuation silently mis-values positions (VIB-5527 / ALM-2895 Defect C).
        # The guard lives at the point of use (`_default_price_oracle`), NOT in the
        # constructor: the lending intent compilers construct this adapter purely to
        # build transactions (no pricing), so a constructor raise would break all
        # hosted morpho_blue SUPPLY/BORROW/REPAY/WITHDRAW compilation for no safety gain.
        self._using_placeholder_prices = False
        self._placeholder_prices_authorized = False
        if price_oracle is not None:
            self._price_oracle = price_oracle
        elif config.price_provider:
            # Create oracle from price_provider dict. An EMPTY dict is treated as
            # "no provider" — it would otherwise price every asset at 0 (see
            # `_create_price_oracle_from_dict`), another silent mis-valuation.
            self._price_oracle = self._create_price_oracle_from_dict(config.price_provider)
        elif config.allow_placeholder_prices:
            logger.warning(
                "MorphoBlueAdapter using PLACEHOLDER PRICES for chain=%s. "
                "Health factor calculations WILL BE INACCURATE.",
                config.chain,
            )
            self._price_oracle = self._default_price_oracle
            self._using_placeholder_prices = True
            self._placeholder_prices_authorized = True
        else:
            # allow_placeholder_prices=False AND no real price source supplied.
            # Wire the placeholder oracle but mark it unauthorized: the point-of-use
            # guard fails loud in hosted mode if a price is ever actually consumed.
            logger.warning(
                "MorphoBlueAdapter: No price_oracle or price_provider provided. "
                "Using placeholder prices. For production, use create_adapter_with_prices()."
            )
            self._price_oracle = self._default_price_oracle
            self._using_placeholder_prices = True

        logger.info(
            f"MorphoBlueAdapter initialized for chain={config.chain}, "
            f"wallet={config.wallet_address[:10]}..., "
            f"sdk={'enabled' if self._sdk_enabled else 'disabled'}"
        )

    # =========================================================================
    # SDK Property (Lazy Initialization)
    # =========================================================================

    @property
    def sdk(self) -> Any:
        """Get the SDK instance (lazy initialization).

        Returns:
            MorphoBlueSDK instance

        Raises:
            RuntimeError: If SDK is disabled
        """
        if not self._sdk_enabled:
            raise RuntimeError("SDK is disabled. Set enable_sdk=True in config to use on-chain reads.")

        if self._sdk is None:
            # Import here to avoid circular imports
            from .sdk import MorphoBlueSDK

            self._sdk = MorphoBlueSDK(
                chain=self.chain,
                rpc_url=self.config.rpc_url,
                gateway_client=self.config.gateway_client,
            )

        return self._sdk

    # =========================================================================
    # On-Chain Reading Methods
    # =========================================================================

    def get_position_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> MorphoBluePosition:
        """Get user position from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            MorphoBluePosition with supply, borrow, and collateral data
        """
        user_address = user or self.wallet_address
        sdk_position = self.sdk.get_position(market_id, user_address)

        return MorphoBluePosition(
            market_id=sdk_position.market_id,
            supply_shares=Decimal(sdk_position.supply_shares),
            borrow_shares=Decimal(sdk_position.borrow_shares),
            collateral=Decimal(sdk_position.collateral),
        )

    def get_market_state_on_chain(self, market_id: str) -> MorphoBlueMarketState:
        """Get market state from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketState with current market totals
        """
        sdk_state = self.sdk.get_market_state(market_id)

        return MorphoBlueMarketState(
            market_id=sdk_state.market_id,
            total_supply_assets=Decimal(sdk_state.total_supply_assets),
            total_supply_shares=Decimal(sdk_state.total_supply_shares),
            total_borrow_assets=Decimal(sdk_state.total_borrow_assets),
            total_borrow_shares=Decimal(sdk_state.total_borrow_shares),
            last_update=sdk_state.last_update,
            fee=Decimal(sdk_state.fee),
        )

    def get_market_params_on_chain(self, market_id: str) -> MorphoBlueMarketParams:
        """Get market parameters from on-chain data.

        Requires SDK to be enabled.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketParams with loan token, collateral token, oracle, IRM, LLTV
        """
        sdk_params = self.sdk.get_market_params(market_id)

        return MorphoBlueMarketParams(
            loan_token=sdk_params.loan_token,
            collateral_token=sdk_params.collateral_token,
            oracle=sdk_params.oracle,
            irm=sdk_params.irm,
            lltv=sdk_params.lltv,
        )

    def discover_markets_on_chain(self) -> list[str]:
        """Discover all markets from on-chain events.

        Requires SDK to be enabled.

        Returns:
            List of market IDs (bytes32 hex strings)
        """
        return self.sdk.discover_markets()

    def get_supply_assets_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> Decimal:
        """Get user's supply amount in assets (not shares) from on-chain.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            Supply amount in asset units
        """
        user_address = user or self.wallet_address
        assets = self.sdk.get_supply_assets(market_id, user_address)
        return Decimal(assets)

    def get_borrow_assets_on_chain(
        self,
        market_id: str,
        user: str | None = None,
    ) -> Decimal:
        """Get user's borrow amount in assets (not shares) from on-chain.

        Args:
            market_id: Market identifier
            user: User address (defaults to wallet_address)

        Returns:
            Borrow amount in asset units
        """
        user_address = user or self.wallet_address
        assets = self.sdk.get_borrow_assets(market_id, user_address)
        return Decimal(assets)

    # =========================================================================
    # Price Oracle Helpers
    # =========================================================================

    def _create_price_oracle_from_dict(
        self,
        prices: dict[str, Decimal],
    ) -> PriceOracle:
        """Create price oracle callback from a dictionary.

        Args:
            prices: Dict mapping token symbols to USD prices

        Returns:
            PriceOracle callback function
        """

        def price_oracle(asset: str) -> Decimal:
            # Try exact match first
            if asset in prices:
                return prices[asset]
            # Try uppercase
            if asset.upper() in prices:
                return prices[asset.upper()]
            # Try lowercase
            if asset.lower() in prices:
                return prices[asset.lower()]
            # Log warning and return 0
            logger.warning(f"No price found for asset: {asset}")
            return Decimal("0")

        return price_oracle

    # =========================================================================
    # Supply Operations (Lending)
    # =========================================================================

    def supply(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
    ) -> TransactionResult:
        """Build a supply transaction for lending assets.

        Supplies loan tokens to the market to earn interest.

        Args:
            market_id: Market identifier
            amount: Amount to supply (in token units) or shares if shares_mode=True
            on_behalf_of: Address to credit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = on_behalf_of or self.wallet_address

            if shares_mode:
                # amount is shares (always 18 decimals in Morpho Blue)
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                # amount is assets
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: supply(MarketParams,uint256,uint256,address,bytes)
            # MarketParams is a tuple: (loanToken, collateralToken, oracle, irm, lltv)
            calldata = self._build_supply_calldata(market_params, assets_wei, shares_wei, recipient, b"")

            amount_str = f"{amount} shares" if shares_mode else f"{amount} {loan_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount_str} to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw transaction for withdrawing supplied assets.

        Withdraws loan tokens from the market.

        Args:
            market_id: Market identifier
            amount: Amount to withdraw (in token units) or shares if shares_mode=True
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets
            withdraw_all: If True, withdraws all supplied assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            if withdraw_all:
                # Query actual supply shares from position (MAX_UINT256 causes
                # overflow in Morpho's mulDiv and uint128 cast, same as repay_all)
                position = self.get_position_on_chain(market_id, owner)
                if position.supply_shares <= 0:
                    return TransactionResult(
                        success=False,
                        error="No supply position to withdraw",
                    )
                assets_wei = 0
                shares_wei = int(position.supply_shares)
            elif shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: withdraw(MarketParams,uint256,uint256,address,address)
            calldata = self._build_withdraw_calldata(market_params, assets_wei, shares_wei, owner, recipient)

            if withdraw_all:
                amount_str = "all"
            elif shares_mode:
                amount_str = f"{amount} shares"
            else:
                amount_str = f"{amount} {loan_token}"

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
                description=f"Withdraw {amount_str} from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Collateral Operations
    # =========================================================================

    def supply_collateral(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply collateral transaction.

        Supplies collateral tokens to the market for borrowing.

        Args:
            market_id: Market identifier
            amount: Amount of collateral to supply (in token units)
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)
            recipient = on_behalf_of or self.wallet_address

            amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: supplyCollateral(MarketParams,uint256,address,bytes)
            calldata = self._build_supply_collateral_calldata(market_params, amount_wei, recipient, b"")

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_collateral"],
                description=f"Supply {amount} {collateral_token} as collateral to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw_collateral(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw collateral transaction.

        Withdraws collateral tokens from the market.

        Args:
            market_id: Market identifier
            amount: Amount of collateral to withdraw (in token units)
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            withdraw_all: If True, withdraws all collateral

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            if withdraw_all:
                # Query actual collateral from position (MAX_UINT256 exceeds
                # Morpho's internal uint128 cast, causing revert)
                position = self.get_position_on_chain(market_id, owner)
                if position.collateral <= 0:
                    return TransactionResult(
                        success=False,
                        error="No collateral position to withdraw",
                    )
                amount_wei = int(position.collateral)
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: withdrawCollateral(MarketParams,uint256,address,address)
            calldata = self._build_withdraw_collateral_calldata(market_params, amount_wei, owner, recipient)

            amount_str = "all" if withdraw_all else f"{amount} {collateral_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw_collateral"],
                description=f"Withdraw {amount_str} collateral from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Borrow Operations
    # =========================================================================

    def borrow(
        self,
        market_id: str,
        amount: Decimal,
        receiver: str | None = None,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Borrows loan tokens from the market against collateral.

        Args:
            market_id: Market identifier
            amount: Amount to borrow (in token units) or shares if shares_mode=True
            receiver: Address to receive tokens (defaults to wallet_address)
            on_behalf_of: Address to debit (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            recipient = receiver or self.wallet_address
            owner = on_behalf_of or self.wallet_address

            if shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

            # Build calldata: borrow(MarketParams,uint256,uint256,address,address)
            calldata = self._build_borrow_calldata(market_params, assets_wei, shares_wei, owner, recipient)

            amount_str = f"{amount} shares" if shares_mode else f"{amount} {loan_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount_str} from Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        market_id: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
        shares_mode: bool = False,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Repays borrowed loan tokens to the market.

        Args:
            market_id: Market identifier
            amount: Amount to repay (in token units) or shares if shares_mode=True
            on_behalf_of: Address with debt (defaults to wallet_address)
            shares_mode: If True, amount represents shares instead of assets
            repay_all: If True, repays full debt

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            loan_token = market_info["loan_token"]
            decimals = self._get_decimals(loan_token)
            owner = on_behalf_of or self.wallet_address

            if repay_all:
                # Query actual borrow shares from position (MAX_UINT256 causes overflow)
                position = self.get_position_on_chain(market_id, owner)
                if position.borrow_shares <= 0:
                    return TransactionResult(
                        success=False,
                        error="No borrow position to repay",
                    )
                assets_wei = 0
                shares_wei = int(position.borrow_shares)
            elif shares_mode:
                assets_wei = 0
                shares_wei = int(amount * Decimal(10**18))
            else:
                assets_wei = int(amount * Decimal(10**decimals))
                shares_wei = 0

                # Guard: Morpho Blue panics (0x11 underflow) if repay amount > actual debt.
                # Unlike Aave V3/Compound V3, Morpho does NOT cap repay at outstanding debt.
                # Query on-chain debt and cap the amount to prevent revert.
                if self._sdk_enabled:
                    try:
                        actual_debt_wei = self.sdk.get_borrow_assets(market_id, owner)
                    except Exception as e:
                        logger.warning(
                            "Could not query on-chain debt for repay cap, proceeding with requested amount: %s",
                            e,
                        )
                        actual_debt_wei = None
                    if actual_debt_wei is not None and assets_wei > actual_debt_wei:
                        if actual_debt_wei == 0:
                            # VIB-4531: the SDK occasionally reports 0 debt
                            # (stale view / wrong owner / RPC race). Capping
                            # ``assets_wei`` to 0 would produce ``repay(0, 0)``
                            # calldata that violates Morpho's
                            # ``exactlyOneZero(assets, shares)`` invariant and
                            # reverts with ``INCONSISTENT_INPUT``.
                            #
                            # Skip the cap and proceed with the originally
                            # requested ``assets_wei``. Three possible
                            # outcomes, all preferable to silent (0, 0):
                            #
                            # 1. SDK was wrong + real debt covers the request →
                            #    Morpho accepts and repays the requested
                            #    amount (the production case VIB-4531 was
                            #    filed for).
                            # 2. SDK was wrong + real debt is below the
                            #    request → Morpho's own underflow guard
                            #    reverts with a clear error. The caller
                            #    sees the revert reason and can re-plan.
                            # 3. SDK was right + there's genuinely no debt →
                            #    Morpho rejects with a clear "no debt to
                            #    repay" error. Same observable outcome as a
                            #    pre-flight refusal but cheaper.
                            #
                            # Audit PR #2343 (CI repro): an earlier draft of
                            # this branch refused at compile-time when
                            # actual_debt_wei was 0. That broke the synthetic
                            # intent discovery used by the Zodiac manifest
                            # builder: synthetic owners have no debt by
                            # construction, so every synthetic ``RepayIntent``
                            # got rejected, the ``repay`` selector dropped
                            # from the manifest, and real teardown repays
                            # failed authz. Skipping the cap (rather than
                            # refusing) keeps the calldata shape intact for
                            # the discovery path while still avoiding the
                            # (0, 0) shape the original bug produced.
                            logger.info(
                                "Morpho repay: SDK reports debt=0 for %s on %s but caller requested %d wei; "
                                "skipping cap to avoid (0, 0) calldata (VIB-4531)",
                                owner,
                                market_id,
                                assets_wei,
                            )
                        else:
                            logger.info(
                                "Morpho repay amount %d exceeds actual debt %d, capping to actual debt",
                                assets_wei,
                                actual_debt_wei,
                            )
                            assets_wei = actual_debt_wei

            # Build calldata: repay(MarketParams,uint256,uint256,address,bytes)
            calldata = self._build_repay_calldata(market_params, assets_wei, shares_wei, owner, b"")

            if repay_all:
                amount_str = "full debt"
            elif shares_mode:
                amount_str = f"{amount} shares"
            else:
                amount_str = f"{amount} {loan_token}"

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_str} to Morpho Blue market {market_info['name']}",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Flash Loan Operations
    # =========================================================================

    def flash_loan(
        self,
        token: str,
        amount: Decimal,
        callback_data: bytes = b"",
    ) -> TransactionResult:
        """Build a flash loan transaction.

        Borrows assets in a flash loan that must be repaid within the same transaction.

        Note: Flash loans require a callback contract to receive and repay the loan.
        The callback_data is passed to the flash loan receiver.

        Args:
            token: Token symbol or address to borrow
            amount: Amount to borrow
            callback_data: Data passed to flash loan receiver callback

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_address = self._resolve_token(token)
            if token_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            decimals = self._get_decimals(token)
            amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: flashLoan(address token, uint256 assets, bytes calldata data)
            calldata = self._build_flash_loan_calldata(token_address, amount_wei, callback_data)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["flash_loan"],
                description=f"Flash loan {amount} {token} from Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build flash loan transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Liquidation Operations
    # =========================================================================

    def liquidate(
        self,
        market_id: str,
        borrower: str,
        seized_assets: Decimal,
        repaid_shares: Decimal | None = None,
        callback_data: bytes = b"",
    ) -> TransactionResult:
        """Build a liquidation transaction.

        Liquidates an unhealthy position by repaying debt and seizing collateral.

        In Morpho Blue, liquidators specify the amount of collateral to seize,
        and the protocol calculates how much debt to repay based on the oracle price
        and liquidation incentive.

        Args:
            market_id: Market identifier
            borrower: Address of the borrower to liquidate
            seized_assets: Amount of collateral to seize (in collateral token units)
            repaid_shares: Optional amount of debt shares to repay (if 0, uses seized_assets)
            callback_data: Data passed to liquidation callback (for flash liquidations)

        Returns:
            TransactionResult with transaction data
        """
        try:
            market_info = self._get_market_info(market_id)
            if market_info is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown market: {market_id}",
                )

            market_params = self._get_market_params(market_info)
            collateral_token = market_info["collateral_token"]
            decimals = self._get_decimals(collateral_token)

            seized_assets_wei = int(seized_assets * Decimal(10**decimals))
            repaid_shares_wei = int(repaid_shares * Decimal(10**18)) if repaid_shares is not None else 0

            # Build calldata: liquidate(MarketParams, address borrower, uint256 seizedAssets, uint256 repaidShares, bytes data)
            calldata = self._build_liquidate_calldata(
                market_params,
                borrower,
                seized_assets_wei,
                repaid_shares_wei,
                callback_data,
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["liquidate"],
                description=f"Liquidate {borrower[:10]}... in Morpho Blue market {market_info['name']}, seize {seized_assets} {collateral_token}",
            )

        except Exception as e:
            logger.exception(f"Failed to build liquidate transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Authorization Operations
    # =========================================================================

    def set_authorization(
        self,
        authorized: str,
        is_authorized: bool,
    ) -> TransactionResult:
        """Build a set authorization transaction.

        Authorizes another address to manage positions on behalf of the caller.

        Args:
            authorized: Address to authorize/deauthorize
            is_authorized: Whether to grant or revoke authorization

        Returns:
            TransactionResult with transaction data
        """
        try:
            # Build calldata: setAuthorization(address authorized, bool newIsAuthorized)
            calldata = (
                MORPHO_SET_AUTHORIZATION_SELECTOR
                + self._pad_address(authorized)
                + self._pad_uint256(1 if is_authorized else 0)
            )

            action = "Authorize" if is_authorized else "Deauthorize"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.morpho_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["set_authorization"],
                description=f"{action} {authorized[:10]}... for Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build set authorization transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Market Information
    # =========================================================================

    def get_market_info(self, market_id: str) -> dict[str, Any] | None:
        """Get information about a market.

        Args:
            market_id: Market identifier

        Returns:
            Market info dictionary or None if not found
        """
        return self._get_market_info(market_id)

    def get_markets(self) -> dict[str, dict[str, Any]]:
        """Get all known markets for the current chain.

        Returns:
            Dictionary mapping market_id to market info
        """
        return self.markets.copy()

    def get_market_params(self, market_id: str) -> MorphoBlueMarketParams | None:
        """Get market parameters for a market.

        Args:
            market_id: Market identifier

        Returns:
            MorphoBlueMarketParams or None if not found
        """
        market_info = self._get_market_info(market_id)
        if market_info is None:
            return None
        return self._get_market_params(market_info)

    # =========================================================================
    # Health Factor Calculations
    # =========================================================================

    def calculate_health_factor(
        self,
        collateral_amount: Decimal,
        collateral_price_usd: Decimal,
        debt_amount: Decimal,
        debt_price_usd: Decimal,
        lltv: Decimal,
    ) -> MorphoBlueHealthFactor:
        """Calculate health factor for a position.

        Health Factor = (Collateral Value * LLTV) / Debt Value

        Args:
            collateral_amount: Amount of collateral
            collateral_price_usd: Price of collateral in USD
            debt_amount: Amount of debt
            debt_price_usd: Price of debt token in USD
            lltv: Liquidation LTV (0-1 scale)

        Returns:
            MorphoBlueHealthFactor with calculated values
        """
        collateral_value_usd = collateral_amount * collateral_price_usd
        debt_value_usd = debt_amount * debt_price_usd

        if debt_value_usd == 0:
            health_factor = Decimal("999999")  # Effectively infinite
        else:
            health_factor = (collateral_value_usd * lltv) / debt_value_usd

        max_borrow_usd = collateral_value_usd * lltv - debt_value_usd
        if max_borrow_usd < 0:
            max_borrow_usd = Decimal("0")

        return MorphoBlueHealthFactor(
            collateral_value_usd=collateral_value_usd,
            debt_value_usd=debt_value_usd,
            lltv=lltv,
            health_factor=health_factor,
            max_borrow_usd=max_borrow_usd,
        )

    # =========================================================================
    # Approval Helpers
    # =========================================================================

    def build_approve_transaction(
        self,
        token: str,
        amount: Decimal | None = None,
        spender: str | None = None,
    ) -> TransactionResult:
        """Build an ERC20 approve transaction.

        Args:
            token: Token symbol or address to approve
            amount: Amount to approve (None for max)
            spender: Address to approve (defaults to Morpho Blue contract)

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_address = self._resolve_token(token)
            if token_address is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            target = spender or self.morpho_address

            if amount is None:
                amount_wei = MAX_UINT256
            else:
                decimals = self._get_decimals(token)
                amount_wei = int(amount * Decimal(10**decimals))

            # Build calldata: approve(address spender, uint256 amount)
            calldata = ERC20_APPROVE_SELECTOR + self._pad_address(target) + self._pad_uint256(amount_wei)

            amount_str = "unlimited" if amount is None else str(amount)
            return TransactionResult(
                success=True,
                tx_data={
                    "to": token_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve {amount_str} {token} for Morpho Blue",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _get_market_info(self, market_id: str) -> dict[str, Any] | None:
        """Get market info by market_id.

        Falls back to on-chain lookup via SDK if the market is not in the local registry.
        This allows any Morpho Blue market to be used without pre-registration.
        """
        # Normalize market_id
        if not market_id.startswith("0x"):
            market_id = "0x" + market_id
        market_id = market_id.lower()

        # Check in known markets
        for mid, info in self.markets.items():
            if mid.lower() == market_id:
                return info

        # Fallback: fetch params on-chain if SDK is initialized
        if self._sdk_enabled and self._sdk is not None:
            try:
                sdk_params = self._sdk.get_market_params(market_id)
                logger.info(
                    f"Market {market_id[:18]}... not in local registry; resolved on-chain: "
                    f"loan={sdk_params.loan_token}, collateral={sdk_params.collateral_token}, "
                    f"lltv={sdk_params.lltv_percent:.1f}%"
                )
                return {
                    "name": f"on-chain:{market_id[:10]}",
                    "loan_token": sdk_params.loan_token,
                    "loan_token_address": sdk_params.loan_token,
                    "collateral_token": sdk_params.collateral_token,
                    "collateral_token_address": sdk_params.collateral_token,
                    "oracle": sdk_params.oracle,
                    "irm": sdk_params.irm,
                    "lltv": sdk_params.lltv,
                }
            except Exception as e:
                logger.warning(f"On-chain market lookup failed for {market_id[:18]}...: {e}")

        return None

    def _get_market_params(self, market_info: dict[str, Any]) -> MorphoBlueMarketParams:
        """Create MorphoBlueMarketParams from market info."""
        return MorphoBlueMarketParams(
            loan_token=market_info["loan_token_address"],
            collateral_token=market_info["collateral_token_address"],
            oracle=market_info["oracle"],
            irm=market_info["irm"],
            lltv=market_info["lltv"],
        )

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[MorphoBlueAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Number of decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[MorphoBlueAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _default_price_oracle(self, token: str) -> Decimal:
        """Placeholder price oracle (returns 1.0).

        Fails loud on the hosted perimeter when the placeholder was an unauthorized
        silent fallback (no real price source supplied and allow_placeholder_prices
        was not set). Returning a fabricated 1.0 for a real valuation in production
        silently mis-values positions (VIB-5527 / ALM-2895 Defect C). Local mode and
        explicit `allow_placeholder_prices=True` opt-ins keep the legacy behavior.
        """
        if is_hosted() and not self._placeholder_prices_authorized:
            raise ValueError(
                f"MorphoBlueAdapter: placeholder price requested for token={token!r} "
                "but no real price source was supplied and allow_placeholder_prices=False. "
                "Production deployments must supply real prices via "
                "create_adapter_with_prices() or config.price_provider. Placeholder prices "
                "silently mis-value positions and are not permitted on the hosted platform "
                "(VIB-5527 / ALM-2895)."
            )
        return Decimal("1.0")

    # =========================================================================
    # Calldata Building Helpers
    # =========================================================================

    def _build_supply_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for supply function."""
        # supply(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (shares) + 1 (onBehalfOf) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_SUPPLY_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_withdraw_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for withdraw function."""
        # withdraw(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, address receiver)
        return (
            MORPHO_WITHDRAW_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_supply_collateral_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for supplyCollateral function."""
        # supplyCollateral(MarketParams memory marketParams, uint256 assets, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (onBehalfOf) + 1 (bytes offset) = 8
        offset_part, tail_part = self._encode_bytes(data, static_slots=8)
        return (
            MORPHO_SUPPLY_COLLATERAL_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_withdraw_collateral_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for withdrawCollateral function."""
        # withdrawCollateral(MarketParams memory marketParams, uint256 assets, address onBehalfOf, address receiver)
        return (
            MORPHO_WITHDRAW_COLLATERAL_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_borrow_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        receiver: str,
    ) -> str:
        """Build calldata for borrow function."""
        # borrow(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, address receiver)
        return (
            MORPHO_BORROW_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + self._pad_address(receiver)
        )

    def _build_repay_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        assets: int,
        shares: int,
        on_behalf_of: str,
        data: bytes,
    ) -> str:
        """Build calldata for repay function."""
        # repay(MarketParams memory marketParams, uint256 assets, uint256 shares, address onBehalfOf, bytes memory data)
        # Static slots: 5 (MarketParams) + 1 (assets) + 1 (shares) + 1 (onBehalfOf) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_REPAY_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_uint256(assets)
            + self._pad_uint256(shares)
            + self._pad_address(on_behalf_of)
            + offset_part
            + tail_part
        )

    def _build_flash_loan_calldata(
        self,
        token: str,
        assets: int,
        data: bytes,
    ) -> str:
        """Build calldata for flashLoan function."""
        # flashLoan(address token, uint256 assets, bytes calldata data)
        # Static slots: 1 (token) + 1 (assets) + 1 (bytes offset) = 3
        offset_part, tail_part = self._encode_bytes(data, static_slots=3)
        return (
            MORPHO_FLASH_LOAN_SELECTOR + self._pad_address(token) + self._pad_uint256(assets) + offset_part + tail_part
        )

    def _build_liquidate_calldata(
        self,
        market_params: MorphoBlueMarketParams,
        borrower: str,
        seized_assets: int,
        repaid_shares: int,
        data: bytes,
    ) -> str:
        """Build calldata for liquidate function."""
        # liquidate(MarketParams memory marketParams, address borrower, uint256 seizedAssets, uint256 repaidShares, bytes calldata data)
        # Static slots: 5 (MarketParams) + 1 (borrower) + 1 (seizedAssets) + 1 (repaidShares) + 1 (bytes offset) = 9
        offset_part, tail_part = self._encode_bytes(data, static_slots=9)
        return (
            MORPHO_LIQUIDATE_SELECTOR
            + self._encode_market_params(market_params)
            + self._pad_address(borrower)
            + self._pad_uint256(seized_assets)
            + self._pad_uint256(repaid_shares)
            + offset_part
            + tail_part
        )

    def _encode_market_params(self, params: MorphoBlueMarketParams) -> str:
        """Encode MarketParams struct.

        MarketParams is: (address loanToken, address collateralToken, address oracle, address irm, uint256 lltv)
        This is encoded inline as 5 slots (not ABI dynamic struct encoding).
        """
        return (
            self._pad_address(params.loan_token)
            + self._pad_address(params.collateral_token)
            + self._pad_address(params.oracle)
            + self._pad_address(params.irm)
            + self._pad_uint256(params.lltv)
        )

    def _encode_bytes(self, data: bytes, static_slots: int = 8) -> tuple[str, str]:
        """Encode bytes for calldata (dynamic type).

        For ABI encoding of dynamic bytes:
        - Returns (offset_part, tail_part) where:
          - offset_part: 32-byte offset pointing to where the length starts
          - tail_part: length (32 bytes) + padded data

        Args:
            data: The bytes to encode
            static_slots: Number of 32-byte slots before the bytes data starts
                         (used to calculate the offset). Default 8 for supplyCollateral.

        Returns:
            Tuple of (offset_hex, tail_hex) to be placed in appropriate positions
        """
        offset = static_slots * 32  # Offset in bytes
        offset_part = self._pad_uint256(offset)

        if len(data) == 0:
            # Empty bytes: just the length = 0
            tail_part = self._pad_uint256(0)
        else:
            # Non-empty: length + padded data
            length = len(data)
            padded_length = (length + 31) // 32 * 32
            padded_data = data.ljust(padded_length, b"\x00")
            tail_part = self._pad_uint256(length) + padded_data.hex()

        return (offset_part, tail_part)

    def _pad_address(self, address: str) -> str:
        """Pad address to 32 bytes."""
        addr = address.lower().replace("0x", "")
        return addr.zfill(64)

    def _pad_uint256(self, value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    def _pad_uint16(self, value: int) -> str:
        """Pad uint16 to 32 bytes (for ABI encoding)."""
        return hex(value)[2:].zfill(64)


# =============================================================================
# Factory Functions
# =============================================================================


def create_adapter_with_prices(
    config: MorphoBlueConfig,
    prices: dict[str, Decimal],
) -> MorphoBlueAdapter:
    """Create an adapter with a dictionary of real prices.

    This is the recommended way to create an adapter for production use.
    It ensures accurate health factor calculations by providing real prices.

    Args:
        config: Adapter configuration
        prices: Dict mapping token symbols to USD prices

    Returns:
        MorphoBlueAdapter configured with real prices

    Example:
        prices = {
            "USDC": Decimal("1.00"),
            "USDT": Decimal("1.00"),
            "wstETH": Decimal("3500.00"),
            "WETH": Decimal("3100.00"),
            "WBTC": Decimal("98000.00"),
        }
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = create_adapter_with_prices(config, prices)
    """

    def price_oracle(asset: str) -> Decimal:
        """Look up price from the provided dictionary."""
        # Try exact match
        if asset in prices:
            return prices[asset]
        # Try uppercase
        if asset.upper() in prices:
            return prices[asset.upper()]
        # Try lowercase
        if asset.lower() in prices:
            return prices[asset.lower()]
        raise KeyError(f"No price found for '{asset}' in provided prices dict")

    return MorphoBlueAdapter(config, price_oracle=price_oracle)


def create_test_adapter(
    chain: str = "ethereum",
    wallet_address: str = "0x1234567890123456789012345678901234567890",
) -> MorphoBlueAdapter:
    """Create a test adapter with placeholder prices and SDK disabled.

    For unit testing only. DO NOT use in production.

    Args:
        chain: Chain name (default: ethereum)
        wallet_address: Wallet address (default: test address)

    Returns:
        MorphoBlueAdapter configured for testing
    """
    config = MorphoBlueConfig(
        chain=chain,
        wallet_address=wallet_address,
        allow_placeholder_prices=True,
        enable_sdk=False,  # Disable SDK for unit tests
    )
    return MorphoBlueAdapter(config)
