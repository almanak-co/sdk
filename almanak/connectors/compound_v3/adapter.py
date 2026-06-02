"""Compound V3 (Comet) Adapter.

This module provides an adapter for interacting with Compound V3 (Comet) lending protocol,
supporting supply, withdraw, borrow, and repay operations.

Compound V3 (Comet) is a lending protocol that allows:
- Supplying base assets to earn yield
- Supplying collateral assets for borrowing
- Borrowing base assets against collateral
- Variable interest rates per market

Key differences from traditional Compound:
- Single borrowable asset (base) per market (e.g., USDC, WETH)
- Multiple collateral assets per market
- No cTokens for collateral (only for base asset lending)
- Simplified liquidation model

Supported chains:
- Ethereum
- Arbitrum
- Base
- Optimism
- Polygon

Example:
    from almanak.connectors.compound_v3 import CompoundV3Adapter, CompoundV3Config

    config = CompoundV3Config(
        chain="ethereum",
        wallet_address="0x...",
        market="usdc",  # or "weth", "usdt"
    )
    adapter = CompoundV3Adapter(config)

    # Supply base asset to earn interest
    result = adapter.supply(
        amount=Decimal("1000"),
    )

    # Supply collateral for borrowing
    result = adapter.supply_collateral(
        asset="WETH",
        amount=Decimal("1.0"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        amount=Decimal("500"),
    )
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Address / market literals are owned by ``addresses.py`` (VIB-4929 PR-3b);
# re-exported here for the ~36 callers that import them from ``adapter``.
from almanak.connectors.compound_v3.addresses import (  # noqa: E402,F401  (re-export)
    _DEFAULT_MARKET_BY_CHAIN,
    COMPOUND_V3_COMET_ADDRESSES,
    COMPOUND_V3_MARKETS,
    default_compound_v3_market_for_chain,
)

# Compound V3 function selectors
COMPOUND_V3_SUPPLY_SELECTOR = "0xf2b9fdb8"  # supply(address,uint256)
COMPOUND_V3_SUPPLY_TO_SELECTOR = "0x4232cd63"  # supplyTo(address,address,uint256)
COMPOUND_V3_SUPPLY_FROM_SELECTOR = "0x2a7c6ef0"  # supplyFrom(address,address,address,uint256)
COMPOUND_V3_WITHDRAW_SELECTOR = "0xf3fef3a3"  # withdraw(address,uint256)
COMPOUND_V3_WITHDRAW_TO_SELECTOR = "0x8013f3a7"  # withdrawTo(address,address,uint256)
COMPOUND_V3_WITHDRAW_FROM_SELECTOR = "0x7eb8ff0d"  # withdrawFrom(address,address,address,uint256)
COMPOUND_V3_ABSORB_SELECTOR = "0xf8138c6e"  # absorb(address,address[])
COMPOUND_V3_BUY_COLLATERAL_SELECTOR = "0x3c447ff5"  # buyCollateral(address,uint256,uint256,address)

# ERC20 approve selector
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max values
MAX_UINT256 = 2**256 - 1

# Gas estimates for Compound V3 operations
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "supply": 150000,
    "supply_collateral": 150000,
    "withdraw": 180000,
    "withdraw_collateral": 180000,
    "borrow": 200000,
    "repay": 150000,
    "absorb": 400000,
    "buy_collateral": 250000,
    "approve": 46000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CompoundV3Config:
    """Configuration for Compound V3 adapter.

    Attributes:
        chain: Blockchain network (ethereum, arbitrum)
        wallet_address: User wallet address
        market: Market identifier (usdc, weth, usdt, etc.)
        default_slippage_bps: Default slippage tolerance in basis points
        rpc_url: DEPRECATED — direct RPC URL kept for backwards compatibility.
            Ignored in production gateway-only containers. Prefer ``gateway_client``.
        gateway_client: Optional gateway client for on-chain queries
            (e.g., collateral balance for withdraw_all). When set, RPC calls
            are routed through the gateway; strategies running in isolated
            containers have no other network access.
    """

    chain: str
    wallet_address: str
    market: str = "usdc"
    default_slippage_bps: int = 50  # 0.5%
    rpc_url: str | None = None  # DEPRECATED — prefer gateway_client
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(COMPOUND_V3_COMET_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        valid_markets = set(COMPOUND_V3_COMET_ADDRESSES.get(self.chain, {}).keys())
        if self.market not in valid_markets:
            raise ValueError(f"Invalid market: {self.market}. Valid markets for {self.chain}: {valid_markets}")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")


@dataclass
class CompoundV3MarketInfo:
    """Information about a Compound V3 market.

    Attributes:
        market_id: Market identifier (e.g., "usdc")
        name: Human-readable market name
        base_token: Symbol of the base token (borrowable asset)
        base_token_address: Address of the base token
        comet_address: Address of the Comet contract
        collaterals: Dictionary of supported collateral assets
    """

    market_id: str
    name: str
    base_token: str
    base_token_address: str
    comet_address: str
    collaterals: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "name": self.name,
            "base_token": self.base_token,
            "base_token_address": self.base_token_address,
            "comet_address": self.comet_address,
            "collaterals": {
                k: {
                    "address": v["address"],
                    "borrow_collateral_factor": str(v["borrow_collateral_factor"]),
                    "liquidation_collateral_factor": str(v["liquidation_collateral_factor"]),
                    "liquidation_factor": str(v["liquidation_factor"]),
                }
                for k, v in self.collaterals.items()
            },
        }


@dataclass
class CompoundV3Position:
    """User position in a Compound V3 market.

    Attributes:
        market_id: Market identifier
        base_balance: Balance of base token (positive = supply, negative = borrow)
        collateral_balances: Balances of collateral tokens
    """

    market_id: str
    base_balance: Decimal = Decimal("0")
    collateral_balances: dict[str, Decimal] = field(default_factory=dict)

    @property
    def is_supplier(self) -> bool:
        """Check if user is a net supplier."""
        return self.base_balance > 0

    @property
    def is_borrower(self) -> bool:
        """Check if user is a net borrower."""
        return self.base_balance < 0

    @property
    def borrow_balance(self) -> Decimal:
        """Get the borrow balance (positive value)."""
        return abs(self.base_balance) if self.base_balance < 0 else Decimal("0")

    @property
    def supply_balance(self) -> Decimal:
        """Get the supply balance."""
        return self.base_balance if self.base_balance > 0 else Decimal("0")

    @property
    def has_collateral(self) -> bool:
        """Check if user has any collateral."""
        return any(bal > 0 for bal in self.collateral_balances.values())

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "base_balance": str(self.base_balance),
            "supply_balance": str(self.supply_balance),
            "borrow_balance": str(self.borrow_balance),
            "is_supplier": self.is_supplier,
            "is_borrower": self.is_borrower,
            "collateral_balances": {k: str(v) for k, v in self.collateral_balances.items()},
            "has_collateral": self.has_collateral,
        }


@dataclass
class CompoundV3HealthFactor:
    """Health factor calculation for a Compound V3 position.

    Attributes:
        collateral_value_usd: Total value of collateral in USD
        borrow_value_usd: Total value of borrowed assets in USD
        borrow_capacity_usd: Maximum borrowable amount based on collateral
        liquidation_threshold_usd: USD debt level at which liquidation occurs
        health_factor: Calculated health factor (liquidation_threshold / borrow)
        is_liquidatable: Whether the position can be liquidated
    """

    collateral_value_usd: Decimal
    borrow_value_usd: Decimal
    borrow_capacity_usd: Decimal
    liquidation_threshold_usd: Decimal
    health_factor: Decimal
    is_liquidatable: bool = False

    @property
    def is_healthy(self) -> bool:
        """Check if position is healthy (HF >= 1)."""
        return self.health_factor >= Decimal("1.0")

    @property
    def available_borrow_usd(self) -> Decimal:
        """Get remaining borrowable amount in USD."""
        return max(Decimal("0"), self.borrow_capacity_usd - self.borrow_value_usd)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "collateral_value_usd": str(self.collateral_value_usd),
            "borrow_value_usd": str(self.borrow_value_usd),
            "borrow_capacity_usd": str(self.borrow_capacity_usd),
            "liquidation_threshold_usd": str(self.liquidation_threshold_usd),
            "health_factor": str(self.health_factor),
            "is_healthy": self.is_healthy,
            "is_liquidatable": self.is_liquidatable,
            "available_borrow_usd": str(self.available_borrow_usd),
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


class CompoundV3Adapter:
    """Adapter for Compound V3 (Comet) lending protocol.

    This adapter provides methods for interacting with Compound V3:
    - Supply/withdraw base assets (lending)
    - Supply/withdraw collateral assets
    - Borrow/repay base assets
    - Health factor calculations

    Compound V3 uses a single borrowable asset (base) per market with multiple
    collateral options. Unlike traditional Compound, collateral does not earn
    interest - only base asset suppliers earn yield.

    Example:
        config = CompoundV3Config(
            chain="ethereum",
            wallet_address="0x...",
            market="usdc",
        )
        adapter = CompoundV3Adapter(config)

        # Supply base asset to earn interest
        result = adapter.supply(amount=Decimal("1000"))

        # Supply collateral for borrowing
        result = adapter.supply_collateral(asset="WETH", amount=Decimal("1.0"))

        # Borrow against collateral
        result = adapter.borrow(amount=Decimal("500"))
    """

    def __init__(
        self,
        config: CompoundV3Config,
        price_oracle: PriceOracle | None = None,
        token_resolver: "TokenResolverType | None" = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            price_oracle: Optional price oracle callback
            token_resolver: Optional TokenResolver instance (defaults to singleton)
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self.market = config.market

        # Contract addresses
        self.comet_address = COMPOUND_V3_COMET_ADDRESSES[config.chain][config.market]

        # Market configuration
        self.market_config = COMPOUND_V3_MARKETS.get(config.chain, {}).get(config.market, {})

        # Price oracle
        self._price_oracle = price_oracle or self._default_price_oracle

        # Token resolver
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Gateway client for on-chain queries (e.g., collateral balance)
        self._gateway_client = config.gateway_client

        logger.info(
            f"CompoundV3Adapter initialized for chain={config.chain}, "
            f"market={config.market}, wallet={config.wallet_address[:10]}..."
        )

    # =========================================================================
    # On-chain query helpers
    # =========================================================================

    def _query_collateral_balance(self, asset_address: str) -> int | None:
        """Query on-chain collateral balance via Comet.userCollateral(address,address).

        Returns the collateral balance in wei, or None if the query fails.
        Requires gateway_client to be set on the config.

        Compound V3 stores collateral amounts as uint128, so MAX_UINT256 cannot be
        used for withdraw_all. Unlike the base asset (which has a MAX_UINT256 shortcut),
        collateral withdrawal reverts on underflow — so we must query the exact balance.

        Note: There is an inherent race window between this query and the subsequent
        withdrawal transaction. If another transaction (e.g., liquidation) reduces the
        collateral between query and execution, the withdrawal may revert. This is the
        standard integration pattern for Compound V3 collateral withdrawals.
        """
        if self._gateway_client is None:
            logger.warning("No gateway_client configured; cannot query on-chain collateral balance")
            return None

        try:
            from web3 import Web3
            from web3.types import HexStr

            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            w3 = Web3(GatewayWeb3Provider(self._gateway_client, chain=self.chain))

            # userCollateral(address,address) returns (uint128 balance, uint128 _reserved)
            # selector = keccak256("userCollateral(address,address)")[0:4] = 0x2b92a07d
            account_padded = self.wallet_address[2:].lower().zfill(64)
            asset_padded = asset_address[2:].lower().zfill(64)
            calldata = HexStr(f"0x2b92a07d{account_padded}{asset_padded}")

            result = w3.eth.call(
                {
                    "to": Web3.to_checksum_address(self.comet_address),
                    "data": calldata,
                }
            )

            if not result or len(result) < 64:
                logger.warning(f"Unexpected RPC result length: {len(result) if result else 0} bytes")
                return None

            # Decode: first 32 bytes = balance (uint128 ABI-padded to 32 bytes)
            balance = int(result[:32].hex(), 16)
            logger.debug(
                f"On-chain collateral balance for {asset_address[:10]}...: {balance} wei "
                f"(wallet={self.wallet_address[:10]}...)"
            )
            return balance

        except Exception as e:
            logger.warning(f"Failed to query on-chain collateral balance: {e}")
            return None

    # =========================================================================
    # Supply Operations (Base Asset Lending)
    # =========================================================================

    def supply(
        self,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply transaction for the base asset.

        Supplies the base asset (e.g., USDC) to earn interest.

        Args:
            amount: Amount of base token to supply
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            if recipient == self.wallet_address:
                # Use simple supply(address,uint256)
                calldata = self._build_supply_calldata(base_token_address, amount_wei)
            else:
                # Use supplyTo(address,address,uint256)
                calldata = self._build_supply_to_calldata(recipient, base_token_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply"],
                description=f"Supply {amount} {base_token} to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw(
        self,
        amount: Decimal,
        receiver: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw transaction for the base asset.

        Withdraws supplied base asset from the market.

        Args:
            amount: Amount of base token to withdraw
            receiver: Address to receive tokens (defaults to wallet_address)
            withdraw_all: If True, withdraws all supplied base asset

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            recipient = receiver or self.wallet_address

            if withdraw_all:
                amount_wei = MAX_UINT256
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            if recipient == self.wallet_address:
                # Use simple withdraw(address,uint256)
                calldata = self._build_withdraw_calldata(base_token_address, amount_wei)
            else:
                # Use withdrawTo(address,address,uint256)
                calldata = self._build_withdraw_to_calldata(recipient, base_token_address, amount_wei)

            amount_str = "all" if withdraw_all else f"{amount} {base_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw"],
                description=f"Withdraw {amount_str} from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Collateral Operations
    # =========================================================================

    def _resolve_collateral_key(self, asset: str) -> str | None:
        """Case-insensitive collateral key lookup.

        The compiler uppercases token symbols (e.g., "wstETH" -> "WSTETH") but
        COMPOUND_V3_MARKETS uses mixed-case keys. This method resolves the
        correct dict key by falling back to case-insensitive comparison.

        Returns:
            The matching collateral key, or None if not found.
        """
        collaterals = self.market_config.get("collaterals", {})
        if asset in collaterals:
            return asset
        asset_lower = asset.lower()
        for key in collaterals:
            if key.lower() == asset_lower:
                return key
        return None

    def supply_collateral(
        self,
        asset: str,
        amount: Decimal,
        on_behalf_of: str | None = None,
    ) -> TransactionResult:
        """Build a supply collateral transaction.

        Supplies collateral to enable borrowing.

        Args:
            asset: Collateral asset symbol (e.g., "WETH")
            amount: Amount of collateral to supply
            on_behalf_of: Address to credit (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            collaterals = self.market_config.get("collaterals", {})
            resolved_key = self._resolve_collateral_key(asset)
            if resolved_key is None:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported collateral: {asset}. Supported: {list(collaterals.keys())}",
                )

            asset_address = collaterals[resolved_key]["address"]
            decimals = self._get_decimals(resolved_key)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = on_behalf_of or self.wallet_address

            if recipient == self.wallet_address:
                # Use simple supply(address,uint256)
                calldata = self._build_supply_calldata(asset_address, amount_wei)
            else:
                # Use supplyTo(address,address,uint256)
                calldata = self._build_supply_to_calldata(recipient, asset_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["supply_collateral"],
                description=f"Supply {amount} {asset} as collateral to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build supply collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def withdraw_collateral(
        self,
        asset: str,
        amount: Decimal,
        receiver: str | None = None,
        withdraw_all: bool = False,
    ) -> TransactionResult:
        """Build a withdraw collateral transaction.

        Withdraws collateral from the market.

        Args:
            asset: Collateral asset symbol (e.g., "WETH")
            amount: Amount of collateral to withdraw
            receiver: Address to receive tokens (defaults to wallet_address)
            withdraw_all: If True, withdraws all collateral for this asset

        Returns:
            TransactionResult with transaction data
        """
        try:
            collaterals = self.market_config.get("collaterals", {})
            resolved_key = self._resolve_collateral_key(asset)
            if resolved_key is None:
                return TransactionResult(
                    success=False,
                    error=f"Unsupported collateral: {asset}. Supported: {list(collaterals.keys())}",
                )

            asset_address = collaterals[resolved_key]["address"]
            decimals = self._get_decimals(resolved_key)
            recipient = receiver or self.wallet_address

            if withdraw_all:
                # Compound V3 stores collateral as uint128 — MAX_UINT256 causes safe128() revert.
                # Must query actual on-chain balance instead.
                on_chain_balance = self._query_collateral_balance(asset_address)
                if on_chain_balance is not None:
                    if on_chain_balance == 0:
                        logger.info("withdraw_all collateral: on-chain balance is 0, nothing to withdraw")
                        return TransactionResult(
                            success=True,
                            tx_data=None,
                            description="No collateral to withdraw (balance is 0)",
                        )
                    amount_wei = on_chain_balance
                    logger.info(
                        f"withdraw_all collateral: using on-chain balance {amount_wei} wei (queried via userCollateral)"
                    )
                else:
                    # Fallback: use the amount parameter if on-chain query fails
                    if amount > 0:
                        amount_wei = int(amount * Decimal(10**decimals))
                        logger.warning(
                            f"withdraw_all collateral: on-chain query unavailable, "
                            f"falling back to provided amount={amount} ({amount_wei} wei)"
                        )
                    else:
                        return TransactionResult(
                            success=False,
                            error=(
                                "Cannot withdraw_all collateral: on-chain balance query failed "
                                "and no fallback amount provided. Set gateway_client on CompoundV3Config "
                                "for on-chain queries."
                            ),
                        )
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            if recipient == self.wallet_address:
                # Use simple withdraw(address,uint256)
                calldata = self._build_withdraw_calldata(asset_address, amount_wei)
            else:
                # Use withdrawTo(address,address,uint256)
                calldata = self._build_withdraw_to_calldata(recipient, asset_address, amount_wei)

            amount_str = "all" if withdraw_all else f"{amount} {asset}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["withdraw_collateral"],
                description=f"Withdraw {amount_str} collateral from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build withdraw collateral transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Borrow Operations
    # =========================================================================

    def borrow(
        self,
        amount: Decimal,
        receiver: str | None = None,
    ) -> TransactionResult:
        """Build a borrow transaction.

        Borrows the base asset against supplied collateral.

        Args:
            amount: Amount of base token to borrow
            receiver: Address to receive borrowed tokens (defaults to wallet_address)

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            amount_wei = int(amount * Decimal(10**decimals))
            recipient = receiver or self.wallet_address

            # In Compound V3, borrowing is done via withdraw when you have collateral
            # but no supplied base asset
            if recipient == self.wallet_address:
                calldata = self._build_withdraw_calldata(base_token_address, amount_wei)
            else:
                calldata = self._build_withdraw_to_calldata(recipient, base_token_address, amount_wei)

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["borrow"],
                description=f"Borrow {amount} {base_token} from Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build borrow transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def repay(
        self,
        amount: Decimal,
        on_behalf_of: str | None = None,
        repay_all: bool = False,
    ) -> TransactionResult:
        """Build a repay transaction.

        Repays borrowed base asset.

        Args:
            amount: Amount of base token to repay
            on_behalf_of: Address with debt (defaults to wallet_address)
            repay_all: If True, repays full debt

        Returns:
            TransactionResult with transaction data
        """
        try:
            base_token = self.market_config.get("base_token", "USDC")
            base_token_address = self.market_config.get("base_token_address")
            if not base_token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown base token for market: {self.market}",
                )

            decimals = self._get_decimals(base_token)
            recipient = on_behalf_of or self.wallet_address

            if repay_all:
                # Use max uint256 to repay all debt
                amount_wei = MAX_UINT256
            else:
                amount_wei = int(amount * Decimal(10**decimals))

            # In Compound V3, repaying is done via supply when you have a borrow position
            if recipient == self.wallet_address:
                calldata = self._build_supply_calldata(base_token_address, amount_wei)
            else:
                calldata = self._build_supply_to_calldata(recipient, base_token_address, amount_wei)

            amount_str = "full debt" if repay_all else f"{amount} {base_token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.comet_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["repay"],
                description=f"Repay {amount_str} to Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build repay transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Market Information
    # =========================================================================

    def get_market_info(self) -> CompoundV3MarketInfo:
        """Get information about the current market.

        Returns:
            CompoundV3MarketInfo with market details
        """
        return CompoundV3MarketInfo(
            market_id=self.market,
            name=self.market_config.get("name", self.market),
            base_token=self.market_config.get("base_token", "USDC"),
            base_token_address=self.market_config.get("base_token_address", ""),
            comet_address=self.comet_address,
            collaterals=self.market_config.get("collaterals", {}),
        )

    def get_supported_collaterals(self) -> list[str]:
        """Get list of supported collateral assets for the current market.

        Returns:
            List of collateral asset symbols
        """
        return list(self.market_config.get("collaterals", {}).keys())

    def get_collateral_info(self, asset: str) -> dict[str, Any] | None:
        """Get information about a collateral asset.

        Args:
            asset: Collateral asset symbol

        Returns:
            Collateral info dictionary or None if not supported
        """
        resolved_key = self._resolve_collateral_key(asset)
        if resolved_key is None:
            return None
        collaterals = self.market_config.get("collaterals", {})
        info = collaterals[resolved_key].copy()
        info["symbol"] = resolved_key
        return info

    # =========================================================================
    # Health Factor Calculations
    # =========================================================================

    def calculate_health_factor(
        self,
        collateral_balances: dict[str, Decimal],
        borrow_balance: Decimal,
    ) -> CompoundV3HealthFactor:
        """Calculate health factor for a position.

        Args:
            collateral_balances: Dictionary of collateral asset balances
            borrow_balance: Amount of borrowed base asset

        Returns:
            CompoundV3HealthFactor with health calculation
        """
        collateral_value_usd = Decimal("0")
        borrow_capacity_usd = Decimal("0")
        liquidation_threshold_usd = Decimal("0")

        collaterals = self.market_config.get("collaterals", {})

        for asset, balance in collateral_balances.items():
            resolved_key = self._resolve_collateral_key(asset)
            if resolved_key is None or balance <= 0:
                continue

            price = self._price_oracle(resolved_key)
            value_usd = balance * price
            collateral_value_usd += value_usd

            collateral_info = collaterals[resolved_key]
            borrow_cf = collateral_info.get("borrow_collateral_factor", Decimal("0"))
            liquidation_cf = collateral_info.get("liquidation_collateral_factor", Decimal("0"))

            borrow_capacity_usd += value_usd * borrow_cf
            liquidation_threshold_usd += value_usd * liquidation_cf

        # Get base token price for borrow value
        base_token = self.market_config.get("base_token", "USDC")
        base_price = self._price_oracle(base_token)
        borrow_value_usd = borrow_balance * base_price

        # Calculate health factor
        if borrow_value_usd > 0:
            health_factor = liquidation_threshold_usd / borrow_value_usd
        else:
            health_factor = Decimal("999999")  # No debt = max health

        is_liquidatable = health_factor < Decimal("1.0") if borrow_value_usd > 0 else False

        return CompoundV3HealthFactor(
            collateral_value_usd=collateral_value_usd,
            borrow_value_usd=borrow_value_usd,
            borrow_capacity_usd=borrow_capacity_usd,
            liquidation_threshold_usd=liquidation_threshold_usd,
            health_factor=health_factor,
            is_liquidatable=is_liquidatable,
        )

    # =========================================================================
    # Approval Operations
    # =========================================================================

    def build_approve_transaction(
        self,
        token: str,
        amount: Decimal | None = None,
    ) -> TransactionResult:
        """Build an ERC20 approval transaction for the Comet contract.

        Args:
            token: Token symbol to approve
            amount: Amount to approve (None for max approval)

        Returns:
            TransactionResult with transaction data
        """
        try:
            # Get token address
            if token == self.market_config.get("base_token"):
                token_address = self.market_config.get("base_token_address")
            else:
                collaterals = self.market_config.get("collaterals", {})
                if token in collaterals:
                    token_address = collaterals[token]["address"]
                else:
                    token_address = self._resolve_token_address(token)

            if not token_address:
                return TransactionResult(
                    success=False,
                    error=f"Unknown token: {token}",
                )

            if amount is None:
                amount_wei = MAX_UINT256
            else:
                decimals = self._get_decimals(token)
                amount_wei = int(amount * Decimal(10**decimals))

            calldata = self._build_approve_calldata(self.comet_address, amount_wei)

            amount_str = "unlimited" if amount is None else f"{amount} {token}"
            return TransactionResult(
                success=True,
                tx_data={
                    "to": token_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["approve"],
                description=f"Approve {amount_str} for Compound V3 {self.market_config.get('name', self.market)}",
            )

        except Exception as e:
            logger.exception(f"Failed to build approve transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _resolve_token_address(self, token: str) -> str:
        """Resolve a token symbol to its address using TokenResolver.

        Args:
            token: Token symbol (e.g., "USDC")

        Returns:
            Token address

        Raises:
            TokenResolutionError: If the token cannot be resolved
        """
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[CompoundV3Adapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver.

        Args:
            token: Token symbol

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
                reason=f"[CompoundV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _default_price_oracle(self, token: str) -> Decimal:
        """Default price oracle (returns placeholder prices).

        In production, this should be replaced with a real price oracle.
        """
        # Placeholder prices for common tokens
        prices = {
            "USDC": Decimal("1.0"),
            "USDC.e": Decimal("1.0"),
            "USDT": Decimal("1.0"),
            "WETH": Decimal("2500.0"),
            "WBTC": Decimal("60000.0"),
            "COMP": Decimal("50.0"),
            "UNI": Decimal("10.0"),
            "LINK": Decimal("15.0"),
            "wstETH": Decimal("2800.0"),
            "cbETH": Decimal("2600.0"),
            "rETH": Decimal("2700.0"),
            "ARB": Decimal("1.5"),
            "GMX": Decimal("40.0"),
            "WMATIC": Decimal("0.50"),
            "MaticX": Decimal("0.55"),
            "USDS": Decimal("1.0"),
            "sUSDe": Decimal("1.0"),
        }
        return prices.get(token, Decimal("1.0"))

    def _build_supply_calldata(self, asset: str, amount: int) -> str:
        """Build calldata for supply(address,uint256)."""
        # supply(address,uint256) = 0xf2b9fdb8
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_SUPPLY_SELECTOR}{asset_padded}{amount_hex}"

    def _build_supply_to_calldata(self, dst: str, asset: str, amount: int) -> str:
        """Build calldata for supplyTo(address,address,uint256)."""
        # supplyTo(address,address,uint256) = 0x4232cd63
        dst_padded = dst[2:].lower().zfill(64)
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_SUPPLY_TO_SELECTOR}{dst_padded}{asset_padded}{amount_hex}"

    def _build_withdraw_calldata(self, asset: str, amount: int) -> str:
        """Build calldata for withdraw(address,uint256)."""
        # withdraw(address,uint256) = 0xf3fef3a3
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_WITHDRAW_SELECTOR}{asset_padded}{amount_hex}"

    def _build_withdraw_to_calldata(self, to: str, asset: str, amount: int) -> str:
        """Build calldata for withdrawTo(address,address,uint256)."""
        # withdrawTo(address,address,uint256) = 0x8013f3a7
        to_padded = to[2:].lower().zfill(64)
        asset_padded = asset[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{COMPOUND_V3_WITHDRAW_TO_SELECTOR}{to_padded}{asset_padded}{amount_hex}"

    def _build_approve_calldata(self, spender: str, amount: int) -> str:
        """Build calldata for ERC20 approve(address,uint256)."""
        # approve(address,uint256) = 0x095ea7b3
        spender_padded = spender[2:].lower().zfill(64)
        amount_hex = hex(amount)[2:].zfill(64)
        return f"{ERC20_APPROVE_SELECTOR}{spender_padded}{amount_hex}"
