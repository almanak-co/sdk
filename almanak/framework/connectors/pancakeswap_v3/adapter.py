"""PancakeSwap V3 Adapter.

This module provides an adapter for interacting with PancakeSwap V3,
which is a Uniswap V3 fork with different fee tiers and addresses.

PancakeSwap V3 is a decentralized exchange supporting:
- Exact input swaps (swap specific amount of input token)
- Exact output swaps (receive specific amount of output token)
- Multiple fee tiers (100, 500, 2500, 10000 bps)

Supported chains:
- BNB Smart Chain (BSC)
- Ethereum
- Arbitrum

Example:
    from almanak.framework.connectors.pancakeswap_v3 import (
        PancakeSwapV3Adapter,
        PancakeSwapV3Config,
    )

    config = PancakeSwapV3Config(
        chain="bnb",
        wallet_address="0x...",
    )
    adapter = PancakeSwapV3Adapter(config)

    # Swap exact input
    result = adapter.swap_exact_input(
        token_in="USDT",
        token_out="WBNB",
        amount_in=Decimal("100"),
    )
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

from almanak.core.contracts import PANCAKESWAP_V3 as PANCAKESWAP_V3_ADDRESSES

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Fallback decimals for unknown tokens
DEFAULT_DECIMALS = 18

# PancakeSwap V3 fee tiers (different from Uniswap V3)
# 0.01%, 0.05%, 0.25%, 1% (in basis points)
FEE_TIERS: set[int] = {100, 500, 2500, 10000}

# Function selectors for SmartRouter / IV3SwapRouter (7-param struct, no deadline)
# See: https://github.com/pancakeswap/pancake-v3-contracts/blob/main/contracts/interfaces/IV3SwapRouter.sol
EXACT_INPUT_SINGLE_SELECTOR = "0x04e45aaf"
EXACT_OUTPUT_SINGLE_SELECTOR = "0x5023b4df"

# Gas estimates
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "swap_exact_input": 200000,
    "swap_exact_output": 250000,
}


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PancakeSwapV3Config:
    """Configuration for PancakeSwap V3 adapter.

    Attributes:
        chain: Blockchain network (bnb, ethereum, arbitrum)
        wallet_address: User wallet address
        default_slippage_bps: Default slippage tolerance in basis points
        default_fee_tier: Default fee tier in basis points
        price_provider: Price oracle dict (token symbol -> USD price). Required for
            production use to calculate accurate slippage amounts.
        allow_placeholder_prices: If False (default), raises ValueError when no
            price_provider is given. Set to True ONLY for unit tests.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50  # 0.5%
    default_fee_tier: int = 100  # 0.01% (main liquidity pools on PancakeSwap V3)
    price_provider: dict[str, Decimal] | None = None
    allow_placeholder_prices: bool = False

    def __post_init__(self) -> None:
        """Validate configuration."""
        valid_chains = set(PANCAKESWAP_V3_ADDRESSES.keys())
        if self.chain not in valid_chains:
            raise ValueError(f"Invalid chain: {self.chain}. Valid chains: {valid_chains}")
        if not self.wallet_address.startswith("0x") or len(self.wallet_address) != 42:
            raise ValueError(f"Invalid wallet address: {self.wallet_address}. Must be 0x-prefixed 40 hex chars.")
        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError(f"Invalid slippage: {self.default_slippage_bps}. Must be 0-10000 bps.")
        if self.default_fee_tier not in FEE_TIERS:
            raise ValueError(f"Invalid fee tier: {self.default_fee_tier}. Valid tiers: {FEE_TIERS}")
        # Validate price_provider requirement
        if self.price_provider is None and not self.allow_placeholder_prices:
            raise ValueError(
                "PancakeSwapV3Config requires price_provider for production use. "
                "Pass a dict mapping token symbols to USD prices "
                "(e.g., {'WBNB': Decimal('300'), 'USDT': Decimal('1')}) "
                "or set allow_placeholder_prices=True for testing only. "
                "Using placeholder prices will cause incorrect slippage calculations."
            )


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


# =============================================================================
# Adapter
# =============================================================================


class PancakeSwapV3Adapter:
    """Adapter for PancakeSwap V3 decentralized exchange.

    This adapter provides methods for swapping tokens on PancakeSwap V3:
    - Exact input swaps (specify input amount, receive variable output)
    - Exact output swaps (specify output amount, send variable input)

    PancakeSwap V3 is a Uniswap V3 fork with different fee tiers (100, 500, 2500, 10000 bps).

    Example:
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x...",
        )
        adapter = PancakeSwapV3Adapter(config)

        # Swap 100 USDT for WBNB
        result = adapter.swap_exact_input("USDT", "WBNB", Decimal("100"))
    """

    def __init__(self, config: PancakeSwapV3Config, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton from get_token_resolver().
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Contract addresses
        addresses = PANCAKESWAP_V3_ADDRESSES[config.chain]
        self.swap_router_address = addresses["swap_router"]
        self.factory_address = addresses["factory"]
        self.quoter_address = addresses["quoter"]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Price provider - use provided or fall back to placeholders (only if allowed)
        self._using_placeholders = config.price_provider is None
        if self._using_placeholders:
            logger.warning(
                "PancakeSwapV3Adapter using PLACEHOLDER PRICES. "
                "Slippage calculations will be INCORRECT. "
                "This is only acceptable for unit tests."
            )
            self._price_provider: dict[str, Decimal] = self._get_placeholder_prices()
        else:
            # Config validation ensures price_provider is not None here
            assert config.price_provider is not None
            self._price_provider = config.price_provider

        logger.info(
            f"PancakeSwapV3Adapter initialized for chain={config.chain}, "
            f"wallet={config.wallet_address[:10]}..., "
            f"using_placeholders={self._using_placeholders}"
        )

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        amount_out_min: Decimal | None = None,
        fee_tier: int | None = None,
        recipient: str | None = None,
        deadline: int | None = None,
    ) -> TransactionResult:
        """Build an exact input swap transaction.

        Swaps a specific amount of input token for a variable amount of output token.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Exact amount of input token to swap
            amount_out_min: Minimum output amount (uses slippage if None)
            fee_tier: Pool fee tier in bps (default from config)
            recipient: Address to receive output (default: wallet_address)
            deadline: Transaction deadline timestamp (default: 20 min from now)

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_in_addr = self._resolve_token(token_in)
            token_out_addr = self._resolve_token(token_out)

            if token_in_addr is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown input token: {token_in}",
                )
            if token_out_addr is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown output token: {token_out}",
                )

            # Get decimals and convert amount
            decimals_in = self._get_decimals(token_in)
            amount_in_wei = int(amount_in * Decimal(10**decimals_in))

            # Calculate minimum output with slippage if not provided
            if amount_out_min is None:
                # Calculate expected output based on price oracle
                amount_out_min_wei = self._calculate_min_output(
                    token_in=token_in,
                    token_out=token_out,
                    amount_in=amount_in,
                    slippage_bps=self.config.default_slippage_bps,
                    fee_tier=fee_tier or self.config.default_fee_tier,
                )
            else:
                decimals_out = self._get_decimals(token_out)
                amount_out_min_wei = int(amount_out_min * Decimal(10**decimals_out))

            # Use default fee tier if not specified
            fee = fee_tier or self.config.default_fee_tier
            if fee not in FEE_TIERS:
                return TransactionResult(
                    success=False,
                    error=f"Invalid fee tier: {fee}. Valid tiers: {FEE_TIERS}",
                )

            # Use default recipient if not specified
            to = recipient or self.wallet_address

            # Note: deadline parameter is kept for API compatibility but not used
            # SmartRouter uses IV3SwapRouter interface (7-param struct, no deadline)
            _ = deadline  # Explicitly mark as unused for API compatibility

            # Build calldata: exactInputSingle((address,address,uint24,address,uint256,uint256,uint160))
            # Struct: tokenIn, tokenOut, fee, recipient, amountIn, amountOutMinimum, sqrtPriceLimitX96
            calldata = (
                EXACT_INPUT_SINGLE_SELECTOR
                + self._pad_address(token_in_addr)
                + self._pad_address(token_out_addr)
                + self._pad_uint256(fee)
                + self._pad_address(to)
                + self._pad_uint256(amount_in_wei)
                + self._pad_uint256(amount_out_min_wei)
                + self._pad_uint256(0)  # sqrtPriceLimitX96 = 0 (no price limit)
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.swap_router_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["swap_exact_input"],
                description=f"Swap {amount_in} {token_in} for {token_out} on PancakeSwap V3",
            )

        except Exception as e:
            logger.exception(f"Failed to build swap_exact_input transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    def swap_exact_output(
        self,
        token_in: str,
        token_out: str,
        amount_out: Decimal,
        amount_in_max: Decimal | None = None,
        fee_tier: int | None = None,
        recipient: str | None = None,
        deadline: int | None = None,
    ) -> TransactionResult:
        """Build an exact output swap transaction.

        Swaps a variable amount of input token for a specific amount of output token.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_out: Exact amount of output token to receive
            amount_in_max: Maximum input amount (uses slippage if None)
            fee_tier: Pool fee tier in bps (default from config)
            recipient: Address to receive output (default: wallet_address)
            deadline: Transaction deadline timestamp (default: 20 min from now)

        Returns:
            TransactionResult with transaction data
        """
        try:
            token_in_addr = self._resolve_token(token_in)
            token_out_addr = self._resolve_token(token_out)

            if token_in_addr is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown input token: {token_in}",
                )
            if token_out_addr is None:
                return TransactionResult(
                    success=False,
                    error=f"Unknown output token: {token_out}",
                )

            # Get decimals and convert amount
            decimals_out = self._get_decimals(token_out)
            amount_out_wei = int(amount_out * Decimal(10**decimals_out))

            # Calculate maximum input with slippage if not provided
            if amount_in_max is None:
                # Calculate expected input based on price oracle and apply slippage
                amount_in_max_wei = self._calculate_max_input(
                    token_in=token_in,
                    token_out=token_out,
                    amount_out=amount_out,
                    slippage_bps=self.config.default_slippage_bps,
                    fee_tier=fee_tier or self.config.default_fee_tier,
                )
            else:
                decimals_in = self._get_decimals(token_in)
                amount_in_max_wei = int(amount_in_max * Decimal(10**decimals_in))

            # Use default fee tier if not specified
            fee = fee_tier or self.config.default_fee_tier
            if fee not in FEE_TIERS:
                return TransactionResult(
                    success=False,
                    error=f"Invalid fee tier: {fee}. Valid tiers: {FEE_TIERS}",
                )

            # Use default recipient if not specified
            to = recipient or self.wallet_address

            # Note: deadline parameter is kept for API compatibility but not used
            # SmartRouter uses IV3SwapRouter interface (7-param struct, no deadline)
            _ = deadline  # Explicitly mark as unused for API compatibility

            # Build calldata: exactOutputSingle((address,address,uint24,address,uint256,uint256,uint160))
            # Struct: tokenIn, tokenOut, fee, recipient, amountOut, amountInMaximum, sqrtPriceLimitX96
            calldata = (
                EXACT_OUTPUT_SINGLE_SELECTOR
                + self._pad_address(token_in_addr)
                + self._pad_address(token_out_addr)
                + self._pad_uint256(fee)
                + self._pad_address(to)
                + self._pad_uint256(amount_out_wei)
                + self._pad_uint256(amount_in_max_wei)
                + self._pad_uint256(0)  # sqrtPriceLimitX96 = 0 (no price limit)
            )

            return TransactionResult(
                success=True,
                tx_data={
                    "to": self.swap_router_address,
                    "value": 0,
                    "data": calldata,
                },
                gas_estimate=DEFAULT_GAS_ESTIMATES["swap_exact_output"],
                description=f"Swap {token_in} for {amount_out} {token_out} on PancakeSwap V3",
            )

        except Exception as e:
            logger.exception(f"Failed to build swap_exact_output transaction: {e}")
            return TransactionResult(success=False, error=str(e))

    # =========================================================================
    # Utility Methods
    # =========================================================================

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
                reason=f"[PancakeSwapV3Adapter] Cannot resolve token: {e.reason}",
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
                reason=f"[PancakeSwapV3Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        addr_clean = addr.lower().replace("0x", "")
        return addr_clean.zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    def _calculate_min_output(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int,
        fee_tier: int,
    ) -> int:
        """Calculate minimum output amount with slippage protection.

        Uses price oracle to estimate output and applies slippage tolerance.

        Args:
            token_in: Input token symbol
            token_out: Output token symbol
            amount_in: Input amount in token units
            slippage_bps: Slippage tolerance in basis points
            fee_tier: Pool fee tier in basis points

        Returns:
            Minimum output amount in wei

        Raises:
            ValueError: If price data is not available for tokens
        """
        # Normalize token symbols
        token_in_upper = token_in.upper() if not token_in.startswith("0x") else token_in
        token_out_upper = token_out.upper() if not token_out.startswith("0x") else token_out

        # Get prices from oracle
        price_in = self._price_provider.get(token_in_upper)
        price_out = self._price_provider.get(token_out_upper)

        if price_in is None or price_out is None:
            raise ValueError(
                f"Price data not available for {token_in} -> {token_out}. "
                f"Please provide explicit amount_out_min parameter or configure price_provider."
            )

        if price_out == 0:
            raise ValueError(f"Invalid price for {token_out}: 0")

        # Calculate USD value of input
        usd_value = amount_in * price_in

        # Apply pool fee
        fee_percent = Decimal(str(fee_tier)) / Decimal("1000000")
        usd_after_fee = usd_value * (Decimal("1") - fee_percent)

        # Calculate expected output amount
        expected_output = usd_after_fee / price_out

        # Apply slippage tolerance
        min_output = expected_output * Decimal(10000 - slippage_bps) / Decimal(10000)

        # Convert to wei
        decimals_out = self._get_decimals(token_out)
        return int(min_output * Decimal(10**decimals_out))

    def _calculate_max_input(
        self,
        token_in: str,
        token_out: str,
        amount_out: Decimal,
        slippage_bps: int,
        fee_tier: int,
    ) -> int:
        """Calculate maximum input amount with slippage protection.

        Uses price oracle to estimate input and applies slippage tolerance.

        Args:
            token_in: Input token symbol
            token_out: Output token symbol
            amount_out: Desired output amount in token units
            slippage_bps: Slippage tolerance in basis points
            fee_tier: Pool fee tier in basis points

        Returns:
            Maximum input amount in wei

        Raises:
            ValueError: If price data is not available for tokens
        """
        # Normalize token symbols
        token_in_upper = token_in.upper() if not token_in.startswith("0x") else token_in
        token_out_upper = token_out.upper() if not token_out.startswith("0x") else token_out

        # Get prices from oracle
        price_in = self._price_provider.get(token_in_upper)
        price_out = self._price_provider.get(token_out_upper)

        if price_in is None or price_out is None:
            raise ValueError(
                f"Price data not available for {token_in} -> {token_out}. "
                f"Please provide explicit amount_in_max parameter or configure price_provider."
            )

        if price_in == 0:
            raise ValueError(f"Invalid price for {token_in}: 0")

        # Calculate USD value of desired output
        usd_value = amount_out * price_out

        # Account for pool fee (we need to pay more to receive the exact output)
        fee_percent = Decimal(str(fee_tier)) / Decimal("1000000")
        usd_before_fee = usd_value / (Decimal("1") - fee_percent)

        # Calculate expected input amount
        expected_input = usd_before_fee / price_in

        # Apply slippage tolerance (add slippage since we're setting maximum)
        max_input = expected_input * Decimal(10000 + slippage_bps) / Decimal(10000)

        # Convert to wei
        decimals_in = self._get_decimals(token_in)
        return int(max_input * Decimal(10**decimals_in))

    def _get_placeholder_prices(self) -> dict[str, Decimal]:
        """Get placeholder price data for testing only.

        WARNING: These prices are HARDCODED and OUTDATED.
        DO NOT USE IN PRODUCTION - they will cause:
        - Incorrect slippage calculations
        - Swap reverts (amountOutMinimum too high)

        Real prices as of 2026-01: BNB ~$700, ETH ~$3400, BTC ~$105,000
        These placeholders show BNB at $300, ETH at $2000 - significantly wrong!
        """
        logger.warning(
            "PLACEHOLDER PRICES being used - NOT SAFE FOR PRODUCTION. BNB=$300 (real ~$700), ETH=$2000 (real ~$3400)"
        )
        return {
            "WBNB": Decimal("300"),
            "BNB": Decimal("300"),
            "ETH": Decimal("2000"),
            "WETH": Decimal("2000"),
            "USDT": Decimal("1"),
            "USDC": Decimal("1"),
            "BUSD": Decimal("1"),
            "BTCB": Decimal("45000"),
            "WBTC": Decimal("45000"),
            "CAKE": Decimal("2.50"),
            "DAI": Decimal("1"),
            "ARB": Decimal("1.20"),
        }
