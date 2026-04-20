"""TraderJoe Liquidity Book V2 Protocol Adapter.

This module provides the TraderJoeV2Adapter class for executing swaps and
managing liquidity positions on TraderJoe V2's Liquidity Book on Avalanche.

TraderJoe V2 Architecture:
- LBRouter: Main entry point for swaps and liquidity operations
- LBFactory: Creates and manages LBPair pools
- LBPair: Liquidity pool with discrete bins

Key Concepts:
- Bin: Discrete price point (unlike Uniswap V3's continuous ticks)
- BinStep: Fee tier in basis points between bins
- Fungible LP Tokens: ERC1155-like tokens for each bin (no NFTs)

Supported chains:
- Avalanche (Chain ID: 43114)

Example:
    from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config

    config = TraderJoeV2Config(
        chain="avalanche",
        wallet_address="0x...",
        rpc_url="https://api.avax.network/ext/bc/C/rpc",
    )
    adapter = TraderJoeV2Adapter(config)

    # Get a swap quote
    quote = adapter.get_swap_quote(
        token_in="WAVAX",
        token_out="USDC",
        amount_in=Decimal("1.0"),
        bin_step=20,
    )

    # Execute swap
    result = adapter.swap_exact_input(
        token_in="WAVAX",
        token_out="USDC",
        amount_in=Decimal("1.0"),
        bin_step=20,
        slippage_bps=50,
    )
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

from almanak.framework.data.tokens.exceptions import TokenResolutionError

from .sdk import (
    DEFAULT_GAS_ESTIMATES,
    PoolNotFoundError,
    TraderJoeV2SDK,
    TraderJoeV2SDKError,
)

# Re-export for external use
__all__ = [
    "TraderJoeV2Adapter",
    "TraderJoeV2Config",
    "SwapQuote",
    "SwapResult",
    "LiquidityPosition",
    "TransactionData",
    "CollectFeesResult",
]

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class SwapType(StrEnum):
    """Type of swap to execute."""

    EXACT_INPUT = "exact_input"
    EXACT_OUTPUT = "exact_output"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class TraderJoeV2Config:
    """Configuration for TraderJoe V2 adapter.

    Args:
        chain: Chain name (must be "avalanche")
        wallet_address: Address of the wallet executing transactions
        rpc_url: RPC endpoint URL
        private_key: Private key for signing (optional, for non-simulation)
        default_slippage_bps: Default slippage in basis points (default: 50 = 0.5%)
        default_deadline_seconds: Default transaction deadline (default: 300 = 5 min)
    """

    chain: str
    wallet_address: str
    rpc_url: str
    private_key: str | None = None
    default_slippage_bps: int = 50  # 0.5%
    default_deadline_seconds: int = 300  # 5 minutes


@dataclass
class SwapQuote:
    """Quote for a swap operation.

    Attributes:
        token_in: Input token symbol or address
        token_out: Output token symbol or address
        amount_in: Amount of input token (in token units)
        amount_out: Expected amount of output token (in token units)
        bin_step: Bin step used for the swap
        price: Execution price (amount_out / amount_in)
        price_impact: Estimated price impact as percentage
        path: Token path for the swap
        gas_estimate: Estimated gas for the transaction
    """

    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    bin_step: int
    price: Decimal
    price_impact: Decimal
    path: list[str]
    gas_estimate: int = DEFAULT_GAS_ESTIMATES["swap"]


@dataclass
class SwapResult:
    """Result of an executed swap.

    Attributes:
        success: Whether the swap succeeded
        tx_hash: Transaction hash
        token_in: Input token
        token_out: Output token
        amount_in: Amount of input token used
        amount_out: Amount of output token received
        gas_used: Actual gas used
        block_number: Block number of the transaction
        timestamp: Timestamp of the transaction
        error: Error message if failed
    """

    success: bool
    tx_hash: str | None = None
    token_in: str | None = None
    token_out: str | None = None
    amount_in: Decimal | None = None
    amount_out: Decimal | None = None
    gas_used: int | None = None
    block_number: int | None = None
    timestamp: datetime | None = None
    error: str | None = None


@dataclass
class LiquidityPosition:
    """Represents a liquidity position in TraderJoe V2.

    Attributes:
        pool_address: Address of the LBPair pool
        token_x: Address of token X
        token_y: Address of token Y
        bin_step: Bin step of the pool
        bin_ids: List of bin IDs where position has liquidity
        balances: Dict mapping bin ID to LB token balance
        amount_x: Total amount of token X in position
        amount_y: Total amount of token Y in position
        active_bin: Current active bin ID of the pool
    """

    pool_address: str
    token_x: str
    token_y: str
    bin_step: int
    bin_ids: list[int]
    balances: dict[int, int]
    amount_x: int
    amount_y: int
    active_bin: int


@dataclass
class CollectFeesResult:
    """Result of a fee collection query.

    Attributes:
        pool_address: Address of the LBPair pool
        bin_ids: List of bin IDs with fees
        pending_fees_x: Estimated pending fees for token X (in wei)
        pending_fees_y: Estimated pending fees for token Y (in wei)
        has_fees: Whether there are any pending fees to collect
    """

    pool_address: str
    bin_ids: list[int]
    pending_fees_x: int = 0
    pending_fees_y: int = 0
    has_fees: bool = False


@dataclass
class TransactionData:
    """Transaction data ready for execution.

    Attributes:
        to: Target contract address
        data: Encoded calldata
        value: ETH/AVAX value to send
        gas: Gas limit
        chain_id: Chain ID
    """

    to: str
    data: str
    value: int
    gas: int
    chain_id: int = 43114  # Default to Avalanche


# =============================================================================
# Adapter Class
# =============================================================================


class TraderJoeV2Adapter:
    """Adapter for TraderJoe Liquidity Book V2 protocol.

    Provides high-level methods for:
    - Token swaps (exact input)
    - Liquidity management (add/remove)
    - Position queries
    - Quote generation

    The adapter handles token resolution, slippage calculations, and
    transaction building internally.

    Example:
        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address="0x...",
            rpc_url="https://api.avax.network/ext/bc/C/rpc",
        )
        adapter = TraderJoeV2Adapter(config)

        # Get quote
        quote = adapter.get_swap_quote("WAVAX", "USDC", Decimal("1.0"), bin_step=20)

        # Execute swap
        result = adapter.swap_exact_input("WAVAX", "USDC", Decimal("1.0"), bin_step=20)
    """

    def __init__(self, config: TraderJoeV2Config, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.

        Raises:
            TraderJoeV2SDKError: If chain is not supported or connection fails
        """
        self.config = config
        self.chain = config.chain.lower()

        # Validate chain
        if self.chain != "avalanche":
            raise TraderJoeV2SDKError(f"Chain '{config.chain}' not supported. TraderJoe V2 is only on Avalanche.")

        # Initialize SDK
        self.sdk = TraderJoeV2SDK(
            chain=self.chain,
            rpc_url=config.rpc_url,
            wallet_address=config.wallet_address,
        )

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        logger.info(f"TraderJoeV2Adapter initialized for {self.chain}: wallet={config.wallet_address[:10]}...")

    # =========================================================================
    # Token Utilities
    # =========================================================================

    def resolve_token_address(self, token: str) -> str:
        """Resolve a token symbol or address to a checksummed address using TokenResolver.

        Args:
            token: Token symbol (e.g., "WAVAX") or address

        Returns:
            Checksummed token address

        Raises:
            TokenResolutionError: If token cannot be resolved
        """
        from web3 import Web3

        if token.startswith("0x") and len(token) == 42:
            return Web3.to_checksum_address(token)
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return Web3.to_checksum_address(resolved.address)
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[TraderJoeV2Adapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def get_token_decimals(self, token: str) -> int:
        """Get decimals for a token using TokenResolver.

        Args:
            token: Token symbol or address

        Returns:
            Token decimals

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
                reason=f"[TraderJoeV2Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def to_wei(self, amount: Decimal, token: str) -> int:
        """Convert token amount to wei (smallest unit).

        Args:
            amount: Amount in token units
            token: Token symbol or address

        Returns:
            Amount in wei
        """
        decimals = self.get_token_decimals(token)
        return int(amount * Decimal(10**decimals))

    def from_wei(self, amount: int, token: str) -> Decimal:
        """Convert wei to token amount.

        Args:
            amount: Amount in wei
            token: Token symbol or address

        Returns:
            Amount in token units
        """
        decimals = self.get_token_decimals(token)
        return Decimal(amount) / Decimal(10**decimals)

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def get_swap_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        bin_step: int = 20,
    ) -> SwapQuote:
        """Get a quote for a swap.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token
            bin_step: Bin step for the pair (default 20 = 0.2%)

        Returns:
            SwapQuote with expected output and price info

        Raises:
            PoolNotFoundError: If pool doesn't exist
        """
        # Resolve addresses
        token_in_addr = self.resolve_token_address(token_in)
        token_out_addr = self.resolve_token_address(token_out)
        amount_in_wei = self.to_wei(amount_in, token_in)

        # Get pool and calculate quote
        pool_addr = self.sdk.get_pool_address(token_in_addr, token_out_addr, bin_step)
        pool_info = self.sdk.get_pool_info(pool_addr)

        # Get expected output using router's getSwapOut
        pair = self.sdk.get_pair_contract(pool_addr)
        swap_for_y = token_in_addr.lower() == pool_info.token_x.lower()

        # Query router for swap output
        router = self.sdk._router_contract
        try:
            swap_out = router.functions.getSwapOut(
                pair.address,
                amount_in_wei,
                swap_for_y,
            ).call()
            amount_out_wei = swap_out[1]  # (amountInLeft, amountOut, fee)
        except Exception as e:
            # Fallback: estimate from spot rate
            logger.warning(f"getSwapOut failed, using spot rate estimate: {e}")
            spot_rate = self.sdk.get_pool_spot_rate(pool_addr)
            if swap_for_y:
                amount_out_wei = int(amount_in_wei * Decimal(str(spot_rate)))
            else:
                amount_out_wei = int(amount_in_wei / Decimal(str(spot_rate)))

        amount_out = self.from_wei(amount_out_wei, token_out)

        # Calculate price and impact
        if amount_in > 0:
            price = amount_out / amount_in
            spot_price = Decimal(str(self.sdk.get_pool_spot_rate(pool_addr)))
            if swap_for_y:
                price_impact = abs(price - spot_price) / spot_price * 100
            else:
                price_impact = abs((1 / price) - spot_price) / spot_price * 100
        else:
            price = Decimal(0)
            price_impact = Decimal(0)

        return SwapQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            bin_step=bin_step,
            price=price,
            price_impact=price_impact,
            path=[token_in_addr, token_out_addr],
            gas_estimate=DEFAULT_GAS_ESTIMATES["swap"],
        )

    def build_swap_transaction(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        bin_step: int = 20,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> TransactionData:
        """Build a swap transaction without executing it.

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token
            bin_step: Bin step for the pair
            slippage_bps: Slippage tolerance in basis points
            recipient: Recipient address (default: wallet_address)

        Returns:
            TransactionData ready for signing and execution
        """
        # Resolve parameters
        token_in_addr = self.resolve_token_address(token_in)
        token_out_addr = self.resolve_token_address(token_out)
        amount_in_wei = self.to_wei(amount_in, token_in)
        slippage = slippage_bps or self.config.default_slippage_bps
        recipient_addr = recipient or self.config.wallet_address

        # Get quote for minimum output
        quote = self.get_swap_quote(token_in, token_out, amount_in, bin_step)
        amount_out_min = self.to_wei(
            quote.amount_out * Decimal(10000 - slippage) / Decimal(10000),
            token_out,
        )

        # Build transaction
        tx, gas = self.sdk.build_swap_exact_tokens_for_tokens(
            amount_in=amount_in_wei,
            amount_out_min=amount_out_min,
            path=[token_in_addr, token_out_addr],
            bin_steps=[bin_step],
            recipient=recipient_addr,
        )

        return TransactionData(
            to=tx["to"],
            data=tx["data"].hex() if isinstance(tx["data"], bytes) else tx["data"],
            value=tx.get("value", 0),
            gas=gas,
            chain_id=43114,
        )

    def swap_exact_input(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        bin_step: int = 20,
        slippage_bps: int | None = None,
    ) -> SwapResult:
        """Execute a swap with exact input amount.

        Note: This method requires a private key to be configured.
        For building unsigned transactions, use build_swap_transaction().

        Args:
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Exact amount of input token
            bin_step: Bin step for the pair
            slippage_bps: Slippage tolerance in basis points

        Returns:
            SwapResult with execution details

        Raises:
            TraderJoeV2SDKError: If execution fails
        """
        if not self.config.private_key:
            return SwapResult(
                success=False,
                error="Private key not configured. Use build_swap_transaction() for unsigned tx.",
            )

        try:
            # Build transaction
            tx_data = self.build_swap_transaction(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                bin_step=bin_step,
                slippage_bps=slippage_bps,
            )

            # Sign and send (simplified - in production use proper signing)

            w3 = self.sdk.web3
            account = w3.eth.account.from_key(self.config.private_key)

            tx = {
                "to": tx_data.to,
                "data": tx_data.data,
                "value": tx_data.value,
                "gas": tx_data.gas,
                "chainId": tx_data.chain_id,
                "nonce": w3.eth.get_transaction_count(account.address),
                "maxFeePerGas": w3.eth.gas_price * 2,
                "maxPriorityFeePerGas": w3.to_wei(2, "gwei"),
            }

            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

            return SwapResult(
                success=receipt["status"] == 1,
                tx_hash=tx_hash.hex(),
                token_in=token_in,
                token_out=token_out,
                amount_in=amount_in,
                gas_used=receipt["gasUsed"],
                block_number=receipt["blockNumber"],
                timestamp=datetime.now(UTC),
            )

        except Exception as e:
            logger.error(f"Swap failed: {e}")
            return SwapResult(success=False, error=str(e))

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def get_position(
        self,
        token_x: str,
        token_y: str,
        bin_step: int = 20,
        wallet: str | None = None,
    ) -> LiquidityPosition | None:
        """Get liquidity position for a wallet in a pool.

        Args:
            token_x: Token X symbol or address
            token_y: Token Y symbol or address
            bin_step: Bin step of the pool
            wallet: Wallet address (default: configured wallet)

        Returns:
            LiquidityPosition if found, None if no position
        """
        token_x_addr = self.resolve_token_address(token_x)
        token_y_addr = self.resolve_token_address(token_y)
        wallet_addr = wallet or self.config.wallet_address

        try:
            pool_addr = self.sdk.get_pool_address(token_x_addr, token_y_addr, bin_step)
        except PoolNotFoundError:
            return None

        balances = self.sdk.get_position_balances(pool_addr, wallet_addr)

        if not balances:
            return None

        # Pass pre-computed balances to avoid a redundant get_position_balances() call
        amount_x, amount_y = self.sdk.get_total_position_value(pool_addr, wallet_addr, precomputed_balances=balances)
        pool_info = self.sdk.get_pool_info(pool_addr)

        return LiquidityPosition(
            pool_address=pool_addr,
            token_x=pool_info.token_x,
            token_y=pool_info.token_y,
            bin_step=pool_info.bin_step,
            bin_ids=list(balances.keys()),
            balances=balances,
            amount_x=amount_x,
            amount_y=amount_y,
            active_bin=pool_info.active_id,
        )

    def build_add_liquidity_transaction(
        self,
        token_x: str,
        token_y: str,
        amount_x: Decimal,
        amount_y: Decimal,
        bin_step: int = 20,
        bin_range: int = 10,
        slippage_bps: int | None = None,
    ) -> TransactionData:
        """Build an add liquidity transaction.

        Creates a uniform distribution across bins around the active bin.

        Args:
            token_x: Token X symbol or address
            token_y: Token Y symbol or address
            amount_x: Amount of token X to add
            amount_y: Amount of token Y to add
            bin_step: Bin step of the pool
            bin_range: Number of bins on each side of active bin
            slippage_bps: Slippage tolerance in basis points

        Returns:
            TransactionData ready for signing and execution
        """
        token_x_addr = self.resolve_token_address(token_x)
        token_y_addr = self.resolve_token_address(token_y)
        amount_x_wei = self.to_wei(amount_x, token_x)
        amount_y_wei = self.to_wei(amount_y, token_y)
        slippage = slippage_bps or self.config.default_slippage_bps

        # Get pool info
        pool_addr = self.sdk.get_pool_address(token_x_addr, token_y_addr, bin_step)
        pool_info = self.sdk.get_pool_info(pool_addr)

        # Create uniform distribution
        num_bins = bin_range * 2 + 1
        delta_ids = list(range(-bin_range, bin_range + 1))

        # Distribute tokens across bins
        # X goes in bins above active, Y goes in bins below
        distribution_x = [0] * num_bins
        distribution_y = [0] * num_bins

        # X distribution (bins above active)
        bins_above = bin_range
        if bins_above > 0:
            share = 10**18 // bins_above
            for i in range(bin_range + 1, num_bins):
                distribution_x[i] = share
            # Adjust remainder
            distribution_x[-1] += 10**18 - sum(distribution_x)

        # Y distribution (bins at and below active)
        bins_below = bin_range + 1
        share = 10**18 // bins_below
        for i in range(bins_below):
            distribution_y[i] = share
        distribution_y[bin_range] += 10**18 - sum(distribution_y)

        # For LP operations, set minimums to 0.
        # TraderJoe V2's Liquidity Book doesn't guarantee all tokens will be added -
        # the actual amounts depend on current bin composition. Unused tokens are refunded.
        # Price slippage protection is handled by id_slippage parameter.
        # The slippage_bps parameter is preserved for future use but not applied here.
        _ = slippage  # Acknowledge but don't use for LP
        amount_x_min = 0
        amount_y_min = 0

        tx, gas = self.sdk.build_add_liquidity(
            token_x=token_x_addr,
            token_y=token_y_addr,
            bin_step=bin_step,
            amount_x=amount_x_wei,
            amount_y=amount_y_wei,
            amount_x_min=amount_x_min,
            amount_y_min=amount_y_min,
            active_id_desired=pool_info.active_id,
            id_slippage=5,
            delta_ids=delta_ids,
            distribution_x=distribution_x,
            distribution_y=distribution_y,
            to=self.config.wallet_address,
            refund_to=self.config.wallet_address,
        )

        return TransactionData(
            to=tx["to"],
            data=tx["data"].hex() if isinstance(tx["data"], bytes) else tx["data"],
            value=tx.get("value", 0),
            gas=gas,
            chain_id=43114,
        )

    def build_remove_liquidity_transaction(
        self,
        token_x: str,
        token_y: str,
        bin_step: int = 20,
        slippage_bps: int | None = None,
        position: LiquidityPosition | None = None,
    ) -> TransactionData | None:
        """Build a remove liquidity transaction for all positions.

        Args:
            token_x: Token X symbol or address
            token_y: Token Y symbol or address
            bin_step: Bin step of the pool
            slippage_bps: Slippage tolerance in basis points
            position: Optional pre-fetched position. If provided, skips the
                get_position() call (avoids redundant RPC round trips when the
                caller already holds the position data).

        Returns:
            TransactionData if position exists, None otherwise
        """
        if position is None:
            position = self.get_position(token_x, token_y, bin_step)
        if not position or not position.bin_ids:
            return None

        token_x_addr = self.resolve_token_address(token_x)
        token_y_addr = self.resolve_token_address(token_y)
        slippage = slippage_bps or self.config.default_slippage_bps

        # Calculate minimums with slippage
        amount_x_min = int(position.amount_x * (10000 - slippage) // 10000)
        amount_y_min = int(position.amount_y * (10000 - slippage) // 10000)

        tx, gas = self.sdk.build_remove_liquidity(
            token_x=token_x_addr,
            token_y=token_y_addr,
            bin_step=bin_step,
            amount_x_min=amount_x_min,
            amount_y_min=amount_y_min,
            ids=position.bin_ids,
            amounts=list(position.balances.values()),
            to=self.config.wallet_address,
        )

        return TransactionData(
            to=tx["to"],
            data=tx["data"].hex() if isinstance(tx["data"], bytes) else tx["data"],
            value=tx.get("value", 0),
            gas=gas,
            chain_id=43114,
        )

    # =========================================================================
    # Fee Collection Operations
    # =========================================================================

    def get_pending_fees(
        self,
        token_x: str,
        token_y: str,
        bin_step: int = 20,
        wallet: str | None = None,
    ) -> CollectFeesResult | None:
        """Query pending fees for an LP position.

        Args:
            token_x: Token X symbol or address
            token_y: Token Y symbol or address
            bin_step: Bin step of the pool
            wallet: Wallet address (default: configured wallet)

        Returns:
            CollectFeesResult with pending fee info, or None if no position
        """
        position = self.get_position(token_x, token_y, bin_step, wallet)
        if not position or not position.bin_ids:
            return None

        fees_x, fees_y = self.sdk.get_pending_fees(
            pool_address=position.pool_address,
            account=wallet or self.config.wallet_address,
            ids=position.bin_ids,
        )

        return CollectFeesResult(
            pool_address=position.pool_address,
            bin_ids=position.bin_ids,
            pending_fees_x=fees_x,
            pending_fees_y=fees_y,
            has_fees=(fees_x > 0 or fees_y > 0),
        )

    def build_collect_fees_transaction(
        self,
        token_x: str,
        token_y: str,
        bin_step: int = 20,
    ) -> TransactionData | None:
        """Build a transaction to collect fees from an LP position without closing it.

        Calls LBPair.collectFees(account, binIds) which harvests accumulated
        fees while keeping the liquidity position intact.

        Args:
            token_x: Token X symbol or address
            token_y: Token Y symbol or address
            bin_step: Bin step of the pool

        Returns:
            TransactionData if position exists, None otherwise
        """
        position = self.get_position(token_x, token_y, bin_step)
        if not position or not position.bin_ids:
            return None

        tx, gas = self.sdk.build_collect_fees(
            pool_address=position.pool_address,
            account=self.config.wallet_address,
            ids=position.bin_ids,
        )

        return TransactionData(
            to=tx["to"],
            data=tx["data"].hex() if isinstance(tx["data"], bytes) else tx["data"],
            value=tx.get("value", 0),
            gas=gas,
            chain_id=43114,
        )
