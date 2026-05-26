"""TraderJoe Liquidity Book V2 SDK.

TraderJoe V2 uses Liquidity Book - a novel AMM design with:
- Discrete liquidity bins instead of continuous ticks
- Fungible liquidity positions (no NFTs like Uniswap V3)
- Dynamic fees based on market volatility
- binStep parameter instead of fee tiers

Key differences from Uniswap V3:
- Uses binStep (basis points between bins)
- Path struct with pairBinSteps, versions, and tokenPath
- Liquidity is fungible (ERC1155-like)
- No position NFTs - positions tracked by bin ID and balance

Documentation: https://docs.lfj.gg/

Supported chains:
- Avalanche (Chain ID: 43114)

Example:
    from almanak.connectors.traderjoe_v2 import TraderJoeV2SDK

    sdk = TraderJoeV2SDK(chain="avalanche", rpc_url="https://api.avax.network/ext/bc/C/rpc")

    # Get pool address
    pool = sdk.get_pool_address(wavax_addr, usdc_addr, bin_step=20)

    # Build swap transaction
    tx = sdk.build_swap_exact_tokens_for_tokens(
        amount_in=10**18,
        amount_out_min=0,
        path=[wavax_addr, usdc_addr],
        bin_steps=[20],
        recipient="0x...",
    )
"""

import json
import logging
import math
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from web3 import Web3
from web3.contract import Contract

from almanak.core.contracts import TRADERJOE_V2 as TRADERJOE_V2_ADDRESSES

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Common binSteps (basis points between price bins)
# 1 = 0.01%, 5 = 0.05%, 10 = 0.1%, 15 = 0.15%, 25 = 0.25%, 50 = 0.5%, 100 = 1%
BIN_STEPS: list[int] = [1, 5, 10, 15, 20, 25, 50, 100]

# Default gas estimates
# Note: add_liquidity for 11 bins uses ~600K gas, so 700K provides safety margin
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "approve": 50_000,
    "swap": 200_000,
    "add_liquidity": 700_000,
    "remove_liquidity": 400_000,
    "collect_fees": 200_000,
}

# TraderJoe V2 constants
MAX_UINT256 = 2**256 - 1
# VIB-2579: Reduced from 100 days (8_640_000) to 5 minutes. A loose deadline
# combined with a tight id_slippage caused non-deterministic reverts on
# Avalanche where the active bin shifts between TX simulation and execution.
DEADLINE_SECONDS = 300  # 5 minutes

# Bin ID offset (2^23)
BIN_ID_OFFSET = 8388608


# =============================================================================
# Exceptions
# =============================================================================


class TraderJoeV2SDKError(Exception):
    """Base exception for TraderJoe V2 SDK errors."""

    pass


class PoolNotFoundError(TraderJoeV2SDKError):
    """Pool does not exist."""

    def __init__(self, token_x: str, token_y: str, bin_step: int) -> None:
        self.token_x = token_x
        self.token_y = token_y
        self.bin_step = bin_step
        super().__init__(f"Pool not found for {token_x}/{token_y} with binStep {bin_step}")


class InvalidBinStepError(TraderJoeV2SDKError):
    """Invalid bin step provided."""

    def __init__(self, bin_step: int) -> None:
        self.bin_step = bin_step
        super().__init__(f"Invalid bin step: {bin_step}. Common values: {BIN_STEPS}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SwapQuote:
    """Quote for a swap operation."""

    amount_in: int
    amount_out: int
    path: list[str]
    bin_steps: list[int]
    price_impact: Decimal
    fee: int


@dataclass
class PoolInfo:
    """Information about a TraderJoe V2 LBPair pool."""

    address: str
    token_x: str
    token_y: str
    bin_step: int
    active_id: int
    reserve_x: int
    reserve_y: int


@dataclass
class TransactionData:
    """Transaction data for execution."""

    to: str
    data: str
    value: int
    gas: int


# =============================================================================
# SDK Class
# =============================================================================


class TraderJoeV2SDK:
    """TraderJoe Liquidity Book V2 SDK.

    Provides methods for:
    - Token swaps (exact input)
    - Add/remove liquidity
    - Pool queries
    - Bin math utilities

    Args:
        chain: Chain name (e.g., "avalanche")
        rpc_url: RPC endpoint URL
        wallet_address: Optional wallet address for transactions

    Example:
        sdk = TraderJoeV2SDK(
            chain="avalanche",
            rpc_url="https://api.avax.network/ext/bc/C/rpc",
        )

        # Get pool info
        pool = sdk.get_pool_address(wavax, usdc, bin_step=20)

        # Build swap transaction
        tx, gas = sdk.build_swap_exact_tokens_for_tokens(
            amount_in=10**18,
            amount_out_min=0,
            path=[wavax, usdc],
            bin_steps=[20],
            recipient="0x...",
        )
    """

    def __init__(
        self,
        chain: str,
        rpc_url: str | None = None,
        wallet_address: str | None = None,
        gateway_client: "GatewayClient | None" = None,
    ) -> None:
        self.chain = chain.lower()
        self.rpc_url = rpc_url
        self.wallet_address = wallet_address
        self._gateway_client = gateway_client

        # Validate chain
        if self.chain not in TRADERJOE_V2_ADDRESSES:
            raise TraderJoeV2SDKError(
                f"Chain '{chain}' not supported. Supported: {list(TRADERJOE_V2_ADDRESSES.keys())}"
            )

        if rpc_url is None and gateway_client is None:
            raise TraderJoeV2SDKError("TraderJoeV2SDK requires either rpc_url (deprecated) or gateway_client")

        # Initialize Web3 — route through gateway when available; otherwise
        # direct RPC (deprecated ad-hoc use).
        if gateway_client is not None:
            from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

            self.web3 = Web3(GatewayWeb3Provider(gateway_client, chain=self.chain))
        else:
            self.web3 = Web3(Web3.HTTPProvider(rpc_url))  # vib-2986-exempt: gateway-internal fallback
            # Preflight only for direct-RPC path. The gateway sidecar runs on
            # localhost and this round-trip adds no value over connecting lazily.
            if not self.web3.is_connected():
                raise TraderJoeV2SDKError(f"Failed to connect to RPC: {rpc_url}")

        # Inject POA middleware for chains with non-standard extraData (Avalanche, BSC, Polygon).
        # POA middleware is a client-side concern; the gateway forwards raw JSON-RPC unchanged.
        from almanak.gateway.utils.rpc_provider import is_poa_chain

        if is_poa_chain(self.chain):
            try:
                from web3.middleware import ExtraDataToPOAMiddleware

                poa_mw = ExtraDataToPOAMiddleware
            except ImportError:
                from web3.middleware import geth_poa_middleware  # type: ignore[attr-defined]

                poa_mw = geth_poa_middleware
            self.web3.middleware_onion.inject(poa_mw, layer=0)

        # Get contract addresses
        addresses = TRADERJOE_V2_ADDRESSES[self.chain]
        self.factory_address = Web3.to_checksum_address(addresses["factory"])
        self.router_address = Web3.to_checksum_address(addresses["router"])

        # WAVAX address
        self.wavax_address = Web3.to_checksum_address("0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7")

        # Load ABIs
        abi_dir = os.path.join(os.path.dirname(__file__), "abis")
        with open(os.path.join(abi_dir, "LBRouter.json")) as f:
            self.router_abi = json.load(f)
        with open(os.path.join(abi_dir, "LBFactory.json")) as f:
            self.factory_abi = json.load(f)
        with open(os.path.join(abi_dir, "LBPair.json")) as f:
            self.pair_abi = json.load(f)
        with open(os.path.join(abi_dir, "erc20_token.json")) as f:
            self.erc20_abi = json.load(f)

        # Initialize contracts
        self._router_contract = self.web3.eth.contract(address=self.router_address, abi=self.router_abi)
        self._factory_contract = self.web3.eth.contract(address=self.factory_address, abi=self.factory_abi)

        # Contract caches
        self._token_contracts: dict[str, Contract] = {}
        self._pair_contracts: dict[str, Contract] = {}
        # Pool address cache: (token_x_lower, token_y_lower, bin_step) -> pool_address
        # LBFactory.getLBPairInformation is immutable (pair address never changes)
        self._pool_address_cache: dict[tuple[str, str, int], str] = {}

        # Default deadline (100 days)
        self.deadline = int(time.time()) + DEADLINE_SECONDS

        logger.debug(
            f"TraderJoe V2 SDK initialized for {chain}: Router={self.router_address}, Factory={self.factory_address}"
        )

    # =========================================================================
    # Token Utilities
    # =========================================================================

    def get_token_contract(self, token_address: str) -> Contract:
        """Get or create a token contract instance."""
        address = Web3.to_checksum_address(token_address)
        if address not in self._token_contracts:
            self._token_contracts[address] = self.web3.eth.contract(address=address, abi=self.erc20_abi)
        return self._token_contracts[address]

    def get_balance(self, token_address: str, account: str) -> int:
        """Get the token balance of an account."""
        contract = self.get_token_contract(token_address)
        return contract.functions.balanceOf(Web3.to_checksum_address(account)).call()

    def get_allowance(self, token_address: str, owner: str, spender: str) -> int:
        """Get the allowance of a spender for a token owner."""
        contract = self.get_token_contract(token_address)
        return contract.functions.allowance(
            Web3.to_checksum_address(owner),
            Web3.to_checksum_address(spender),
        ).call()

    # =========================================================================
    # Pool Utilities
    # =========================================================================

    def get_pool_address(self, token_x: str, token_y: str, bin_step: int) -> str:
        """Get the LBPair (pool) address for a given token pair and binStep.

        Results are cached in-process: the LBFactory pair address is immutable
        and does not change after pool creation.

        Args:
            token_x: Address of token X
            token_y: Address of token Y
            bin_step: Bin step of the pair (e.g., 20 for 0.2%)

        Returns:
            Address of the LBPair contract

        Raises:
            PoolNotFoundError: If no pool exists for the pair/binStep
        """
        token_x = Web3.to_checksum_address(token_x)
        token_y = Web3.to_checksum_address(token_y)

        # Check in-process cache first (pool address is immutable once created).
        # Use sorted (canonical) key so reversed token order hits the same entry.
        cache_key = (min(token_x.lower(), token_y.lower()), max(token_x.lower(), token_y.lower()), bin_step)
        if cache_key in self._pool_address_cache:
            logger.debug(f"LBFactory cache hit for {token_x[:8]}/{token_y[:8]} binStep={bin_step}")
            return self._pool_address_cache[cache_key]

        try:
            # getLBPairInformation returns (binStep, LBPair, createdByOwner, ignoredForRouting)
            t0 = time.perf_counter()
            pair_info = self._factory_contract.functions.getLBPairInformation(token_x, token_y, bin_step).call()
            logger.debug(f"LBFactory.getLBPairInformation: {time.perf_counter() - t0:.2f}s")

            pair_address = pair_info[1]

            if pair_address == "0x0000000000000000000000000000000000000000":
                raise PoolNotFoundError(token_x, token_y, bin_step)

            self._pool_address_cache[cache_key] = pair_address
            return pair_address

        except Exception as e:
            if "PoolNotFoundError" in str(type(e).__name__):
                raise
            raise PoolNotFoundError(token_x, token_y, bin_step) from e

    def get_pair_contract(self, pool_address: str) -> Contract:
        """Get or create a pair contract instance."""
        address = Web3.to_checksum_address(pool_address)
        if address not in self._pair_contracts:
            self._pair_contracts[address] = self.web3.eth.contract(address=address, abi=self.pair_abi)
        return self._pair_contracts[address]

    def get_pool_info(self, pool_address: str) -> PoolInfo:
        """Get information about a pool."""
        pair = self.get_pair_contract(pool_address)

        active_id = pair.functions.getActiveId().call()
        bin_step = pair.functions.getBinStep().call()
        token_x = pair.functions.getTokenX().call()
        token_y = pair.functions.getTokenY().call()
        reserves = pair.functions.getReserves().call()

        return PoolInfo(
            address=pool_address,
            token_x=token_x,
            token_y=token_y,
            bin_step=bin_step,
            active_id=active_id,
            reserve_x=reserves[0],
            reserve_y=reserves[1],
        )

    def get_pool_spot_rate(self, pool_address: str) -> float:
        """Get the current spot price from a TraderJoe V2 LBPair pool.

        Price is calculated from the active bin ID using the formula:
        price = (1 + binStep/10000)^(activeId - 8388608) * 10^(decimalsX - decimalsY)

        Args:
            pool_address: Address of the LBPair contract

        Returns:
            Current spot price (tokenY per tokenX)
        """
        pair = self.get_pair_contract(pool_address)

        active_id = pair.functions.getActiveId().call()
        bin_step = pair.functions.getBinStep().call()

        token_x = pair.functions.getTokenX().call()
        token_y = pair.functions.getTokenY().call()

        decimals_x = self.get_token_contract(token_x).functions.decimals().call()
        decimals_y = self.get_token_contract(token_y).functions.decimals().call()

        return self.bin_id_to_price(bin_id=active_id, bin_step=bin_step, decimals_x=decimals_x, decimals_y=decimals_y)

    # =========================================================================
    # Bin Math Utilities
    # =========================================================================

    @staticmethod
    def bin_id_to_price(
        bin_id: int,
        bin_step: int,
        decimals_x: int = 18,
        decimals_y: int = 18,
    ) -> float:
        """Convert a bin ID to price using TraderJoe V2 formula.

        Formula: price = (1 + binStep/10000)^(binId - 8388608) * 10^(decimalsX - decimalsY)

        Args:
            bin_id: The bin ID
            bin_step: The bin step for the pair
            decimals_x: Decimals of tokenX (default 18)
            decimals_y: Decimals of tokenY (default 18)

        Returns:
            Price (tokenY per tokenX), adjusted for decimals
        """
        exponent = bin_id - BIN_ID_OFFSET
        base = 1 + (bin_step / 10000)

        # Use logarithms for numerical stability
        log_price = exponent * math.log(base) + (decimals_x - decimals_y) * math.log(10)
        price = math.exp(log_price)

        return price

    @staticmethod
    def price_to_bin_id(
        price: float,
        bin_step: int,
        decimals_x: int = 18,
        decimals_y: int = 18,
    ) -> int:
        """Convert a price to the nearest bin ID using TraderJoe V2 formula.

        Inverse formula: binId = (log(price) - (decimalsX - decimalsY) * log(10)) / log(1 + binStep/10000) + 8388608

        Args:
            price: Target price (tokenY per tokenX)
            bin_step: The bin step for the pair
            decimals_x: Decimals of tokenX (default 18)
            decimals_y: Decimals of tokenY (default 18)

        Returns:
            Nearest bin ID
        """
        log_price = math.log(price)
        log_base = math.log(1 + bin_step / 10000)
        decimal_adjustment = (decimals_x - decimals_y) * math.log(10)

        bin_id = int((log_price - decimal_adjustment) / log_base + BIN_ID_OFFSET)
        return bin_id

    # =========================================================================
    # Transaction Builders
    # =========================================================================

    def build_approve_transaction(
        self,
        token_address: str,
        spender_address: str,
        amount: int,
        from_address: str,
    ) -> tuple[dict[str, Any], int]:
        """Build an approve transaction for a token.

        Args:
            token_address: Address of the token to approve
            spender_address: Address of the spender (usually router)
            amount: Amount to approve (in wei)
            from_address: Address of the token owner

        Returns:
            Tuple of (transaction dict, estimated gas)
        """
        token = self.get_token_contract(token_address)
        from_addr = Web3.to_checksum_address(from_address)
        spender = Web3.to_checksum_address(spender_address)

        tx = token.functions.approve(spender, amount).build_transaction(
            {
                "from": from_addr,
                "gas": DEFAULT_GAS_ESTIMATES["approve"],
                "nonce": self.web3.eth.get_transaction_count(from_addr),
            }
        )

        return dict(tx), tx["gas"]

    def build_approve_for_all_transaction(
        self,
        pool_address: str,
        spender_address: str,
        from_address: str,
        approved: bool = True,
    ) -> tuple[dict[str, Any], int]:
        """Build approveForAll transaction for LB token (ERC1155-like).

        LB tokens require approveForAll before the router can remove liquidity.

        Args:
            pool_address: Address of the LBPair contract
            spender_address: Address of the spender (usually router)
            from_address: Address of the token owner
            approved: Whether to approve or revoke

        Returns:
            Tuple of (transaction dict, estimated gas)
        """
        pair = self.get_pair_contract(pool_address)
        from_addr = Web3.to_checksum_address(from_address)
        spender = Web3.to_checksum_address(spender_address)

        tx = pair.functions.approveForAll(spender, approved).build_transaction(
            {
                "from": from_addr,
                "gas": DEFAULT_GAS_ESTIMATES["approve"],
                "nonce": self.web3.eth.get_transaction_count(from_addr),
            }
        )

        return dict(tx), tx["gas"]

    def build_swap_exact_tokens_for_tokens(
        self,
        amount_in: int,
        amount_out_min: int,
        path: list[str],
        bin_steps: list[int],
        recipient: str,
        deadline: int | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Build transaction for swapping exact tokens for tokens.

        Args:
            amount_in: Amount of input token (in wei)
            amount_out_min: Minimum amount of output token (in wei)
            path: List of token addresses [tokenIn, tokenOut] or multi-hop
            bin_steps: List of binSteps for each pair in the path
            recipient: Address to receive output tokens
            deadline: Transaction deadline (default: current time + 100 days)

        Returns:
            Tuple of (transaction dict, estimated gas)
        """
        if deadline is None:
            deadline = int(time.time()) + DEADLINE_SECONDS

        # Convert addresses
        path = [Web3.to_checksum_address(addr) for addr in path]
        recipient = Web3.to_checksum_address(recipient)

        # Build Path struct: versions are all 2 for V2 pairs
        versions = [2] * len(bin_steps)

        path_struct = {
            "pairBinSteps": bin_steps,
            "versions": versions,
            "tokenPath": path,
        }

        tx = self._router_contract.functions.swapExactTokensForTokens(
            amount_in,
            amount_out_min,
            path_struct,
            recipient,
            deadline,
        ).build_transaction(
            {
                "from": recipient,
                "gas": DEFAULT_GAS_ESTIMATES["swap"],
                "nonce": self.web3.eth.get_transaction_count(recipient),
            }
        )

        return dict(tx), tx["gas"]

    def build_add_liquidity(
        self,
        token_x: str,
        token_y: str,
        bin_step: int,
        amount_x: int,
        amount_y: int,
        amount_x_min: int,
        amount_y_min: int,
        active_id_desired: int,
        id_slippage: int,
        delta_ids: list[int],
        distribution_x: list[int],
        distribution_y: list[int],
        to: str,
        refund_to: str,
        deadline: int | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Build transaction for adding liquidity to a TraderJoe V2 pair.

        Args:
            token_x: Address of token X
            token_y: Address of token Y
            bin_step: Bin step of the pair
            amount_x: Amount of token X to add
            amount_y: Amount of token Y to add
            amount_x_min: Minimum amount of token X (slippage protection)
            amount_y_min: Minimum amount of token Y (slippage protection)
            active_id_desired: Desired active bin ID
            id_slippage: Allowed slippage on active bin ID
            delta_ids: Delta IDs for liquidity distribution (relative to active)
            distribution_x: Distribution of token X across bins (sum to 10^18)
            distribution_y: Distribution of token Y across bins (sum to 10^18)
            to: Address to mint LB tokens to
            refund_to: Address to refund excess tokens to
            deadline: Transaction deadline

        Returns:
            Tuple of (transaction dict, estimated gas)
        """
        if deadline is None:
            deadline = int(time.time()) + DEADLINE_SECONDS

        liquidity_params = {
            "tokenX": Web3.to_checksum_address(token_x),
            "tokenY": Web3.to_checksum_address(token_y),
            "binStep": bin_step,
            "amountX": amount_x,
            "amountY": amount_y,
            "amountXMin": amount_x_min,
            "amountYMin": amount_y_min,
            "activeIdDesired": active_id_desired,
            "idSlippage": id_slippage,
            "deltaIds": delta_ids,
            "distributionX": distribution_x,
            "distributionY": distribution_y,
            "to": Web3.to_checksum_address(to),
            "refundTo": Web3.to_checksum_address(refund_to),
            "deadline": deadline,
        }

        to_addr = Web3.to_checksum_address(to)

        tx = self._router_contract.functions.addLiquidity(liquidity_params).build_transaction(
            {
                "from": to_addr,
                "gas": DEFAULT_GAS_ESTIMATES["add_liquidity"],
                "nonce": self.web3.eth.get_transaction_count(to_addr),
            }
        )

        return dict(tx), tx["gas"]

    def build_remove_liquidity(
        self,
        token_x: str,
        token_y: str,
        bin_step: int,
        amount_x_min: int,
        amount_y_min: int,
        ids: list[int],
        amounts: list[int],
        to: str,
        deadline: int | None = None,
    ) -> tuple[dict[str, Any], int]:
        """Build transaction for removing liquidity from a TraderJoe V2 pair.

        Args:
            token_x: Address of token X
            token_y: Address of token Y
            bin_step: Bin step of the pair
            amount_x_min: Minimum amount of token X to receive
            amount_y_min: Minimum amount of token Y to receive
            ids: Array of bin IDs to remove liquidity from
            amounts: Array of amounts of LB tokens to burn for each bin
            to: Address to receive tokens
            deadline: Transaction deadline

        Returns:
            Tuple of (transaction dict, estimated gas)
        """
        if deadline is None:
            deadline = int(time.time()) + DEADLINE_SECONDS

        to_addr = Web3.to_checksum_address(to)

        tx = self._router_contract.functions.removeLiquidity(
            Web3.to_checksum_address(token_x),
            Web3.to_checksum_address(token_y),
            bin_step,
            amount_x_min,
            amount_y_min,
            ids,
            amounts,
            to_addr,
            deadline,
        ).build_transaction(
            {
                "from": to_addr,
                "gas": DEFAULT_GAS_ESTIMATES["remove_liquidity"],
                "nonce": self.web3.eth.get_transaction_count(to_addr),
            }
        )

        return dict(tx), tx["gas"]

    def build_collect_fees(
        self,
        pool_address: str,
        account: str,
        ids: list[int],
    ) -> tuple[dict[str, Any], int]:
        """Build transaction for collecting accumulated fees from an LP position.

        Calls LBPair.collectFees(account, ids) which collects fees without
        removing any liquidity. This is a V2.1 feature of TraderJoe's Liquidity Book.

        The returned bytes32[] encodes fee amounts where each bytes32 has
        amountX in the upper 128 bits and amountY in the lower 128 bits.

        Args:
            pool_address: Address of the LBPair contract
            account: Address of the account to collect fees for
            ids: Array of bin IDs to collect fees from

        Returns:
            Tuple of (transaction dict, estimated gas)

        Raises:
            TraderJoeV2SDKError: If no bin IDs provided
        """
        if not ids:
            raise TraderJoeV2SDKError("No bin IDs provided for fee collection")

        pair = self.get_pair_contract(pool_address)
        account_addr = Web3.to_checksum_address(account)

        tx = pair.functions.collectFees(
            account_addr,
            ids,
        ).build_transaction(
            {
                "from": account_addr,
                "gas": DEFAULT_GAS_ESTIMATES["collect_fees"],
                "nonce": self.web3.eth.get_transaction_count(account_addr),
            }
        )

        return dict(tx), tx["gas"]

    def get_pending_fees(
        self,
        pool_address: str,
        account: str,
        ids: list[int],
    ) -> tuple[int, int]:
        """Query pending (uncollected) fees for a position.

        Args:
            pool_address: Address of the LBPair contract
            account: Address of the account to query
            ids: Array of bin IDs to query fees for

        Returns:
            Tuple of (total_fees_x, total_fees_y) in wei
        """
        pair = self.get_pair_contract(pool_address)
        account_addr = Web3.to_checksum_address(account)

        total_fees_x = 0
        total_fees_y = 0

        for bin_id in ids:
            try:
                fees = pair.functions.pendingFees(account_addr, bin_id).call()
                total_fees_x += fees[0]
                total_fees_y += fees[1]
            except Exception:
                # pendingFees may not be available on all versions
                continue

        return total_fees_x, total_fees_y

    # =========================================================================
    # Position Queries
    # =========================================================================

    def get_position_balances(
        self,
        pool_address: str,
        wallet_address: str,
        bin_range: int = 50,
    ) -> dict[int, int]:
        """Get LB token balances for a wallet across bins.

        Args:
            pool_address: Address of the LBPair contract
            wallet_address: Address to query balances for
            bin_range: Number of bins to check on each side of active bin

        Returns:
            Dict mapping bin ID to balance
        """
        pair = self.get_pair_contract(pool_address)

        t0 = time.perf_counter()
        active_id = pair.functions.getActiveId().call()
        logger.debug(f"LBPair.getActiveId: {time.perf_counter() - t0:.2f}s")

        wallet = Web3.to_checksum_address(wallet_address)
        candidate_bins = list(range(active_id - bin_range, active_id + bin_range + 1))
        balances: dict[int, int] = {}

        t0 = time.perf_counter()
        accounts = [wallet] * len(candidate_bins)
        try:
            raw_balances = pair.functions.balanceOfBatch(accounts, candidate_bins).call()
            for bin_id, balance in zip(candidate_bins, raw_balances, strict=True):
                if balance > 0:
                    balances[bin_id] = balance
        except Exception:
            logger.warning("balanceOfBatch unavailable on %s, falling back to per-bin scan", pool_address)
            failed_bins = 0
            for bin_id in candidate_bins:
                try:
                    balance = pair.functions.balanceOf(wallet, bin_id).call()
                    if balance > 0:
                        balances[bin_id] = balance
                except Exception:
                    failed_bins += 1
                    continue
            if failed_bins == len(candidate_bins) and not balances:
                logger.error(
                    "All per-bin balanceOf calls failed for %s (wallet=%s, bins=%d) — balance unknown",
                    pool_address,
                    wallet,
                    failed_bins,
                )
            elif failed_bins:
                logger.debug("Per-bin fallback: %d/%d bins failed", failed_bins, len(candidate_bins))
        logger.debug(
            "Position balance batch scan (%s bins, %s with balance): %.2fs",
            len(candidate_bins),
            len(balances),
            time.perf_counter() - t0,
        )

        return balances

    def get_position_balances_for_ids(
        self,
        pool_address: str,
        wallet_address: str,
        bin_ids: list[int],
    ) -> dict[int, int]:
        """Get LB token balances for a wallet in specific bins.

        Use this when the caller already knows which bins matter and wants to
        avoid the broader active-bin scan performed by get_position_balances().

        Args:
            pool_address: Address of the LBPair contract
            wallet_address: Address to query balances for
            bin_ids: Exact bin IDs to query

        Returns:
            Dict mapping bin ID to balance for bins with non-zero balance
        """
        pair = self.get_pair_contract(pool_address)
        wallet = Web3.to_checksum_address(wallet_address)
        candidate_bins = [int(raw_bin_id) for raw_bin_id in bin_ids]
        balances: dict[int, int] = {}

        t0 = time.perf_counter()
        if not candidate_bins:
            return balances
        accounts = [wallet] * len(candidate_bins)
        try:
            raw_balances = pair.functions.balanceOfBatch(accounts, candidate_bins).call()
            for bin_id, balance in zip(candidate_bins, raw_balances, strict=True):
                if balance > 0:
                    balances[bin_id] = balance
        except Exception:
            logger.warning("balanceOfBatch unavailable on %s, falling back to per-bin scan", pool_address)
            failed_bins = 0
            for bin_id in candidate_bins:
                try:
                    balance = pair.functions.balanceOf(wallet, bin_id).call()
                    if balance > 0:
                        balances[bin_id] = balance
                except Exception:
                    failed_bins += 1
                    continue
            if failed_bins == len(candidate_bins) and not balances:
                logger.error(
                    "All per-bin balanceOf calls failed for %s (wallet=%s, bins=%d) — balance unknown",
                    pool_address,
                    wallet,
                    failed_bins,
                )
            elif failed_bins:
                logger.debug("Per-bin fallback: %d/%d bins failed", failed_bins, len(candidate_bins))
        logger.debug(
            "Position balance targeted batch scan (%s bins, %s with balance): %.2fs",
            len(candidate_bins),
            len(balances),
            time.perf_counter() - t0,
        )

        return balances

    def get_total_position_value(
        self,
        pool_address: str,
        wallet_address: str,
        precomputed_balances: dict[int, int] | None = None,
        *,
        strict: bool = False,
        sanity_floor: float = 0.9,
    ) -> tuple[int, int]:
        """Get total token amounts for a wallet's position in a pool.

        Computes per-bin share of LBPair reserves: for each bin where the
        wallet holds LB tokens, reads the bin's reserves and total supply
        and accumulates the wallet's share. Returns ``(total_x, total_y)``
        in raw units.

        Failure modes & ``strict`` parameter:
            VIB-3757 investigation discovered that the historical "best-effort"
            behaviour (silently skipping bins on revert) was masking a real
            bug: the LBPair ABI declared ``getBin(uint256)`` while the
            deployed contract is ``getBin(uint24)``, so the function selector
            was wrong and EVERY call reverted. The function returned
            ``(0, 0)`` for every position, and any caller deriving slippage
            minimums from this got zero protection.

            With the ABI fixed to ``uint24``, well-behaved RPCs should not
            revert — this is the strict path. ``strict=True`` raises on the
            first per-bin read error so a caller deriving money-critical
            values (e.g. slippage minimums) fails loudly instead of silently
            shipping a position close with no slippage protection.

            ``strict=False`` (default for backward compatibility) keeps the
            tolerant per-bin behaviour but applies a sanity floor: if more
            than ``(1 - sanity_floor)`` of the queried bins fail, the
            function raises rather than returning a misleading partial
            valuation. ``sanity_floor=0.9`` means at least 90% of bins must
            succeed; below that we treat the result as untrustworthy.

            **Practical effect of the 0.9 default on small positions**:
            because the threshold is a ratio, small-N positions effectively
            behave like ``strict=True`` for any per-bin failure:

                | bins | success ratio after 1 failure | passes 0.9 floor? |
                | ---- | ----------------------------- | ----------------- |
                |   5  | 4/5 = 80%                     | NO  -> raises     |
                |   9  | 8/9 = 89%                     | NO  -> raises     |
                |  10  | 9/10 = 90%                    | YES -> warns      |
                |  21  | 20/21 = 95%                   | YES -> warns      |

            Real TJ V2 LP positions are typically 5-21 bins, so callers
            should expect any single per-bin RPC blip on a sub-10-bin
            position to surface as a ``RuntimeError`` rather than a
            silently degraded valuation. With the ABI selector now correct,
            well-behaved RPCs do not revert here, so this is rare in
            practice — but the failure mode IS now loud.

        Args:
            pool_address: Address of the LBPair contract.
            wallet_address: Address to query.
            precomputed_balances: Optional pre-fetched bin balances to
                avoid a redundant ``get_position_balances()`` call (pass
                the result from a prior call).
            strict: When ``True``, raise on the first per-bin read error.
                When ``False`` (default), tolerate per-bin reverts up to
                the ``sanity_floor`` threshold.
            sanity_floor: Minimum fraction of bins that must succeed when
                ``strict=False`` (default 0.9). Below this threshold the
                method raises ``RuntimeError`` rather than returning a
                misleading partial valuation. Set to 0 to restore the
                original silent-degradation behaviour (NOT recommended for
                money-critical callers).

        Returns:
            Tuple of (amount_x, amount_y) the wallet would receive if
            removing all liquidity.

        Raises:
            RuntimeError: When ``strict=True`` and any per-bin call reverts,
                or when ``strict=False`` and the success ratio falls below
                ``sanity_floor``.
        """
        balances = (
            precomputed_balances
            if precomputed_balances is not None
            else self.get_position_balances(pool_address, wallet_address)
        )

        if not balances:
            return 0, 0

        pair = self.get_pair_contract(pool_address)
        total_x = 0
        total_y = 0
        failed_bins: list[tuple[int, str]] = []

        t0 = time.perf_counter()
        for bin_id, balance in balances.items():
            try:
                bin_reserves = pair.functions.getBin(bin_id).call()
                bin_reserve_x = bin_reserves[0]
                bin_reserve_y = bin_reserves[1]

                total_supply = pair.functions.totalSupply(bin_id).call()

                if total_supply > 0:
                    share_x = (balance * bin_reserve_x) // total_supply
                    share_y = (balance * bin_reserve_y) // total_supply
                    total_x += share_x
                    total_y += share_y
            except Exception as exc:  # noqa: BLE001 — RPC-level error
                if strict:
                    raise RuntimeError(
                        f"get_total_position_value: per-bin read failed for "
                        f"bin_id={bin_id} on pool {pool_address} (strict=True): {exc}"
                    ) from exc
                failed_bins.append((bin_id, type(exc).__name__))

        logger.debug(
            "Position value calculation (%d/%d bins ok): %.2fs",
            len(balances) - len(failed_bins),
            len(balances),
            time.perf_counter() - t0,
        )

        # Sanity floor for non-strict mode: too many failures → caller's
        # slippage protection would be silently degraded. Raise instead so
        # the compiler can surface the issue rather than ship a close with
        # near-zero amount_x_min/amount_y_min.
        if not strict and failed_bins:
            success_ratio = (len(balances) - len(failed_bins)) / len(balances)
            if success_ratio < sanity_floor:
                samples = ", ".join(f"{bid}({err})" for bid, err in failed_bins[:5])
                # Surface the failure as a structured ERROR so on-call sees
                # how often the new sanity-floor abort fires in production.
                # Without this, a noisy RPC turns into "the strategy keeps
                # raising RuntimeError" with no protocol-layer context.
                logger.error(
                    "get_total_position_value sanity-floor abort: pool=%s "
                    "success_ratio=%.1f%% sanity_floor=%.0f%% failed=%d/%d "
                    "samples=%s",
                    pool_address,
                    success_ratio * 100,
                    sanity_floor * 100,
                    len(failed_bins),
                    len(balances),
                    samples,
                )
                raise RuntimeError(
                    f"get_total_position_value: only {success_ratio:.1%} of "
                    f"{len(balances)} bins returned data on pool {pool_address} "
                    f"(sanity_floor={sanity_floor:.0%}). "
                    f"Refusing to return a partial valuation that would silently "
                    f"degrade slippage protection. Failed samples: {samples}"
                )
            # Some failures, but above the floor — note them for operator visibility.
            logger.warning(
                "get_total_position_value: %d/%d bins failed on pool %s "
                "(success_ratio=%.1f%%, above sanity_floor=%.0f%%). Continuing "
                "with partial valuation.",
                len(failed_bins),
                len(balances),
                pool_address,
                success_ratio * 100,
                sanity_floor * 100,
            )

        return total_x, total_y
