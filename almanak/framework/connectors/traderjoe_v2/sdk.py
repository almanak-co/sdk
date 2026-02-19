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
    from almanak.framework.connectors.traderjoe_v2 import TraderJoeV2SDK

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
from typing import Any

from web3 import Web3
from web3.contract import Contract

from almanak.core.contracts import TRADERJOE_V2 as TRADERJOE_V2_ADDRESSES

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
}

# TraderJoe V2 constants
MAX_UINT256 = 2**256 - 1
DEADLINE_100_DAYS = 8_640_000

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
        rpc_url: str,
        wallet_address: str | None = None,
    ) -> None:
        self.chain = chain.lower()
        self.rpc_url = rpc_url
        self.wallet_address = wallet_address

        # Validate chain
        if self.chain not in TRADERJOE_V2_ADDRESSES:
            raise TraderJoeV2SDKError(
                f"Chain '{chain}' not supported. Supported: {list(TRADERJOE_V2_ADDRESSES.keys())}"
            )

        # Initialize Web3
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        if not self.web3.is_connected():
            raise TraderJoeV2SDKError(f"Failed to connect to RPC: {rpc_url}")

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

        # Default deadline (100 days)
        self.deadline = int(time.time()) + DEADLINE_100_DAYS

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

        try:
            # getLBPairInformation returns (binStep, LBPair, createdByOwner, ignoredForRouting)
            pair_info = self._factory_contract.functions.getLBPairInformation(token_x, token_y, bin_step).call()

            pair_address = pair_info[1]

            if pair_address == "0x0000000000000000000000000000000000000000":
                raise PoolNotFoundError(token_x, token_y, bin_step)

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
            deadline = int(time.time()) + DEADLINE_100_DAYS

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
            deadline = int(time.time()) + DEADLINE_100_DAYS

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
            deadline = int(time.time()) + DEADLINE_100_DAYS

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
        active_id = pair.functions.getActiveId().call()
        wallet = Web3.to_checksum_address(wallet_address)

        balances: dict[int, int] = {}

        for delta in range(-bin_range, bin_range + 1):
            bin_id = active_id + delta
            try:
                balance = pair.functions.balanceOf(wallet, bin_id).call()
                if balance > 0:
                    balances[bin_id] = balance
            except Exception:
                continue

        return balances

    def get_total_position_value(
        self,
        pool_address: str,
        wallet_address: str,
    ) -> tuple[int, int]:
        """Get total token amounts for a wallet's position in a pool.

        Args:
            pool_address: Address of the LBPair contract
            wallet_address: Address to query

        Returns:
            Tuple of (amount_x, amount_y) the wallet would receive if removing all liquidity
        """
        balances = self.get_position_balances(pool_address, wallet_address)

        if not balances:
            return 0, 0

        pair = self.get_pair_contract(pool_address)
        total_x = 0
        total_y = 0

        for bin_id, balance in balances.items():
            try:
                # Get bin reserves and total supply to calculate share
                bin_reserves = pair.functions.getBin(bin_id).call()
                bin_reserve_x = bin_reserves[0]
                bin_reserve_y = bin_reserves[1]

                total_supply = pair.functions.totalSupply(bin_id).call()

                if total_supply > 0:
                    share_x = (balance * bin_reserve_x) // total_supply
                    share_y = (balance * bin_reserve_y) // total_supply
                    total_x += share_x
                    total_y += share_y
            except Exception:
                continue

        return total_x, total_y
