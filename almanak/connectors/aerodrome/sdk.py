"""Aerodrome/Velodrome Finance SDK for Solidly-fork AMMs.

Supports Aerodrome on Base and Velodrome V2 on Optimism. Both are Solidly-based AMMs with dual pool types:
- Volatile pools: x*y=k formula (0.3% fee)
- Stable pools: x^3*y + y^3*x formula (0.05% fee)

Key difference from Uniswap V2: All operations require `stable` parameter to select pool type.
Aerodrome uses fungible LP tokens (not NFTs like Uniswap V3).

Contract Architecture:
- Router: Main entry point for swaps and liquidity operations
- Factory: Creates and manages pools
- Pool: Individual AMM pools with reserves

Example:
    from almanak.connectors.aerodrome import AerodromeSDK

    sdk = AerodromeSDK(chain="base", rpc_url="https://mainnet.base.org")

    # Get pool info
    pool = sdk.get_pool(token_a, token_b, stable=False)

    # Get swap quote
    amount_out = sdk.get_amount_out(amount_in, token_in, token_out, stable=False)

    # Build swap transaction
    tx = sdk.build_swap_tx(amount_in, amount_out_min, routes, recipient, deadline, sender)
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

from almanak.core.contracts import AERODROME as AERODROME_ADDRESSES

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================


# Gas estimates
AERODROME_GAS_ESTIMATES: dict[str, int] = {
    "approve": 46000,
    "swap": 180000,
    "swap_multi_hop": 250000,
    "add_liquidity": 220000,
    "remove_liquidity": 200000,
    "wrap": 30000,
    "unwrap": 30000,
    # Slipstream CL operations
    "cl_mint": 500000,
    "cl_decrease_liquidity": 150000,
    "cl_collect": 120000,
    "cl_approve": 46000,
}

# Maximum uint256 value
MAX_UINT256 = 2**256 - 1

# Default deadline (100 days in seconds)
DEFAULT_DEADLINE_SECONDS = 8640000


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PoolInfo:
    """Information about an Aerodrome pool.

    Attributes:
        address: Pool contract address
        token0: First token address
        token1: Second token address
        stable: True for stable pool, False for volatile
        reserve0: Current reserve of token0
        reserve1: Current reserve of token1
        decimals0: Decimals of token0
        decimals1: Decimals of token1
    """

    address: str
    token0: str
    token1: str
    stable: bool
    reserve0: int = 0
    reserve1: int = 0
    decimals0: int = 18
    decimals1: int = 18

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "address": self.address,
            "token0": self.token0,
            "token1": self.token1,
            "stable": self.stable,
            "reserve0": str(self.reserve0),
            "reserve1": str(self.reserve1),
            "decimals0": self.decimals0,
            "decimals1": self.decimals1,
        }


@dataclass
class CLPositionInfo:
    """Information about a Slipstream CL NFT position.

    Attributes:
        token_id: NFT token ID
        token0: Address of token0
        token1: Address of token1
        tick_spacing: Pool tick spacing
        tick_lower: Lower tick of the position range
        tick_upper: Upper tick of the position range
        liquidity: Current liquidity in the position
        tokens_owed0: Uncollected fees for token0
        tokens_owed1: Uncollected fees for token1
    """

    token_id: int
    token0: str
    token1: str
    tick_spacing: int
    tick_lower: int
    tick_upper: int
    liquidity: int
    tokens_owed0: int
    tokens_owed1: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "token_id": self.token_id,
            "token0": self.token0,
            "token1": self.token1,
            "tick_spacing": self.tick_spacing,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "liquidity": str(self.liquidity),
            "tokens_owed0": str(self.tokens_owed0),
            "tokens_owed1": str(self.tokens_owed1),
        }


@dataclass
class SwapRoute:
    """A single hop in a swap route.

    Attributes:
        from_token: Input token address
        to_token: Output token address
        stable: Pool type (True=stable, False=volatile)
        factory: Factory address (optional, uses default)
    """

    from_token: str
    to_token: str
    stable: bool
    factory: str | None = None

    def to_tuple(self, default_factory: str) -> tuple:
        """Convert to tuple format for contract call.

        All addresses are checksummed to prevent web3.py rejection
        which would silently fall back to zero slippage protection.
        """
        from web3 import Web3

        return (
            Web3.to_checksum_address(self.from_token),
            Web3.to_checksum_address(self.to_token),
            self.stable,
            Web3.to_checksum_address(self.factory or default_factory),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from": self.from_token,
            "to": self.to_token,
            "stable": self.stable,
        }


@dataclass
class SwapQuote:
    """Quote for a swap operation.

    Attributes:
        amount_in: Input amount
        amount_out: Expected output amount
        routes: List of swap routes
        price_impact_bps: Estimated price impact in basis points
        gas_estimate: Estimated gas for the swap
    """

    amount_in: int
    amount_out: int
    routes: list[SwapRoute]
    price_impact_bps: int = 0
    gas_estimate: int = AERODROME_GAS_ESTIMATES["swap"]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "routes": [r.to_dict() for r in self.routes],
            "price_impact_bps": self.price_impact_bps,
            "gas_estimate": self.gas_estimate,
        }


# =============================================================================
# Exceptions
# =============================================================================


class AerodromeSDKError(Exception):
    """Base exception for Aerodrome SDK errors."""

    pass


class PoolNotFoundError(AerodromeSDKError):
    """Raised when a pool doesn't exist."""

    pass


class InsufficientLiquidityError(AerodromeSDKError):
    """Raised when pool has insufficient liquidity."""

    pass


# =============================================================================
# Aerodrome SDK
# =============================================================================


class AerodromeSDK:
    """Low-level SDK for Aerodrome/Velodrome Finance (Solidly forks).

    This SDK provides direct interaction with Solidly-fork contracts (Aerodrome on Base, Velodrome V2 on Optimism):
    - Pool queries (reserves, amounts)
    - Transaction building (swaps, liquidity)
    - ABI encoding for all operations

    Example:
        sdk = AerodromeSDK(chain="base")

        # Get quote for swap
        quote = sdk.get_swap_quote(
            token_in="0x...",
            token_out="0x...",
            amount_in=1000000,
            stable=False,
        )

        # Build swap transaction
        tx = sdk.build_swap_exact_tokens_tx(
            amount_in=1000000,
            amount_out_min=990000,
            routes=[SwapRoute(token_in, token_out, stable=False)],
            recipient="0x...",
            deadline=int(time.time()) + 300,
            sender="0x...",
        )
    """

    def __init__(
        self,
        chain: str = "base",
        rpc_url: str | None = None,
        token_resolver: "TokenResolverType | None" = None,
        gateway_client: "GatewayClient | None" = None,
    ) -> None:
        """Initialize the SDK.

        Args:
            chain: Target chain ("base" for Aerodrome, "optimism" for Velodrome V2)
            rpc_url: DEPRECATED — direct RPC URL. Bypasses the gateway and is
                only used for ad-hoc scripts. Prefer gateway_client for any
                code path that runs in a strategy container.
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
            gateway_client: Gateway client used to route on-chain queries
                (eth_call) through the gateway's RpcService. Preferred over
                rpc_url for all production code paths.
        """
        if chain not in AERODROME_ADDRESSES:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(AERODROME_ADDRESSES.keys())}")

        self.chain = chain
        self.rpc_url = rpc_url
        self._gateway_client = gateway_client

        # Load contract addresses
        self.addresses = AERODROME_ADDRESSES[chain]

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Load ABIs
        self._abi_dir = os.path.join(os.path.dirname(__file__), "abis")
        self._router_abi = self._load_abi("router")
        self._factory_abi = self._load_abi("pool_factory")
        self._pool_abi = self._load_abi("pool")
        self._erc20_abi = self._load_abi("erc20")
        self._weth_abi = self._load_abi("weth")

        # Gas buffer for Base chain (higher base fees)
        self.gas_buffer = 0.5

        # Load CL ABIs (only if cl_nft address is present for chain)
        self._cl_nft_abi: list[dict] = []
        self._cl_pool_abi: list[dict] = []
        if "cl_nft" in self.addresses:
            self._cl_nft_abi = self._load_abi("cl_nft")
            self._cl_pool_abi = self._load_abi("cl_pool")

        logger.info(f"AerodromeSDK initialized for chain={chain}")

    def _load_abi(self, name: str) -> list[dict]:
        """Load ABI from file."""
        abi_path = os.path.join(self._abi_dir, f"{name}.json")
        try:
            with open(abi_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"ABI file not found: {abi_path}")
            return []

    # =========================================================================
    # Pool Queries
    # =========================================================================

    def get_pool_address(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
    ) -> str | None:
        """Get pool address from factory.

        Routes eth_call through the gateway when gateway_client is set on
        the SDK. Falls back to direct RPC only for ad-hoc script usage
        (deprecated).

        Args:
            token_a: First token address
            token_b: Second token address
            stable: Pool type

        Returns:
            Pool address if exists, None otherwise
        """
        try:
            from web3 import Web3

            if self._gateway_client is not None:
                from almanak.framework.web3.gateway_provider import GatewayWeb3Provider

                web3 = Web3(GatewayWeb3Provider(self._gateway_client, chain=self.chain))
                return self.get_pool_address_from_factory(token_a, token_b, stable, web3)

            # Fallback: direct RPC (deprecated, ad-hoc scripts only)
            rpc_url = self.rpc_url
            if not rpc_url:
                try:
                    from almanak.gateway.utils.rpc_provider import get_rpc_url

                    rpc_url = get_rpc_url(self.chain)
                except (ImportError, ValueError):
                    pass

            if not rpc_url:
                logger.warning("No gateway_client or RPC URL available - cannot query pool address from factory")
                return None

            web3 = Web3(Web3.HTTPProvider(rpc_url))  # vib-2986-exempt: gateway-internal fallback
            return self.get_pool_address_from_factory(token_a, token_b, stable, web3)
        except Exception as e:
            logger.error(f"Failed to query pool address: {e}")
            return None

    def get_pool_address_from_factory(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        web3: Any,
    ) -> str | None:
        """Get pool address from factory contract.

        Args:
            token_a: First token address
            token_b: Second token address
            stable: Pool type
            web3: Web3 instance

        Returns:
            Pool address if exists, None otherwise
        """
        factory = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["factory"]),
            abi=self._factory_abi,
        )

        pool_address = factory.functions.getPool(
            web3.to_checksum_address(token_a),
            web3.to_checksum_address(token_b),
            stable,
        ).call()

        # Returns address(0) if pool doesn't exist
        if pool_address == "0x0000000000000000000000000000000000000000":
            return None

        return pool_address

    def get_pool_info(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        web3: Any,
    ) -> PoolInfo | None:
        """Get full pool information.

        Args:
            token_a: First token address
            token_b: Second token address
            stable: Pool type
            web3: Web3 instance

        Returns:
            PoolInfo if pool exists, None otherwise
        """
        pool_address = self.get_pool_address_from_factory(token_a, token_b, stable, web3)
        if not pool_address:
            return None

        pool = web3.eth.contract(
            address=web3.to_checksum_address(pool_address),
            abi=self._pool_abi,
        )

        # Get metadata (returns tuple with reserves, decimals, tokens, stable)
        metadata = pool.functions.metadata().call()

        return PoolInfo(
            address=pool_address,
            token0=metadata[5],  # token0 address
            token1=metadata[6],  # token1 address
            stable=metadata[4],  # stable flag
            reserve0=metadata[2],  # reserve0
            reserve1=metadata[3],  # reserve1
            decimals0=metadata[0],  # decimals0
            decimals1=metadata[1],  # decimals1
        )

    def get_amount_out(
        self,
        amount_in: int,
        token_in: str,
        token_out: str,
        stable: bool,
        web3: Any,
    ) -> int | None:
        """Get expected output amount for a swap.

        Args:
            amount_in: Input amount
            token_in: Input token address
            token_out: Output token address
            stable: Pool type
            web3: Web3 instance

        Returns:
            Output amount or None if pool doesn't exist
        """
        pool_address = self.get_pool_address_from_factory(token_in, token_out, stable, web3)
        if not pool_address:
            return None

        pool = web3.eth.contract(
            address=web3.to_checksum_address(pool_address),
            abi=self._pool_abi,
        )

        try:
            amount_out = pool.functions.getAmountOut(
                amount_in,
                web3.to_checksum_address(token_in),
            ).call()
            return amount_out
        except Exception as e:
            logger.warning(f"Error getting amount out: {e}")
            return None

    def get_amounts_out(
        self,
        amount_in: int,
        routes: list[SwapRoute],
        web3: Any,
    ) -> list[int] | None:
        """Get expected output amounts for multi-hop swap.

        Args:
            amount_in: Input amount
            routes: List of swap routes
            web3: Web3 instance

        Returns:
            List of amounts for each hop, or None on error
        """
        router = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["router"]),
            abi=self._router_abi,
        )

        # Convert routes to tuple format
        route_tuples = [r.to_tuple(self.addresses["factory"]) for r in routes]

        try:
            amounts = router.functions.getAmountsOut(amount_in, route_tuples).call()
            return list(amounts)
        except Exception as e:
            logger.warning(f"Error getting amounts out: {e}")
            return None

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build ERC-20 approve transaction.

        Args:
            token_address: Token to approve
            spender: Address to approve for spending
            amount: Amount to approve (use MAX_UINT256 for unlimited)
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        token = web3.eth.contract(
            address=web3.to_checksum_address(token_address),
            abi=self._erc20_abi,
        )

        tx = token.functions.approve(
            web3.to_checksum_address(spender),
            amount,
        ).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "gas": AERODROME_GAS_ESTIMATES["approve"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        return tx

    def build_swap_exact_tokens_tx(
        self,
        amount_in: int,
        amount_out_min: int,
        routes: list[SwapRoute],
        recipient: str,
        deadline: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build swapExactTokensForTokens transaction.

        Args:
            amount_in: Input token amount
            amount_out_min: Minimum output amount (slippage protection)
            routes: Swap routes
            recipient: Recipient address
            deadline: Unix timestamp deadline
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        router = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["router"]),
            abi=self._router_abi,
        )

        # Convert routes to tuple format
        route_tuples = [r.to_tuple(self.addresses["factory"]) for r in routes]

        tx = router.functions.swapExactTokensForTokens(
            amount_in,
            amount_out_min,
            route_tuples,
            web3.to_checksum_address(recipient),
            deadline,
        ).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        # Apply gas buffer
        tx["gas"] = int(tx["gas"] * (1 + self.gas_buffer))

        return tx

    def build_add_liquidity_tx(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        amount_a_desired: int,
        amount_b_desired: int,
        amount_a_min: int,
        amount_b_min: int,
        recipient: str,
        deadline: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build addLiquidity transaction.

        Args:
            token_a: First token address
            token_b: Second token address
            stable: Pool type
            amount_a_desired: Desired amount of token A
            amount_b_desired: Desired amount of token B
            amount_a_min: Minimum amount of token A
            amount_b_min: Minimum amount of token B
            recipient: LP token recipient
            deadline: Unix timestamp deadline
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        router = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["router"]),
            abi=self._router_abi,
        )

        tx = router.functions.addLiquidity(
            web3.to_checksum_address(token_a),
            web3.to_checksum_address(token_b),
            stable,
            amount_a_desired,
            amount_b_desired,
            amount_a_min,
            amount_b_min,
            web3.to_checksum_address(recipient),
            deadline,
        ).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        # Apply gas buffer
        tx["gas"] = int(tx["gas"] * (1 + self.gas_buffer))

        return tx

    def build_remove_liquidity_tx(
        self,
        token_a: str,
        token_b: str,
        stable: bool,
        liquidity: int,
        amount_a_min: int,
        amount_b_min: int,
        recipient: str,
        deadline: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build removeLiquidity transaction.

        Args:
            token_a: First token address
            token_b: Second token address
            stable: Pool type
            liquidity: LP token amount to burn
            amount_a_min: Minimum token A to receive
            amount_b_min: Minimum token B to receive
            recipient: Token recipient
            deadline: Unix timestamp deadline
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        router = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["router"]),
            abi=self._router_abi,
        )

        tx = router.functions.removeLiquidity(
            web3.to_checksum_address(token_a),
            web3.to_checksum_address(token_b),
            stable,
            liquidity,
            amount_a_min,
            amount_b_min,
            web3.to_checksum_address(recipient),
            deadline,
        ).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        # Apply gas buffer
        tx["gas"] = int(tx["gas"] * (1 + self.gas_buffer))

        return tx

    def build_wrap_eth_tx(
        self,
        amount: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build WETH wrap (deposit) transaction.

        Args:
            amount: ETH amount to wrap
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        # Resolve WETH address
        weth_address = self.resolve_token("WETH")
        if not weth_address:
            raise ValueError("WETH token address not found")

        weth = web3.eth.contract(
            address=web3.to_checksum_address(weth_address),
            abi=self._weth_abi,
        )

        tx = weth.functions.deposit().build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "value": amount,
                "gas": AERODROME_GAS_ESTIMATES["wrap"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        return tx

    def build_unwrap_eth_tx(
        self,
        amount: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build WETH unwrap (withdraw) transaction.

        Args:
            amount: WETH amount to unwrap
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        # Resolve WETH address
        weth_address = self.resolve_token("WETH")
        if not weth_address:
            raise ValueError("WETH token address not found")

        weth = web3.eth.contract(
            address=web3.to_checksum_address(weth_address),
            abi=self._weth_abi,
        )

        tx = weth.functions.withdraw(amount).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "gas": AERODROME_GAS_ESTIMATES["unwrap"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )

        return tx

    # =========================================================================
    # Slipstream CL Methods
    # =========================================================================

    def get_cl_pool_address(
        self,
        token_a: str,
        token_b: str,
        tick_spacing: int,
        web3: Any,
    ) -> str | None:
        """Get Slipstream CL pool address from cl_factory.

        Args:
            token_a: First token address
            token_b: Second token address
            tick_spacing: Pool tick spacing (int24)
            web3: Web3 instance

        Returns:
            Pool address if exists, None otherwise
        """
        if "cl_factory" not in self.addresses:
            logger.warning(f"No cl_factory address for chain {self.chain}")
            return None
        try:
            # Minimal ABI for getPool(address,address,int24)
            cl_factory_abi = [
                {
                    "type": "function",
                    "name": "getPool",
                    "inputs": [
                        {"name": "tokenA", "type": "address"},
                        {"name": "tokenB", "type": "address"},
                        {"name": "tickSpacing", "type": "int24"},
                    ],
                    "outputs": [{"name": "pool", "type": "address"}],
                    "stateMutability": "view",
                }
            ]
            factory = web3.eth.contract(
                address=web3.to_checksum_address(self.addresses["cl_factory"]),
                abi=cl_factory_abi,
            )
            pool_address = factory.functions.getPool(
                web3.to_checksum_address(token_a),
                web3.to_checksum_address(token_b),
                tick_spacing,
            ).call()
            if pool_address == "0x0000000000000000000000000000000000000000":
                return None
            return pool_address
        except Exception as e:
            logger.warning(f"Failed to query CL pool address: {e}")
            return None

    def get_cl_pool_slot0(
        self,
        pool_address: str,
        web3: Any,
    ) -> tuple[int, int] | None:
        """Get sqrtPriceX96 and current tick from Slipstream CL pool slot0.

        Args:
            pool_address: CL pool contract address
            web3: Web3 instance

        Returns:
            Tuple of (sqrtPriceX96, current_tick) or None if query failed
        """
        try:
            pool = web3.eth.contract(
                address=web3.to_checksum_address(pool_address),
                abi=self._cl_pool_abi,
            )
            slot0 = pool.functions.slot0().call()
            # slot0 returns (sqrtPriceX96, tick, observationIndex, observationCardinality, observationCardinalityNext, unlocked)
            return int(slot0[0]), int(slot0[1])
        except Exception as e:
            logger.warning(f"Failed to query CL pool slot0 for {pool_address}: {e}")
            return None

    def get_cl_position(
        self,
        token_id: int,
        web3: Any,
    ) -> "CLPositionInfo | None":
        """Get Slipstream CL position info by NFT token ID.

        Args:
            token_id: NFT token ID
            web3: Web3 instance

        Returns:
            CLPositionInfo if found, None otherwise
        """
        if "cl_nft" not in self.addresses:
            logger.warning(f"No cl_nft address for chain {self.chain}")
            return None
        try:
            nft = web3.eth.contract(
                address=web3.to_checksum_address(self.addresses["cl_nft"]),
                abi=self._cl_nft_abi,
            )
            pos = nft.functions.positions(token_id).call()
            # positions returns: (nonce, operator, token0, token1, tickSpacing, tickLower, tickUpper, liquidity, feeGrowth0, feeGrowth1, tokensOwed0, tokensOwed1)
            return CLPositionInfo(
                token_id=token_id,
                token0=pos[2].lower(),
                token1=pos[3].lower(),
                tick_spacing=int(pos[4]),
                tick_lower=int(pos[5]),
                tick_upper=int(pos[6]),
                liquidity=int(pos[7]),
                tokens_owed0=int(pos[10]),
                tokens_owed1=int(pos[11]),
            )
        except Exception as e:
            logger.warning(f"Failed to query CL position {token_id}: {e}")
            return None

    def build_cl_mint_tx(
        self,
        token0: str,
        token1: str,
        tick_spacing: int,
        tick_lower: int,
        tick_upper: int,
        amount0_desired: int,
        amount1_desired: int,
        amount0_min: int,
        amount1_min: int,
        recipient: str,
        deadline: int,
        sender: str,
        web3: Any,
        sqrt_price_x96: int = 0,
    ) -> dict[str, Any]:
        """Build Slipstream CL NonfungiblePositionManager mint transaction.

        Args:
            token0: Token0 address (must be < token1 by address)
            token1: Token1 address
            tick_spacing: Pool tick spacing
            tick_lower: Lower tick bound
            tick_upper: Upper tick bound
            amount0_desired: Desired token0 amount
            amount1_desired: Desired token1 amount
            amount0_min: Minimum token0 amount (slippage protection)
            amount1_min: Minimum token1 amount (slippage protection)
            recipient: NFT recipient address
            deadline: Transaction deadline (unix timestamp)
            sender: Transaction sender
            web3: Web3 instance
            sqrt_price_x96: Initial sqrt price (0 for existing pool)

        Returns:
            Transaction dictionary
        """
        if "cl_nft" not in self.addresses:
            raise ValueError(f"cl_nft not configured for chain {self.chain}")

        nft = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["cl_nft"]),
            abi=self._cl_nft_abi,
        )

        params = (
            web3.to_checksum_address(token0),
            web3.to_checksum_address(token1),
            tick_spacing,
            tick_lower,
            tick_upper,
            amount0_desired,
            amount1_desired,
            amount0_min,
            amount1_min,
            web3.to_checksum_address(recipient),
            deadline,
            sqrt_price_x96,
        )

        tx = nft.functions.mint(params).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "gas": AERODROME_GAS_ESTIMATES["cl_mint"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )
        return tx

    def build_cl_decrease_liquidity_tx(
        self,
        token_id: int,
        liquidity: int,
        amount0_min: int,
        amount1_min: int,
        deadline: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build Slipstream CL decreaseLiquidity transaction.

        Args:
            token_id: NFT token ID
            liquidity: Amount of liquidity to remove
            amount0_min: Minimum token0 to receive
            amount1_min: Minimum token1 to receive
            deadline: Transaction deadline
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        if "cl_nft" not in self.addresses:
            raise ValueError(f"cl_nft not configured for chain {self.chain}")

        nft = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["cl_nft"]),
            abi=self._cl_nft_abi,
        )

        params = (token_id, liquidity, amount0_min, amount1_min, deadline)

        tx = nft.functions.decreaseLiquidity(params).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "gas": AERODROME_GAS_ESTIMATES["cl_decrease_liquidity"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )
        return tx

    def build_cl_collect_tx(
        self,
        token_id: int,
        recipient: str,
        amount0_max: int,
        amount1_max: int,
        sender: str,
        web3: Any,
    ) -> dict[str, Any]:
        """Build Slipstream CL collect transaction.

        Args:
            token_id: NFT token ID
            recipient: Token recipient address
            amount0_max: Maximum token0 to collect (use MAX_UINT128 for all)
            amount1_max: Maximum token1 to collect (use MAX_UINT128 for all)
            sender: Transaction sender
            web3: Web3 instance

        Returns:
            Transaction dictionary
        """
        if "cl_nft" not in self.addresses:
            raise ValueError(f"cl_nft not configured for chain {self.chain}")

        nft = web3.eth.contract(
            address=web3.to_checksum_address(self.addresses["cl_nft"]),
            abi=self._cl_nft_abi,
        )

        params = (token_id, web3.to_checksum_address(recipient), amount0_max, amount1_max)

        tx = nft.functions.collect(params).build_transaction(
            {
                "from": (sender_cs := web3.to_checksum_address(sender)),
                "gas": AERODROME_GAS_ESTIMATES["cl_collect"],
                "nonce": web3.eth.get_transaction_count(sender_cs),
            }
        )
        return tx

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def resolve_token(self, token: str) -> str:
        """Resolve token symbol to address using TokenResolver.

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
                reason=f"[AerodromeSDK] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def get_token_symbol(self, address: str) -> str:
        """Get token symbol from address using TokenResolver.

        Args:
            address: Token address

        Returns:
            Token symbol
        """
        if not address.startswith("0x"):
            return address
        resolved = self._token_resolver.resolve(address, self.chain)
        return resolved.symbol

    def get_token_decimals(self, symbol: str) -> int:
        """Get token decimals from symbol using TokenResolver.

        Args:
            symbol: Token symbol

        Returns:
            Token decimals

        Raises:
            TokenResolutionError: If decimals cannot be determined
        """
        try:
            resolved = self._token_resolver.resolve(symbol, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=str(self.chain),
                reason=f"[AerodromeSDK] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "AerodromeSDK",
    "CLPositionInfo",
    "PoolInfo",
    "SwapRoute",
    "SwapQuote",
    "AerodromeSDKError",
    "PoolNotFoundError",
    "InsufficientLiquidityError",
    "AERODROME_ADDRESSES",
    "AERODROME_GAS_ESTIMATES",
    "MAX_UINT256",
]
