"""Curve Finance Protocol Adapter.

This module provides the CurveAdapter class for executing swaps and
managing liquidity positions on Curve Finance pools.

Curve Pool Types:
- StableSwap: Optimized for stablecoin pairs (low slippage)
- CryptoSwap: For volatile asset pairs (2 coins)
- Tricrypto: For 3-coin volatile pools

Key Contracts:
- Router: CurveRouterNG for multi-hop swaps
- Pools: Individual pool contracts for direct swaps and LP operations
- Factory: Creates new pools

Function Selectors:
- exchange(int128,int128,uint256,uint256): 0x3df02124 (StableSwap)
- exchange(uint256,uint256,uint256,uint256): 0x5b41b908 (CryptoSwap/Tricrypto)
- add_liquidity(uint256[2],uint256): varies by pool size
- remove_liquidity(uint256,uint256[2]): varies by pool size
- remove_liquidity_one_coin(uint256,int128,uint256): 0x1a4d01d2
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Curve contract addresses per chain
CURVE_ADDRESSES: dict[str, dict[str, str]] = {
    "ethereum": {
        "router": "0x16C6521Dff6baB339122a0FE25a9116693265353",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0x0c0e5f2fF0ff18a3be9b835635039256dC4B4963",
        "crv_token": "0xD533a949740bb3306d119CC777fa900bA034cd52",
    },
    "arbitrum": {
        "router": "0x2191718CD32d02B8E60BAdFFeA33E4B5DD9A0A0D",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x9AF14D26075f142eb3F292D5065EB3faa646167b",
        "twocrypto_factory": "0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F",
        "tricrypto_factory": "0xbC0797015fcFc47d9C1856639CaE50D0e69FbEE8",
    },
    "base": {
        "router": "0xd6681e74eEA20d196c15038C580f721EF2aB6320",  # CurveRouterNG on Base
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",  # Same across all EVM chains
        "stableswap_factory": "0x3093f9B57A428F3EB6285a589cb35bEA6e78c336",  # StableswapFactory NG
        "twocrypto_factory": "0xc9FE0c63AF9a39402E8a5514F9c21af076813f1b",  # TwocryptoFactory NG
        "tricrypto_factory": "0xa5961898d4539B95e3B8571c74f86D5E5b48DB25",  # TricryptoFactory NG
    },
    "optimism": {
        "router": "0xF0d4c12A5768D806021F80a262B4d39d26C58b8D",  # CurveRouterNG on Optimism
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",  # Universal
        "stableswap_factory": "0xA9B52d3CfB60073b7cC3D53dD3f25a8C619Afd78",
    },
}

# Popular Curve pools per chain
# TECH_DEBT(VIB-581): virtual_price values are approximate snapshots. Curve virtual_price
# increases monotonically as fees accumulate, so these will drift over time. The safe direction
# is under-estimating (lower min_lp = worse slippage protection but no reverts). A future
# improvement should query virtual_price() from the pool contract at runtime via gateway RPC,
# falling back to these static values if the RPC call fails.
CURVE_POOLS: dict[str, dict[str, dict[str, Any]]] = {
    "ethereum": {
        "3pool": {
            "address": "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            "lp_token": "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490",
            "coins": ["DAI", "USDC", "USDT"],
            "coin_addresses": [
                "0x6B175474E89094C44Da98b954EedeAC495271d0F",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
            "virtual_price": Decimal("1.04"),
        },
        "frax_usdc": {
            "address": "0xDcEF968d416a41Cdac0ED8702fAC8128A64241A2",
            "lp_token": "0x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC",
            "coins": ["FRAX", "USDC"],
            "coin_addresses": [
                "0x853d955aCEf822Db058eb8505911ED77F175b99e",
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.01"),
        },
        "steth": {
            "address": "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
            "lp_token": "0x06325440D014e39736583c165C2963BA99fAf14E",
            "coins": ["ETH", "stETH"],
            "coin_addresses": [
                "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
                "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.06"),
        },
        "tricrypto2": {
            "address": "0xD51a44d3FaE010294C616388b506AcdA1bfAAE46",
            "lp_token": "0xc4AD29ba4B3c580e6D59105FFf484999997675Ff",
            "coins": ["USDT", "WBTC", "WETH"],
            "coin_addresses": [
                "0xdAC17F958D2ee523a2206206994597C13D831ec7",
                "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
                "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            ],
            "pool_type": "tricrypto",
            "n_coins": 3,
            "virtual_price": Decimal("1.0"),
        },
    },
    "arbitrum": {
        "2pool": {
            "address": "0x7f90122BF0700F9E7e1F688fe926940E8839F353",
            "lp_token": "0x7f90122BF0700F9E7e1F688fe926940E8839F353",
            "coins": ["USDC.e", "USDT"],
            "coin_addresses": [
                "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC.e (bridged), NOT native USDC
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            ],
            "pool_type": "stableswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.022"),
        },
        "tricrypto": {
            "address": "0x960ea3e3C7FB317332d990873d354E18d7645590",
            "lp_token": "0x8e0B8c8BB9db49a46697F3a5Bb8A308e744821D2",
            "coins": ["USDT", "WBTC", "WETH"],
            "coin_addresses": [
                "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            ],
            "pool_type": "tricrypto",
            "n_coins": 3,
            "virtual_price": Decimal("1.0"),
        },
    },
    "base": {
        # WETH/cbETH Twocrypto pool — ETH liquid staking yield, ~3% APY
        # Pool: 0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59 (old Twocrypto, NOT NG)
        # LP token: 0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9 (SEPARATE from pool — old-style Twocrypto)
        # NOTE: This pool uses an OLD Twocrypto factory (not TwocryptoNG), so the LP token
        # is a separate ERC20 contract, not the pool address itself.
        # Verified on-chain 2026-03-19: pool.token() = 0x98244d93...F32f9
        # TECH_DEBT(VIB-581): virtual_price is a snapshot; query pool.virtual_price() at runtime for accuracy.
        # Queried on-chain 2026-03-19: pool.virtual_price() = 1017035434756947721 -> 1.0170
        # Pool reserves (2026-03-19): 3804.66 WETH + 3223.99 cbETH, LP supply: 3445 LP tokens
        "weth_cbeth": {
            "address": "0x11C1fBd4b3De66bC0565779b35171a6CF3E71f59",
            "lp_token": "0x98244d93D42b42aB3E3A4D12A5dc0B3e7f8F32f9",  # Separate LP token (old-style Twocrypto)
            "coins": ["WETH", "cbETH"],
            "coin_addresses": [
                "0x4200000000000000000000000000000000000006",  # WETH on Base
                "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22",  # cbETH on Base
            ],
            "pool_type": "cryptoswap",
            "n_coins": 2,
            "virtual_price": Decimal("1.017"),
        },
    },
    "optimism": {
        "3pool": {
            # Curve 3pool on Optimism (DAI/USDC.e/USDT)
            # USDC.e = bridged USDC (0x7F5...); native USDC (0x0b2...) is in a separate pool
            "address": "0x1337BedC9D22ecbe766dF105c9623922A27963EC",
            "lp_token": "0x1337BedC9D22ecbe766dF105c9623922A27963EC",
            "coins": ["DAI", "USDC.e", "USDT"],
            "coin_addresses": [
                "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",  # DAI on Optimism
                "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",  # USDC.e (bridged) on Optimism
                "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",  # USDT on Optimism
            ],
            "pool_type": "stableswap",
            "n_coins": 3,
            "virtual_price": Decimal("1.02"),
        },
    },
}


# Gas estimates for Curve operations
CURVE_GAS_ESTIMATES: dict[str, int] = {
    "approve": 65000,  # 65K to accommodate proxy tokens (USDC FiatTokenProxy ~56-65K)
    "exchange": 500000,
    "exchange_underlying": 300000,
    "add_liquidity_2": 250000,
    "add_liquidity_3": 350000,
    "remove_liquidity": 200000,
    "remove_liquidity_one_coin": 250000,
    "remove_liquidity_imbalance": 300000,
    "router_exchange": 400000,
}

# Function selectors
EXCHANGE_SELECTOR = "0x3df02124"  # exchange(int128,int128,uint256,uint256) - StableSwap
EXCHANGE_UINT256_SELECTOR = "0x5b41b908"  # exchange(uint256,uint256,uint256,uint256) - CryptoSwap/Tricrypto
EXCHANGE_UNDERLYING_SELECTOR = "0xa6417ed6"  # exchange_underlying(int128,int128,uint256,uint256)
ADD_LIQUIDITY_2_SELECTOR = "0x0b4c7e4d"  # add_liquidity(uint256[2],uint256)
ADD_LIQUIDITY_3_SELECTOR = "0x4515cef3"  # add_liquidity(uint256[3],uint256)
REMOVE_LIQUIDITY_2_SELECTOR = "0x5b36389c"  # remove_liquidity(uint256,uint256[2])
REMOVE_LIQUIDITY_3_SELECTOR = "0xecb586a5"  # remove_liquidity(uint256,uint256[3])
REMOVE_LIQUIDITY_ONE_SELECTOR = "0x1a4d01d2"  # remove_liquidity_one_coin(uint256,int128,uint256)
GET_DY_SELECTOR = "0x5e0d443f"  # get_dy(int128,int128,uint256)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max uint256 for unlimited approvals
MAX_UINT256 = 2**256 - 1


# =============================================================================
# Enums
# =============================================================================


class PoolType(Enum):
    """Curve pool type."""

    STABLESWAP = "stableswap"
    CRYPTOSWAP = "cryptoswap"
    TRICRYPTO = "tricrypto"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CurveConfig:
    """Configuration for CurveAdapter.

    Attributes:
        chain: Target blockchain (ethereum, arbitrum)
        wallet_address: Address executing transactions
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        deadline_seconds: Transaction deadline in seconds (default 300 = 5 minutes)
        rpc_url: Optional JSON-RPC URL for on-chain state queries (e.g., pool balances
            for accurate remove_liquidity slippage estimates). When provided, the adapter
            queries pool.balances(i) and lp_token.totalSupply() to compute proportional
            min_amounts rather than returning zeros. When absent or on RPC failure,
            min_amounts fall back to [0, 0, ..., 0] with a warning.
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50
    deadline_seconds: int = 300
    rpc_url: str | None = None

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in CURVE_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(CURVE_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "default_slippage_bps": self.default_slippage_bps,
            "deadline_seconds": self.deadline_seconds,
            "rpc_url": self.rpc_url,
        }


@dataclass
class PoolInfo:
    """Information about a Curve pool.

    Attributes:
        address: Pool contract address
        lp_token: LP token address
        coins: List of coin symbols
        coin_addresses: List of coin addresses
        pool_type: Type of pool (stableswap, cryptoswap, tricrypto)
        n_coins: Number of coins in pool
        name: Pool name
        virtual_price: Pool virtual price (LP token value relative to underlying).
            Mature pools accumulate fees so virtual_price > 1.0. Used to adjust
            LP token estimates to prevent over-estimation that causes add_liquidity reverts.
    """

    address: str
    lp_token: str
    coins: list[str]
    coin_addresses: list[str]
    pool_type: PoolType
    n_coins: int
    name: str = ""
    virtual_price: Decimal = field(default_factory=lambda: Decimal("1.0"))

    def get_coin_index(self, coin: str) -> int:
        """Get the index of a coin in the pool.

        Args:
            coin: Coin symbol or address

        Returns:
            Index of the coin

        Raises:
            ValueError: If coin not found in pool
        """
        # Check by symbol
        for i, c in enumerate(self.coins):
            if c.upper() == coin.upper():
                return i

        # Check by address
        for i, addr in enumerate(self.coin_addresses):
            if addr.lower() == coin.lower():
                return i

        raise ValueError(f"Coin {coin} not found in pool. Available: {self.coins}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "address": self.address,
            "lp_token": self.lp_token,
            "coins": self.coins,
            "coin_addresses": self.coin_addresses,
            "pool_type": self.pool_type.value,
            "n_coins": self.n_coins,
            "name": self.name,
            "virtual_price": str(self.virtual_price),
        }


@dataclass
class TransactionData:
    """Transaction data for execution.

    Attributes:
        to: Target contract address
        value: Native token value to send
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
        tx_type: Type of transaction (approve, swap, add_liquidity, remove_liquidity)
    """

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str
    tx_type: str = "swap"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
            "tx_type": self.tx_type,
        }


@dataclass
class SwapResult:
    """Result of a swap operation.

    Attributes:
        success: Whether the swap was built successfully
        transactions: List of transactions to execute
        pool_address: Pool used for swap
        amount_in: Input amount in wei
        amount_out_minimum: Minimum output amount (with slippage)
        token_in: Input token address
        token_out: Output token address
        error: Error message if failed
        gas_estimate: Total gas estimate
    """

    success: bool
    transactions: list[TransactionData] = field(default_factory=list)
    pool_address: str = ""
    amount_in: int = 0
    amount_out_minimum: int = 0
    token_in: str = ""
    token_out: str = ""
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "pool_address": self.pool_address,
            "amount_in": str(self.amount_in),
            "amount_out_minimum": str(self.amount_out_minimum),
            "token_in": self.token_in,
            "token_out": self.token_out,
            "error": self.error,
            "gas_estimate": self.gas_estimate,
        }


@dataclass
class LiquidityResult:
    """Result of a liquidity operation.

    Attributes:
        success: Whether the operation was built successfully
        transactions: List of transactions to execute
        pool_address: Pool address
        operation: Operation type (add_liquidity, remove_liquidity, remove_liquidity_one_coin)
        amounts: Token amounts for the operation
        lp_amount: LP token amount (minted or burned)
        error: Error message if failed
        gas_estimate: Total gas estimate
    """

    success: bool
    transactions: list[TransactionData] = field(default_factory=list)
    pool_address: str = ""
    operation: str = ""
    amounts: list[int] = field(default_factory=list)
    lp_amount: int = 0
    error: str | None = None
    gas_estimate: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transactions": [tx.to_dict() for tx in self.transactions],
            "pool_address": self.pool_address,
            "operation": self.operation,
            "amounts": [str(a) for a in self.amounts],
            "lp_amount": str(self.lp_amount),
            "error": self.error,
            "gas_estimate": self.gas_estimate,
        }


# =============================================================================
# Curve Adapter
# =============================================================================


class CurveAdapter:
    """Adapter for Curve Finance DEX protocol.

    This adapter provides methods for:
    - Executing token swaps via Curve pools
    - Adding liquidity to pools (LP_OPEN)
    - Removing liquidity from pools (LP_CLOSE)
    - Handling ERC-20 approvals
    - Managing slippage protection

    Example:
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x...",
        )
        adapter = CurveAdapter(config)

        # Execute a swap on 3pool
        result = adapter.swap(
            pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
            token_in="USDC",
            token_out="DAI",
            amount_in=Decimal("1000"),
        )
    """

    def __init__(self, config: CurveConfig, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: Curve adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address
        self._rpc_url = config.rpc_url

        # Load contract addresses
        self.addresses = CURVE_ADDRESSES[self.chain]
        self.pools = CURVE_POOLS.get(self.chain, {})

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Allowance cache (token -> amount approved)
        self._allowance_cache: dict[str, int] = {}

        logger.info(f"CurveAdapter initialized for chain={self.chain}, wallet={self.wallet_address[:10]}...")

    # =========================================================================
    # Pool Information
    # =========================================================================

    def get_pool_info(self, pool_address: str) -> PoolInfo | None:
        """Get information about a pool.

        Args:
            pool_address: Pool contract address

        Returns:
            PoolInfo if known, None otherwise
        """
        for name, pool_data in self.pools.items():
            if pool_data["address"].lower() == pool_address.lower():
                return PoolInfo(
                    address=pool_data["address"],
                    lp_token=pool_data["lp_token"],
                    coins=pool_data["coins"],
                    coin_addresses=pool_data["coin_addresses"],
                    pool_type=PoolType(pool_data["pool_type"]),
                    n_coins=pool_data["n_coins"],
                    name=name,
                    virtual_price=pool_data.get("virtual_price", Decimal("1.0")),
                )
        return None

    def get_pool_by_name(self, name: str) -> PoolInfo | None:
        """Get pool info by name.

        Args:
            name: Pool name (e.g., "3pool", "frax_usdc")

        Returns:
            PoolInfo if found, None otherwise
        """
        pool_data = self.pools.get(name)
        if pool_data:
            return PoolInfo(
                address=pool_data["address"],
                lp_token=pool_data["lp_token"],
                coins=pool_data["coins"],
                coin_addresses=pool_data["coin_addresses"],
                pool_type=PoolType(pool_data["pool_type"]),
                n_coins=pool_data["n_coins"],
                name=name,
                virtual_price=pool_data.get("virtual_price", Decimal("1.0")),
            )
        return None

    # =========================================================================
    # Swap Operations
    # =========================================================================

    def swap(
        self,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        price_ratio: Decimal | None = None,
    ) -> SwapResult:
        """Build a swap transaction on a Curve pool.

        Args:
            pool_address: Pool contract address
            token_in: Input token symbol or address
            token_out: Output token symbol or address
            amount_in: Amount of input token (in token units, not wei)
            slippage_bps: Slippage tolerance in basis points (default from config)
            recipient: Address to receive output tokens (default: wallet_address)
            price_ratio: Price of input token / price of output token (e.g., if
                swapping USDT at $1 for WETH at $2500, price_ratio = 1/2500 = 0.0004).
                Required for CryptoSwap/Tricrypto pools; StableSwap pools ignore it.
                When None and pool is CryptoSwap, the swap fails (fail-closed) rather
                than executing with inaccurate slippage protection.

        Returns:
            SwapResult with transaction data
        """
        try:
            if price_ratio is not None and price_ratio <= 0:
                raise ValueError(f"price_ratio must be positive, got {price_ratio}")

            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return SwapResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            # Get coin indices
            try:
                i = pool_info.get_coin_index(token_in)
                j = pool_info.get_coin_index(token_out)
            except ValueError as e:
                return SwapResult(success=False, error=str(e))

            # Resolve token addresses
            token_in_address = pool_info.coin_addresses[i]
            token_out_address = pool_info.coin_addresses[j]

            # Get token decimals
            token_in_symbol = pool_info.coins[i]
            token_in_decimals = self._get_token_decimals(token_in_symbol)

            # Convert amount to wei
            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            # Estimate output for min_amount_out slippage protection
            amount_out_estimate = self._estimate_swap_output(pool_info, i, j, amount_in_wei, price_ratio=price_ratio)
            amount_out_minimum = max(1, int(amount_out_estimate * (10000 - slippage_bps) // 10000))

            # Build transactions
            transactions: list[TransactionData] = []

            # Check if input is native ETH
            is_native_input = self._is_native_token(token_in_address)

            # Build approve transaction if needed (skip for native token)
            if not is_native_input:
                approve_tx = self._build_approve_tx(
                    token_in_address,
                    pool_address,
                    amount_in_wei,
                )
                if approve_tx is not None:
                    transactions.append(approve_tx)

            # Build swap transaction
            swap_tx = self._build_exchange_tx(
                pool_address=pool_address,
                i=i,
                j=j,
                amount_in=amount_in_wei,
                min_amount_out=amount_out_minimum,
                value=amount_in_wei if is_native_input else 0,
                token_in_symbol=token_in_symbol,
                token_out_symbol=pool_info.coins[j],
                pool_type=pool_info.pool_type,
            )
            transactions.append(swap_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Curve swap: {token_in_symbol} -> {pool_info.coins[j]}, "
                f"pool={pool_info.name}, amount_in={amount_in}"
            )

            return SwapResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                token_in=token_in_address,
                token_out=token_out_address,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build Curve swap: {e}")
            return SwapResult(success=False, error=str(e))

    # =========================================================================
    # Liquidity Operations
    # =========================================================================

    def add_liquidity(
        self,
        pool_address: str,
        amounts: list[Decimal],
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build an add_liquidity transaction (LP_OPEN).

        Args:
            pool_address: Pool contract address
            amounts: List of token amounts to deposit (in token units)
            slippage_bps: Slippage tolerance for min LP tokens (default from config)
            recipient: Address to receive LP tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            if len(amounts) != pool_info.n_coins:
                return LiquidityResult(
                    success=False,
                    error=f"Expected {pool_info.n_coins} amounts, got {len(amounts)}",
                )

            # Convert amounts to wei
            amounts_wei: list[int] = []
            for idx, amt in enumerate(amounts):
                decimals = self._get_token_decimals(pool_info.coins[idx])
                amounts_wei.append(int(amt * Decimal(10**decimals)))

            # Estimate LP tokens (simplified)
            min_lp_tokens = self._estimate_add_liquidity(pool_info, amounts_wei)
            min_lp_tokens = int(min_lp_tokens * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Build approve transactions for each non-zero amount
            native_value: int = 0
            for amount_wei, coin_addr in zip(amounts_wei, pool_info.coin_addresses, strict=False):
                if amount_wei > 0:
                    if self._is_native_token(coin_addr):
                        native_value = amount_wei
                    else:
                        approve_tx = self._build_approve_tx(coin_addr, pool_address, amount_wei)
                        if approve_tx is not None:
                            transactions.append(approve_tx)

            # Build add_liquidity transaction
            add_liq_tx = self._build_add_liquidity_tx(
                pool_address=pool_address,
                amounts=amounts_wei,
                min_lp_tokens=min_lp_tokens,
                n_coins=pool_info.n_coins,
                value=native_value,
                pool_name=pool_info.name,
            )
            transactions.append(add_liq_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built Curve add_liquidity: pool={pool_info.name}, amounts={amounts}, min_lp={min_lp_tokens}")

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="add_liquidity",
                amounts=amounts_wei,
                lp_amount=min_lp_tokens,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build add_liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity(
        self,
        pool_address: str,
        lp_amount: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove_liquidity transaction (LP_CLOSE, proportional).

        Args:
            pool_address: Pool contract address
            lp_amount: Amount of LP tokens to burn
            slippage_bps: Slippage tolerance for min output (default from config)
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            # Convert LP amount to wei (18 decimals)
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            # Estimate output amounts via on-chain query (or fallback to zeros)
            self._last_estimation_error: str | None = None
            min_amounts = self._estimate_remove_liquidity(pool_info, lp_amount_wei)
            min_amounts = [int(a * (10000 - slippage_bps) // 10000) for a in min_amounts]

            # Guard: fail closed when min_amounts are all zero — proceeding without slippage
            # protection would expose the full withdrawal to sandwich attacks.
            # This can happen when: (a) rpc_url is not configured and LP amount is very small
            # (1% floor rounds to 0 via integer division), or (b) on-chain estimation fails
            # and the fallback also rounds to 0. Either way, refusing is safer than proceeding.
            if all(a == 0 for a in min_amounts):
                reason = self._last_estimation_error or "unknown"
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity: cannot compute slippage protection (min_amounts are all zero). "
                        f"Cause: {reason}. "
                        f"Set CurveConfig.rpc_url for on-chain estimation."
                    ),
                )

            # Build transactions
            transactions: list[TransactionData] = []

            # Approve LP token if needed
            approve_tx = self._build_approve_tx(pool_info.lp_token, pool_address, lp_amount_wei)
            if approve_tx is not None:
                transactions.append(approve_tx)

            # Build remove_liquidity transaction
            remove_tx = self._build_remove_liquidity_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                min_amounts=min_amounts,
                n_coins=pool_info.n_coins,
                pool_name=pool_info.name,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(f"Built Curve remove_liquidity: pool={pool_info.name}, lp_amount={lp_amount}")

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity",
                amounts=min_amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove_liquidity: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity_one_coin(
        self,
        pool_address: str,
        lp_amount: Decimal,
        coin_index: int,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove_liquidity_one_coin transaction (LP_CLOSE, single-sided).

        Args:
            pool_address: Pool contract address
            lp_amount: Amount of LP tokens to burn
            coin_index: Index of the coin to receive
            slippage_bps: Slippage tolerance (default from config)
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        try:
            slippage_bps = slippage_bps or self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

            # Get pool info
            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            if coin_index < 0 or coin_index >= pool_info.n_coins:
                return LiquidityResult(
                    success=False,
                    error=f"Invalid coin index: {coin_index}. Pool has {pool_info.n_coins} coins.",
                )

            # Convert LP amount to wei
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            # Estimate output (simplified)
            min_amount = self._estimate_remove_liquidity_one(pool_info, lp_amount_wei, coin_index)
            min_amount = int(min_amount * (10000 - slippage_bps) // 10000)

            # Build transactions
            transactions: list[TransactionData] = []

            # Approve LP token if needed
            approve_tx = self._build_approve_tx(pool_info.lp_token, pool_address, lp_amount_wei)
            if approve_tx is not None:
                transactions.append(approve_tx)

            # Build remove_liquidity_one_coin transaction
            remove_tx = self._build_remove_liquidity_one_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                coin_index=coin_index,
                min_amount=min_amount,
                coin_symbol=pool_info.coins[coin_index],
                pool_name=pool_info.name,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Curve remove_liquidity_one_coin: pool={pool_info.name}, "
                f"lp_amount={lp_amount}, coin={pool_info.coins[coin_index]}"
            )

            # Build amounts list with only the withdrawn coin
            amounts = [0] * pool_info.n_coins
            amounts[coin_index] = min_amount

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity_one_coin",
                amounts=amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove_liquidity_one_coin: {e}")
            return LiquidityResult(success=False, error=str(e))

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def _build_exchange_tx(
        self,
        pool_address: str,
        i: int,
        j: int,
        amount_in: int,
        min_amount_out: int,
        value: int = 0,
        token_in_symbol: str = "",
        token_out_symbol: str = "",
        pool_type: PoolType = PoolType.STABLESWAP,
    ) -> TransactionData:
        """Build exchange transaction.

        StableSwap:          exchange(int128 i, int128 j, uint256 dx, uint256 min_dy)
        CryptoSwap/Tricrypto: exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy)
        """
        if pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            # CryptoSwap and Tricrypto pools use uint256 indices
            selector = EXCHANGE_UINT256_SELECTOR
            pad_index = self._pad_uint256
        else:
            # StableSwap pools use int128 indices
            selector = EXCHANGE_SELECTOR
            pad_index = self._pad_int128

        calldata = (
            selector + pad_index(i) + pad_index(j) + self._pad_uint256(amount_in) + self._pad_uint256(min_amount_out)
        )

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["exchange"],
            description=f"Curve swap {token_in_symbol} -> {token_out_symbol}",
            tx_type="swap",
        )

    def _build_add_liquidity_tx(
        self,
        pool_address: str,
        amounts: list[int],
        min_lp_tokens: int,
        n_coins: int,
        value: int = 0,
        pool_name: str = "",
    ) -> TransactionData:
        """Build add_liquidity transaction.

        add_liquidity(uint256[N_COINS] amounts, uint256 min_mint_amount)
        """
        # Select correct selector based on n_coins
        if n_coins == 2:
            selector = ADD_LIQUIDITY_2_SELECTOR
            gas_estimate = CURVE_GAS_ESTIMATES["add_liquidity_2"]
        else:  # n_coins == 3
            selector = ADD_LIQUIDITY_3_SELECTOR
            gas_estimate = CURVE_GAS_ESTIMATES["add_liquidity_3"]

        # Encode amounts array
        calldata = selector
        for amount in amounts:
            calldata += self._pad_uint256(amount)
        calldata += self._pad_uint256(min_lp_tokens)

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=gas_estimate,
            description=f"Add liquidity to Curve {pool_name}",
            tx_type="add_liquidity",
        )

    def _build_remove_liquidity_tx(
        self,
        pool_address: str,
        lp_amount: int,
        min_amounts: list[int],
        n_coins: int,
        pool_name: str = "",
    ) -> TransactionData:
        """Build remove_liquidity transaction.

        remove_liquidity(uint256 _amount, uint256[N_COINS] min_amounts)
        """
        # Select correct selector based on n_coins
        if n_coins == 2:
            selector = REMOVE_LIQUIDITY_2_SELECTOR
        else:  # n_coins == 3
            selector = REMOVE_LIQUIDITY_3_SELECTOR

        # Encode calldata
        calldata = selector + self._pad_uint256(lp_amount)
        for min_amount in min_amounts:
            calldata += self._pad_uint256(min_amount)

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["remove_liquidity"],
            description=f"Remove liquidity from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_remove_liquidity_one_tx(
        self,
        pool_address: str,
        lp_amount: int,
        coin_index: int,
        min_amount: int,
        coin_symbol: str = "",
        pool_name: str = "",
    ) -> TransactionData:
        """Build remove_liquidity_one_coin transaction.

        remove_liquidity_one_coin(uint256 _token_amount, int128 i, uint256 _min_amount)
        """
        calldata = (
            REMOVE_LIQUIDITY_ONE_SELECTOR
            + self._pad_uint256(lp_amount)
            + self._pad_int128(coin_index)
            + self._pad_uint256(min_amount)
        )

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["remove_liquidity_one_coin"],
            description=f"Remove {coin_symbol} from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_approve_tx(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> TransactionData | None:
        """Build an ERC-20 approve transaction if needed.

        Args:
            token_address: Token to approve
            spender: Address to approve
            amount: Amount to approve

        Returns:
            TransactionData for approve, or None if sufficient allowance exists
        """
        # Check cache for existing allowance
        cache_key = f"{token_address}:{spender}"
        cached = self._allowance_cache.get(cache_key, 0)
        if cached >= amount:
            logger.debug(f"Sufficient allowance exists for {token_address}")
            return None

        # Build approve calldata
        calldata = ERC20_APPROVE_SELECTOR + self._pad_address(spender) + self._pad_uint256(MAX_UINT256)

        # Update cache
        self._allowance_cache[cache_key] = MAX_UINT256

        # _get_token_symbol falls back to truncated address for unresolved tokens (e.g., 3CRV)
        token_symbol = self._get_token_symbol(token_address)

        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["approve"],
            description=f"Approve {token_symbol} for Curve",
            tx_type="approve",
        )

    # =========================================================================
    # Estimation Methods
    # =========================================================================

    def _estimate_swap_output(
        self,
        pool_info: PoolInfo,
        i: int,
        j: int,
        amount_in: int,
        price_ratio: Decimal | None = None,
    ) -> int:
        """Estimate swap output amount for min_amount_out calculation.

        For StableSwap pools: assumes 1:1 price ratio, adjusts for decimals.
        For CryptoSwap/Tricrypto pools: requires price_ratio from the compiler
        (which has access to oracle prices). If price_ratio is not provided,
        raises ValueError (fail closed) — decimal-only adjustment is wrong for
        volatile pairs and would produce astronomically incorrect min_amount_out.

        Args:
            pool_info: Pool metadata
            i: Input coin index
            j: Output coin index
            amount_in: Input amount in wei (input token decimals)
            price_ratio: price_in / price_out ratio. E.g., USDT($1)->WETH($2500)
                gives price_ratio=0.0004. When provided, the estimate is:
                amount_in * price_ratio * (10^(out_decimals - in_decimals))

        Returns:
            Estimated output amount in wei (output token decimals)
        """
        in_decimals = self._get_token_decimals(pool_info.coins[i])
        out_decimals = self._get_token_decimals(pool_info.coins[j])
        decimal_diff = out_decimals - in_decimals

        if pool_info.pool_type == PoolType.STABLESWAP:
            # Stablecoins: assume 1:1, adjust for decimals only
            if decimal_diff > 0:
                return amount_in * (10**decimal_diff)
            elif decimal_diff < 0:
                return amount_in // (10 ** abs(decimal_diff))
            return amount_in

        # CryptoSwap / Tricrypto: volatile pairs need price-based estimation
        if price_ratio is not None:
            # price_ratio = price_in / price_out
            # expected_output_tokens = amount_in_tokens * price_ratio
            # Convert: amount_in is in input wei, output must be in output wei
            # amount_out_wei = amount_in_wei * price_ratio * 10^(out_decimals - in_decimals)
            estimate = Decimal(amount_in) * price_ratio
            if decimal_diff > 0:
                estimate = estimate * Decimal(10**decimal_diff)
            elif decimal_diff < 0:
                estimate = estimate / Decimal(10 ** abs(decimal_diff))
            return int(estimate)

        # Fail closed: CryptoSwap pools swap volatile assets with very different prices.
        # Without price_ratio, decimal-only adjustment produces astronomically wrong
        # min_amount_out values (e.g. 100 USDT -> WETH would set min_amount_out to
        # 100*10^12 wei = ~100 billion WETH, guaranteeing a revert). The compiler
        # always provides price_ratio from oracle prices; reaching this path means
        # the price oracle was unavailable. Fail closed rather than execute unprotected.
        raise ValueError(
            f"CryptoSwap pool {pool_info.name} ({pool_info.coins[i]} -> {pool_info.coins[j]}): "
            "price_ratio is required for accurate slippage protection but was not provided. "
            "Ensure price oracle data is available for both tokens before swapping volatile pairs."
        )

    def _estimate_add_liquidity(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Estimate LP tokens from add_liquidity.

        For StableSwap pools: divides total deposit by virtual_price.
        Mature pools have virtual_price > 1.0 because fees increase LP token value.
        The sum/virtual_price formula works for stablecoin pools because deposit
        value is proportional to LP supply.

        For CryptoSwap/Tricrypto pools: returns 0 (no LP slippage protection).
        Volatile-asset pools track LP tokens as a share of D-invariant, not deposit
        value. Accurate estimation requires querying calc_token_amount() on-chain
        (which depends on pool reserves, A, gamma, and current prices). Without that
        data, any static estimate risks setting min_lp higher than actual minted
        tokens, causing the add_liquidity to revert with "Slippage".
        TECH_DEBT(VIB-581): Add on-chain calc_token_amount query for production use.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            # Cannot accurately estimate LP tokens for volatile pools without on-chain query.
            # Return 0 to accept any minted amount — production strategies should use
            # calc_token_amount() on-chain for proper slippage protection.
            logger.debug(
                f"CryptoSwap/Tricrypto pool {pool_info.name}: returning min_lp=0 "
                "(no slippage protection — use calc_token_amount on-chain for production)"
            )
            return 0

        total = 0
        for i, amount in enumerate(amounts):
            decimals = self._get_token_decimals(pool_info.coins[i])
            # Normalize to 18 decimals
            normalized = amount * (10 ** (18 - decimals))
            total += normalized

        # Adjust for virtual_price: each LP token is worth virtual_price underlying
        total = int(Decimal(total) / pool_info.virtual_price)

        return total

    def _estimate_remove_liquidity(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Estimate expected per-coin amounts for proportional remove_liquidity.

        When rpc_url is configured, queries on-chain pool.balances(i) and
        lp_token.totalSupply() to compute accurate proportional amounts:
            expected_i = pool.balances(i) * lp_amount / lp_token.totalSupply()

        This is the only correct approach for imbalanced pools (e.g., Curve 3pool
        where DAI is ~7% of pool, not 33%). A slippage tolerance is then applied by
        the caller: min_amount_i = expected_i * (10000 - slippage_bps) / 10000

        When rpc_url is not configured or the RPC call fails, returns [0, ..., 0] and
        logs a warning. Callers that receive all-zeros should log an additional warning
        about absent slippage protection.

        Args:
            pool_info: Pool configuration including address, lp_token, and coin list
            lp_amount: LP token amount to burn (in wei, 18 decimals)

        Returns:
            List of expected token amounts (in native token decimals), one per coin.
            Returns [0, ..., 0] when on-chain estimation is unavailable.
        """
        zero_amounts = [0] * pool_info.n_coins
        if not self._rpc_url:
            logger.warning(
                f"remove_liquidity: rpc_url not configured for {pool_info.name} -- "
                "min_amounts will be [0, ..., 0] (no slippage protection). "
                "Set CurveConfig.rpc_url to enable on-chain estimation."
            )
            self._last_estimation_error = "rpc_url not configured"
            return zero_amounts

        try:
            return self._query_proportional_amounts_onchain(pool_info, lp_amount)
        except Exception as e:
            logger.warning(
                f"remove_liquidity: on-chain estimation failed for {pool_info.name}: {e} -- "
                "falling back to [0, ..., 0] (no slippage protection)"
            )
            self._last_estimation_error = str(e)
            return zero_amounts

    def _query_proportional_amounts_onchain(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Query on-chain pool balances and LP totalSupply to compute proportional amounts.

        Makes synchronous JSON-RPC eth_call requests:
        1. lp_token.totalSupply() -> total LP supply
        2. pool.balances(i) for each coin -> current pool reserves

        Proportional amount for coin i:
            expected_i = pool.balances(i) * lp_amount / totalSupply

        This is exact for proportional remove_liquidity because Curve V1 StableSwap
        pools charge no fee on proportional withdrawals (only imbalanced ones do).

        Args:
            pool_info: Pool configuration
            lp_amount: LP token amount in wei

        Returns:
            List of expected token amounts in native decimals

        Raises:
            ValueError: If RPC returns unexpected data
            Exception: On network or parsing errors (caller handles fallback)
        """
        import httpx

        assert self._rpc_url is not None

        # ABI selectors
        TOTAL_SUPPLY_SELECTOR = "18160ddd"  # totalSupply() -> uint256
        BALANCES_UINT256_SELECTOR = "4903b0d1"  # balances(uint256) -> uint256 (factory/newer pools)
        BALANCES_INT128_SELECTOR = "065a80d8"  # balances(int128) -> uint256 (old Vyper pools, e.g. 3pool)

        def _encode_uint256_arg(value: int) -> str:
            """Encode a single uint256 argument (32 bytes, no 0x prefix)."""
            return hex(value)[2:].zfill(64)

        def _eth_call(to: str, data: str) -> int:
            """Make a synchronous eth_call and return the result as int."""
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
                "id": 1,
            }
            response = httpx.post(self._rpc_url, json=payload, timeout=10.0)  # type: ignore[arg-type]
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                raise ValueError(f"eth_call error: {result['error'].get('message', result['error'])}")
            hex_result = result.get("result", "0x0")
            if not hex_result or hex_result == "0x":
                raise ValueError("eth_call returned empty result")
            return int(hex_result, 16)

        # 1. Query LP totalSupply
        total_supply = _eth_call(
            pool_info.lp_token,
            f"0x{TOTAL_SUPPLY_SELECTOR}",
        )
        if total_supply == 0:
            raise ValueError(f"LP totalSupply is zero for pool {pool_info.name}")

        # 2. Query balances for each coin
        # Try balances(uint256) first (newer/factory pools), fall back to balances(int128)
        # (old Vyper pools like Ethereum 3pool). We detect the correct selector on the
        # first coin query and reuse it for the rest.
        amounts = []
        balances_selector = BALANCES_UINT256_SELECTOR
        for i in range(pool_info.n_coins):
            try:
                balance_raw = _eth_call(
                    pool_info.address,
                    f"0x{balances_selector}{_encode_uint256_arg(i)}",
                )
            except (ValueError, Exception):
                if i == 0 and balances_selector == BALANCES_UINT256_SELECTOR:
                    # First call failed with uint256 selector, retry with int128
                    balances_selector = BALANCES_INT128_SELECTOR
                    balance_raw = _eth_call(
                        pool_info.address,
                        f"0x{balances_selector}{_encode_uint256_arg(i)}",
                    )
                else:
                    raise
            # Proportional share: balance * lp_amount / total_supply
            expected = balance_raw * lp_amount // total_supply
            amounts.append(expected)

        logger.debug(
            f"remove_liquidity on-chain estimate for {pool_info.name}: "
            f"lp={lp_amount}, total_supply={total_supply}, amounts={amounts}"
        )
        return amounts

    def _estimate_remove_liquidity_one(self, pool_info: PoolInfo, lp_amount: int, coin_index: int) -> int:
        """Estimate tokens from remove_liquidity_one_coin.

        Multiplies by virtual_price because LP tokens are worth virtual_price
        underlying value. This is different from _estimate_remove_liquidity
        (proportional): single-sided withdrawal doesn't have balance-ratio issues,
        so virtual_price adjustment is both safe and necessary.

        Applies 1% penalty for single-sided removal (pool charges a fee for
        imbalanced withdrawals).
        """
        # Adjust LP amount by virtual_price to get underlying value
        adjusted_lp = int(Decimal(lp_amount) * pool_info.virtual_price)
        decimals = self._get_token_decimals(pool_info.coins[coin_index])
        # Convert from 18 decimals, apply small penalty for single-sided
        return (adjusted_lp // (10 ** (18 - decimals))) * 99 // 100

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _resolve_token(self, token: str) -> str:
        """Resolve token symbol or address to address using TokenResolver."""
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[CurveAdapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_token_symbol(self, address: str) -> str:
        """Get token symbol from address using TokenResolver.

        Falls back to truncated address if token is not in registry
        (e.g., Curve LP tokens like 3Crv). This is used only for log
        descriptions, not for transaction logic.
        """
        if not address.startswith("0x"):
            return address
        try:
            resolved = self._token_resolver.resolve(address, self.chain)
            return resolved.symbol
        except TokenResolutionError:
            logger.debug(f"Cannot resolve symbol for {address}, using truncated address")
            return f"{address[:10]}..."

    def _get_token_decimals(self, symbol: str) -> int:
        """Get token decimals from symbol using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(symbol, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=symbol,
                chain=str(self.chain),
                reason=f"[CurveAdapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _is_native_token(self, token: str) -> bool:
        """Check if token is native ETH."""
        native_address = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        if token.upper() == "ETH":
            return True
        if token.lower() == native_address:
            return True
        return False

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    @staticmethod
    def _pad_int128(value: int) -> str:
        """Pad int128 to 32 bytes (signed)."""
        if value < 0:
            # Two's complement for negative values
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # State Management
    # =========================================================================

    def set_allowance(self, token: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing).

        Args:
            token: Token address
            spender: Spender address
            amount: Allowance amount
        """
        cache_key = f"{token}:{spender}"
        self._allowance_cache[cache_key] = amount

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "CurveAdapter",
    "CurveConfig",
    "SwapResult",
    "LiquidityResult",
    "PoolInfo",
    "PoolType",
    "TransactionData",
    "CURVE_ADDRESSES",
    "CURVE_POOLS",
    "CURVE_GAS_ESTIMATES",
]
