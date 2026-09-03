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
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.connectors._strategy_base.base.approval_sequencing import build_approval_sequence
from almanak.connectors._strategy_base.erc20_abi import (
    MAX_UINT256,
    AllowanceCache,
    encode_allowance,
    encode_approve,
    pad_address,
    pad_uint256,
)
from almanak.connectors._strategy_base.pool_validation_base import ZERO_ADDRESS, decode_address
from almanak.connectors._strategy_base.rpc import eth_call, eth_call_uint256, eth_estimate_gas
from almanak.connectors._strategy_base.slippage import compute_min_amount_out_from_bps
from almanak.connectors._strategy_base.swap_oracle_guard import (
    DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS,
    DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS,
    DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS,
    DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS,
    DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS,
    check_swap_oracle_divergence,
    clamp_min_out_to_oracle,
)
from almanak.connectors.curve.receipt_parser import CURVE_LP_TOKEN_DECIMALS
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens.decimals import resolve_token_decimals
from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType
    from almanak.framework.gateway_client import GatewayClient

logger = logging.getLogger(__name__)


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
        "router": "0xd6681e74eEA20d196c15038C580f721EF2aB6320",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x3093f9B57A428F3EB6285a589cb35bEA6e78c336",
        "twocrypto_factory": "0xc9FE0c63AF9a39402E8a5514F9c21af076813f1b",
        "tricrypto_factory": "0xa5961898d4539B95e3B8571c74f86D5E5b48DB25",
    },
    "optimism": {
        "router": "0xF0d4c12A5768D806021F80a262B4d39d26C58b8D",
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0xA9B52d3CfB60073b7cC3D53dD3f25a8C619Afd78",
    },
    "polygon": {
        "address_provider": "0x5ffe7FB82894076ECB99A30D6A32e969e6e35E98",
        "stableswap_factory": "0x722272D36ef0Da72FF51c5A65Db7b870E2e8D4ee",
    },
}

# Conservative fallback floors when live gateway gas estimation is unavailable.
# Limits are deliberately high because unused gas is refunded and under-sizing can OOG.
CURVE_GAS_ESTIMATES: dict[str, int] = {
    "approve": 65000,  # Accommodates proxy tokens such as USDC FiatTokenProxy.
    "exchange": 500000,
    # Aave-style exchange_underlying wraps and unwraps interest-bearing tokens.
    "exchange_underlying": 500000,
    # Metapool underlying swaps execute through both the metapool and base pool.
    "exchange_underlying_metapool": 600000,
    # Metapool zaps execute liquidity changes in both component pools.
    "metapool_zap_add_liquidity": 700000,
    "metapool_zap_remove_liquidity": 600000,
    # StableSwap excludes the CryptoSwap price-scale rebalance surcharge.
    "add_liquidity_2": 250000,
    "add_liquidity_3": 350000,
    # CryptoSwap may run a state-dependent price-scale rebalance on liquidity changes.
    # These floors include the extra invariant work and storage writes to avoid OOG.
    "add_liquidity_2_crypto": 450000,
    "add_liquidity_3_crypto": 600000,
    # Four-coin Aave-style adds may wrap every underlying token.
    "add_liquidity_4": 600000,
    # Proportional removal returns every coin, so its floor scales with N.
    "remove_liquidity": 250000,
    "remove_liquidity_2": 250000,
    "remove_liquidity_3": 350000,
    "remove_liquidity_4": 450000,
    # Single-coin exits recompute the invariant; volatile pools also update price scale.
    "remove_liquidity_one_coin": 350000,
    "remove_liquidity_one_coin_crypto": 500000,
    # Imbalanced four-coin exits are the heaviest withdrawal shape.
    "remove_liquidity_imbalance": 450000,
    "router_exchange": 400000,
}

# Live estimates need headroom for state-dependent loops, oracle writes, and storage warmth.
CURVE_GAS_ESTIMATE_BUFFER: float = 1.20

EXCHANGE_SELECTOR = "0x3df02124"  # exchange(int128,int128,uint256,uint256) - StableSwap
EXCHANGE_UINT256_SELECTOR = "0x5b41b908"  # exchange(uint256,uint256,uint256,uint256) - CryptoSwap/Tricrypto
EXCHANGE_UNDERLYING_SELECTOR = "0xa6417ed6"  # exchange_underlying(int128,int128,uint256,uint256)
ADD_LIQUIDITY_2_SELECTOR = "0x0b4c7e4d"  # add_liquidity(uint256[2],uint256)
ADD_LIQUIDITY_3_SELECTOR = "0x4515cef3"  # add_liquidity(uint256[3],uint256)
ADD_LIQUIDITY_4_SELECTOR = "0x029b2f34"  # add_liquidity(uint256[4],uint256)
ADD_LIQUIDITY_DYN_SELECTOR = "0xb72df5de"  # add_liquidity(uint256[],uint256) — StableSwap NG
REMOVE_LIQUIDITY_2_SELECTOR = "0x5b36389c"  # remove_liquidity(uint256,uint256[2])
REMOVE_LIQUIDITY_3_SELECTOR = "0xecb586a5"  # remove_liquidity(uint256,uint256[3])
REMOVE_LIQUIDITY_4_SELECTOR = "0x7d49d875"  # remove_liquidity(uint256,uint256[4])
REMOVE_LIQUIDITY_DYN_SELECTOR = "0xd40ddb8c"  # remove_liquidity(uint256,uint256[]) — StableSwap NG
REMOVE_LIQUIDITY_ONE_SELECTOR = "0x1a4d01d2"  # remove_liquidity_one_coin(uint256,int128,uint256) — StableSwap
# CryptoSwap single-coin exits use a uint256 index and a distinct selector.
REMOVE_LIQUIDITY_ONE_CRYPTO_SELECTOR = "0xf1dc3cc9"  # remove_liquidity_one_coin(uint256,uint256,uint256) — CryptoSwap
# Quote selectors follow the same StableSwap int128 versus CryptoSwap uint256 split.
CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR = "0xcc2b27d7"  # calc_withdraw_one_coin(uint256,int128) — StableSwap
CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR = "0x4fb08c5e"  # calc_withdraw_one_coin(uint256,uint256) — CryptoSwap
# Imbalanced withdrawal is StableSwap-only: amounts are exact outputs and
# max_burn_amount is the LP-spend ceiling. Legacy selectors are keyed by N;
# StableSwap NG uses a dynamic array.
REMOVE_LIQUIDITY_IMBALANCE_SELECTORS: dict[int, str] = {
    2: "0xe3103273",  # remove_liquidity_imbalance(uint256[2],uint256)
    3: "0x9fdaea0c",  # remove_liquidity_imbalance(uint256[3],uint256)
    4: "0x18a7bd76",  # remove_liquidity_imbalance(uint256[4],uint256)
}
REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR = "0x7706db75"  # remove_liquidity_imbalance(uint256[],uint256) — StableSwap NG
# StableSwap fixed-array quote selectors are keyed by N. is_deposit=False quotes
# the LP burn used for the fail-closed max-burn ceiling.
STABLE_CALC_TOKEN_AMOUNT_SELECTORS: dict[int, str] = {
    2: "0xed8e84f3",  # calc_token_amount(uint256[2],bool)
    3: "0x3883e119",  # calc_token_amount(uint256[3],bool)
    4: "0xcf701ff7",  # calc_token_amount(uint256[4],bool)
}
GET_DY_SELECTOR = "0x5e0d443f"  # get_dy(int128,int128,uint256)
GET_DY_UINT256_SELECTOR = "0x556d6e9f"  # get_dy(uint256,uint256,uint256)
GET_DY_UNDERLYING_SELECTOR = "0x07211ef7"  # get_dy_underlying(int128,int128,uint256)
ERC20_DECIMALS_SELECTOR = "0x313ce567"  # decimals() -> uint8

# Live pool reads protect calldata coin order, ABI-family dispatch, decimals, and NAV.
COINS_UINT256_SELECTOR = "0xc6610657"  # coins(uint256) -> address (factory/NG/newer pools)
COINS_INT128_SELECTOR = "0x23746eb8"  # coins(int128) -> address (older Vyper pools, e.g. 3pool)
BALANCES_UINT256_SELECTOR = "0x4903b0d1"  # balances(uint256) -> uint256 (factory/NG/newer pools)
BALANCES_INT128_SELECTOR = "0x065a80d8"  # balances(int128) -> uint256 (older Vyper pools, e.g. 3pool)
GET_VIRTUAL_PRICE_SELECTOR = "0xbb7b8b80"  # get_virtual_price() -> uint256 (1e18-scaled)
# A successful dynamic-array quote fingerprints the StableSwap NG ABI.
NG_CALC_TOKEN_AMOUNT_SELECTOR = "0x3db06dd8"

# DepositZap selectors take the pool first and use combined ordering:
# index 0 is the meta coin; indices 1..N are base-pool coins.
ZAP_ADD_LIQUIDITY_4_SELECTOR = "0x384e03db"  # add_liquidity(address,uint256[4],uint256)
ZAP_REMOVE_LIQUIDITY_4_SELECTOR = "0xad5cc918"  # remove_liquidity(address,uint256,uint256[4])
ZAP_CALC_TOKEN_AMOUNT_4_SELECTOR = "0x861cdef0"  # calc_token_amount(address,uint256[4],bool)
ZAP_GET_DY_UNDERLYING_SELECTOR = "0x07211ef7"  # exchange_underlying lives on the metapool itself

# Crypto pool versions differ: newer pools include the deposit bool while older
# twocrypto pools omit it. Both use inline fixed-size arrays, unlike StableSwap NG.
# Probe bool-first, then no-bool, instead of trusting static version metadata.
CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS: dict[int, tuple[str, str]] = {
    2: ("0xed8e84f3", "0x8d8ea727"),  # calc_token_amount(uint256[2],bool) | (uint256[2])
    3: ("0x3883e119", "0x5b6f1b5a"),  # calc_token_amount(uint256[3],bool) | (uint256[3])
}


class PoolType(Enum):
    """Curve pool type."""

    STABLESWAP = "stableswap"
    CRYPTOSWAP = "cryptoswap"
    TRICRYPTO = "tricrypto"


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
    rpc_url: str | None = None  # DEPRECATED — use gateway_client
    gateway_client: "GatewayClient | None" = field(default=None, repr=False, compare=False)
    # Discovery never signs or submits. Synthetic positive quotes preserve the
    # fail-closed guards while allowing deterministic selector extraction.
    # force_is_ng compiles both StableSwap ABI variants for permission manifests.
    permission_discovery: bool = False
    force_is_ng: bool | None = None
    # Permission overrides are deployment-scoped; runtime exact addresses resolve live.
    permission_pool_overrides: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.chain not in CURVE_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(CURVE_ADDRESSES.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps >= 10000:
            raise ValueError("Slippage must be between 0 (inclusive) and 10000 (exclusive) basis points")
        if self.permission_pool_overrides and not self.permission_discovery:
            raise ValueError("permission_pool_overrides are only valid during permission discovery")

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
    use_underlying: bool = False  # When True, use exchange_underlying() (aave-type pools)
    # StableSwap NG liquidity calls use dynamic arrays; legacy pools use fixed-size arrays.
    is_ng: bool = False
    # Per-coin decimals, positionally aligned with ``coins`` / ``coin_addresses``.
    # None means unread, so callers fall back to token resolution rather than zero.
    coin_decimals: list[int] | None = None
    # Metapools natively pair [meta coin, base LP]. Underlying zap routing uses
    # combined order [meta coin, *base-pool coins].
    is_metapool: bool = False
    base_pool: str | None = None
    base_pool_coins: list[str] | None = None
    base_pool_coin_addresses: list[str] | None = None
    zap_address: str | None = None

    @staticmethod
    def _match_coin(coin: str, symbols: list[str], addresses: list[str]) -> int | None:
        """Index of ``coin`` in the parallel symbol / address lists, or ``None``.

        Matches case-insensitively by symbol first, then by address — so a caller
        may pass either form. ``symbols`` and ``addresses`` are positionally
        aligned (entry ``k`` is the same coin in both); a match in either list
        returns that shared index.
        """
        for k, sym in enumerate(symbols):
            if sym.upper() == coin.upper():
                return k
        for k, addr in enumerate(addresses):
            if addr.lower() == coin.lower():
                return k
        return None

    def underlying_coin_index(self, coin: str) -> int | None:
        """Return the COMBINED-space index of ``coin`` for a metapool, or ``None``.

        Combined index 0 is always the meta coin (``coins[0]``); indices 1..N map
        to ``base_pool_coins`` / ``base_pool_coin_addresses`` in order. ``coin``
        may be a symbol or an address. Returns ``None`` when this is not a
        metapool or ``coin`` is neither the meta coin nor a base-pool coin — the
        caller then falls back to the native 2-coin path.
        """
        if not self.is_metapool:
            return None
        combined_syms = [self.coins[0], *(self.base_pool_coins or [])]
        combined_addrs = [self.coin_addresses[0], *(self.base_pool_coin_addresses or [])]
        return self._match_coin(coin, combined_syms, combined_addrs)

    def get_coin_index(self, coin: str) -> int:
        """Get the index of a coin in the pool.

        Args:
            coin: Coin symbol or address

        Returns:
            Index of the coin

        Raises:
            ValueError: If coin not found in pool
        """
        for i, c in enumerate(self.coins):
            if c.upper() == coin.upper():
                return i

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
            "use_underlying": self.use_underlying,
            "is_ng": self.is_ng,
            "coin_decimals": self.coin_decimals,
            "is_metapool": self.is_metapool,
            "base_pool": self.base_pool,
            "base_pool_coins": self.base_pool_coins,
            "base_pool_coin_addresses": self.base_pool_coin_addresses,
            "zap_address": self.zap_address,
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
    amount_out_estimate: int = 0  # Pre-slippage quote in raw output units.
    token_out_decimals: int = 18  # decimal-policy-exempt: display fallback only; measured value overwrites on success
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
            "amount_out_estimate": str(self.amount_out_estimate),
            "token_out_decimals": self.token_out_decimals,
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
        self._gateway_client = config.gateway_client
        self._permission_discovery = config.permission_discovery
        self._force_is_ng = config.force_is_ng

        self.addresses = CURVE_ADDRESSES[self.chain]
        self.pools = dict(config.permission_pool_overrides)

        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        self._allowance_cache = AllowanceCache(self.wallet_address)

        # Cache live metadata by lowercased pool address for one adapter lifetime.
        self._pool_refresh_cache: dict[str, PoolInfo] = {}

        logger.info(f"CurveAdapter initialized for chain={self.chain}, wallet={self.wallet_address[:10]}...")

    @staticmethod
    def _build_cold_start_pool_info(name: str, pool_data: dict[str, Any]) -> PoolInfo:
        """Build a ``PoolInfo`` from a deployment-bound pool description.

        This is an admission-verified binding used verbatim when no transport is
        available during offline permission compilation. The
        refresh-on-read path (``_refresh_pool_info_from_chain``) overrides the
        safety-critical fields (coins / coin_addresses / decimals / virtual_price
        / is_ng) with live chain truth when a gateway / RPC is wired.
        """
        return PoolInfo(
            address=pool_data["address"],
            lp_token=pool_data["lp_token"],
            coins=pool_data["coins"],
            coin_addresses=pool_data["coin_addresses"],
            pool_type=PoolType(pool_data["pool_type"]),
            n_coins=pool_data["n_coins"],
            name=name,
            virtual_price=pool_data.get("virtual_price", Decimal("1.0")),
            use_underlying=pool_data.get("use_underlying", False),
            is_ng=pool_data.get("is_ng", False),
            is_metapool=pool_data.get("is_metapool", False),
            base_pool=pool_data.get("base_pool"),
            base_pool_coins=pool_data.get("base_pool_coins"),
            base_pool_coin_addresses=pool_data.get("base_pool_coin_addresses"),
            zap_address=pool_data.get("zap_address"),
            coin_decimals=pool_data.get("coin_decimals"),
        )

    def get_pool_info(self, pool_address: str, *, refresh: bool = True) -> PoolInfo | None:
        """Get information about a pool.

        The deployment binding is the cold-start value; when a gateway
        or RPC is wired the registry is reconciled against live chain state
        (coins / coin_addresses / decimals / virtual_price / is_ng) before being
        returned — see ``_refresh_pool_info_from_chain`` (VIB-5423 / VIB-5424).

        Args:
            pool_address: Pool contract address
            refresh: When ``True`` (default, calldata-producing paths) bound
                metadata is reconciled against chain truth. When ``False`` the
                read-only quote / pair-resolution path opts out of the network
                reconcile (VIB-5423) — a warm cache entry is still reused, but no
                new RPC reads are issued. ``is_ng`` / ``virtual_price`` are
                irrelevant to a ``get_dy`` quote, so paying for them on every
                slippage estimate (with a fresh per-quote adapter) is wasteful.

        Returns:
            PoolInfo if known, None otherwise
        """
        for name, pool_data in self.pools.items():
            if pool_data["address"].lower() == pool_address.lower():
                return self._resolve_pool_info(self._build_cold_start_pool_info(name, pool_data), refresh=refresh)
        # Exact addresses resolve through MetaRegistry; unresolved pools fail closed.
        if self._has_read_transport():
            return self._build_pool_info_from_metaregistry(pool_address, refresh=refresh)
        return None

    def _build_pool_info_from_metaregistry(self, pool_address: str, *, refresh: bool) -> PoolInfo | None:
        """Build a ``PoolInfo`` for an UNCURATED Curve pool from the MetaRegistry (VIB-5628).

        Resolves the pool's coins / decimals / lp_token / metapool shape and the
        gamma-discriminated pool_type from Curve's on-chain MetaRegistry (via the
        gateway-first ``resolve_pool_metadata`` seam), assembles a cold-start
        ``PoolInfo``, then runs it through the SAME refresh-on-read reconcile the
        static path uses (``_resolve_pool_info`` → ``_refresh_pool_info_from_chain``)
        so ``is_ng`` / ``virtual_price`` / live coin order are filled and the
        result is cached in ``_pool_refresh_cache``.

        Returns ``None`` (fail closed) when the resolver cannot safely and fully
        resolve the address (not a Curve pool, aave-type/wrapped, no transport) —
        preserving today's unknown-pool contract for a genuine miss.
        """
        cached = self._pool_refresh_cache.get(pool_address.lower())
        if cached is not None:
            return self._resolve_pool_info(cached, refresh=refresh)

        from almanak.connectors.curve.pool_resolver import resolve_pool_metadata

        meta = resolve_pool_metadata(
            chain=self.chain,
            pool_address=pool_address,
            gateway_client=self._gateway_client,
            rpc_url=self._rpc_url,
        )
        if meta is None:
            return None

        cold_start = PoolInfo(
            address=meta.address,
            lp_token=meta.lp_token,
            coins=list(meta.coin_symbols),
            coin_addresses=list(meta.coin_addresses),
            pool_type=PoolType(meta.pool_type),
            n_coins=meta.n_coins,
            name=f"dynamic:{meta.address[:10]}",
            coin_decimals=list(meta.coin_decimals),
            is_metapool=meta.is_metapool,
            base_pool=meta.base_pool,
            base_pool_coin_addresses=(
                list(meta.base_pool_coin_addresses) if meta.base_pool_coin_addresses is not None else None
            ),
        )
        return self._resolve_pool_info(cold_start, refresh=refresh)

    def get_pool_by_name(self, name: str, *, refresh: bool = True) -> PoolInfo | None:
        """Get pool info by name.

        Same refresh-on-read reconciliation as ``get_pool_info`` (VIB-5423);
        ``refresh=False`` opts the read-only quote path out of the reconcile.

        Args:
            name: Deployment-scoped pool name, when one was explicitly supplied.
            refresh: See ``get_pool_info``.

        Returns:
            PoolInfo if found, None otherwise
        """
        pool_data = self.pools.get(name)
        if pool_data:
            return self._resolve_pool_info(self._build_cold_start_pool_info(name, pool_data), refresh=refresh)
        return None

    def _resolve_pool_info(self, cold_start: PoolInfo, *, refresh: bool) -> PoolInfo:
        """Resolve a cold-start ``PoolInfo`` to the value callers receive.

        A warm refresh-cache entry is always reused (returned as a defensive
        copy — including fresh list objects, since ``dataclasses.replace`` is
        shallow — so a caller mutating the returned coins/addresses/decimals
        can't poison later resolves). With no warm entry: ``refresh=True``
        reconciles against chain truth; ``refresh=False`` returns the cold-start
        static verbatim without issuing any RPC reads.
        """
        cached = self._pool_refresh_cache.get(cold_start.address.lower())
        if cached is not None:
            return self._apply_forced_is_ng(
                replace(
                    cached,
                    coins=list(cached.coins),
                    coin_addresses=list(cached.coin_addresses),
                    coin_decimals=list(cached.coin_decimals) if cached.coin_decimals is not None else None,
                )
            )
        if not refresh:
            return self._apply_forced_is_ng(cold_start)
        return self._apply_forced_is_ng(self._refresh_pool_info_from_chain(cold_start))

    def _apply_forced_is_ng(self, pool_info: PoolInfo) -> PoolInfo:
        """Pin the ABI variant when discovery asked for a specific one.

        No-op unless ``CurveConfig.force_is_ng`` was set, which only permission
        discovery does. Applied AFTER the refresh/cache resolve so it overrides
        both the cold-start binding value and any live probe result — the point is
        to compile a chosen variant, not to observe one.

        **StableSwap family only.** ``is_ng`` distinguishes the StableSwap-NG
        dynamic-array ABI from the legacy fixed-size one; CryptoSwap/Tricrypto
        pools implement neither NG form, so forcing it there authorises a
        selector the pool does not have and the compiler would never emit for
        it — an over-grant, and precisely the failure class this PR exists to
        close. Measured before this guard: three non-StableSwap fixtures
        (arbitrum/tricrypto, base/weth_cbeth, ethereum/tricrypto2) carried
        ``0xb72df5de`` and ``0xd40ddb8c`` for nothing.

        Restricting here costs no drift protection: ``_probe_is_ng``
        fingerprints the NG ABI, so it cannot return ``True`` for a pool that
        does not implement it.
        """
        if self._force_is_ng is None or self._is_cryptoswap(pool_info) or pool_info.is_ng == self._force_is_ng:
            return pool_info
        return replace(pool_info, is_ng=self._force_is_ng)

    def _has_read_transport(self) -> bool:
        """True when a connected gateway client or a direct RPC URL can serve reads."""
        gateway_client = self._gateway_client
        if gateway_client is not None and getattr(gateway_client, "is_connected", False):
            return True
        return bool(self._rpc_url)

    # Synthetic quotes are permission-discovery-only; funds-moving paths keep
    # positive min-output and bounded max-burn requirements fail closed.

    def _synthetic_quote_scale(self, notional: int, *, floor: int = 10**6) -> int:
        """A positive, deterministic stand-in for an on-chain quote.

        Scaled off the caller's own notional so the value stays in a sane
        range for the pool's decimals, with a floor so that the downstream
        ``* (10000 - slippage_bps) // 10000`` integer math can never round it
        back to zero and re-trip the guard it is meant to satisfy.
        """
        return max(int(notional), floor)

    def _discovery_min_amounts(self, pool_info: PoolInfo, lp_amount: int, length: int | None = None) -> list[int]:
        """Synthetic per-coin proportional withdrawal vector for discovery.

        Splits the burned LP evenly across the coins. The real path derives
        this from live reserves; the split ratio is irrelevant to discovery,
        which only reads the selector off the encoded call — but every entry
        must be POSITIVE or ``remove_liquidity``'s all-zero guard trips.
        """
        n = length if length is not None else pool_info.n_coins
        per_coin = self._synthetic_quote_scale(lp_amount // max(n, 1))
        return [per_coin] * n

    def _resolve_gas(self, *, to: str, data: str, value: int, static_gas: int) -> int:
        """Live ``eth_estimateGas`` × safety buffer, clamped to a conservative static floor.

        The per-op constants in ``CURVE_GAS_ESTIMATES`` were hand-typed and
        optimistic for 4-coin / native-ETH / aave-type pools, risking an L1
        out-of-gas revert that strands a tx on-chain (VIB-5440). This seeds the
        gas limit from a live estimate against the pool's ACTUAL shape, routed
        through the gateway (``eth_estimateGas`` is on the gateway RPC allowlist,
        so there is no strategy-container egress — it rides the same gateway
        channel the M0 metadata reads use).

        The result is ``max(estimate × buffer, static_floor)`` so the estimate
        may only RAISE the limit above the floor, never lower it.

        Empty≠Zero: an unavailable estimate — no read transport, an RPC error,
        or a revert under pre-approval state (the estimate for an ``add_liquidity``
        that follows an unexecuted ``approve`` in the same bundle) — returns the
        conservative static floor, never 0 or a raw under-buffered value.
        """
        if not self._has_read_transport():
            return static_gas
        try:
            raw = eth_estimate_gas(
                chain=self.chain,
                to=to,
                data=data,
                from_address=self.wallet_address,
                value=value,
                gateway_client=self._gateway_client,
            )
        except Exception as exc:  # noqa: BLE001 — any estimate failure → conservative static floor
            logger.debug("Curve gas estimate raised for %s: %s; using static floor %d", to, exc, static_gas)
            raw = None
        if raw is None or raw <= 0:
            return static_gas
        buffered = round(raw * CURVE_GAS_ESTIMATE_BUFFER)
        resolved = max(buffered, static_gas)
        logger.debug(
            "Curve gas for %s: raw=%d buffered=%d (x%.2f) static_floor=%d -> %d",
            to,
            raw,
            buffered,
            CURVE_GAS_ESTIMATE_BUFFER,
            static_gas,
            resolved,
        )
        return resolved

    def _refresh_pool_info_from_chain(self, pool_info: PoolInfo) -> PoolInfo:
        """Return ``pool_info`` with safety-critical fields refreshed from chain.

        Reads live ``coins(i)`` / ``decimals()`` / ``get_virtual_price()`` and
        probes the NG ABI to override the bound/resolved ``coins`` / ``coin_addresses``
        / ``coin_decimals`` / ``virtual_price`` / ``is_ng`` with chain truth. Drift
        between the cold-start identity and the live value is logged loudly.

        Fail-safe: when no transport is available, every live read fails, or any
        unexpected error escapes a helper, the cold-start ``pool_info`` is
        returned unchanged (so a correctly-typed pool resolves to exactly today's
        values, and a transient RPC outage never downgrades the binding).
        ``is_ng`` is only overridden once transport health is independently
        confirmed by a sibling read AND the probe returns positive evidence —
            a transient probe failure keeps the cold-start value, never "legacy ABI".
        """
        if not self._has_read_transport():
            logger.debug(
                "Curve pool refresh skipped for %s: no gateway/RPC transport; using cold-start values",
                pool_info.name,
            )
            return pool_info

        try:
            refreshed = replace(pool_info)
            transport_healthy = False

            # Aave-style metadata tracks underlying tokens while coins(i) returns
            # wrapped aTokens; refreshing that coin set would corrupt routing.
            if not pool_info.use_underlying:
                live_addresses = self._read_pool_coins(pool_info)
                if live_addresses is not None:
                    transport_healthy = True
                    if [a.lower() for a in live_addresses] != [a.lower() for a in pool_info.coin_addresses]:
                        logger.warning(
                            "Curve coin drift for %s: cold_start=%s live=%s; trusting live chain order",
                            pool_info.name,
                            pool_info.coin_addresses,
                            live_addresses,
                        )
                    refreshed.coin_addresses = live_addresses
                    refreshed.coins = self._realign_coin_symbols(pool_info, live_addresses)
                    refreshed.coin_decimals = self._read_coin_decimals(live_addresses)

            live_vp = self._read_virtual_price(pool_info)
            if live_vp is not None:
                transport_healthy = True
                refreshed.virtual_price = live_vp

            if transport_healthy:
                try:
                    live_is_ng = self._probe_is_ng(pool_info)
                except Exception as exc:  # noqa: BLE001
                    # Ambiguous probe failures retain the cold-start ABI family.
                    logger.warning(
                        "Curve is_ng probe inconclusive for %s (%s); keeping cold-start is_ng=%s",
                        pool_info.name,
                        exc,
                        pool_info.is_ng,
                    )
                    live_is_ng = pool_info.is_ng
                if live_is_ng != pool_info.is_ng:
                    logger.warning(
                        "Curve is_ng drift for %s: cold_start=%s live=%s; trusting live ABI fingerprint",
                        pool_info.name,
                        pool_info.is_ng,
                        live_is_ng,
                    )
                refreshed.is_ng = live_is_ng
        except Exception as exc:  # noqa: BLE001 — fail-safe: any unexpected error keeps cold-start static
            logger.warning(
                "Curve pool refresh raised unexpectedly for %s (%s); using cold-start values",
                pool_info.name,
                exc,
            )
            return pool_info

        if not transport_healthy:
            logger.warning(
                "Curve pool refresh: no live read succeeded for %s; using cold-start values",
                pool_info.name,
            )
            return pool_info

        self._pool_refresh_cache[pool_info.address.lower()] = refreshed
        return refreshed

    def _read_pool_coins(self, pool_info: PoolInfo) -> list[str] | None:
        """Read live ``coins(i)`` addresses, or ``None`` if any read fails.

        Probes ``coins(uint256)`` first (factory / NG / newer pools) then
        ``coins(int128)`` (older Vyper pools like Ethereum 3pool), mirroring the
        ``balances`` selector probe used elsewhere in this adapter. Returns the
        full set of ``n_coins`` addresses only when every read succeeds with a
        non-zero address — a partial read is treated as a failure so the caller
        keeps the static set rather than splicing chain + literal coins.
        """
        for selector in (COINS_UINT256_SELECTOR, COINS_INT128_SELECTOR):
            addresses: list[str] = []
            for i in range(pool_info.n_coins):
                try:
                    raw = eth_call(
                        chain=self.chain,
                        to=pool_info.address,
                        data=selector + self._pad_uint256(i),
                        rpc_url=self._rpc_url,
                        gateway_client=self._gateway_client,
                        timeout=10.0,
                    )
                except Exception:  # noqa: BLE001 — wrong selector reverts; try the next candidate
                    break
                if raw is None:
                    break
                addr = decode_address(raw)
                if addr == ZERO_ADDRESS:
                    break
                addresses.append(addr)
            if len(addresses) == pool_info.n_coins:
                return addresses
        return None

    def _realign_coin_symbols(self, pool_info: PoolInfo, live_addresses: list[str]) -> list[str]:
        """Symbols positionally aligned to ``live_addresses``.

        Reuses the static symbol for any address already known in the literal
        (so a mere coin-order reversal re-orders the symbols correctly and keeps
        them TokenResolver-valid); for a genuinely new address, resolves the
        symbol on-chain, falling back to a truncated address for display.
        """
        static_by_addr = {
            addr.lower(): sym for addr, sym in zip(pool_info.coin_addresses, pool_info.coins, strict=False)
        }
        symbols: list[str] = []
        for addr in live_addresses:
            sym = static_by_addr.get(addr.lower())
            symbols.append(sym if sym is not None else self._get_token_symbol(addr))
        return symbols

    def _read_coin_decimals(self, coin_addresses: list[str]) -> list[int] | None:
        """Read live ``decimals()`` for each coin, or ``None`` if any read fails.

        Curve's native-coin placeholder (0xEeee…) is not an ERC-20 and has no
        ``decimals()`` — it is the chain's 18-decimal native coin, handled
        directly. ``None`` on any failure so the caller falls back to the
        TokenResolver (Empty≠Zero: an unread decimal is unknown, not zero).
        """
        decimals: list[int] = []
        for addr in coin_addresses:
            if self._is_native_token(addr):
                decimals.append(18)
                continue
            try:
                value = eth_call_uint256(
                    chain=self.chain,
                    to=addr,
                    data=ERC20_DECIMALS_SELECTOR,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
            except Exception:  # noqa: BLE001 — unread decimals → resolver fallback
                return None
            if value is None:
                return None
            decimals.append(int(value))
        return decimals

    def _read_virtual_price(self, pool_info: PoolInfo) -> Decimal | None:
        """Read live ``get_virtual_price()`` (1e18-scaled) as a Decimal, or ``None``.

        ``None`` on failure or a zero read so the caller keeps the static
        snapshot — a real virtual_price is ``>= ~1e18``.
        """
        try:
            raw = eth_call_uint256(
                chain=self.chain,
                to=pool_info.address,
                data=GET_VIRTUAL_PRICE_SELECTOR,
                rpc_url=self._rpc_url,
                gateway_client=self._gateway_client,
                timeout=10.0,
            )
        except Exception as exc:  # noqa: BLE001 — keep static snapshot on any read failure
            logger.debug("Curve get_virtual_price read failed for %s (%s); keeping static", pool_info.name, exc)
            return None
        if not raw:
            return None
        return Decimal(raw) / Decimal(10**18)

    def _probe_is_ng(self, pool_info: PoolInfo) -> bool:
        """Whether the live pool exposes the StableSwap-NG dynamic-array ABI.

        Probes ``calc_token_amount(uint256[],bool)`` — implemented only by NG
        pools — with a zero-amount deposit. A clean (non-None) return means the
        NG selector is present (dynamic ``uint256[]`` add/remove ABI); a revert /
        empty return (raises / ``None``) means the pool only speaks the legacy
        fixed-size ABI. Crypto / Tricrypto pools are a different ABI family and
        are never NG — short-circuit without a call. Callers MUST only trust the
        result once transport health is independently confirmed, so a transient
        RPC failure can't be misread as "legacy".
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            return False
        calldata = (
            NG_CALC_TOKEN_AMOUNT_SELECTOR
            + self._pad_uint256(0x40)
            + self._pad_uint256(1)  # is_deposit
            + self._pad_uint256(pool_info.n_coins)
            + "".join(self._pad_uint256(0) for _ in range(pool_info.n_coins))
        )
        result = eth_call_uint256(
            chain=self.chain,
            to=pool_info.address,
            data=calldata,
            rpc_url=self._rpc_url,
            gateway_client=self._gateway_client,
            timeout=10.0,
        )
        return result is not None

    def _swap_oracle_guard_error(
        self,
        *,
        pool_info: "PoolInfo",
        token_in_symbol: str,
        token_out_symbol: str,
        amount_in: Decimal,
        amount_out_estimate: int,
        token_out_decimals: int,
        price_ratio: Decimal | None,
        oracle_guard_bps: int | None,
        strict_oracle_guard: bool,
        oracle_prices_real: bool,
    ) -> str | None:
        """Run the P0-8 oracle/MEV min-out guard; return an error to fail with, or
        ``None`` to proceed.

        **Scoped to StableSwap pools.** The check compares the pool's *execution*
        rate (``get_dy``) to the oracle mid. On a StableSwap pool that gap is just
        fee + a few bps of impact, so a material shortfall is a real depeg /
        displacement signal (the audit's P0-8 priority). On a **CryptoSwap /
        Tricrypto** pool the same gap legitimately includes genuine price impact
        that scales with trade size and pool depth without bound — so the
        execution-rate check cannot distinguish a bad fill from a large-but-fair
        one and **false-blocks legitimate swaps** (CI surfaced a 637 bps fill on
        arb-tricrypto). Volatile-pool min-out protection is the slippage floor; an
        impact-immune spot-price-vs-oracle guard is the correct mechanism and is
        tracked separately. So volatile pools skip this check entirely.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            logger.debug(
                "Curve swap oracle guard skipped for volatile pool %s "
                "(execution-rate vs oracle conflates real price impact with manipulation).",
                pool_info.name,
            )
            return None
        threshold_bps = oracle_guard_bps if oracle_guard_bps is not None else DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS
        guard = check_swap_oracle_divergence(
            amount_in=amount_in,
            pool_quoted_out=Decimal(amount_out_estimate) / Decimal(10**token_out_decimals),
            price_ratio=price_ratio if oracle_prices_real else None,
            threshold_bps=threshold_bps,
            strict_when_unmeasured=strict_oracle_guard,
        )
        if not guard.ok:
            if guard.reason == "oracle_unmeasured":
                return (
                    f"Curve swap oracle guard (strict): no oracle price for "
                    f"{token_in_symbol}->{token_out_symbol} on {pool_info.name}; "
                    f"refusing to trade without an independent oracle reference"
                )
            return (
                f"Curve swap blocked: {pool_info.name} quote is {guard.shortfall_bps} bps "
                f"below oracle-fair (threshold {threshold_bps} bps) — pre-moved / displaced "
                f"(stale depeg / persistent imbalance) pool; refusing to build a bad-fill swap"
            )
        if guard.reason == "oracle_unmeasured":
            logger.warning(
                "Curve swap oracle guard unmeasured for %s->%s on %s (no oracle price_ratio); "
                "proceeding with pool-self-referential min-out only (degrade-open).",
                token_in_symbol,
                token_out_symbol,
                pool_info.name,
            )
        return None

    def _oracle_anchored_min_out(
        self,
        *,
        pool_info: "PoolInfo",
        pool_floor_wei: int,
        pool_quoted_out_wei: int,
        amount_in: Decimal,
        token_out_decimals: int,
        price_ratio: Decimal | None,
        oracle_guard_bps: int | None,
        oracle_prices_real: bool,
        token_in_symbol: str,
        token_out_symbol: str,
    ) -> int:
        """Anchor the EXECUTED min-out floor to the independent oracle (VIB-5490).

        The pool-self-referential floor (``pool_quote × (1 − slippage)``) stays
        within a same-block sandwich no matter how wide ``slippage`` is: the
        floor never references anything but the (attacker-moved) pool. This
        raises the floor toward ``oracle_fair × (1 − tolerance)`` so atomic-
        sandwich extraction is bounded by the oracle tolerance instead.

        Pool-type-aware tolerance AND residual (revert risk is why this is
        separate from the VIB-5439 detection guard): STABLE pools reuse the
        detection threshold (default 150 bps tolerance) with a small 50 bps drift
        residual; VOLATILE pools use a wide 500 bps tolerance with a wider 200 bps
        drift residual. The clamp caps the oracle floor at
        ``pool_quote × (1 − residual)`` (NOT the raw quote) inside
        :func:`clamp_min_out_to_oracle`, so a genuine >tolerance-impact swap keeps
        a benign inter-block-drift buffer and still fills — closing the zero-buffer
        revert an earlier raw-quote cap would have caused on a drifted volatile
        pool (e.g. a large tricrypto teardown). Degrade-open: a placeholder /
        unmeasured oracle (or unusable quote) leaves the pool floor untouched
        (never fabricates a higher floor).

        The residual is pool-type-fixed (a benign-drift reality) and NOT driven by
        the operator's ``oracle_guard_bps`` override — even a tight override cannot
        tighten the residual below the drift buffer, so it cannot force a revert.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            default_tolerance = DEFAULT_VOLATILE_ORACLE_FLOOR_TOLERANCE_BPS
            residual_bps = DEFAULT_VOLATILE_ORACLE_FLOOR_RESIDUAL_BPS
        else:
            default_tolerance = DEFAULT_STABLE_ORACLE_FLOOR_TOLERANCE_BPS
            residual_bps = DEFAULT_STABLE_ORACLE_FLOOR_RESIDUAL_BPS

        # The override changes tolerance, never the pool-family drift residual.
        tolerance_bps = oracle_guard_bps if oracle_guard_bps is not None else default_tolerance

        clamp = clamp_min_out_to_oracle(
            pool_floor_wei=pool_floor_wei,
            pool_quoted_out_wei=pool_quoted_out_wei,
            amount_in=amount_in,
            # Placeholder prices are unmeasured and must not fabricate an oracle floor.
            price_ratio=price_ratio if oracle_prices_real else None,
            token_out_decimals=token_out_decimals,
            tolerance_bps=tolerance_bps,
            residual_bps=residual_bps,
        )
        if clamp.clamped:
            logger.info(
                "Curve executed-floor oracle anchor RAISED min-out for %s->%s on %s: "
                "pool_floor=%d -> oracle_floor=%s (tolerance %d bps, residual %d bps, %s pool).",
                token_in_symbol,
                token_out_symbol,
                pool_info.name,
                clamp.pool_floor_wei,
                clamp.oracle_floor_wei,
                tolerance_bps,
                residual_bps,
                pool_info.pool_type.value,
            )
        elif clamp.reason == "oracle_config_invalid":
            # Invalid tolerance must be visible rather than silently weakening the floor.
            logger.warning(
                "Curve executed-floor oracle anchor DISABLED for %s->%s on %s: invalid "
                "tolerance %d bps / residual %d bps (must be in (0, 10000]); executed floor "
                "falls back to the pool-self-referential min-out — check oracle_guard_bps.",
                token_in_symbol,
                token_out_symbol,
                pool_info.name,
                tolerance_bps,
                residual_bps,
            )
        elif clamp.reason == "oracle_unmeasured":
            logger.debug(
                "Curve executed-floor oracle anchor unmeasured for %s->%s on %s "
                "(no real oracle price_ratio); pool-self-referential floor kept (degrade-open).",
                token_in_symbol,
                token_out_symbol,
                pool_info.name,
            )
        return clamp.min_out_wei

    def swap(
        self,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        price_ratio: Decimal | None = None,
        oracle_guard_bps: int | None = None,
        strict_oracle_guard: bool = False,
        oracle_prices_real: bool = True,
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
                than executing with inaccurate slippage protection. Also the
                independent oracle reference for the P0-8 min-out guard below.
            oracle_guard_bps: max bps the pool quote may sit below oracle-fair
                before the swap is blocked as pre-moved (VIB-5439). ``None`` uses
                ``DEFAULT_SWAP_ORACLE_DIVERGENCE_BPS``. Separate from
                ``slippage_bps`` (which buffers the floor below the pool quote).
            strict_oracle_guard: when no oracle ``price_ratio`` is available, fail
                closed instead of degrading open to pool-self-referential min-out.
            oracle_prices_real: whether ``price_ratio`` is a real oracle reference.
                ``False`` (placeholder / offline-price mode) makes the guard treat
                the oracle as unmeasured so it never fires on a known-fake price,
                while ``price_ratio`` still feeds the CryptoSwap slippage estimate.

        Returns:
            SwapResult with transaction data
        """
        self.clear_planned_allowance_cache()
        try:
            if price_ratio is not None and price_ratio <= 0:
                raise ValueError(f"price_ratio must be positive, got {price_ratio}")

            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return SwapResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            try:
                i = pool_info.get_coin_index(token_in)
                j = pool_info.get_coin_index(token_out)
            except ValueError as e:
                return SwapResult(success=False, error=str(e))

            token_in_address = pool_info.coin_addresses[i]
            token_out_address = pool_info.coin_addresses[j]

            # Use refreshed decimals for raw-unit approvals and quoting.
            token_in_symbol = pool_info.coins[i]
            token_in_decimals = self._coin_decimals(pool_info, i)

            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            # Retain the pre-slippage raw quote for realized-slippage accounting.
            if self._gateway_client is not None or self._rpc_url:
                try:
                    amount_out_estimate = self.quote_swap_output(
                        pool_address=pool_address,
                        token_in=token_in,
                        token_out=token_out,
                        amount_in_wei=amount_in_wei,
                    )
                except Exception as exc:
                    logger.warning(
                        "Curve on-chain quote unavailable for %s (%s -> %s): %s. "
                        "Falling back to deterministic pool estimate.",
                        pool_info.name,
                        token_in,
                        token_out,
                        exc,
                    )
                    amount_out_estimate = self._estimate_swap_output(
                        pool_info,
                        i,
                        j,
                        amount_in_wei,
                        price_ratio=price_ratio,
                    )
            else:
                amount_out_estimate = self._estimate_swap_output(
                    pool_info, i, j, amount_in_wei, price_ratio=price_ratio
                )
            amount_out_minimum = max(1, compute_min_amount_out_from_bps(amount_out_estimate, slippage_bps))
            token_out_decimals = self._coin_decimals(pool_info, j)

            # StableSwap quotes fail closed when materially below the independent oracle.
            guard_error = self._swap_oracle_guard_error(
                pool_info=pool_info,
                token_in_symbol=token_in_symbol,
                token_out_symbol=pool_info.coins[j],
                amount_in=amount_in,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                strict_oracle_guard=strict_oracle_guard,
                oracle_prices_real=oracle_prices_real,
            )
            if guard_error is not None:
                return SwapResult(success=False, error=guard_error)

            # Anchor execution to the oracle while retaining the family-specific drift buffer.
            amount_out_minimum = self._oracle_anchored_min_out(
                pool_info=pool_info,
                pool_floor_wei=amount_out_minimum,
                pool_quoted_out_wei=amount_out_estimate,
                amount_in=amount_in,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                oracle_prices_real=oracle_prices_real,
                token_in_symbol=token_in_symbol,
                token_out_symbol=pool_info.coins[j],
            )

            transactions: list[TransactionData] = []

            is_native_input = self._is_native_token(token_in_address)

            # Native input is routed as msg.value and requires no ERC-20 approval.
            if not is_native_input:
                transactions.extend(self._build_approve_txs(token_in_address, pool_address, amount_in_wei))

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
                use_underlying=pool_info.use_underlying,
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
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                token_in=token_in_address,
                token_out=token_out_address,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build Curve swap: {e}")
            return SwapResult(success=False, error=str(e))

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
            slippage_bps: Slippage tolerance for min LP tokens (default from config).
                For CryptoSwap/Tricrypto (volatile) pools the min_lp floor is the
                build-time on-chain quote × (1 − slippage); if the pool price drifts
                between build and execution by more than ``slippage_bps`` the
                add_liquidity reverts with "Slippage". A revert is fail-safe (no
                loss, vs the old min_lp=0 which could be sandwiched), but a volatile
                or large deposit may need a wider ``slippage_bps`` than the stable
                default to avoid a benign revert (VIB-5441).
            recipient: Address to receive LP tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data
        """
        self.clear_planned_allowance_cache()
        try:
            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

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

            amounts_wei: list[int] = []
            for idx, amt in enumerate(amounts):
                decimals = self._coin_decimals(pool_info, idx)
                amounts_wei.append(int(amt * Decimal(10**decimals)))

            lp_quote = self._estimate_add_liquidity(pool_info, amounts_wei)
            min_lp_tokens = compute_min_amount_out_from_bps(lp_quote, slippage_bps)
            # Volatile deposits fail closed if slippage rounding removes the LP floor.
            if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO) and lp_quote > 0 and min_lp_tokens <= 0:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"CryptoSwap/Tricrypto pool {pool_info.name}: slippage-adjusted min_lp "
                        f"rounded to {min_lp_tokens} from quote {lp_quote} (slippage_bps={slippage_bps}); "
                        f"refusing to ship min_lp=0"
                    ),
                )

            transactions: list[TransactionData] = []

            native_value: int = 0
            for amount_wei, coin_addr in zip(amounts_wei, pool_info.coin_addresses, strict=False):
                if amount_wei > 0:
                    if self._is_native_token(coin_addr):
                        native_value = amount_wei
                    else:
                        transactions.extend(self._build_approve_txs(coin_addr, pool_address, amount_wei))

            add_liq_tx = self._build_add_liquidity_tx(
                pool_address=pool_address,
                amounts=amounts_wei,
                min_lp_tokens=min_lp_tokens,
                n_coins=pool_info.n_coins,
                value=native_value,
                pool_name=pool_info.name,
                is_ng=pool_info.is_ng,
                is_cryptoswap=pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO),
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

        A proportional withdrawal mirrors the pool's current reserve
        composition: burning LP pays out each coin pro rata to the on-chain
        reserves. A skewed pool therefore returns skewed per-coin amounts even
        when the position was funded evenly. That output shape is expected.
        Callers that need a specific exit shape should use
        ``remove_liquidity_one_coin`` or ``remove_liquidity_imbalance``.

        Per-coin ``min_amounts`` floors are derived from the on-chain
        proportional estimate after applying ``slippage_bps``. The build fails
        closed when no non-zero estimate is available.

        Args:
            pool_address: Pool contract address
            lp_amount: Amount of LP tokens to burn
            slippage_bps: Slippage tolerance for min output (default from config)
            recipient: Address to receive tokens (default: wallet_address)

        Returns:
            LiquidityResult with transaction data. ``amounts`` contains the
            per-coin minimum-received floors in native token base units.
        """
        self.clear_planned_allowance_cache()
        try:
            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(
                    success=False,
                    error=f"Unknown pool: {pool_address}",
                )

            # Curve LP token amounts use 18 decimals.
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            self._last_estimation_error: str | None = None
            min_amounts = self._estimate_remove_liquidity(pool_info, lp_amount_wei)
            min_amounts = [compute_min_amount_out_from_bps(a, slippage_bps) for a in min_amounts]

            # Never submit a proportional withdrawal without a non-zero slippage floor.
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

            transactions: list[TransactionData] = []

            transactions.extend(self._build_approve_txs(pool_info.lp_token, pool_address, lp_amount_wei))

            remove_tx = self._build_remove_liquidity_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                min_amounts=min_amounts,
                n_coins=pool_info.n_coins,
                pool_name=pool_info.name,
                is_ng=pool_info.is_ng,
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
        self.clear_planned_allowance_cache()
        try:
            # Zero means an exact quote; only None selects the configured default.
            if slippage_bps is None:
                slippage_bps = self.config.default_slippage_bps
            recipient = recipient or self.wallet_address

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

            # Curve LP token amounts use 18 decimals.
            lp_amount_wei = int(lp_amount * Decimal(10**18))

            # Build with the same ABI family whose on-chain quote selector succeeded.
            expected_out, used_cryptoswap = self._query_calc_withdraw_one_coin_onchain(
                pool_info, lp_amount_wei, coin_index
            )
            min_amount = compute_min_amount_out_from_bps(expected_out, slippage_bps)

            # A non-positive floor would make the withdrawal unprotected or malformed.
            if min_amount <= 0:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_one_coin: computed min_amount is {min_amount} (must be > 0) "
                        f"(expected_out={expected_out}, slippage_bps={slippage_bps}). "
                        f"Refusing to ship a non-positive floor (sandwich/theft vector)."
                    ),
                )

            transactions: list[TransactionData] = []

            transactions.extend(self._build_approve_txs(pool_info.lp_token, pool_address, lp_amount_wei))

            remove_tx = self._build_remove_liquidity_one_tx(
                pool_address=pool_address,
                lp_amount=lp_amount_wei,
                coin_index=coin_index,
                min_amount=min_amount,
                coin_symbol=pool_info.coins[coin_index],
                pool_name=pool_info.name,
                is_cryptoswap=used_cryptoswap,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                f"Built Curve remove_liquidity_one_coin: pool={pool_info.name}, "
                f"lp_amount={lp_amount}, coin={pool_info.coins[coin_index]}"
            )

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

    def remove_liquidity_imbalance(
        self,
        pool_address: str,
        amounts: list[Decimal],
        lp_amount: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a remove_liquidity_imbalance transaction (LP_CLOSE, imbalanced).

        ``remove_liquidity_imbalance(uint256[N] amounts, uint256 max_burn_amount)``
        works the OPPOSITE way to single-sided removal: the caller names the EXACT
        per-coin amounts to receive, and the pool burns however much LP is needed,
        capped at ``max_burn_amount``. So the safety floor here is a MAX-BURN
        CEILING (the most LP we will spend), NOT a min-out (VIB-5438, audit P0-4).

        The ceiling is derived from the pool's on-chain ``calc_token_amount(amounts,
        is_deposit=False)`` LP-burn quote, padded UP by ``slippage_bps`` (the
        inverse of the single-sided min-out, which pads DOWN). The slippage buffer
        absorbs the imbalance fee the legacy ``calc_token_amount`` excludes; if it
        is too tight the pool reverts on-chain ("Slippage screwed you") — safe.

        Fail-closed: this NEVER emits ``max_burn_amount = MAX_UINT256`` or any
        unbounded cap (that would let the pool burn the entire LP balance for a
        tiny withdrawal — a theft/sandwich vector). If the on-chain quote is
        unavailable/reverts, or the requested withdrawal would need more LP than
        the position holds, the compile fails loudly (mirrors the #3092 min-out and
        #3073 min_lp logic).

        Args:
            pool_address: Pool contract address
            amounts: EXACT per-coin amounts to withdraw, in human units, positional
                by pool-coin index (length MUST equal the pool's coin count).
            lp_amount: LP tokens HELD by the position (human units), the upper
                bound on what may be burned. The derived ``max_burn`` is capped at
                this; a request needing more fails closed.
            slippage_bps: Max-burn buffer over the on-chain quote (default config).
            recipient: Address to receive tokens (default: wallet_address).

        Returns:
            LiquidityResult with transaction data (operation
            ``remove_liquidity_imbalance``), or ``success=False`` on any guard.
        """
        self.clear_planned_allowance_cache()
        try:
            # Zero means an exact quote; only None selects the configured default.
            if slippage_bps is None:
                slippage_bps = self.config.default_slippage_bps
            # Validate direct callers so the max-burn ceiling cannot be weakened.
            if slippage_bps < 0 or slippage_bps > 10_000:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_imbalance: slippage_bps must be in [0, 10000] (got {slippage_bps}); "
                        f"refusing to build an unsafe max_burn ceiling."
                    ),
                )
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(success=False, error=f"Unknown pool: {pool_address}")

            # CryptoSwap families do not expose imbalanced withdrawal.
            if self._is_cryptoswap(pool_info):
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_imbalance is not supported on CryptoSwap/Tricrypto pool "
                        f"{pool_info.name}; use a single-sided (coin_index) or proportional close."
                    ),
                )

            # The amounts vector is positional and must contain all N coins.
            if len(amounts) != pool_info.n_coins:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_imbalance: amounts length {len(amounts)} != pool coin count "
                        f"{pool_info.n_coins} for {pool_info.name}."
                    ),
                )

            # Encode human-unit amounts with each coin's decimals, preserving pool order.
            amounts_wei: list[int] = []
            for i, amt in enumerate(amounts):
                if amt < 0:
                    return LiquidityResult(
                        success=False,
                        error=f"remove_liquidity_imbalance: negative amount for coin {i} ({amt}).",
                    )
                decimals = self._coin_decimals(pool_info, i)
                amounts_wei.append(int(amt * Decimal(10**decimals)))
            if not any(a > 0 for a in amounts_wei):
                return LiquidityResult(
                    success=False,
                    error="remove_liquidity_imbalance: all requested amounts are zero (nothing to withdraw).",
                )

            # Pool-balance bounds fail closed when reserves cannot be read.
            pool_balances = self._query_pool_balances_onchain(pool_info)
            for i, want in enumerate(amounts_wei):
                if want > pool_balances[i]:
                    return LiquidityResult(
                        success=False,
                        error=(
                            f"remove_liquidity_imbalance: requested {want} of coin {i} "
                            f"({pool_info.coins[i]}) exceeds pool balance {pool_balances[i]} for {pool_info.name}."
                        ),
                    )

            # Pad the quoted LP burn upward: this is a spend ceiling, not a min-out.
            # Legacy StableSwap quotes exclude imbalance fees, so zero slippage may
            # safely revert; NG quotes include the fee and can be exact.
            lp_burn_quote = self._query_calc_token_amount_withdraw_onchain(pool_info, amounts_wei)
            max_burn = int(lp_burn_quote * (10000 + slippage_bps) // 10000)

            # Never submit a non-positive or unbounded LP-burn ceiling.
            if max_burn <= 0:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_imbalance: computed max_burn is {max_burn} (must be > 0) "
                        f"(lp_burn_quote={lp_burn_quote}, slippage_bps={slippage_bps}). "
                        f"Refusing to ship a non-positive/unbounded ceiling (theft vector)."
                    ),
                )

            # A request needing more LP than the position holds fails before spending gas.
            lp_held_wei = int(lp_amount * Decimal(10**CURVE_LP_TOKEN_DECIMALS))
            if lp_burn_quote > lp_held_wei:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_imbalance: requested withdrawal needs ~{lp_burn_quote} LP wei but the "
                        f"position holds only {lp_held_wei} for {pool_info.name}; reduce the requested amounts."
                    ),
                )
            max_burn = min(max_burn, lp_held_wei)

            transactions: list[TransactionData] = []
            transactions.extend(self._build_approve_txs(pool_info.lp_token, pool_address, max_burn))

            remove_tx = self._build_remove_liquidity_imbalance_tx(
                pool_address=pool_address,
                amounts=amounts_wei,
                max_burn=max_burn,
                n_coins=pool_info.n_coins,
                pool_name=pool_info.name,
                is_ng=pool_info.is_ng,
            )
            transactions.append(remove_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            logger.info(
                "Built Curve remove_liquidity_imbalance: pool=%s, amounts=%s, max_burn=%s (quote=%s)",
                pool_info.name,
                amounts_wei,
                max_burn,
                lp_burn_quote,
            )

            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity_imbalance",
                amounts=amounts_wei,
                lp_amount=max_burn,
                gas_estimate=total_gas,
            )

        except Exception as e:
            logger.exception(f"Failed to build remove_liquidity_imbalance: {e}")
            return LiquidityResult(success=False, error=str(e))

    def swap_underlying(
        self,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
        price_ratio: Decimal | None = None,
        oracle_guard_bps: int | None = None,
        strict_oracle_guard: bool = False,
        oracle_prices_real: bool = True,
    ) -> SwapResult:
        """Build a metapool underlying swap via ``exchange_underlying``.

        Routes a swap across the COMBINED coin space of a metapool (index 0 =
        meta coin, 1..N = base-pool coins) — e.g. FRAX -> USDC through a
        FRAX/3CRV metapool. ``exchange_underlying`` lives on the metapool
        contract itself (NOT the zap); the metapool transparently routes the
        leg through its base pool.

        Stablecoin-only assumption: every coin on a 3CRV/FRAX-style metapool's
        combined space is a USD stable, so the 1:1 decimal-adjusted estimate
        (the same the StableSwap path uses) is the correct slippage floor, and
        the on-chain ``get_dy_underlying`` quote is preferred when a gateway /
        rpc is wired.
        """
        self.clear_planned_allowance_cache()
        try:
            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return SwapResult(success=False, error=f"Unknown pool: {pool_address}")
            if not pool_info.is_metapool:
                return SwapResult(
                    success=False,
                    error=f"swap_underlying requires a metapool; {pool_info.name} is not one",
                )

            i = pool_info.underlying_coin_index(token_in)
            j = pool_info.underlying_coin_index(token_out)
            if i is None or j is None:
                return SwapResult(
                    success=False,
                    error=(
                        f"Underlying swap {token_in}->{token_out} not on metapool "
                        f"{pool_info.name} combined coin space "
                        f"[{pool_info.coins[0]}]+{pool_info.base_pool_coins}"
                    ),
                )
            if i == j:
                return SwapResult(success=False, error="token_in and token_out resolve to the same coin")

            token_in_address = self._underlying_coin_address(pool_info, i)
            token_out_address = self._underlying_coin_address(pool_info, j)
            token_in_symbol = self._underlying_coin_symbol(pool_info, i)
            token_out_symbol = self._underlying_coin_symbol(pool_info, j)

            token_in_decimals = self._get_token_decimals(token_in_symbol)
            amount_in_wei = int(amount_in * Decimal(10**token_in_decimals))

            amount_out_estimate = self._estimate_underlying_swap_output(
                pool_info, i, j, amount_in_wei, token_in_symbol, token_out_symbol
            )
            amount_out_minimum = max(1, compute_min_amount_out_from_bps(amount_out_estimate, slippage_bps))
            token_out_decimals = self._get_token_decimals(token_out_symbol)

            # Stable metapool quotes fail closed when materially below the oracle.
            guard_error = self._swap_oracle_guard_error(
                pool_info=pool_info,
                token_in_symbol=token_in_symbol,
                token_out_symbol=token_out_symbol,
                amount_in=amount_in,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                strict_oracle_guard=strict_oracle_guard,
                oracle_prices_real=oracle_prices_real,
            )
            if guard_error is not None:
                return SwapResult(success=False, error=guard_error)

            # Anchor execution to the oracle while preserving a benign-drift residual.
            amount_out_minimum = self._oracle_anchored_min_out(
                pool_info=pool_info,
                pool_floor_wei=amount_out_minimum,
                pool_quoted_out_wei=amount_out_estimate,
                amount_in=amount_in,
                token_out_decimals=token_out_decimals,
                price_ratio=price_ratio,
                oracle_guard_bps=oracle_guard_bps,
                oracle_prices_real=oracle_prices_real,
                token_in_symbol=token_in_symbol,
                token_out_symbol=token_out_symbol,
            )

            transactions: list[TransactionData] = []
            transactions.extend(self._build_approve_txs(token_in_address, pool_address, amount_in_wei))

            # exchange_underlying(int128 i, int128 j, uint256 dx, uint256 min_dy)
            calldata = (
                EXCHANGE_UNDERLYING_SELECTOR
                + self._pad_int128(i)
                + self._pad_int128(j)
                + self._pad_uint256(amount_in_wei)
                + self._pad_uint256(amount_out_minimum)
            )
            transactions.append(
                TransactionData(
                    to=pool_address,
                    value=0,
                    data=calldata,
                    gas_estimate=self._resolve_gas(
                        to=pool_address,
                        data=calldata,
                        value=0,
                        static_gas=CURVE_GAS_ESTIMATES["exchange_underlying_metapool"],
                    ),
                    description=f"Curve metapool underlying swap {token_in_symbol} -> {token_out_symbol}",
                    tx_type="swap",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool underlying swap: %s(%d) -> %s(%d), pool=%s, amount_in=%s",
                token_in_symbol,
                i,
                token_out_symbol,
                j,
                pool_info.name,
                amount_in,
            )
            return SwapResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                amount_in=amount_in_wei,
                amount_out_minimum=amount_out_minimum,
                amount_out_estimate=amount_out_estimate,
                token_out_decimals=token_out_decimals,
                token_in=token_in_address,
                token_out=token_out_address,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build Curve metapool underlying swap: {e}")
            return SwapResult(success=False, error=str(e))

    def add_liquidity_underlying(
        self,
        pool_address: str,
        underlying_amounts: list[Decimal],
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a metapool deposit over the COMBINED coin space via the zap.

        ``underlying_amounts`` is indexed in COMBINED order: index 0 = meta coin,
        indices 1..N = base-pool coins (DAI/USDC/USDT). The generic 3CRV
        DepositZap's ABI takes the metapool as the first argument:
        ``add_liquidity(address _pool, uint256[N+1] _deposit, uint256 _min_mint)``.
        It deposits the base coins into the base pool (minting the base-LP), then
        the base-LP plus the meta coin into the metapool — a user only has to
        hold/approve the underlying coins.
        """
        self.clear_planned_allowance_cache()
        try:
            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(success=False, error=f"Unknown pool: {pool_address}")
            zap, combined_len = self._require_metapool_zap(pool_info)

            if len(underlying_amounts) != combined_len:
                return LiquidityResult(
                    success=False,
                    error=(
                        f"underlying_amounts has {len(underlying_amounts)} entries but metapool "
                        f"'{pool_info.name}' combined coin space has {combined_len} "
                        f"([{pool_info.coins[0]}]+{pool_info.base_pool_coins})"
                    ),
                )

            amounts_wei: list[int] = []
            for idx, amt in enumerate(underlying_amounts):
                decimals = self._get_token_decimals(self._underlying_coin_symbol(pool_info, idx))
                amounts_wei.append(int(amt * Decimal(10**decimals)))

            min_lp_tokens = self._estimate_add_liquidity_underlying(pool_info, zap, amounts_wei)
            min_lp_tokens = compute_min_amount_out_from_bps(min_lp_tokens, slippage_bps)

            transactions: list[TransactionData] = []
            for idx, amount_wei in enumerate(amounts_wei):
                if amount_wei > 0:
                    coin_addr = self._underlying_coin_address(pool_info, idx)
                    transactions.extend(self._build_approve_txs(coin_addr, zap, amount_wei))

            # add_liquidity(address _pool, uint256[4] _deposit_amounts, uint256 _min_mint_amount)
            calldata = ZAP_ADD_LIQUIDITY_4_SELECTOR + self._pad_address(pool_address)
            for amount in amounts_wei:
                calldata += self._pad_uint256(amount)
            calldata += self._pad_uint256(min_lp_tokens)
            transactions.append(
                TransactionData(
                    to=zap,
                    value=0,
                    data=calldata,
                    gas_estimate=self._resolve_gas(
                        to=zap,
                        data=calldata,
                        value=0,
                        static_gas=CURVE_GAS_ESTIMATES["metapool_zap_add_liquidity"],
                    ),
                    description=f"Add underlying liquidity to Curve metapool {pool_info.name} (zap)",
                    tx_type="add_liquidity",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool zap add_liquidity: pool=%s, underlying_amounts=%s, min_lp=%s",
                pool_info.name,
                underlying_amounts,
                min_lp_tokens,
            )
            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="add_liquidity_underlying",
                amounts=amounts_wei,
                lp_amount=min_lp_tokens,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build metapool add_liquidity_underlying: {e}")
            return LiquidityResult(success=False, error=str(e))

    def remove_liquidity_underlying(
        self,
        pool_address: str,
        lp_amount: Decimal,
        slippage_bps: int | None = None,
        recipient: str | None = None,
    ) -> LiquidityResult:
        """Build a metapool proportional withdrawal to underlying coins via the zap.

        Burns ``lp_amount`` metapool LP and returns the COMBINED underlying coins
        (meta coin + base-pool coins) using the generic zap's
        ``remove_liquidity(address _pool, uint256 _amount, uint256[N+1] _min_amounts)``.
        The min-amounts vector is derived from the metapool's native proportional
        split (meta coin + base-LP), then the base-LP leg is decomposed across the
        base pool's coins by its on-chain reserves. When the on-chain reads are
        unavailable, fails closed (no slippage floor) — mirrors the native
        ``remove_liquidity`` guard.
        """
        self.clear_planned_allowance_cache()
        try:
            slippage_bps = self.config.default_slippage_bps if slippage_bps is None else slippage_bps
            recipient = recipient or self.wallet_address

            pool_info = self.get_pool_info(pool_address)
            if not pool_info:
                return LiquidityResult(success=False, error=f"Unknown pool: {pool_address}")
            zap, combined_len = self._require_metapool_zap(pool_info)

            lp_amount_wei = int(lp_amount * Decimal(10**18))

            self._last_estimation_error = None
            min_amounts = self._estimate_remove_liquidity_underlying(pool_info, lp_amount_wei)
            min_amounts = [compute_min_amount_out_from_bps(a, slippage_bps) for a in min_amounts]

            if all(a == 0 for a in min_amounts):
                reason = self._last_estimation_error or "unknown"
                return LiquidityResult(
                    success=False,
                    error=(
                        f"remove_liquidity_underlying: cannot compute slippage protection "
                        f"(min_amounts are all zero). Cause: {reason}. "
                        f"Set CurveConfig.gateway_client for on-chain estimation."
                    ),
                )

            transactions: list[TransactionData] = []
            # The zap, not the pool, pulls the metapool LP token.
            transactions.extend(self._build_approve_txs(pool_info.lp_token, zap, lp_amount_wei))

            # remove_liquidity(address _pool, uint256 _amount, uint256[4] _min_amounts)
            calldata = (
                ZAP_REMOVE_LIQUIDITY_4_SELECTOR + self._pad_address(pool_address) + self._pad_uint256(lp_amount_wei)
            )
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)
            transactions.append(
                TransactionData(
                    to=zap,
                    value=0,
                    data=calldata,
                    gas_estimate=self._resolve_gas(
                        to=zap,
                        data=calldata,
                        value=0,
                        static_gas=CURVE_GAS_ESTIMATES["metapool_zap_remove_liquidity"],
                    ),
                    description=f"Remove underlying liquidity from Curve metapool {pool_info.name} (zap)",
                    tx_type="remove_liquidity",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            logger.info(
                "Built Curve metapool zap remove_liquidity: pool=%s, lp_amount=%s",
                pool_info.name,
                lp_amount,
            )
            return LiquidityResult(
                success=True,
                transactions=transactions,
                pool_address=pool_address,
                operation="remove_liquidity_underlying",
                amounts=min_amounts,
                lp_amount=lp_amount_wei,
                gas_estimate=total_gas,
            )
        except Exception as e:
            logger.exception(f"Failed to build metapool remove_liquidity_underlying: {e}")
            return LiquidityResult(success=False, error=str(e))

    @staticmethod
    def _require_metapool_zap(pool_info: PoolInfo) -> tuple[str, int]:
        """Return ``(zap_address, combined_coin_count)`` or raise for a non-zap metapool."""
        if not pool_info.is_metapool:
            raise ValueError(f"{pool_info.name} is not a metapool")
        if not pool_info.zap_address:
            raise ValueError(f"metapool {pool_info.name} has no zap_address configured")
        combined_len = 1 + len(pool_info.base_pool_coins or [])
        return pool_info.zap_address, combined_len

    def _underlying_coin_symbol(self, pool_info: PoolInfo, combined_index: int) -> str:
        """Symbol for a COMBINED-space index (0 = meta coin, 1..N = base coins)."""
        if combined_index == 0:
            return pool_info.coins[0]
        return (pool_info.base_pool_coins or [])[combined_index - 1]

    def _underlying_coin_address(self, pool_info: PoolInfo, combined_index: int) -> str:
        """Address for a COMBINED-space index (0 = meta coin, 1..N = base coins)."""
        if combined_index == 0:
            return pool_info.coin_addresses[0]
        return (pool_info.base_pool_coin_addresses or [])[combined_index - 1]

    def _estimate_underlying_swap_output(
        self,
        pool_info: PoolInfo,
        i: int,
        j: int,
        amount_in: int,
        token_in_symbol: str,
        token_out_symbol: str,
    ) -> int:
        """Estimate an underlying-swap output (prefer on-chain get_dy_underlying)."""
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = (
                    GET_DY_UNDERLYING_SELECTOR
                    + self._pad_int128(i)
                    + self._pad_int128(j)
                    + self._pad_uint256(amount_in)
                )
                amount_out = eth_call_uint256(
                    chain=self.chain,
                    to=pool_info.address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if amount_out is not None and amount_out > 0:
                    return amount_out
            except Exception as exc:  # noqa: BLE001 — fall back to the stable 1:1 estimate
                logger.warning(
                    "Curve metapool get_dy_underlying unavailable for %s (%s -> %s): %s; "
                    "falling back to decimal-adjusted stable estimate",
                    pool_info.name,
                    token_in_symbol,
                    token_out_symbol,
                    exc,
                )
        # Combined 3CRV/FRAX coin space is USD-denominated, so use a 1:1 fallback.
        in_decimals = self._get_token_decimals(token_in_symbol)
        out_decimals = self._get_token_decimals(token_out_symbol)
        decimal_diff = out_decimals - in_decimals
        if decimal_diff > 0:
            return amount_in * (10**decimal_diff)
        if decimal_diff < 0:
            return amount_in // (10 ** abs(decimal_diff))
        return amount_in

    def _estimate_add_liquidity_underlying(self, pool_info: PoolInfo, zap: str, amounts: list[int]) -> int:
        """Estimate metapool LP minted for a combined-space deposit via the zap.

        Prefers the zap's ``calc_token_amount(address,uint256[4],bool)`` on-chain
        quote; falls back to the deposit-sum / virtual_price stable estimate
        (the combined coins are all USD-denominated 1.0 stables).
        """
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = ZAP_CALC_TOKEN_AMOUNT_4_SELECTOR + self._pad_address(pool_info.address)
                for amount in amounts:
                    calldata += self._pad_uint256(amount)
                calldata += self._pad_uint256(1)  # is_deposit = True
                minted = eth_call_uint256(
                    chain=self.chain,
                    to=zap,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if minted is not None and minted > 0:
                    return minted
            except Exception as exc:  # noqa: BLE001 — fall back to naive estimate
                logger.warning(
                    "Curve metapool zap calc_token_amount unavailable for %s (%s); naive estimate",
                    pool_info.name,
                    exc,
                )
        total = 0
        for idx, amount in enumerate(amounts):
            decimals = self._get_token_decimals(self._underlying_coin_symbol(pool_info, idx))
            total += amount * (10 ** (18 - decimals))
        return int(Decimal(total) / pool_info.virtual_price)

    def _estimate_remove_liquidity_underlying(self, pool_info: PoolInfo, lp_amount: int) -> list[int]:
        """Estimate combined-space min amounts for a proportional metapool withdrawal.

        Splits the burned LP across the metapool's NATIVE coins (meta + base-LP)
        by on-chain reserves, then decomposes the base-LP leg across the base
        pool's underlying coins by ITS reserves — yielding the combined vector
        the zap returns: [meta, base_coin_0, base_coin_1, ...]. Returns all-zeros
        (fail closed) when on-chain reads are unavailable.
        """
        combined_len = 1 + len(pool_info.base_pool_coins or [])
        zero = [0] * combined_len
        if self._permission_discovery:
            # Funds-moving paths never use synthetic withdrawal floors.
            return self._discovery_min_amounts(pool_info, lp_amount, length=combined_len)
        if self._gateway_client is None and not self._rpc_url:
            self._last_estimation_error = "gateway_client or rpc_url not configured"
            return zero
        try:
            # Preserve combined ordering: meta coin first, then base-pool reserve order.
            native = self._query_proportional_amounts_onchain(pool_info, lp_amount)
            meta_amount = native[0]
            base_lp_amount = native[1]
            base_amounts = self._query_base_pool_underlying_amounts(pool_info, base_lp_amount)
            return [meta_amount, *base_amounts]
        except Exception as e:  # noqa: BLE001
            self._last_estimation_error = str(e)
            logger.warning(
                "remove_liquidity_underlying: on-chain estimation failed for %s: %s -- "
                "falling back to all-zeros (no slippage protection)",
                pool_info.name,
                e,
            )
            return zero

    def _query_base_pool_underlying_amounts(self, pool_info: PoolInfo, base_lp_amount: int) -> list[int]:
        """Proportional share of base-pool reserves for ``base_lp_amount`` base-LP tokens.

        Reuses the proportional-amounts query against the base pool (3pool) by
        building a transient PoolInfo for it: base_lp / base_pool.totalSupply()
        times each base reserve. The base LP token is the base pool's coins[1]
        (3CRV) on the metapool, i.e. ``coin_addresses[1]``.
        """
        base_pool_addr = pool_info.base_pool or ""
        base_addrs = pool_info.base_pool_coin_addresses or []
        # Metapool native coin 1 is the base LP token; incomplete metadata fails loudly.
        if not base_pool_addr or not base_addrs or len(pool_info.coin_addresses) < 2:
            raise ValueError(f"metapool {pool_info.name} missing base-pool metadata")
        base_lp_token = pool_info.coin_addresses[1]
        base_info = PoolInfo(
            address=base_pool_addr,
            lp_token=base_lp_token,
            coins=list(pool_info.base_pool_coins or []),
            coin_addresses=list(base_addrs),
            pool_type=PoolType.STABLESWAP,
            n_coins=len(base_addrs),
            name=f"{pool_info.name}:base_pool",
        )
        return self._query_proportional_amounts_onchain(base_info, base_lp_amount)

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
        use_underlying: bool = False,
    ) -> TransactionData:
        """Build exchange transaction.

        StableSwap:           exchange(int128 i, int128 j, uint256 dx, uint256 min_dy)
        CryptoSwap/Tricrypto: exchange(uint256 i, uint256 j, uint256 dx, uint256 min_dy)
        Aave-type (underlying): exchange_underlying(int128 i, int128 j, uint256 dx, uint256 min_dy)
        """
        if use_underlying:
            # Aave-style pools route underlying tokens with int128 indices.
            selector = EXCHANGE_UNDERLYING_SELECTOR
            pad_index = self._pad_int128
        elif pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            # CryptoSwap families use uint256 indices; StableSwap uses int128.
            selector = EXCHANGE_UINT256_SELECTOR
            pad_index = self._pad_uint256
        else:
            selector = EXCHANGE_SELECTOR
            pad_index = self._pad_int128

        calldata = (
            selector + pad_index(i) + pad_index(j) + self._pad_uint256(amount_in) + self._pad_uint256(min_amount_out)
        )

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=self._resolve_gas(
                to=pool_address,
                data=calldata,
                value=value,
                static_gas=CURVE_GAS_ESTIMATES["exchange_underlying" if use_underlying else "exchange"],
            ),
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
        is_ng: bool = False,
        is_cryptoswap: bool = False,
    ) -> TransactionData:
        """Build add_liquidity transaction.

        Legacy:        add_liquidity(uint256[N_COINS] amounts, uint256 min_mint_amount)
        StableSwap NG: add_liquidity(uint256[] amounts, uint256 min_mint_amount)

        ``is_cryptoswap`` selects the family-scoped static gas floor: only
        CryptoSwap/Tricrypto pools pay the ``tweak_price`` rebalance surcharge.
        """
        suffix = "_crypto" if is_cryptoswap else ""
        gas_estimate = CURVE_GAS_ESTIMATES.get(
            f"add_liquidity_{n_coins}{suffix}",
            CURVE_GAS_ESTIMATES.get(f"add_liquidity_{n_coins}", CURVE_GAS_ESTIMATES["add_liquidity_4"]),
        )

        if is_ng:
            # Dynamic array data follows the two-word ABI head at offset 0x40.
            calldata = ADD_LIQUIDITY_DYN_SELECTOR
            calldata += self._pad_uint256(0x40)
            calldata += self._pad_uint256(min_lp_tokens)
            calldata += self._pad_uint256(n_coins)
            for amount in amounts:
                calldata += self._pad_uint256(amount)
        else:
            if n_coins == 2:
                selector = ADD_LIQUIDITY_2_SELECTOR
            elif n_coins == 3:
                selector = ADD_LIQUIDITY_3_SELECTOR
            elif n_coins == 4:
                selector = ADD_LIQUIDITY_4_SELECTOR
            else:
                raise ValueError(f"Unsupported n_coins={n_coins} for add_liquidity (expected 2, 3, or 4)")

            calldata = selector
            for amount in amounts:
                calldata += self._pad_uint256(amount)
            calldata += self._pad_uint256(min_lp_tokens)

        return TransactionData(
            to=pool_address,
            value=value,
            data=calldata,
            gas_estimate=self._resolve_gas(to=pool_address, data=calldata, value=value, static_gas=gas_estimate),
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
        is_ng: bool = False,
    ) -> TransactionData:
        """Build remove_liquidity transaction.

        Legacy:        remove_liquidity(uint256 _amount, uint256[N_COINS] min_amounts)
        StableSwap NG: remove_liquidity(uint256 _amount, uint256[] min_amounts)
        """
        if is_ng:
            # Dynamic array data follows the two-word ABI head at offset 0x40.
            calldata = REMOVE_LIQUIDITY_DYN_SELECTOR
            calldata += self._pad_uint256(lp_amount)
            calldata += self._pad_uint256(0x40)
            calldata += self._pad_uint256(n_coins)
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)
        else:
            if n_coins == 2:
                selector = REMOVE_LIQUIDITY_2_SELECTOR
            elif n_coins == 3:
                selector = REMOVE_LIQUIDITY_3_SELECTOR
            elif n_coins == 4:
                selector = REMOVE_LIQUIDITY_4_SELECTOR
            else:
                raise ValueError(f"Unsupported n_coins={n_coins} for remove_liquidity (expected 2, 3, or 4)")

            calldata = selector + self._pad_uint256(lp_amount)
            for min_amount in min_amounts:
                calldata += self._pad_uint256(min_amount)

        # Proportional removal returns all N coins, so the gas floor scales with N.
        static_gas = CURVE_GAS_ESTIMATES.get(f"remove_liquidity_{n_coins}", CURVE_GAS_ESTIMATES["remove_liquidity_4"])
        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=self._resolve_gas(to=pool_address, data=calldata, value=0, static_gas=static_gas),
            description=f"Remove liquidity from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_remove_liquidity_imbalance_tx(
        self,
        pool_address: str,
        amounts: list[int],
        max_burn: int,
        n_coins: int,
        pool_name: str = "",
        is_ng: bool = False,
    ) -> TransactionData:
        """Build remove_liquidity_imbalance transaction (VIB-5438).

        Legacy:        remove_liquidity_imbalance(uint256[N] amounts, uint256 max_burn_amount)
        StableSwap NG: remove_liquidity_imbalance(uint256[] amounts, uint256 max_burn_amount)

        The ``amounts`` vector is the EXACT per-coin withdrawal target (positional
        by pool-coin index); ``max_burn_amount`` is the LP-burn ceiling. StableSwap
        family only — the caller guards against CryptoSwap/Tricrypto pools.
        """
        if is_ng:
            # Dynamic array data follows the two-word ABI head at offset 0x40.
            calldata = REMOVE_LIQUIDITY_IMBALANCE_DYN_SELECTOR
            calldata += self._pad_uint256(0x40)
            calldata += self._pad_uint256(max_burn)
            calldata += self._pad_uint256(n_coins)
            for amount in amounts:
                calldata += self._pad_uint256(amount)
        else:
            selector = REMOVE_LIQUIDITY_IMBALANCE_SELECTORS.get(n_coins)
            if selector is None:
                raise ValueError(f"Unsupported n_coins={n_coins} for remove_liquidity_imbalance (expected 2, 3, or 4)")
            calldata = selector
            for amount in amounts:
                calldata += self._pad_uint256(amount)
            calldata += self._pad_uint256(max_burn)

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=self._resolve_gas(
                to=pool_address,
                data=calldata,
                value=0,
                static_gas=CURVE_GAS_ESTIMATES["remove_liquidity_imbalance"],
            ),
            description=f"Remove liquidity (imbalanced) from Curve {pool_name}",
            tx_type="remove_liquidity_imbalance",
        )

    def _build_remove_liquidity_one_tx(
        self,
        pool_address: str,
        lp_amount: int,
        coin_index: int,
        min_amount: int,
        coin_symbol: str = "",
        pool_name: str = "",
        is_cryptoswap: bool = False,
    ) -> TransactionData:
        """Build remove_liquidity_one_coin transaction.

        StableSwap: remove_liquidity_one_coin(uint256 _token_amount, int128 i, uint256 _min_amount)
        CryptoSwap: remove_liquidity_one_coin(uint256 _token_amount, uint256 i, uint256 _min_amount)

        The coin index is non-negative, so ``int128`` and ``uint256`` pad to the
        same bytes — only the 4-byte selector differs between the two families
        (VIB-5437, verified on-chain 2026-06-29).
        """
        selector = REMOVE_LIQUIDITY_ONE_CRYPTO_SELECTOR if is_cryptoswap else REMOVE_LIQUIDITY_ONE_SELECTOR
        calldata = (
            selector + self._pad_uint256(lp_amount) + self._pad_int128(coin_index) + self._pad_uint256(min_amount)
        )

        return TransactionData(
            to=pool_address,
            value=0,
            data=calldata,
            gas_estimate=self._resolve_gas(
                to=pool_address,
                data=calldata,
                value=0,
                # Only CryptoSwap families pay the price-scale rebalance surcharge.
                static_gas=CURVE_GAS_ESTIMATES[
                    "remove_liquidity_one_coin_crypto" if is_cryptoswap else "remove_liquidity_one_coin"
                ],
            ),
            description=f"Remove {coin_symbol} from Curve {pool_name}",
            tx_type="remove_liquidity",
        )

    def _build_approve_txs(
        self,
        token_address: str,
        spender: str,
        amount: int,
    ) -> list[TransactionData]:
        """Build the ERC-20 approve transaction(s) needed to spend ``amount`` (VIB-5442).

        Returns an empty list when the current allowance already covers ``amount``,
        a single ``approve(MAX)`` when there is no existing allowance, or a
        ``approve(0)`` + ``approve(MAX)`` pair when an existing NON-ZERO allowance
        must be changed — USDT-class tokens revert on a non-zero → non-zero
        ``approve`` (``require(value == 0 || allowance == 0)``), which silently
        kills the whole bundle. The current allowance is **seeded from on-chain
        ``allowance()``** (not assumed 0), so a token already approved in a prior
        run is not needlessly (and, for USDT, revertingly) re-approved.

        Args:
            token_address: Token to approve
            spender: Address to approve
            amount: Amount that must be spendable

        Returns:
            Zero, one, or two ``TransactionData`` (reset + approve) in order.
        """
        current = self._current_allowance(token_address, spender)
        # Unknown allowances fail safe through reset-before-approve ordering.
        txs = build_approval_sequence(
            amount=amount,
            current_allowance=current,
            reset_before_change=True,
            approval_amount=MAX_UINT256,
            build_reset_tx=lambda: self._single_approve_tx(token_address, spender, 0),
            build_approve_tx=lambda value: self._single_approve_tx(token_address, spender, value),
        )
        if not txs:
            logger.debug("Sufficient allowance for %s (%d >= %d)", token_address, current, amount)
            return []
        self._allowance_cache.record_planned(token_address, spender, MAX_UINT256)
        return txs

    def _current_allowance(self, token_address: str, spender: str) -> int | None:
        """Return the current allowance, or ``None`` when it cannot be confirmed.

        Returns the cached value when present (set by a prior approve in this bundle
        or by ``set_allowance`` in tests). Otherwise queries ``allowance(wallet,
        spender)`` via the gateway / RPC and returns the on-chain value. Returns
        ``None`` whenever the allowance cannot be **positively confirmed** — the
        read failed (RPC error / no result) OR no transport is configured to read
        with — so the caller fails toward a safe reset rather than assuming zero
        and emitting a lone ``approve(MAX)`` that could revert on a USDT-class token.
        """
        cached = self._allowance_cache.get(token_address, spender)
        if cached is not None:
            return cached
        if self._gateway_client is not None or self._rpc_url:
            try:
                calldata = encode_allowance(self.wallet_address, spender)
                onchain = eth_call_uint256(
                    chain=self.chain,
                    to=token_address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=self._gateway_client,
                    timeout=10.0,
                )
                if onchain is not None:
                    self._allowance_cache.record_confirmed(token_address, spender, onchain)
                    return onchain
            except Exception as exc:  # noqa: BLE001 — unknown allowance on any read failure
                logger.debug("On-chain allowance read failed for %s: %s; treating as unknown", token_address, exc)
        return None  # Unknown allowance requires reset-before-approve.

    def _single_approve_tx(self, token_address: str, spender: str, value: int) -> TransactionData:
        """Build one ERC-20 ``approve(spender, value)`` transaction."""
        calldata = encode_approve(spender, value)
        token_symbol = self._get_token_symbol(token_address)
        action = "Reset approval for" if value == 0 else "Approve"
        return TransactionData(
            to=token_address,
            value=0,
            data=calldata,
            gas_estimate=CURVE_GAS_ESTIMATES["approve"],
            description=f"{action} {token_symbol} for Curve",
            tx_type="approve",
        )

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
        in_decimals = self._coin_decimals(pool_info, i)
        out_decimals = self._coin_decimals(pool_info, j)
        decimal_diff = out_decimals - in_decimals

        if pool_info.pool_type == PoolType.STABLESWAP:
            # StableSwap fallback assumes parity and adjusts only for token decimals.
            if decimal_diff > 0:
                return amount_in * (10**decimal_diff)
            elif decimal_diff < 0:
                return amount_in // (10 ** abs(decimal_diff))
            return amount_in

        if price_ratio is not None:
            # Apply the oracle token-price ratio across raw token decimal scales.
            estimate = Decimal(amount_in) * price_ratio
            if decimal_diff > 0:
                estimate = estimate * Decimal(10**decimal_diff)
            elif decimal_diff < 0:
                estimate = estimate / Decimal(10 ** abs(decimal_diff))
            return int(estimate)

        # Decimal-only estimation is unsafe for volatile assets; missing oracle data fails closed.
        raise ValueError(
            f"CryptoSwap pool {pool_info.name} ({pool_info.coins[i]} -> {pool_info.coins[j]}): "
            "price_ratio is required for accurate slippage protection but was not provided. "
            "Ensure price oracle data is available for both tokens before swapping volatile pairs."
        )

    def quote_swap_output(
        self,
        *,
        pool_address: str,
        token_in: str,
        token_out: str,
        amount_in_wei: int,
    ) -> int:
        """Quote a Curve exact-input swap with the pool's on-chain quote method."""
        if self._gateway_client is None and not self._rpc_url:
            raise ValueError("Curve on-chain swap quote requires either a gateway client or rpc_url")
        if amount_in_wei <= 0:
            raise ValueError(f"amount_in_wei must be positive, got {amount_in_wei}")

        # get_dy needs live coin order but not the full ABI/valuation refresh.
        pool_info = self.get_pool_info(pool_address, refresh=False)
        if not pool_info:
            raise ValueError(f"Unknown Curve pool: {pool_address}")
        pool_info = self._ensure_live_coin_order(pool_info)
        i = pool_info.get_coin_index(token_in)
        j = pool_info.get_coin_index(token_out)
        return self._query_swap_output_onchain(pool_info, i, j, amount_in_wei)

    def _ensure_live_coin_order(self, pool_info: PoolInfo) -> PoolInfo:
        """Refresh ONLY the live coin order for an accurate standalone ``get_dy`` quote.

        The read-only quote path opts out of the full reconcile (``is_ng`` /
        ``virtual_price`` / decimals are irrelevant to a ``get_dy`` quote), but
        ``get_coin_index`` still needs the live coin ORDER to pick the right
        ``(i, j)`` before quoting. This issues the minimal coins-only read (via
        ``eth_call`` — no ``eth_call_uint256``, so the lean single-quote contract
        holds) only when the pool was resolved cold; a warm refresh-cache entry
        already carries live coins and is returned untouched, and ``use_underlying``
        pools keep their static underlying set (chain ``coins()`` are internal
        aTokens). Any read failure leaves the static order in place (fail-safe).
        """
        if pool_info.use_underlying:
            return pool_info
        if self._pool_refresh_cache.get(pool_info.address.lower()) is not None:
            return pool_info
        if not self._has_read_transport():
            return pool_info
        live_addresses = self._read_pool_coins(pool_info)
        if live_addresses is None:
            return pool_info
        return replace(
            pool_info,
            coin_addresses=list(live_addresses),
            coins=self._realign_coin_symbols(pool_info, live_addresses),
        )

    def _query_swap_output_onchain(self, pool_info: PoolInfo, i: int, j: int, amount_in: int) -> int:
        """Query Curve get_dy for a swap output quote."""
        if pool_info.use_underlying:
            selector = GET_DY_UNDERLYING_SELECTOR
            pad_index = self._pad_int128
        elif pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            selector = GET_DY_UINT256_SELECTOR
            pad_index = self._pad_uint256
        else:
            selector = GET_DY_SELECTOR
            pad_index = self._pad_int128

        calldata = selector + pad_index(i) + pad_index(j) + self._pad_uint256(amount_in)
        amount_out = eth_call_uint256(
            chain=self.chain,
            to=pool_info.address,
            data=calldata,
            rpc_url=self._rpc_url,
            gateway_client=self._gateway_client,
            timeout=10.0,
        )
        if amount_out is None:
            raise ValueError(f"Curve get_dy returned no result for {pool_info.name}")
        if amount_out <= 0:
            raise ValueError(f"Curve get_dy returned non-positive amount_out for {pool_info.name}: {amount_out}")
        return amount_out

    def _estimate_add_liquidity(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Estimate LP tokens from add_liquidity.

        For StableSwap pools: divides total deposit by virtual_price.
        Mature pools have virtual_price > 1.0 because fees increase LP token value.
        The sum/virtual_price formula works for stablecoin pools because deposit
        value is proportional to LP supply.

        For StableSwap NG pools (``pool_info.is_ng``): if a gateway client or
        RPC URL is configured, query ``calc_token_amount(uint256[], bool)``
        on-chain for an accurate quote. The naive sum/virtual_price estimate
        is too tight for NG pools because their imbalance-fee model means the
        actual minted amount is meaningfully below the deposit value on small
        single-asset-heavy deposits, and the configured ``virtual_price``
        drifts as fees accrue (VIB-4836).

        For CryptoSwap/Tricrypto pools (VIB-5441 / audit P1-7): query
        ``calc_token_amount`` on-chain and return the real LP-mint quote. A
        volatile-asset pool tracks LP tokens as a share of the D-invariant (it
        depends on reserves, A, gamma, and current prices), so there is no safe
        static estimate — the previous behaviour returned ``min_lp=0`` (accept any
        output), an MEV/sandwich theft vector. This method now **fails closed**:
        with no gateway/rpc, or if the on-chain quote cannot be obtained, it
        raises rather than returning 0, so ``add_liquidity`` rejects the deposit
        instead of shipping an unprotected ``min_lp=0``.
        """
        if pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO):
            if self._permission_discovery:
                # Funds-moving paths never use synthetic LP floors.
                return self._synthetic_quote_scale(sum(amounts))
            if self._gateway_client is None and not self._rpc_url:
                raise ValueError(
                    f"CryptoSwap/Tricrypto pool {pool_info.name}: cannot compute min_lp without a "
                    "gateway client or rpc_url; refusing to ship min_lp=0 (MEV theft vector). "
                    "Configure CurveConfig.gateway_client to enable the on-chain calc_token_amount quote."
                )
            # Query failures propagate so add_liquidity fails closed.
            return self._query_calc_token_amount_crypto_onchain(pool_info, amounts)

        if pool_info.is_ng and (self._gateway_client is not None or self._rpc_url):
            try:
                return self._query_calc_token_amount_ng_onchain(pool_info, amounts)
            except Exception as exc:  # noqa: BLE001 — fall back to naive estimate
                logger.warning(
                    "StableSwap NG calc_token_amount query failed for pool %s (%s); falling back to naive estimate",
                    pool_info.name,
                    exc,
                )

        total = 0
        for i, amount in enumerate(amounts):
            decimals = self._coin_decimals(pool_info, i)
            # Normalize deposits to 18-decimal LP-value units.
            normalized = amount * (10 ** (18 - decimals))
            total += normalized

        total = int(Decimal(total) / pool_info.virtual_price)

        return total

    def _query_calc_token_amount_crypto_onchain(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Query ``calc_token_amount`` on a CryptoSwap/Tricrypto pool (VIB-5441).

        Returns the real LP-mint quote so ``_estimate_add_liquidity`` can derive a
        non-zero ``min_lp`` for a volatile deposit. The deposit array is a fixed
        ``uint256[N]`` encoded inline; we probe the bool-carrying selector first
        then the deposit-only one (see ``CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS``),
        using whichever returns a positive quote. Raises when no selector yields a
        quote so the caller fails closed (never ships ``min_lp=0``).
        """
        selectors = CRYPTO_CALC_TOKEN_AMOUNT_SELECTORS.get(pool_info.n_coins)
        if selectors is None:
            raise ValueError(
                f"No CryptoSwap calc_token_amount selector for {pool_info.n_coins}-coin pool {pool_info.name}"
            )
        # A disconnected gateway falls back to rpc_url or fails closed before eth_call.
        gateway_client = self._gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            if not self._rpc_url:
                raise ValueError(
                    f"CryptoSwap/Tricrypto pool {pool_info.name}: gateway client present but "
                    "disconnected and no rpc_url; cannot compute min_lp, refusing to ship min_lp=0."
                )
            gateway_client = None
        inline_amounts = "".join(self._pad_uint256(a) for a in amounts)
        last_error: Exception | None = None
        for selector, has_deposit_flag in ((selectors[0], True), (selectors[1], False)):
            calldata = selector + inline_amounts + (self._pad_uint256(1) if has_deposit_flag else "")
            try:
                minted = eth_call_uint256(
                    chain=self.chain,
                    to=pool_info.address,
                    data=calldata,
                    rpc_url=self._rpc_url,
                    gateway_client=gateway_client,
                    timeout=10.0,
                )
            except Exception as exc:  # noqa: BLE001 — wrong selector may revert; try the next
                last_error = exc
                continue
            if minted is not None and minted > 0:
                return minted
        raise ValueError(
            f"CryptoSwap calc_token_amount returned no quote for {pool_info.name} "
            f"({pool_info.n_coins}-coin); cannot derive min_lp (last error: {last_error})"
        )

    def _resolve_onchain_gateway_client(self, pool_info: PoolInfo, what: str) -> "GatewayClient | None":
        """Return the gateway client to use for an on-chain read, fail-closed (VIB-5438).

        Mirrors the connect/transport handling in
        ``_query_calc_token_amount_crypto_onchain``: raises when neither a gateway
        client nor an rpc_url is available, and drops a present-but-disconnected
        gateway to rpc_url (else refuses). ``what`` names the read for the error.
        """
        if self._gateway_client is None and not self._rpc_url:
            raise ValueError(
                f"Curve pool {pool_info.name}: cannot compute {what} without a gateway client or rpc_url; "
                "refusing to ship an unbounded max_burn (theft vector). "
                "Configure CurveConfig.gateway_client to enable the on-chain quote."
            )
        gateway_client = self._gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            if not self._rpc_url:
                raise ValueError(
                    f"Curve pool {pool_info.name}: gateway client present but disconnected and no rpc_url; "
                    f"cannot compute {what}, refusing to ship an unbounded max_burn."
                )
            return None
        return gateway_client

    def _query_calc_token_amount_withdraw_onchain(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Quote the LP that an imbalanced withdrawal would BURN (VIB-5438).

        Calls ``calc_token_amount(amounts, is_deposit=False)`` on a StableSwap pool
        and returns the LP-burn estimate that seeds the max-burn ceiling. NG pools
        speak the dynamic-array ABI (``calc_token_amount(uint256[],bool)``); legacy
        pools speak the fixed-array ABI keyed by coin count. Verified on-chain
        2026-06-29 against 3pool (fixed-array, 0x3883e119).

        Fail-closed, exactly like ``_query_calc_token_amount_crypto_onchain``: with
        no gateway/rpc, or if no selector yields a positive quote, it RAISES rather
        than returning 0 (never ships an unbounded/zero ``max_burn``).
        """
        if self._permission_discovery:
            # Funds-moving paths never use synthetic max-burn quotes.
            return self._synthetic_quote_scale(sum(amounts))
        gateway_client = self._resolve_onchain_gateway_client(pool_info, "imbalanced max_burn")

        if pool_info.is_ng:
            # Dynamic array data follows the two-word ABI head at offset 0x40.
            calldata = (
                NG_CALC_TOKEN_AMOUNT_SELECTOR
                + self._pad_uint256(0x40)
                + self._pad_uint256(0)
                + self._pad_uint256(len(amounts))
                + "".join(self._pad_uint256(a) for a in amounts)
            )
        else:
            selector = STABLE_CALC_TOKEN_AMOUNT_SELECTORS.get(pool_info.n_coins)
            if selector is None:
                raise ValueError(
                    f"No StableSwap calc_token_amount selector for {pool_info.n_coins}-coin pool {pool_info.name}"
                )
            calldata = selector + "".join(self._pad_uint256(a) for a in amounts) + self._pad_uint256(0)

        burned = eth_call_uint256(
            chain=self.chain,
            to=pool_info.address,
            data=calldata,
            rpc_url=self._rpc_url,
            gateway_client=gateway_client,
            timeout=10.0,
        )
        if burned is None or burned <= 0:
            raise ValueError(
                f"Curve calc_token_amount(is_deposit=False) returned no quote for {pool_info.name}; "
                f"cannot derive imbalanced max_burn (got {burned})"
            )
        return burned

    def _query_pool_balances_onchain(self, pool_info: PoolInfo) -> list[int]:
        """Read each coin's on-chain ``balances(i)`` reserve, fail-closed (VIB-5438).

        Used to reject an imbalanced withdrawal that requests more of a coin than
        the pool holds. Probes the ``uint256`` selector first then the legacy
        ``int128`` form (3pool-era pools). Raises if any coin cannot be read so the
        caller fails closed rather than skipping the bound check.
        """
        if self._permission_discovery:
            # Synthetic reserves exist only to extract permission selectors.
            return [self._synthetic_quote_scale(0, floor=10**24)] * pool_info.n_coins
        gateway_client = self._resolve_onchain_gateway_client(pool_info, "pool balances")
        balances: list[int] = []
        for i in range(pool_info.n_coins):
            arg = self._pad_uint256(i)
            value: int | None = None
            for selector in (BALANCES_UINT256_SELECTOR, BALANCES_INT128_SELECTOR):
                try:
                    value = eth_call_uint256(
                        chain=self.chain,
                        to=pool_info.address,
                        data=selector + arg,
                        rpc_url=self._rpc_url,
                        gateway_client=gateway_client,
                        timeout=10.0,
                    )
                except Exception:  # noqa: BLE001 — wrong selector may revert; try the next
                    value = None
                    continue
                if value is not None:
                    break
            if value is None:
                raise ValueError(
                    f"Curve pool {pool_info.name}: could not read balances({i}); "
                    "cannot bound imbalanced withdrawal against pool reserves."
                )
            balances.append(value)
        return balances

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
        if self._permission_discovery:
            # Funds-moving paths never use synthetic withdrawal floors.
            return self._discovery_min_amounts(pool_info, lp_amount)
        if self._gateway_client is None and not self._rpc_url:
            logger.warning(
                f"remove_liquidity: no gateway_client or rpc_url configured for {pool_info.name} -- "
                "min_amounts will be [0, ..., 0] (no slippage protection). "
                "Set CurveConfig.gateway_client to enable on-chain estimation."
            )
            self._last_estimation_error = "gateway_client or rpc_url not configured"
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
        import json as _json

        TOTAL_SUPPLY_SELECTOR = "18160ddd"  # totalSupply() -> uint256
        BALANCES_UINT256_SELECTOR = "4903b0d1"  # balances(uint256) -> uint256 (factory/newer pools)
        BALANCES_INT128_SELECTOR = "065a80d8"  # balances(int128) -> uint256 (old Vyper pools, e.g. 3pool)

        def _encode_uint256_arg(value: int) -> str:
            """Encode a single uint256 argument (32 bytes, no 0x prefix)."""
            return hex(value)[2:].zfill(64)

        def _eth_call(to: str, data: str) -> int:
            """Make a synchronous eth_call and return the result as int.

            Routes through the gateway when gateway_client is configured.
            Falls back to direct httpx POST only for ad-hoc script usage.
            """
            if self._gateway_client is not None:
                from almanak.gateway.proto import gateway_pb2

                rpc_request = gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_call",
                    params=_json.dumps([{"to": to, "data": data}, "latest"]),
                    id="curve_remove_liquidity",
                )
                response = self._gateway_client.rpc.Call(rpc_request, timeout=10.0)
                if not response.success:
                    raise ValueError(f"eth_call error: {response.error or 'gateway returned failure'}")
                hex_result = _json.loads(response.result) if response.result else "0x"
                if not hex_result or hex_result == "0x":
                    raise ValueError("eth_call returned empty result")
                return self._decode_first_uint256_word(hex_result)

            # Direct RPC remains only for deprecated ad-hoc adapter usage.
            import httpx

            assert self._rpc_url is not None
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
                "id": 1,
            }
            response = httpx.post(
                self._rpc_url, json=payload, timeout=10.0
            )  # vib-2986-exempt: gateway-internal fallback
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                raise ValueError(f"eth_call error: {result['error'].get('message', result['error'])}")
            hex_result = result.get("result", "0x0")
            if not hex_result or hex_result == "0x":
                raise ValueError("eth_call returned empty result")
            return self._decode_first_uint256_word(hex_result)

        total_supply = _eth_call(
            pool_info.lp_token,
            f"0x{TOTAL_SUPPLY_SELECTOR}",
        )
        if total_supply == 0:
            raise ValueError(f"LP totalSupply is zero for pool {pool_info.name}")

        # Probe uint256 then legacy int128 balances and reuse the successful selector.
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
                    balances_selector = BALANCES_INT128_SELECTOR
                    balance_raw = _eth_call(
                        pool_info.address,
                        f"0x{balances_selector}{_encode_uint256_arg(i)}",
                    )
                else:
                    raise
            expected = balance_raw * lp_amount // total_supply
            amounts.append(expected)

        logger.debug(
            f"remove_liquidity on-chain estimate for {pool_info.name}: "
            f"lp={lp_amount}, total_supply={total_supply}, amounts={amounts}"
        )
        return amounts

    def _query_calc_token_amount_ng_onchain(self, pool_info: PoolInfo, amounts: list[int]) -> int:
        """Query ``calc_token_amount(uint256[], bool)`` on a StableSwap NG pool.

        Returns the exact LP-token mint quote the pool would produce for these
        deposit amounts. Used by ``_estimate_add_liquidity`` for NG pools so
        slippage protection is computed against real pool math rather than the
        naive sum/virtual_price estimator (which drifts as fees accrue and
        ignores imbalance fees). VIB-4836.

        Routing mirrors the existing ``_eth_call`` helper used by
        ``_estimate_remove_liquidity_proportional``: gateway-first when a
        ``GatewayRpcClient`` is wired, falling back to ``rpc_url`` for
        intent-test / ad-hoc adapter constructions that don't go through the
        gateway. The httpx fallback carries the same ``vib-2986-exempt`` marker
        as the rest of this connector's gateway-internal RPC paths.
        """
        import json as _json

        # Dynamic amounts follow the two-word head at offset 0x40.
        calldata = "0x3db06dd8"
        calldata += self._pad_uint256(0x40)
        calldata += self._pad_uint256(1)
        calldata += self._pad_uint256(pool_info.n_coins)
        for amount in amounts:
            calldata += self._pad_uint256(amount)

        if self._gateway_client is not None:
            from almanak.gateway.proto import gateway_pb2

            rpc_request = gateway_pb2.RpcRequest(
                chain=self.chain,
                method="eth_call",
                params=_json.dumps([{"to": pool_info.address, "data": calldata}, "latest"]),
                id="curve_add_liquidity_ng",
            )
            response = self._gateway_client.rpc.Call(rpc_request, timeout=10.0)
            if not response.success:
                raise ValueError(f"calc_token_amount eth_call failed: {response.error or 'gateway failure'}")
            hex_result = _json.loads(response.result) if response.result else "0x"
        else:
            import httpx

            assert self._rpc_url is not None, "calc_token_amount on-chain requires either a gateway client or rpc_url"
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": pool_info.address, "data": calldata}, "latest"],
                "id": 1,
            }
            response = httpx.post(
                self._rpc_url, json=payload, timeout=10.0
            )  # vib-2986-exempt: gateway-internal fallback
            response.raise_for_status()
            result = response.json()
            if "error" in result:
                raise ValueError(f"calc_token_amount eth_call failed: {result['error']}")
            hex_result = result.get("result", "0x0")

        if not hex_result or hex_result == "0x":
            raise ValueError("calc_token_amount returned empty result")
        return self._decode_first_uint256_word(hex_result)

    @staticmethod
    def _decode_first_uint256_word(hex_result: str) -> int:
        """Decode the FIRST 32-byte word of an ``eth_call`` hex response.

        A single-``uint256`` return is the first 32-byte (64 hex char) word.
        Some Vyper pools — notably factory METAPOOLS — return extra trailing
        words for getters like ``balances(uint256)``, so decoding the whole
        response with ``int(hex_result, 16)`` builds a multi-thousand-digit
        integer that trips Python's ``int``-string-conversion guard (VIB-5419).
        Slicing the first word is correct for every single-value getter and a
        no-op for the legacy flat pools that already return exactly 32 bytes.
        """
        body = hex_result[2:] if hex_result.startswith("0x") else hex_result
        if len(body) < 64:
            # Accept short edge responses by decoding all available bytes.
            return int(body or "0", 16)
        return int(body[:64], 16)

    @staticmethod
    def _is_cryptoswap(pool_info: PoolInfo) -> bool:
        """True for volatile-asset pool families (CryptoSwap / Tricrypto).

        These encode the coin index as ``uint256`` (not ``int128``), so their
        single-sided ``calc_withdraw_one_coin`` / ``remove_liquidity_one_coin``
        selectors differ from the StableSwap forms.
        """
        return pool_info.pool_type in (PoolType.CRYPTOSWAP, PoolType.TRICRYPTO)

    def _query_calc_withdraw_one_coin_onchain(
        self, pool_info: PoolInfo, lp_amount: int, coin_index: int
    ) -> tuple[int, bool]:
        """Query ``calc_withdraw_one_coin`` on-chain for single-sided min-out (VIB-5437).

        Returns ``(expected_out, used_cryptoswap)`` — the pool's exact expected
        output (in the target coin's native decimals) for burning ``lp_amount`` LP
        wei to ``coin_index``, plus whether the CryptoSwap-family selector is the
        one that actually answered. Mirrors the fail-closed shape of
        ``_query_calc_token_amount_crypto_onchain``: with no gateway/rpc, or if
        neither selector yields a positive quote, it RAISES rather than returning 0
        (never ships an unprotected ``min_amount=0``).

        StableSwap pools expose ``calc_withdraw_one_coin(uint256,int128)`` while
        CryptoSwap/Tricrypto pools expose ``calc_withdraw_one_coin(uint256,uint256)``
        (verified on-chain 2026-06-29). The coin index is non-negative, so the
        padded argument bytes are identical for both ABI types — only the 4-byte
        selector differs. We dispatch by pool family and fall back to the other
        selector defensively. Returning which family answered lets the caller build
        the matching ``remove_liquidity_one_coin`` selector, so a pool mislabelled
        in the static config does not get a valid quote paired with a wrong-family
        remove tx that would revert on execution.
        """
        if self._permission_discovery:
            # Preserve pool-family dispatch while extracting permission selectors.
            return self._synthetic_quote_scale(lp_amount), self._is_cryptoswap(pool_info)
        if self._gateway_client is None and not self._rpc_url:
            raise ValueError(
                f"Curve pool {pool_info.name}: cannot compute single-sided min-out without a "
                "gateway client or rpc_url; refusing to ship min_amount=0 (MEV theft vector). "
                "Configure CurveConfig.gateway_client to enable the on-chain calc_withdraw_one_coin quote."
            )
        # A disconnected gateway falls back to rpc_url or fails closed before eth_call.
        gateway_client = self._gateway_client
        if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
            if not self._rpc_url:
                raise ValueError(
                    f"Curve pool {pool_info.name}: gateway client present but disconnected and no "
                    "rpc_url; cannot compute single-sided min-out, refusing to ship min_amount=0."
                )
            gateway_client = None

        if self._is_cryptoswap(pool_info):
            selectors = (CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR, CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR)
        else:
            selectors = (CALC_WITHDRAW_ONE_COIN_STABLE_SELECTOR, CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR)
        # int128(i>=0) and uint256(i) pad to identical bytes; only the selector differs.
        args = self._pad_uint256(lp_amount) + self._pad_uint256(coin_index)
        last_error: Exception | None = None
        for selector in selectors:
            try:
                expected = eth_call_uint256(
                    chain=self.chain,
                    to=pool_info.address,
                    data=selector + args,
                    rpc_url=self._rpc_url,
                    gateway_client=gateway_client,
                    timeout=10.0,
                )
            except Exception as exc:  # noqa: BLE001 — wrong selector may revert; try the next
                last_error = exc
                continue
            if expected is not None and expected > 0:
                return expected, selector == CALC_WITHDRAW_ONE_COIN_CRYPTO_SELECTOR
        raise ValueError(
            f"Curve calc_withdraw_one_coin returned no quote for {pool_info.name} "
            f"(coin {coin_index}); cannot derive single-sided min-out (last error: {last_error})"
        )

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

        Uses skip_gateway=True to avoid 30-second gateway timeouts for
        LP pool addresses that are valid ERC-20s but not in the static registry.
        """
        if not address.startswith("0x"):
            return address
        try:
            resolved = self._token_resolver.resolve(address, self.chain, skip_gateway=True, log_errors=False)
            return resolved.symbol
        except TokenResolutionError:
            logger.debug(f"Cannot resolve symbol for {address}, using truncated address")
            return f"{address[:10]}..."

    def _coin_decimals(self, pool_info: PoolInfo, index: int) -> int:
        """Decimals for pool coin ``index`` — chain-read when available, else resolved.

        Prefers the live ``decimals()`` captured by the refresh-on-read path
        (``pool_info.coin_decimals``); falls back to resolving the coin symbol via
        the TokenResolver when the registry was not refreshed (cold-start) or the
        decimal read was unavailable. Behaviour is identical to the prior
        symbol-only path whenever ``coin_decimals`` is ``None``.
        """
        live = pool_info.coin_decimals
        if live is not None and 0 <= index < len(live):
            return live[index]
        return self._get_token_decimals(pool_info.coins[index])

    def _get_token_decimals(self, symbol: str) -> int:
        """Get token decimals from symbol using TokenResolver."""
        return resolve_token_decimals(symbol, self.chain, resolver=self._token_resolver)

    def _is_native_token(self, token: str) -> bool:
        """Check if ``token`` denotes the CURRENT chain's native coin.

        Callers pass pool coin ADDRESSES, where Curve marks raw native with the
        0xEeee placeholder — that arm does the real work. The symbol arm is
        derived per-chain from ``ChainDescriptor.native`` via
        ``native_symbols_for`` (VIB-4851 A1) instead of the legacy hardcoded
        "ETH", so a symbol caller on polygon gets MATIC/POL right.
        """
        if token.upper() in native_symbols_for(self.chain):
            return True
        native_address = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE".lower()
        return token.lower() == native_address

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return pad_address(addr)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return pad_uint256(value)

    @staticmethod
    def _pad_int128(value: int) -> str:
        """Pad int128 to 32 bytes (signed)."""
        if value < 0:
            # ABI signed integers use two's-complement padding.
            value = (1 << 256) + value
        return hex(value)[2:].zfill(64)

    def set_allowance(self, token: str, spender: str, amount: int) -> None:
        """Set cached allowance (for testing).

        Args:
            token: Token address
            spender: Spender address
            amount: Allowance amount
        """
        self._allowance_cache.record_confirmed(token, spender, amount)

    def clear_allowance_cache(self) -> None:
        """Clear the allowance cache."""
        self._allowance_cache.clear()

    def clear_planned_allowance_cache(self) -> None:
        """Clear optimistic approvals emitted into the current bundle."""
        self._allowance_cache.clear_planned()


__all__ = [
    "CurveAdapter",
    "CurveConfig",
    "SwapResult",
    "LiquidityResult",
    "PoolInfo",
    "PoolType",
    "TransactionData",
    "CURVE_ADDRESSES",
    "CURVE_GAS_ESTIMATES",
]
