"""DEX Pool Data Structures and Interfaces.

This module provides data structures for DEX pool reserve data, supporting
various AMM protocols including Uniswap V2, Uniswap V3, and SushiSwap.

Key Components:
    - PoolReserves: Dataclass representing DEX pool state
    - DexType: Literal type for supported DEX protocols

Example:
    from almanak.framework.data.defi.pools import PoolReserves, DexType
    from almanak.framework.data.tokens import ChainToken, Token

    # Create token references
    weth_token = Token(symbol="WETH", name="Wrapped Ether", decimals=18)
    usdc_token = Token(symbol="USDC", name="USD Coin", decimals=6)

    weth = ChainToken(
        token=weth_token,
        chain="ethereum",
        address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        decimals=18,
    )
    usdc = ChainToken(
        token=usdc_token,
        chain="ethereum",
        address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        decimals=6,
    )

    # Uniswap V2 pool
    v2_pool = PoolReserves(
        pool_address="0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
        dex="uniswap_v2",
        token0=usdc,
        token1=weth,
        reserve0=Decimal("50000000"),  # 50M USDC
        reserve1=Decimal("25000"),  # 25K WETH
        fee_tier=3000,  # 0.3%
        tvl_usd=Decimal("100000000"),  # $100M
        last_updated=datetime.now(timezone.utc),
    )

    # Uniswap V3 pool with additional fields
    v3_pool = PoolReserves(
        pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        dex="uniswap_v3",
        token0=usdc,
        token1=weth,
        reserve0=Decimal("50000000"),
        reserve1=Decimal("25000"),
        fee_tier=500,  # 0.05%
        sqrt_price_x96=1234567890123456789012345678901234567,
        tick=-201234,
        liquidity=1234567890123456789,
        tvl_usd=Decimal("100000000"),
        last_updated=datetime.now(timezone.utc),
    )
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Optional

if TYPE_CHECKING:
    from ..tokens.models import ChainToken

# Supported DEX protocols
DexType = Literal["uniswap_v2", "uniswap_v3", "sushiswap"]

# Valid DEX types for validation
VALID_DEX_TYPES: set[str] = {"uniswap_v2", "uniswap_v3", "sushiswap"}


@dataclass
class PoolReserves:
    """DEX pool reserve data with support for various AMM protocols.

    This dataclass represents the current state of a DEX liquidity pool,
    including reserves, pricing data, and protocol-specific fields.

    Common fields (all DEX types):
        - pool_address: Contract address of the pool
        - dex: DEX protocol type ('uniswap_v2', 'uniswap_v3', 'sushiswap')
        - token0: First token in the pool pair
        - token1: Second token in the pool pair
        - reserve0: Reserve of token0 in human-readable units
        - reserve1: Reserve of token1 in human-readable units
        - fee_tier: Pool fee in basis points (e.g., 3000 = 0.3%)
        - tvl_usd: Total value locked in USD
        - last_updated: Timestamp when data was fetched

    Uniswap V3 specific fields:
        - sqrt_price_x96: Square root of price as Q64.96 fixed-point
        - tick: Current tick of the pool
        - liquidity: Current in-range liquidity

    Attributes:
        pool_address: Pool contract address (checksummed)
        dex: DEX protocol identifier
        token0: ChainToken representing the first token
        token1: ChainToken representing the second token
        reserve0: Reserve amount of token0 (human-readable Decimal)
        reserve1: Reserve amount of token1 (human-readable Decimal)
        fee_tier: Pool fee in basis points (100 = 0.01%, 500 = 0.05%, 3000 = 0.3%, 10000 = 1%)
        sqrt_price_x96: V3 sqrt price as Q64.96 (None for V2)
        tick: V3 current tick (None for V2)
        liquidity: V3 in-range liquidity (None for V2)
        tvl_usd: Total value locked in USD
        last_updated: When the data was observed
    """

    pool_address: str
    dex: DexType
    token0: "ChainToken"
    token1: "ChainToken"
    reserve0: Decimal
    reserve1: Decimal
    fee_tier: int
    tvl_usd: Decimal
    last_updated: datetime
    sqrt_price_x96: int | None = None
    tick: int | None = None
    liquidity: int | None = None

    def __post_init__(self) -> None:
        """Validate and normalize fields."""
        # Validate DEX type
        if self.dex not in VALID_DEX_TYPES:
            raise ValueError(f"Invalid dex type: '{self.dex}'. Must be one of: {', '.join(sorted(VALID_DEX_TYPES))}")

        # Validate pool address
        if not self.pool_address:
            raise ValueError("pool_address cannot be empty")

        # Validate fee tier is non-negative
        if self.fee_tier < 0:
            raise ValueError(f"fee_tier must be non-negative, got {self.fee_tier}")

        # Convert numeric types to Decimal if needed
        for field_name in ("reserve0", "reserve1", "tvl_usd"):
            val = getattr(self, field_name)
            if not isinstance(val, Decimal):
                object.__setattr__(self, field_name, Decimal(str(val)))

        # Validate reserves are non-negative
        if self.reserve0 < 0:
            raise ValueError("reserve0 must be non-negative")
        if self.reserve1 < 0:
            raise ValueError("reserve1 must be non-negative")

        # Validate V3-specific fields only set for V3 pools
        if self.dex == "uniswap_v3":
            # V3 pools should have these fields
            if self.sqrt_price_x96 is not None and self.sqrt_price_x96 < 0:
                raise ValueError("sqrt_price_x96 must be non-negative")
        else:
            # Non-V3 pools shouldn't have V3 fields set
            # But we allow them as None (already the default)
            pass

    @property
    def is_v3(self) -> bool:
        """Check if this is a Uniswap V3 pool."""
        return self.dex == "uniswap_v3"

    @property
    def is_v2(self) -> bool:
        """Check if this is a Uniswap V2 or SushiSwap pool."""
        return self.dex in ("uniswap_v2", "sushiswap")

    @property
    def fee_percent(self) -> Decimal:
        """Get fee as a percentage (e.g., 0.3 for 3000 basis points)."""
        return Decimal(self.fee_tier) / Decimal("10000")

    @property
    def chain(self) -> str:
        """Get the chain from token0 (assumes both tokens are on same chain)."""
        return self.token0.chain

    @property
    def price_token0_in_token1(self) -> Decimal | None:
        """Calculate price of token0 in terms of token1.

        For V2 pools: price = reserve1 / reserve0
        For V3 pools: calculated from sqrt_price_x96

        Returns:
            Price or None if reserves are zero
        """
        if self.reserve0 == 0:
            return None

        if self.is_v3 and self.sqrt_price_x96 is not None:
            # For V3: price = (sqrt_price_x96 / 2^96)^2
            # Adjusted for decimal differences
            sqrt_price = Decimal(self.sqrt_price_x96) / Decimal(2**96)
            raw_price = sqrt_price * sqrt_price

            # Adjust for decimal difference between tokens
            decimal_diff = self.token1.decimals - self.token0.decimals
            return raw_price * Decimal(10**decimal_diff)
        else:
            # For V2: simple ratio
            return self.reserve1 / self.reserve0

    @property
    def price_token1_in_token0(self) -> Decimal | None:
        """Calculate price of token1 in terms of token0.

        Returns:
            Price or None if reserves are zero
        """
        price = self.price_token0_in_token1
        if price is None or price == 0:
            return None
        return Decimal("1") / price

    @property
    def age_seconds(self) -> float:
        """Calculate age of the pool data in seconds."""
        return (datetime.now(UTC) - self.last_updated).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "pool_address": self.pool_address,
            "dex": self.dex,
            "token0": self.token0.to_dict(),
            "token1": self.token1.to_dict(),
            "reserve0": str(self.reserve0),
            "reserve1": str(self.reserve1),
            "fee_tier": self.fee_tier,
            "tvl_usd": str(self.tvl_usd),
            "last_updated": self.last_updated.isoformat(),
        }

        # Include V3 fields if present
        if self.sqrt_price_x96 is not None:
            result["sqrt_price_x96"] = self.sqrt_price_x96
        if self.tick is not None:
            result["tick"] = self.tick
        if self.liquidity is not None:
            result["liquidity"] = self.liquidity

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PoolReserves":
        """Create PoolReserves from dictionary.

        Note: This requires ChainToken.from_dict to work properly.
        Import ChainToken at runtime to avoid circular imports.
        """
        from ..tokens.models import ChainToken

        return cls(
            pool_address=data["pool_address"],
            dex=data["dex"],
            token0=ChainToken.from_dict(data["token0"]),
            token1=ChainToken.from_dict(data["token1"]),
            reserve0=Decimal(data["reserve0"]),
            reserve1=Decimal(data["reserve1"]),
            fee_tier=data["fee_tier"],
            tvl_usd=Decimal(data["tvl_usd"]),
            last_updated=datetime.fromisoformat(data["last_updated"]),
            sqrt_price_x96=data.get("sqrt_price_x96"),
            tick=data.get("tick"),
            liquidity=data.get("liquidity"),
        )


# =============================================================================
# Uniswap V3 Pool ABI
# =============================================================================

# Minimal Uniswap V3 Pool ABI for reading pool state
UNISWAP_V3_POOL_ABI = [
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"name": "sqrtPriceX96", "type": "uint160"},
            {"name": "tick", "type": "int24"},
            {"name": "observationIndex", "type": "uint16"},
            {"name": "observationCardinality", "type": "uint16"},
            {"name": "observationCardinalityNext", "type": "uint16"},
            {"name": "feeProtocol", "type": "uint8"},
            {"name": "unlocked", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "liquidity",
        "outputs": [{"name": "", "type": "uint128"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "fee",
        "outputs": [{"name": "", "type": "uint24"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Minimal ERC20 ABI for token metadata
ERC20_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "name",
        "outputs": [{"name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Uniswap V3 Pool Reader
# =============================================================================

import asyncio
import logging
from typing import TYPE_CHECKING as _TYPE_CHECKING

from web3 import AsyncHTTPProvider, AsyncWeb3

from ..interfaces import DataSourceError, DataSourceUnavailable

if _TYPE_CHECKING:
    from ..interfaces import PriceOracle

logger = logging.getLogger(__name__)


class UniswapV3PoolReader:
    """Reader for Uniswap V3 pool state from on-chain data.

    This class fetches pool reserves, prices, and TVL from Uniswap V3 pools
    by reading directly from the blockchain via RPC.

    Features:
        - Read slot0 for sqrtPriceX96 and tick
        - Read liquidity from the pool
        - Read token0/token1 addresses and metadata
        - Normalize reserves to human-readable units
        - Calculate TVL in USD using price oracle

    Example:
        from almanak.framework.data.defi.pools import UniswapV3PoolReader

        reader = UniswapV3PoolReader(
            rpc_urls={
                "ethereum": "https://eth.llamarpc.com",
                "arbitrum": "https://arb1.arbitrum.io/rpc",
            },
            price_oracle=price_oracle,
        )

        # Get pool reserves
        reserves = await reader.get_pool_reserves(
            pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC/WETH 0.05%
            chain="ethereum",
        )

        print(f"Reserve0: {reserves.reserve0} {reserves.token0.symbol}")
        print(f"Reserve1: {reserves.reserve1} {reserves.token1.symbol}")
        print(f"TVL: ${reserves.tvl_usd}")

    Attributes:
        rpc_urls: Mapping of chain names to RPC endpoint URLs
        price_oracle: Optional PriceOracle for TVL calculation in USD
        request_timeout: HTTP request timeout in seconds (default 10.0)
    """

    def __init__(
        self,
        rpc_urls: dict[str, str],
        price_oracle: Optional["PriceOracle"] = None,
        request_timeout: float = 10.0,
    ) -> None:
        """Initialize the UniswapV3PoolReader.

        Args:
            rpc_urls: Dict mapping chain names to RPC URLs
            price_oracle: Optional PriceOracle for USD TVL calculation.
                          If not provided, tvl_usd will be Decimal("0").
            request_timeout: HTTP request timeout in seconds (default 10.0)
        """
        self._rpc_urls = {k.lower(): v for k, v in rpc_urls.items()}
        self._price_oracle = price_oracle
        self._request_timeout = request_timeout

        # Lazy-initialized Web3 instances per chain
        self._web3_instances: dict[str, AsyncWeb3] = {}

        logger.info(
            "Initialized UniswapV3PoolReader",
            extra={
                "chains": list(self._rpc_urls.keys()),
                "has_price_oracle": price_oracle is not None,
            },
        )

    def _get_web3(self, chain: str) -> AsyncWeb3:
        """Get or create AsyncWeb3 instance for a chain.

        Args:
            chain: Chain name (lowercase)

        Returns:
            AsyncWeb3 instance

        Raises:
            DataSourceUnavailable: If chain RPC URL not configured
        """
        chain_lower = chain.lower()

        if chain_lower not in self._rpc_urls:
            raise DataSourceUnavailable(
                source="uniswap_v3_pool_reader",
                reason=f"No RPC URL configured for chain '{chain}'",
            )

        if chain_lower not in self._web3_instances:
            self._web3_instances[chain_lower] = AsyncWeb3(AsyncHTTPProvider(self._rpc_urls[chain_lower]))

        return self._web3_instances[chain_lower]

    async def get_pool_reserves(self, pool_address: str, chain: str) -> PoolReserves:
        """Get Uniswap V3 pool reserves and state.

        Reads the current pool state from the blockchain including:
        - slot0: sqrtPriceX96, tick
        - liquidity: current in-range liquidity
        - token0/token1: pool tokens with metadata

        Args:
            pool_address: Pool contract address
            chain: Chain identifier (e.g., "ethereum", "arbitrum")

        Returns:
            PoolReserves with full pool state

        Raises:
            DataSourceUnavailable: If RPC is unavailable
            DataSourceError: If pool data cannot be fetched
        """
        chain_lower = chain.lower()
        web3 = self._get_web3(chain_lower)

        try:
            checksum_address = web3.to_checksum_address(pool_address)
            pool_contract = web3.eth.contract(
                address=checksum_address,
                abi=UNISWAP_V3_POOL_ABI,
            )

            # Fetch all pool data in parallel
            slot0_task = asyncio.wait_for(
                pool_contract.functions.slot0().call(),
                timeout=self._request_timeout,
            )
            liquidity_task = asyncio.wait_for(
                pool_contract.functions.liquidity().call(),
                timeout=self._request_timeout,
            )
            token0_addr_task = asyncio.wait_for(
                pool_contract.functions.token0().call(),
                timeout=self._request_timeout,
            )
            token1_addr_task = asyncio.wait_for(
                pool_contract.functions.token1().call(),
                timeout=self._request_timeout,
            )
            fee_task = asyncio.wait_for(
                pool_contract.functions.fee().call(),
                timeout=self._request_timeout,
            )

            (
                slot0_result,
                liquidity,
                token0_address,
                token1_address,
                fee_tier,
            ) = await asyncio.gather(
                slot0_task,
                liquidity_task,
                token0_addr_task,
                token1_addr_task,
                fee_task,
            )

            # Extract slot0 values
            sqrt_price_x96 = int(slot0_result[0])
            tick = int(slot0_result[1])

            # Fetch token metadata
            token0, token1 = await asyncio.gather(
                self._fetch_token_metadata(web3, token0_address, chain_lower),
                self._fetch_token_metadata(web3, token1_address, chain_lower),
            )

            # Get token balances in the pool to calculate reserves
            reserve0_raw, reserve1_raw = await asyncio.gather(
                self._fetch_token_balance(web3, token0_address, checksum_address),
                self._fetch_token_balance(web3, token1_address, checksum_address),
            )

            # Normalize reserves to human-readable units
            reserve0 = Decimal(reserve0_raw) / Decimal(10**token0.decimals)
            reserve1 = Decimal(reserve1_raw) / Decimal(10**token1.decimals)

            # Calculate TVL in USD
            tvl_usd = await self._calculate_tvl_usd(
                reserve0=reserve0,
                reserve1=reserve1,
                token0_symbol=token0.symbol,
                token1_symbol=token1.symbol,
            )

            return PoolReserves(
                pool_address=checksum_address,
                dex="uniswap_v3",
                token0=token0,
                token1=token1,
                reserve0=reserve0,
                reserve1=reserve1,
                fee_tier=int(fee_tier),
                sqrt_price_x96=sqrt_price_x96,
                tick=tick,
                liquidity=int(liquidity),
                tvl_usd=tvl_usd,
                last_updated=datetime.now(UTC),
            )

        except DataSourceError:
            raise
        except TimeoutError:
            raise DataSourceUnavailable(
                source="uniswap_v3_pool_reader",
                reason=f"RPC timeout for chain '{chain}'",
                retry_after=5.0,
            ) from None
        except Exception as e:
            logger.error(
                "Failed to fetch pool reserves for %s on %s: %s",
                pool_address,
                chain,
                str(e),
                exc_info=True,
            )
            raise DataSourceError(f"Failed to fetch pool reserves for '{pool_address}' on '{chain}': {e}") from e

    async def _fetch_token_metadata(self, web3: AsyncWeb3, token_address: str, chain: str) -> "ChainToken":
        """Fetch token metadata from the blockchain.

        Args:
            web3: AsyncWeb3 instance
            token_address: Token contract address
            chain: Chain name

        Returns:
            ChainToken with token metadata
        """
        from ..tokens.models import ChainToken, Token

        checksum_address = web3.to_checksum_address(token_address)
        token_contract = web3.eth.contract(
            address=checksum_address,
            abi=ERC20_ABI,
        )

        try:
            # Fetch token metadata in parallel
            symbol_task = asyncio.wait_for(
                token_contract.functions.symbol().call(),
                timeout=self._request_timeout,
            )
            name_task = asyncio.wait_for(
                token_contract.functions.name().call(),
                timeout=self._request_timeout,
            )
            decimals_task = asyncio.wait_for(
                token_contract.functions.decimals().call(),
                timeout=self._request_timeout,
            )

            symbol, name, decimals = await asyncio.gather(symbol_task, name_task, decimals_task)

            # Create Token and ChainToken
            token = Token(
                symbol=symbol,
                name=name,
                decimals=int(decimals),
                addresses={chain: checksum_address},
            )

            return ChainToken(
                token=token,
                chain=chain,
                address=checksum_address,
                decimals=int(decimals),
            )

        except Exception as e:
            logger.warning(
                "Failed to fetch metadata for token %s: %s",
                token_address,
                str(e),
            )
            # Return a default token with unknown metadata
            token = Token(
                symbol="UNKNOWN",
                name="Unknown Token",
                decimals=18,
                addresses={chain: checksum_address},
            )
            return ChainToken(
                token=token,
                chain=chain,
                address=checksum_address,
                decimals=18,
            )

    async def _fetch_token_balance(self, web3: AsyncWeb3, token_address: str, holder_address: str) -> int:
        """Fetch token balance for a holder.

        Args:
            web3: AsyncWeb3 instance
            token_address: Token contract address
            holder_address: Address to check balance for

        Returns:
            Raw balance in smallest units
        """
        token_contract = web3.eth.contract(
            address=web3.to_checksum_address(token_address),
            abi=ERC20_ABI,
        )

        balance = await asyncio.wait_for(
            token_contract.functions.balanceOf(holder_address).call(),
            timeout=self._request_timeout,
        )
        return int(balance)

    async def _calculate_tvl_usd(
        self,
        reserve0: Decimal,
        reserve1: Decimal,
        token0_symbol: str,
        token1_symbol: str,
    ) -> Decimal:
        """Calculate total value locked in USD.

        Args:
            reserve0: Reserve of token0 in human-readable units
            reserve1: Reserve of token1 in human-readable units
            token0_symbol: Symbol of token0
            token1_symbol: Symbol of token1

        Returns:
            TVL in USD, or Decimal("0") if price oracle unavailable
        """
        if self._price_oracle is None:
            return Decimal("0")

        try:
            # Fetch prices for both tokens
            price0_task = self._price_oracle.get_aggregated_price(token0_symbol, "USD")
            price1_task = self._price_oracle.get_aggregated_price(token1_symbol, "USD")

            price0_result, price1_result = await asyncio.gather(price0_task, price1_task, return_exceptions=True)

            # Calculate value for each token
            tvl_usd = Decimal("0")

            if not isinstance(price0_result, BaseException):
                tvl_usd += reserve0 * price0_result.price
            else:
                logger.warning(
                    "Failed to get price for %s: %s",
                    token0_symbol,
                    str(price0_result),
                )

            if not isinstance(price1_result, BaseException):
                tvl_usd += reserve1 * price1_result.price
            else:
                logger.warning(
                    "Failed to get price for %s: %s",
                    token1_symbol,
                    str(price1_result),
                )

            return tvl_usd.quantize(Decimal("0.01"))  # Round to 2 decimal places

        except Exception as e:
            logger.warning(
                "Failed to calculate TVL: %s",
                str(e),
            )
            return Decimal("0")


__all__ = [
    "DexType",
    "PoolReserves",
    "VALID_DEX_TYPES",
    "UNISWAP_V3_POOL_ABI",
    "UniswapV3PoolReader",
]
