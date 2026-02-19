"""Gas Price Data Structures and Interfaces.

This module provides data structures for gas price data, supporting both
L1 chains (Ethereum mainnet) and L2 chains (Arbitrum, Optimism, Base)
which have additional L1 data cost components.

Key Components:
    - GasPrice: Dataclass representing current gas price data
    - GasOracle: Protocol for gas price providers

Example:
    from almanak.framework.data.defi.gas import GasPrice, GasOracle

    # Create gas price data
    gas = GasPrice(
        chain="ethereum",
        base_fee_gwei=Decimal("25.5"),
        priority_fee_gwei=Decimal("2.0"),
        max_fee_gwei=Decimal("27.5"),
        estimated_cost_usd=Decimal("5.50"),
        timestamp=datetime.now(timezone.utc),
    )

    # For L2 chains
    l2_gas = GasPrice(
        chain="arbitrum",
        base_fee_gwei=Decimal("0.1"),
        priority_fee_gwei=Decimal("0.0"),
        max_fee_gwei=Decimal("0.1"),
        l1_base_fee_gwei=Decimal("25.5"),
        l1_data_cost_gwei=Decimal("12.0"),
        estimated_cost_usd=Decimal("0.25"),
        timestamp=datetime.now(timezone.utc),
    )
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Optional, Protocol, runtime_checkable


@dataclass
class GasPrice:
    """Gas price data with L1/L2 support.

    This dataclass represents the current gas price state for a blockchain,
    including all fee components needed for transaction cost estimation.

    For L1 chains (Ethereum mainnet):
        - base_fee_gwei: Current base fee from the network
        - priority_fee_gwei: Tip to miners/validators (maxPriorityFeePerGas)
        - max_fee_gwei: Maximum fee willing to pay (base_fee + priority_fee)

    For L2 chains (Arbitrum, Optimism, Base):
        - Same L2 execution fees as above
        - l1_base_fee_gwei: L1 base fee used for data posting cost
        - l1_data_cost_gwei: Estimated L1 data availability cost

    Attributes:
        chain: Chain identifier (e.g., "ethereum", "arbitrum", "optimism")
        base_fee_gwei: Network base fee in gwei
        priority_fee_gwei: Priority/tip fee in gwei
        max_fee_gwei: Maximum fee (base + priority) in gwei
        l1_base_fee_gwei: L1 base fee for L2 data posting (None for L1 chains)
        l1_data_cost_gwei: L1 data cost estimate in gwei (None for L1 chains)
        estimated_cost_usd: Estimated transaction cost in USD (for 21000 gas)
        timestamp: When the gas price was observed
    """

    chain: str
    base_fee_gwei: Decimal
    priority_fee_gwei: Decimal
    max_fee_gwei: Decimal
    estimated_cost_usd: Decimal
    timestamp: datetime
    l1_base_fee_gwei: Decimal | None = None
    l1_data_cost_gwei: Decimal | None = None

    def __post_init__(self) -> None:
        """Validate and normalize fields."""
        # Normalize chain to lowercase
        object.__setattr__(self, "chain", self.chain.lower())

        # Convert numeric types to Decimal if needed
        for field_name in (
            "base_fee_gwei",
            "priority_fee_gwei",
            "max_fee_gwei",
            "estimated_cost_usd",
        ):
            val = getattr(self, field_name)
            if not isinstance(val, Decimal):
                object.__setattr__(self, field_name, Decimal(str(val)))

        # Convert optional Decimal fields
        for field_name in ("l1_base_fee_gwei", "l1_data_cost_gwei"):
            val = getattr(self, field_name)
            if val is not None and not isinstance(val, Decimal):
                object.__setattr__(self, field_name, Decimal(str(val)))

        # Validate non-negative values
        if self.base_fee_gwei < 0:
            raise ValueError("base_fee_gwei must be non-negative")
        if self.priority_fee_gwei < 0:
            raise ValueError("priority_fee_gwei must be non-negative")
        if self.max_fee_gwei < 0:
            raise ValueError("max_fee_gwei must be non-negative")
        if self.estimated_cost_usd < 0:
            raise ValueError("estimated_cost_usd must be non-negative")

    @property
    def is_l2(self) -> bool:
        """Check if this is an L2 chain with L1 data costs."""
        return self.l1_base_fee_gwei is not None or self.l1_data_cost_gwei is not None

    @property
    def total_l2_cost_gwei(self) -> Decimal | None:
        """Calculate total L2 cost including L1 data component.

        Returns:
            Total cost in gwei for L2 chains, None for L1 chains
        """
        if not self.is_l2:
            return None
        l1_cost = self.l1_data_cost_gwei or Decimal("0")
        return self.max_fee_gwei + l1_cost

    @property
    def age_seconds(self) -> float:
        """Calculate age of the gas price data in seconds."""
        return (datetime.now(UTC) - self.timestamp).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "chain": self.chain,
            "base_fee_gwei": str(self.base_fee_gwei),
            "priority_fee_gwei": str(self.priority_fee_gwei),
            "max_fee_gwei": str(self.max_fee_gwei),
            "estimated_cost_usd": str(self.estimated_cost_usd),
            "timestamp": self.timestamp.isoformat(),
        }
        if self.l1_base_fee_gwei is not None:
            result["l1_base_fee_gwei"] = str(self.l1_base_fee_gwei)
        if self.l1_data_cost_gwei is not None:
            result["l1_data_cost_gwei"] = str(self.l1_data_cost_gwei)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GasPrice":
        """Create GasPrice from dictionary."""
        return cls(
            chain=data["chain"],
            base_fee_gwei=Decimal(data["base_fee_gwei"]),
            priority_fee_gwei=Decimal(data["priority_fee_gwei"]),
            max_fee_gwei=Decimal(data["max_fee_gwei"]),
            estimated_cost_usd=Decimal(data["estimated_cost_usd"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            l1_base_fee_gwei=(Decimal(data["l1_base_fee_gwei"]) if data.get("l1_base_fee_gwei") else None),
            l1_data_cost_gwei=(Decimal(data["l1_data_cost_gwei"]) if data.get("l1_data_cost_gwei") else None),
        )


@runtime_checkable
class GasOracle(Protocol):
    """Protocol for gas price providers.

    A GasOracle provides real-time gas price data for blockchain networks.
    Implementations should handle:
    - Fetching current gas prices from RPC or external APIs
    - Caching to avoid excessive RPC calls
    - L2-specific data cost calculations

    Example implementation:
        class Web3GasOracle:
            def __init__(self, web3: Web3, price_oracle: PriceOracle):
                self._web3 = web3
                self._price_oracle = price_oracle

            async def get_gas_price(self, chain: str = "ethereum") -> GasPrice:
                # Fetch from RPC
                base_fee = await self._web3.eth.gas_price
                priority_fee = await self._web3.eth.max_priority_fee

                # Calculate USD cost
                eth_price = await self._price_oracle.get_aggregated_price("ETH")
                gas_cost_eth = (base_fee + priority_fee) * 21000 / 1e18
                cost_usd = gas_cost_eth * float(eth_price.price)

                return GasPrice(
                    chain=chain,
                    base_fee_gwei=Decimal(base_fee) / Decimal(1e9),
                    priority_fee_gwei=Decimal(priority_fee) / Decimal(1e9),
                    max_fee_gwei=Decimal(base_fee + priority_fee) / Decimal(1e9),
                    estimated_cost_usd=Decimal(str(cost_usd)),
                    timestamp=datetime.now(timezone.utc),
                )
    """

    async def get_gas_price(self, chain: str = "ethereum") -> GasPrice:
        """Get current gas price for a chain.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum", "optimism")

        Returns:
            GasPrice with current fee data

        Raises:
            DataSourceError: If gas price cannot be fetched
        """
        ...


# =============================================================================
# L2 GasPriceOracle Contract ABIs (Optimism Stack - used by Optimism, Base)
# =============================================================================

# Optimism GasPriceOracle contract (also used by Base)
# Address: 0x420000000000000000000000000000000000000F (OP Stack standard)
OPTIMISM_GAS_PRICE_ORACLE_ABI = [
    {
        "inputs": [],
        "name": "l1BaseFee",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "scalar",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "overhead",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "baseFeeScalar",
        "outputs": [{"name": "", "type": "uint32"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "blobBaseFeeScalar",
        "outputs": [{"name": "", "type": "uint32"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Arbitrum ArbGasInfo precompile ABI
# Address: 0x000000000000000000000000000000000000006C
ARBITRUM_GAS_INFO_ABI = [
    {
        "inputs": [],
        "name": "getL1BaseFeeEstimate",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getPricesInWei",
        "outputs": [
            {"name": "", "type": "uint256"},  # per L2 tx
            {"name": "", "type": "uint256"},  # per L1 calldata byte
            {"name": "", "type": "uint256"},  # per storage allocation
            {"name": "", "type": "uint256"},  # per ArbGas base
            {"name": "", "type": "uint256"},  # per ArbGas congestion
            {"name": "", "type": "uint256"},  # per ArbGas total
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

# Contract addresses per chain
L2_GAS_ORACLE_ADDRESSES: dict[str, str] = {
    "optimism": "0x420000000000000000000000000000000000000F",
    "base": "0x420000000000000000000000000000000000000F",
    "arbitrum": "0x000000000000000000000000000000000000006C",
}

# L2 chains that need special handling
L2_CHAINS = {"arbitrum", "optimism", "base"}

# Standard gas for a simple transfer (used for cost estimation)
STANDARD_GAS_UNITS = 21000


# =============================================================================
# Web3 Gas Oracle Implementation
# =============================================================================


import asyncio
import logging
from typing import TYPE_CHECKING

from web3 import AsyncHTTPProvider, AsyncWeb3
from web3.exceptions import Web3Exception

from ..interfaces import DataSourceError, DataSourceUnavailable

if TYPE_CHECKING:
    from ..interfaces import PriceOracle

logger = logging.getLogger(__name__)


class Web3GasOracle:
    """Gas oracle that fetches gas prices from RPC and calculates USD costs.

    This implementation fetches real-time gas prices from blockchain RPC endpoints
    and calculates estimated transaction costs in USD using a price oracle.

    For L1 chains (Ethereum):
        - Fetches base_fee from latest block
        - Fetches priority_fee from eth_maxPriorityFeePerGas
        - Calculates max_fee = base_fee + priority_fee

    For L2 chains (Arbitrum, Optimism, Base):
        - Fetches L2 execution fees as above
        - Queries GasPriceOracle contract for L1 data cost components
        - Arbitrum: Uses ArbGasInfo precompile
        - Optimism/Base: Uses OP Stack GasPriceOracle

    Example:
        from almanak.framework.data.defi.gas import Web3GasOracle
        from almanak.framework.data.price.aggregator import PriceAggregator

        # Create price oracle for ETH price
        price_oracle = PriceAggregator(sources=[CoinGeckoPriceSource()])

        # Create gas oracle
        gas_oracle = Web3GasOracle(
            rpc_urls={
                "ethereum": "https://eth.llamarpc.com",
                "arbitrum": "https://arb1.arbitrum.io/rpc",
            },
            price_oracle=price_oracle,
        )

        # Get gas price
        gas_price = await gas_oracle.get_gas_price("arbitrum")
        print(f"Base fee: {gas_price.base_fee_gwei} gwei")
        print(f"Estimated cost: ${gas_price.estimated_cost_usd}")

    Attributes:
        rpc_urls: Mapping of chain names to RPC endpoint URLs
        price_oracle: Optional PriceOracle for USD cost estimation
        request_timeout: HTTP request timeout in seconds (default 10.0)
    """

    def __init__(
        self,
        rpc_urls: dict[str, str],
        price_oracle: Optional["PriceOracle"] = None,
        request_timeout: float = 10.0,
    ) -> None:
        """Initialize the Web3GasOracle.

        Args:
            rpc_urls: Dict mapping chain names to RPC URLs
            price_oracle: Optional PriceOracle for USD cost estimation.
                          If not provided, estimated_cost_usd will be Decimal("0").
            request_timeout: HTTP request timeout in seconds (default 10.0)
        """
        self._rpc_urls = {k.lower(): v for k, v in rpc_urls.items()}
        self._price_oracle = price_oracle
        self._request_timeout = request_timeout

        # Lazy-initialized Web3 instances per chain
        self._web3_instances: dict[str, AsyncWeb3] = {}

        logger.info(
            "Initialized Web3GasOracle",
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
                source="web3_gas_oracle",
                reason=f"No RPC URL configured for chain '{chain}'",
            )

        if chain_lower not in self._web3_instances:
            self._web3_instances[chain_lower] = AsyncWeb3(AsyncHTTPProvider(self._rpc_urls[chain_lower]))

        return self._web3_instances[chain_lower]

    async def get_gas_price(self, chain: str = "ethereum") -> GasPrice:
        """Get current gas price for a chain.

        Fetches gas price components from the blockchain RPC and calculates
        the estimated USD cost for a standard 21000 gas transaction.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum", "optimism")

        Returns:
            GasPrice with current fee data

        Raises:
            DataSourceUnavailable: If RPC is unavailable
            DataSourceError: If gas price cannot be fetched
        """
        chain_lower = chain.lower()
        web3 = self._get_web3(chain_lower)

        try:
            # Fetch base fee and priority fee
            base_fee_wei, priority_fee_wei = await self._fetch_gas_fees(web3)

            # Calculate max fee
            max_fee_wei = base_fee_wei + priority_fee_wei

            # Convert to gwei
            base_fee_gwei = Decimal(base_fee_wei) / Decimal(10**9)
            priority_fee_gwei = Decimal(priority_fee_wei) / Decimal(10**9)
            max_fee_gwei = Decimal(max_fee_wei) / Decimal(10**9)

            # Initialize L1 fee components (for L2 chains)
            l1_base_fee_gwei: Decimal | None = None
            l1_data_cost_gwei: Decimal | None = None

            # Fetch L1 data costs for L2 chains
            if chain_lower in L2_CHAINS:
                l1_base_fee_gwei, l1_data_cost_gwei = await self._fetch_l1_data_cost(web3, chain_lower)

            # Calculate estimated USD cost
            estimated_cost_usd = await self._calculate_usd_cost(
                max_fee_wei=max_fee_wei,
                l1_data_cost_wei=(int(l1_data_cost_gwei * Decimal(10**9)) if l1_data_cost_gwei else 0),
            )

            return GasPrice(
                chain=chain_lower,
                base_fee_gwei=base_fee_gwei,
                priority_fee_gwei=priority_fee_gwei,
                max_fee_gwei=max_fee_gwei,
                l1_base_fee_gwei=l1_base_fee_gwei,
                l1_data_cost_gwei=l1_data_cost_gwei,
                estimated_cost_usd=estimated_cost_usd,
                timestamp=datetime.now(UTC),
            )

        except DataSourceError:
            raise
        except TimeoutError:
            raise DataSourceUnavailable(
                source="web3_gas_oracle",
                reason=f"RPC timeout for chain '{chain}'",
                retry_after=5.0,
            ) from None
        except Exception as e:
            logger.error(
                "Failed to fetch gas price for %s: %s",
                chain,
                str(e),
                exc_info=True,
            )
            raise DataSourceError(f"Failed to fetch gas price for chain '{chain}': {e}") from e

    async def _fetch_gas_fees(self, web3: AsyncWeb3) -> tuple[int, int]:
        """Fetch base fee and priority fee from RPC.

        Args:
            web3: AsyncWeb3 instance

        Returns:
            Tuple of (base_fee_wei, priority_fee_wei)
        """
        # Get latest block for base fee
        latest_block = await asyncio.wait_for(
            web3.eth.get_block("latest"),
            timeout=self._request_timeout,
        )
        base_fee = latest_block.get("baseFeePerGas", 0)
        base_fee_wei = int(base_fee) if base_fee else 0

        # Get max priority fee suggestion
        try:
            priority_fee_wei = int(
                await asyncio.wait_for(
                    web3.eth.max_priority_fee,
                    timeout=self._request_timeout,
                )
            )
        except (Web3Exception, Exception) as e:
            # Fallback to 1 gwei if RPC doesn't support eth_maxPriorityFeePerGas
            logger.debug(
                "eth_maxPriorityFeePerGas not supported, using 1 gwei fallback: %s",
                str(e),
            )
            priority_fee_wei = 1_000_000_000  # 1 gwei

        return base_fee_wei, priority_fee_wei

    async def _fetch_l1_data_cost(self, web3: AsyncWeb3, chain: str) -> tuple[Decimal | None, Decimal | None]:
        """Fetch L1 data cost components for L2 chains.

        Args:
            web3: AsyncWeb3 instance
            chain: Chain name (arbitrum, optimism, base)

        Returns:
            Tuple of (l1_base_fee_gwei, l1_data_cost_gwei)
        """
        try:
            if chain == "arbitrum":
                return await self._fetch_arbitrum_l1_cost(web3)
            elif chain in ("optimism", "base"):
                return await self._fetch_optimism_l1_cost(web3)
            else:
                return None, None
        except Exception as e:
            logger.warning(
                "Failed to fetch L1 data cost for %s: %s",
                chain,
                str(e),
            )
            # Return None for L1 costs on error - the L2 gas price is still valid
            return None, None

    async def _fetch_arbitrum_l1_cost(self, web3: AsyncWeb3) -> tuple[Decimal | None, Decimal | None]:
        """Fetch L1 data cost from Arbitrum ArbGasInfo precompile.

        Args:
            web3: AsyncWeb3 instance

        Returns:
            Tuple of (l1_base_fee_gwei, l1_data_cost_gwei)
        """
        contract_address = L2_GAS_ORACLE_ADDRESSES["arbitrum"]
        contract = web3.eth.contract(
            address=web3.to_checksum_address(contract_address),
            abi=ARBITRUM_GAS_INFO_ABI,
        )

        # Get L1 base fee estimate
        l1_base_fee_wei = await asyncio.wait_for(
            contract.functions.getL1BaseFeeEstimate().call(),
            timeout=self._request_timeout,
        )

        l1_base_fee_gwei = Decimal(l1_base_fee_wei) / Decimal(10**9)

        # Get L1 data cost per byte and estimate for a typical transaction
        # A typical swap transaction is ~500-700 bytes, use 600 as estimate
        prices = await asyncio.wait_for(
            contract.functions.getPricesInWei().call(),
            timeout=self._request_timeout,
        )
        l1_calldata_price_per_byte = prices[1]  # per L1 calldata byte

        # Estimate L1 data cost for a typical ~600 byte transaction
        estimated_tx_size = 600
        l1_data_cost_wei = l1_calldata_price_per_byte * estimated_tx_size
        l1_data_cost_gwei = Decimal(l1_data_cost_wei) / Decimal(10**9)

        return l1_base_fee_gwei, l1_data_cost_gwei

    async def _fetch_optimism_l1_cost(self, web3: AsyncWeb3) -> tuple[Decimal | None, Decimal | None]:
        """Fetch L1 data cost from Optimism GasPriceOracle.

        Args:
            web3: AsyncWeb3 instance

        Returns:
            Tuple of (l1_base_fee_gwei, l1_data_cost_gwei)
        """
        # Use optimism address (same for base - OP Stack standard)
        contract_address = L2_GAS_ORACLE_ADDRESSES["optimism"]
        contract = web3.eth.contract(
            address=web3.to_checksum_address(contract_address),
            abi=OPTIMISM_GAS_PRICE_ORACLE_ABI,
        )

        # Get L1 base fee
        l1_base_fee_wei = await asyncio.wait_for(
            contract.functions.l1BaseFee().call(),
            timeout=self._request_timeout,
        )
        l1_base_fee_gwei = Decimal(l1_base_fee_wei) / Decimal(10**9)

        # Get scalar and overhead for L1 data cost calculation
        # L1 data cost = (tx_data_length + overhead) * l1_base_fee * scalar / 1e6
        try:
            scalar = await asyncio.wait_for(
                contract.functions.scalar().call(),
                timeout=self._request_timeout,
            )
            overhead = await asyncio.wait_for(
                contract.functions.overhead().call(),
                timeout=self._request_timeout,
            )

            # Estimate for a typical ~600 byte transaction
            estimated_tx_size = 600
            l1_data_cost_wei = (estimated_tx_size + overhead) * l1_base_fee_wei * scalar // 10**6
            l1_data_cost_gwei = Decimal(l1_data_cost_wei) / Decimal(10**9)

        except Exception as e:
            # Newer Bedrock contracts may use baseFeeScalar instead
            logger.debug(
                "Legacy scalar method failed, trying Bedrock baseFeeScalar: %s",
                str(e),
            )
            try:
                base_fee_scalar = await asyncio.wait_for(
                    contract.functions.baseFeeScalar().call(),
                    timeout=self._request_timeout,
                )

                # Simplified calculation for Bedrock
                # L1 data cost ≈ tx_size * 16 * l1_base_fee * base_fee_scalar / 1e6
                estimated_tx_size = 600
                l1_data_cost_wei = estimated_tx_size * 16 * l1_base_fee_wei * base_fee_scalar // 10**6
                l1_data_cost_gwei = Decimal(l1_data_cost_wei) / Decimal(10**9)

            except Exception:
                # If all methods fail, estimate conservatively
                # Use 10x the L1 base fee as a rough estimate
                l1_data_cost_gwei = l1_base_fee_gwei * Decimal("10")

        return l1_base_fee_gwei, l1_data_cost_gwei

    async def _calculate_usd_cost(
        self,
        max_fee_wei: int,
        l1_data_cost_wei: int = 0,
    ) -> Decimal:
        """Calculate estimated transaction cost in USD.

        Args:
            max_fee_wei: Maximum fee per gas in wei
            l1_data_cost_wei: L1 data cost in wei (for L2 chains)

        Returns:
            Estimated cost in USD for a standard 21000 gas transaction
        """
        if self._price_oracle is None:
            return Decimal("0")

        try:
            # Get ETH price
            eth_price_result = await self._price_oracle.get_aggregated_price("ETH", "USD")
            eth_price = eth_price_result.price

            # Calculate L2 execution cost in ETH
            l2_cost_wei = max_fee_wei * STANDARD_GAS_UNITS
            total_cost_wei = l2_cost_wei + l1_data_cost_wei

            # Convert to ETH
            cost_eth = Decimal(total_cost_wei) / Decimal(10**18)

            # Convert to USD
            cost_usd = cost_eth * eth_price

            return cost_usd.quantize(Decimal("0.0001"))  # Round to 4 decimal places

        except Exception as e:
            logger.warning(
                "Failed to calculate USD cost: %s",
                str(e),
            )
            return Decimal("0")


__all__ = [
    "GasPrice",
    "GasOracle",
    "Web3GasOracle",
    "L2_GAS_ORACLE_ADDRESSES",
    "L2_CHAINS",
    "STANDARD_GAS_UNITS",
]
