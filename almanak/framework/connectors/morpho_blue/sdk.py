"""Morpho Blue SDK Core Functions.

This module provides low-level SDK functions for Morpho Blue operations:
- On-chain position reading
- Market state queries
- Market discovery via events
- Oracle price queries

The SDK handles direct RPC interactions, allowing the adapter to focus
on business logic and transaction building.

Example:
    from almanak.framework.connectors.morpho_blue.sdk import MorphoBlueSDK

    sdk = MorphoBlueSDK(chain="ethereum")

    # Get user position
    position = sdk.get_position(market_id, user_address)

    # Get market state
    state = sdk.get_market_state(market_id)

    # Discover all markets
    markets = sdk.discover_markets()
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3
from web3.exceptions import ContractLogicError
from web3.types import HexStr

from almanak.core.contracts import MORPHO_BLUE_ADDRESS
from almanak.framework.utils.rpc_provider import get_rpc_url, is_poa_chain

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Supported chains
SUPPORTED_CHAINS = {"ethereum", "base"}

# Morpho Blue function selectors for view functions
# position(bytes32 id, address user) -> (uint256 supplyShares, uint128 borrowShares, uint128 collateral)
POSITION_SELECTOR = "0x93c52062"  # keccak256("position(bytes32,address)")[:4]

# market(bytes32 id) -> (uint128 totalSupplyAssets, uint128 totalSupplyShares, uint128 totalBorrowAssets, uint128 totalBorrowShares, uint128 lastUpdate, uint128 fee)
MARKET_SELECTOR = "0x5c60e39a"

# idToMarketParams(bytes32 id) -> (address loanToken, address collateralToken, address oracle, address irm, uint256 lltv)
ID_TO_MARKET_PARAMS_SELECTOR = "0x2c3c9157"

# CreateMarket event topic for market discovery
CREATE_MARKET_EVENT_TOPIC = HexStr("0xac4b2400f169220b0c0afdde7a0b32e775ba727ea1cb30b35f935cdaab8683ac")

# Morpho Blue contract deployment blocks per chain (for efficient event scanning)
MORPHO_DEPLOYMENT_BLOCKS: dict[str, int] = {
    "ethereum": 18883124,  # Dec 2023
    "base": 18883124,  # Approximate
}

# Max uint values
MAX_UINT128 = 2**128 - 1
MAX_UINT256 = 2**256 - 1

# Scale factors
SHARES_SCALE = 10**18  # Shares use 18 decimals internally
LLTV_SCALE = 10**18  # LLTV is in 1e18 format (e.g., 0.86e18 = 86%)


# =============================================================================
# Exceptions
# =============================================================================


class MorphoBlueSDKError(Exception):
    """Base exception for Morpho Blue SDK errors."""

    pass


class MarketNotFoundError(MorphoBlueSDKError):
    """Market does not exist or has not been created."""

    def __init__(self, market_id: str) -> None:
        self.market_id = market_id
        super().__init__(f"Market not found: {market_id}")


class PositionNotFoundError(MorphoBlueSDKError):
    """Position does not exist for user in market."""

    def __init__(self, market_id: str, user: str) -> None:
        self.market_id = market_id
        self.user = user
        super().__init__(f"No position found for {user} in market {market_id}")


class UnsupportedChainError(MorphoBlueSDKError):
    """Chain is not supported by Morpho Blue."""

    def __init__(self, chain: str) -> None:
        self.chain = chain
        super().__init__(f"Unsupported chain: {chain}. Supported chains: {SUPPORTED_CHAINS}")


class RPCError(MorphoBlueSDKError):
    """Error making RPC call."""

    def __init__(self, message: str, method: str) -> None:
        self.method = method
        super().__init__(f"RPC error in {method}: {message}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SDKPosition:
    """User position in a Morpho Blue market (from on-chain read).

    Attributes:
        market_id: Market identifier
        user: User address
        supply_shares: User's supply shares (1e18 scale)
        borrow_shares: User's borrow shares (1e18 scale)
        collateral: User's collateral amount (in token units)
    """

    market_id: str
    user: str
    supply_shares: int
    borrow_shares: int
    collateral: int

    @property
    def has_supply(self) -> bool:
        """Check if user has supply position."""
        return self.supply_shares > 0

    @property
    def has_borrow(self) -> bool:
        """Check if user has borrow position."""
        return self.borrow_shares > 0

    @property
    def has_collateral(self) -> bool:
        """Check if user has collateral."""
        return self.collateral > 0

    @property
    def is_empty(self) -> bool:
        """Check if position is empty."""
        return not (self.has_supply or self.has_borrow or self.has_collateral)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "user": self.user,
            "supply_shares": self.supply_shares,
            "borrow_shares": self.borrow_shares,
            "collateral": self.collateral,
            "has_supply": self.has_supply,
            "has_borrow": self.has_borrow,
            "has_collateral": self.has_collateral,
        }


@dataclass
class SDKMarketState:
    """Current state of a Morpho Blue market (from on-chain read).

    Attributes:
        market_id: Market identifier
        total_supply_assets: Total assets supplied to the market
        total_supply_shares: Total supply shares
        total_borrow_assets: Total assets borrowed
        total_borrow_shares: Total borrow shares
        last_update: Timestamp of last interest accrual
        fee: Protocol fee (1e18 scale, e.g., 0.1e18 = 10%)
    """

    market_id: str
    total_supply_assets: int
    total_supply_shares: int
    total_borrow_assets: int
    total_borrow_shares: int
    last_update: int
    fee: int

    @property
    def utilization(self) -> Decimal:
        """Calculate market utilization rate (0-1 scale)."""
        if self.total_supply_assets == 0:
            return Decimal("0")
        return Decimal(self.total_borrow_assets) / Decimal(self.total_supply_assets)

    @property
    def utilization_percent(self) -> Decimal:
        """Utilization as percentage (0-100)."""
        return self.utilization * 100

    @property
    def available_liquidity(self) -> int:
        """Available liquidity for borrowing."""
        return max(0, self.total_supply_assets - self.total_borrow_assets)

    @property
    def fee_percent(self) -> Decimal:
        """Fee as percentage."""
        return Decimal(self.fee) / Decimal(LLTV_SCALE) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "total_supply_assets": self.total_supply_assets,
            "total_supply_shares": self.total_supply_shares,
            "total_borrow_assets": self.total_borrow_assets,
            "total_borrow_shares": self.total_borrow_shares,
            "last_update": self.last_update,
            "fee": self.fee,
            "utilization": str(self.utilization),
            "available_liquidity": self.available_liquidity,
        }


@dataclass
class SDKMarketParams:
    """Market parameters for a Morpho Blue market (from on-chain read).

    These parameters uniquely identify a market. The market_id is derived as:
    keccak256(abi.encode(loanToken, collateralToken, oracle, irm, lltv))

    Attributes:
        market_id: Market identifier (bytes32 as hex string)
        loan_token: Address of the asset being borrowed
        collateral_token: Address of the collateral asset
        oracle: Address of the price oracle
        irm: Address of the interest rate model
        lltv: Liquidation LTV (1e18 scale, e.g., 0.86e18 = 86%)
    """

    market_id: str
    loan_token: str
    collateral_token: str
    oracle: str
    irm: str
    lltv: int

    @property
    def lltv_percent(self) -> Decimal:
        """LLTV as percentage (0-100)."""
        return Decimal(self.lltv) / Decimal(LLTV_SCALE) * 100

    @property
    def lltv_decimal(self) -> Decimal:
        """LLTV as decimal (0-1)."""
        return Decimal(self.lltv) / Decimal(LLTV_SCALE)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "market_id": self.market_id,
            "loan_token": self.loan_token,
            "collateral_token": self.collateral_token,
            "oracle": self.oracle,
            "irm": self.irm,
            "lltv": self.lltv,
            "lltv_percent": float(self.lltv_percent),
        }


@dataclass
class SDKMarketInfo:
    """Complete market information combining state and params."""

    params: SDKMarketParams
    state: SDKMarketState

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "params": self.params.to_dict(),
            "state": self.state.to_dict(),
        }


# =============================================================================
# SDK Class
# =============================================================================


class MorphoBlueSDK:
    """Low-level SDK for Morpho Blue protocol interactions.

    This SDK handles direct RPC calls to read on-chain state from Morpho Blue.
    It uses raw calldata encoding for efficiency and minimal dependencies.

    Example:
        sdk = MorphoBlueSDK(chain="ethereum")

        # Get user position
        position = sdk.get_position(market_id, user_address)
        print(f"Supply shares: {position.supply_shares}")
        print(f"Borrow shares: {position.borrow_shares}")

        # Get market state
        state = sdk.get_market_state(market_id)
        print(f"Utilization: {state.utilization_percent}%")

        # Discover markets
        market_ids = sdk.discover_markets()
    """

    def __init__(
        self,
        chain: str = "ethereum",
        rpc_url: str | None = None,
    ) -> None:
        """Initialize the SDK.

        Args:
            chain: Chain name (ethereum, base)
            rpc_url: Optional RPC URL. If not provided, uses ALCHEMY_API_KEY.

        Raises:
            UnsupportedChainError: If chain is not supported
        """
        if chain.lower() not in SUPPORTED_CHAINS:
            raise UnsupportedChainError(chain)

        self.chain = chain.lower()
        self.rpc_url = rpc_url or get_rpc_url(self.chain)
        self.morpho_address = Web3.to_checksum_address(MORPHO_BLUE_ADDRESS)

        # Initialize Web3
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))

        # Add POA middleware if needed (for chains like Avalanche, BSC)
        if is_poa_chain(self.chain):
            try:
                from web3.middleware import ExtraDataToPOAMiddleware as poa_middleware
            except ImportError:
                from web3.middleware import geth_poa_middleware as poa_middleware  # type: ignore[attr-defined,no-redef]

            self.w3.middleware_onion.inject(poa_middleware, layer=0)

        # Verify connection
        if not self.w3.is_connected():
            raise RPCError("Failed to connect to RPC", "init")

        logger.info(f"MorphoBlueSDK initialized for chain={self.chain}, rpc={'custom' if rpc_url else 'alchemy'}")

    # =========================================================================
    # Position Reading
    # =========================================================================

    def get_position(self, market_id: str, user: str) -> SDKPosition:
        """Get user position in a market.

        Calls the `position(bytes32 id, address user)` view function.

        Args:
            market_id: Market identifier (bytes32 hex string)
            user: User address

        Returns:
            SDKPosition with supply_shares, borrow_shares, collateral

        Raises:
            RPCError: If RPC call fails
        """
        try:
            market_id_bytes = self._normalize_market_id(market_id)
            user_address = Web3.to_checksum_address(user)

            # Build calldata: position(bytes32, address)
            calldata = (
                POSITION_SELECTOR
                + market_id_bytes[2:]  # Remove 0x prefix
                + self._pad_address(user_address)
            )

            result = self._eth_call(calldata)

            # Decode: (uint256 supplyShares, uint128 borrowShares, uint128 collateral)
            # ABI returns each value padded to 32 bytes (64 hex chars):
            # - Word 0 [0:64]: supplyShares (uint256)
            # - Word 1 [64:128]: borrowShares (uint128 padded)
            # - Word 2 [128:192]: collateral (uint128 padded)
            supply_shares = int(result[0:64], 16)
            borrow_shares = int(result[64:128], 16)
            collateral = int(result[128:192], 16)

            return SDKPosition(
                market_id=market_id_bytes,
                user=user_address,
                supply_shares=supply_shares,
                borrow_shares=borrow_shares,
                collateral=collateral,
            )

        except ContractLogicError as e:
            raise RPCError(str(e), "get_position") from e
        except Exception as e:
            logger.exception(f"Error getting position: {e}")
            raise RPCError(str(e), "get_position") from e

    def get_supply_shares(self, market_id: str, user: str) -> int:
        """Get user's supply shares in a market.

        Args:
            market_id: Market identifier
            user: User address

        Returns:
            Supply shares (1e18 scale)
        """
        position = self.get_position(market_id, user)
        return position.supply_shares

    def get_borrow_shares(self, market_id: str, user: str) -> int:
        """Get user's borrow shares in a market.

        Args:
            market_id: Market identifier
            user: User address

        Returns:
            Borrow shares (1e18 scale)
        """
        position = self.get_position(market_id, user)
        return position.borrow_shares

    def get_collateral(self, market_id: str, user: str) -> int:
        """Get user's collateral in a market.

        Args:
            market_id: Market identifier
            user: User address

        Returns:
            Collateral amount (in token units)
        """
        position = self.get_position(market_id, user)
        return position.collateral

    # =========================================================================
    # Market State Reading
    # =========================================================================

    def get_market_state(self, market_id: str) -> SDKMarketState:
        """Get current state of a market.

        Calls the `market(bytes32 id)` view function.

        Args:
            market_id: Market identifier (bytes32 hex string)

        Returns:
            SDKMarketState with totals and utilization

        Raises:
            MarketNotFoundError: If market doesn't exist
            RPCError: If RPC call fails
        """
        try:
            market_id_bytes = self._normalize_market_id(market_id)

            # Build calldata: market(bytes32)
            calldata = MARKET_SELECTOR + market_id_bytes[2:]

            result = self._eth_call(calldata)

            # Decode: (uint128 totalSupplyAssets, uint128 totalSupplyShares,
            #          uint128 totalBorrowAssets, uint128 totalBorrowShares,
            #          uint128 lastUpdate, uint128 fee)
            # Morpho packs these as 6 uint128 values in 3 uint256 words
            total_supply_assets = int(result[0:32], 16)  # Upper 128 bits of word 0
            total_supply_shares = int(result[32:64], 16)  # Lower 128 bits of word 0
            total_borrow_assets = int(result[64:96], 16)  # Upper 128 bits of word 1
            total_borrow_shares = int(result[96:128], 16)  # Lower 128 bits of word 1
            last_update = int(result[128:160], 16)  # Upper 128 bits of word 2
            fee = int(result[160:192], 16)  # Lower 128 bits of word 2

            # Check if market exists (all zeros = not created)
            if (
                total_supply_assets == 0
                and total_supply_shares == 0
                and total_borrow_assets == 0
                and total_borrow_shares == 0
                and last_update == 0
            ):
                # Could be a new market with no activity - check params
                try:
                    self.get_market_params(market_id)
                except MarketNotFoundError:
                    raise MarketNotFoundError(market_id) from None

            return SDKMarketState(
                market_id=market_id_bytes,
                total_supply_assets=total_supply_assets,
                total_supply_shares=total_supply_shares,
                total_borrow_assets=total_borrow_assets,
                total_borrow_shares=total_borrow_shares,
                last_update=last_update,
                fee=fee,
            )

        except MarketNotFoundError:
            raise
        except ContractLogicError as e:
            raise RPCError(str(e), "get_market_state") from e
        except Exception as e:
            logger.exception(f"Error getting market state: {e}")
            raise RPCError(str(e), "get_market_state") from e

    def get_market_params(self, market_id: str) -> SDKMarketParams:
        """Get market parameters.

        Calls the `idToMarketParams(bytes32 id)` view function.

        Args:
            market_id: Market identifier (bytes32 hex string)

        Returns:
            SDKMarketParams with token addresses, oracle, IRM, and LLTV

        Raises:
            MarketNotFoundError: If market doesn't exist
            RPCError: If RPC call fails
        """
        try:
            market_id_bytes = self._normalize_market_id(market_id)

            # Build calldata: idToMarketParams(bytes32)
            calldata = ID_TO_MARKET_PARAMS_SELECTOR + market_id_bytes[2:]

            result = self._eth_call(calldata)

            # Decode: (address loanToken, address collateralToken, address oracle, address irm, uint256 lltv)
            loan_token = self._decode_address(result[0:64])
            collateral_token = self._decode_address(result[64:128])
            oracle = self._decode_address(result[128:192])
            irm = self._decode_address(result[192:256])
            lltv = int(result[256:320], 16)

            # Check if market exists (zero loan token = not created)
            if loan_token == "0x0000000000000000000000000000000000000000":
                raise MarketNotFoundError(market_id)

            return SDKMarketParams(
                market_id=market_id_bytes,
                loan_token=loan_token,
                collateral_token=collateral_token,
                oracle=oracle,
                irm=irm,
                lltv=lltv,
            )

        except MarketNotFoundError:
            raise
        except ContractLogicError as e:
            raise RPCError(str(e), "get_market_params") from e
        except Exception as e:
            logger.exception(f"Error getting market params: {e}")
            raise RPCError(str(e), "get_market_params") from e

    def get_market_info(self, market_id: str) -> SDKMarketInfo:
        """Get complete market information (params + state).

        Args:
            market_id: Market identifier

        Returns:
            SDKMarketInfo with both params and state
        """
        params = self.get_market_params(market_id)
        state = self.get_market_state(market_id)
        return SDKMarketInfo(params=params, state=state)

    # =========================================================================
    # Market Discovery
    # =========================================================================

    def discover_markets(
        self,
        from_block: int | None = None,
        to_block: int | str = "latest",
        chunk_size: int = 10_000,
    ) -> list[str]:
        """Discover all markets by scanning CreateMarket events.

        Scans the blockchain for CreateMarket events to find all market IDs.
        Automatically chunks requests to stay within RPC provider block range limits.

        Args:
            from_block: Starting block (defaults to Morpho deployment block)
            to_block: Ending block (defaults to "latest")
            chunk_size: Max blocks per eth_getLogs request (default 10,000 for Alchemy compatibility)

        Returns:
            List of market IDs (bytes32 hex strings)

        Raises:
            RPCError: If event scanning fails
        """
        try:
            if from_block is None:
                from_block = MORPHO_DEPLOYMENT_BLOCKS.get(self.chain, 0)

            # Resolve "latest" to a concrete block number for chunking
            if to_block == "latest":
                resolved_to = int(self.w3.eth.block_number)
            else:
                resolved_to = int(to_block)

            # Chunk eth_getLogs to stay within RPC provider limits
            all_logs = []
            current = from_block
            while current <= resolved_to:
                chunk_end = min(current + chunk_size - 1, resolved_to)
                logs = self.w3.eth.get_logs(
                    {
                        "address": self.morpho_address,
                        "topics": [CREATE_MARKET_EVENT_TOPIC],
                        "fromBlock": current,
                        "toBlock": chunk_end,
                    }
                )
                all_logs.extend(logs)
                current = chunk_end + 1

            # Extract market IDs from indexed topic
            market_ids = []
            for log in all_logs:
                if len(log["topics"]) >= 2:
                    market_id = "0x" + log["topics"][1].hex()
                    market_ids.append(market_id)

            logger.info(f"Discovered {len(market_ids)} markets on {self.chain} from block {from_block}")

            return market_ids

        except Exception as e:
            logger.exception(f"Error discovering markets: {e}")
            raise RPCError(str(e), "discover_markets") from e

    def get_market_count(self) -> int:
        """Get the total number of markets created.

        Returns:
            Number of markets discovered
        """
        markets = self.discover_markets()
        return len(markets)

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_block_number(self) -> int:
        """Get current block number.

        Returns:
            Current block number
        """
        return self.w3.eth.block_number

    def get_chain_id(self) -> int:
        """Get chain ID.

        Returns:
            Chain ID
        """
        return self.w3.eth.chain_id

    def is_connected(self) -> bool:
        """Check if connected to RPC.

        Returns:
            True if connected
        """
        return self.w3.is_connected()

    # =========================================================================
    # Shares/Assets Conversion
    # =========================================================================

    def shares_to_assets(
        self,
        shares: int,
        total_assets: int,
        total_shares: int,
    ) -> int:
        """Convert shares to assets.

        Uses the Morpho formula: assets = shares * totalAssets / totalShares

        Args:
            shares: Number of shares
            total_assets: Total assets in market
            total_shares: Total shares in market

        Returns:
            Asset amount
        """
        if total_shares == 0:
            return 0
        return (shares * total_assets) // total_shares

    def assets_to_shares(
        self,
        assets: int,
        total_assets: int,
        total_shares: int,
    ) -> int:
        """Convert assets to shares.

        Uses the Morpho formula: shares = assets * totalShares / totalAssets

        Args:
            assets: Asset amount
            total_assets: Total assets in market
            total_shares: Total shares in market

        Returns:
            Number of shares
        """
        if total_assets == 0:
            return 0
        return (assets * total_shares) // total_assets

    def get_supply_assets(self, market_id: str, user: str) -> int:
        """Get user's supply amount in assets (not shares).

        Args:
            market_id: Market identifier
            user: User address

        Returns:
            Supply amount in asset units
        """
        position = self.get_position(market_id, user)
        state = self.get_market_state(market_id)

        return self.shares_to_assets(
            position.supply_shares,
            state.total_supply_assets,
            state.total_supply_shares,
        )

    def get_borrow_assets(self, market_id: str, user: str) -> int:
        """Get user's borrow amount in assets (not shares).

        Args:
            market_id: Market identifier
            user: User address

        Returns:
            Borrow amount in asset units
        """
        position = self.get_position(market_id, user)
        state = self.get_market_state(market_id)

        return self.shares_to_assets(
            position.borrow_shares,
            state.total_borrow_assets,
            state.total_borrow_shares,
        )

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _eth_call(self, calldata: str) -> str:
        """Make an eth_call to the Morpho Blue contract.

        Args:
            calldata: Hex-encoded calldata (with selector)

        Returns:
            Hex-encoded result (without 0x prefix)
        """
        result = self.w3.eth.call(
            {
                "to": self.morpho_address,
                "data": HexStr(calldata),
            }
        )
        # Return hex without 0x prefix
        return result.hex()

    def _normalize_market_id(self, market_id: str) -> str:
        """Normalize market ID to lowercase 0x-prefixed 66-char hex string.

        Args:
            market_id: Market ID (with or without 0x prefix)

        Returns:
            Normalized market ID
        """
        if not market_id.startswith("0x"):
            market_id = "0x" + market_id
        market_id = market_id.lower()

        # Ensure 32 bytes (64 hex chars + 0x prefix)
        if len(market_id) != 66:
            # Pad with zeros if needed
            market_id = "0x" + market_id[2:].zfill(64)

        return market_id

    def _pad_address(self, address: str) -> str:
        """Pad address to 32 bytes for calldata.

        Args:
            address: Ethereum address

        Returns:
            64-char hex string (without 0x prefix)
        """
        addr = address.lower().replace("0x", "")
        return addr.zfill(64)

    def _decode_address(self, hex_str: str) -> str:
        """Decode address from 32-byte hex string.

        Args:
            hex_str: 64-char hex string

        Returns:
            Checksummed address
        """
        # Address is in the last 20 bytes (40 hex chars)
        addr_hex = hex_str[-40:]
        return Web3.to_checksum_address("0x" + addr_hex)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # SDK
    "MorphoBlueSDK",
    # Data classes
    "SDKPosition",
    "SDKMarketState",
    "SDKMarketParams",
    "SDKMarketInfo",
    # Exceptions
    "MorphoBlueSDKError",
    "MarketNotFoundError",
    "PositionNotFoundError",
    "UnsupportedChainError",
    "RPCError",
    # Constants
    "MORPHO_BLUE_ADDRESS",
    "SUPPORTED_CHAINS",
    "MORPHO_DEPLOYMENT_BLOCKS",
]
