"""Position querying utilities for Paper Trader.

This module provides functions to query on-chain positions from various protocols
(Uniswap V3, GMX, Aave, etc.) for use in paper trading simulations.

These utilities allow Paper Trader to:
1. Sync with on-chain state at session start
2. Reconcile tracked positions vs actual on-chain state
3. Query position details for accurate P&L calculation

Example:
    from web3 import Web3
    from almanak.framework.backtesting.paper.position_queries import (
        query_uniswap_v3_positions,
    )

    web3 = Web3(Web3.HTTPProvider("https://arb1.arbitrum.io/rpc"))
    wallet = "0x..."

    # Query all Uniswap V3 LP positions
    positions = await query_uniswap_v3_positions(wallet, web3, chain="arbitrum")
    for pos in positions:
        print(f"Position #{pos.token_id}: {pos.liquidity} liquidity")
"""

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# NonfungiblePositionManager addresses per chain
UNISWAP_V3_POSITION_MANAGER: dict[str, str] = {
    "ethereum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "arbitrum": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "optimism": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "polygon": "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    "base": "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1",
}

# Function selectors for NonfungiblePositionManager
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)
TOKEN_OF_OWNER_BY_INDEX_SELECTOR = "0x2f745c59"  # tokenOfOwnerByIndex(address,uint256)
POSITIONS_SELECTOR = "0x99fbab88"  # positions(uint256)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class UniswapV3Position:
    """Data class representing a Uniswap V3 LP position.

    Attributes:
        token_id: NFT token ID for this position
        nonce: Position nonce for permit signatures
        operator: Approved operator address (or zero address)
        token0: Address of token0 in the pool
        token1: Address of token1 in the pool
        fee: Pool fee tier (100, 500, 3000, 10000)
        tick_lower: Lower tick boundary of the position
        tick_upper: Upper tick boundary of the position
        liquidity: Current liquidity in the position
        fee_growth_inside0_last_x128: Fee growth for token0 at last interaction
        fee_growth_inside1_last_x128: Fee growth for token1 at last interaction
        tokens_owed0: Uncollected token0 (fees + withdrawn liquidity)
        tokens_owed1: Uncollected token1 (fees + withdrawn liquidity)
    """

    token_id: int
    nonce: int
    operator: str
    token0: str
    token1: str
    fee: int
    tick_lower: int
    tick_upper: int
    liquidity: int
    fee_growth_inside0_last_x128: int
    fee_growth_inside1_last_x128: int
    tokens_owed0: int
    tokens_owed1: int

    @property
    def is_active(self) -> bool:
        """Check if the position has liquidity."""
        return self.liquidity > 0

    @property
    def has_uncollected_fees(self) -> bool:
        """Check if there are uncollected tokens."""
        return self.tokens_owed0 > 0 or self.tokens_owed1 > 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "token_id": self.token_id,
            "nonce": self.nonce,
            "operator": self.operator,
            "token0": self.token0,
            "token1": self.token1,
            "fee": self.fee,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "liquidity": str(self.liquidity),
            "fee_growth_inside0_last_x128": str(self.fee_growth_inside0_last_x128),
            "fee_growth_inside1_last_x128": str(self.fee_growth_inside1_last_x128),
            "tokens_owed0": str(self.tokens_owed0),
            "tokens_owed1": str(self.tokens_owed1),
            "is_active": self.is_active,
            "has_uncollected_fees": self.has_uncollected_fees,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UniswapV3Position":
        """Create from dictionary representation."""
        return cls(
            token_id=data["token_id"],
            nonce=data["nonce"],
            operator=data["operator"],
            token0=data["token0"],
            token1=data["token1"],
            fee=data["fee"],
            tick_lower=data["tick_lower"],
            tick_upper=data["tick_upper"],
            liquidity=int(data["liquidity"]),
            fee_growth_inside0_last_x128=int(data["fee_growth_inside0_last_x128"]),
            fee_growth_inside1_last_x128=int(data["fee_growth_inside1_last_x128"]),
            tokens_owed0=int(data["tokens_owed0"]),
            tokens_owed1=int(data["tokens_owed1"]),
        )


# =============================================================================
# Position Querying Functions
# =============================================================================


async def query_uniswap_v3_positions(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    position_manager: str | None = None,
) -> list[UniswapV3Position]:
    """Query all Uniswap V3 LP positions for a wallet.

    This function queries the NonfungiblePositionManager contract to enumerate
    all LP positions owned by the wallet and extract their details.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier (ethereum, arbitrum, optimism, polygon, base)
        position_manager: Optional custom position manager address
            (defaults to Uniswap V3 NonfungiblePositionManager for the chain)

    Returns:
        List of UniswapV3Position objects for each position owned by the wallet

    Raises:
        ValueError: If chain is not supported and no position_manager provided

    Example:
        from web3 import Web3
        web3 = Web3(Web3.HTTPProvider("https://arb1.arbitrum.io/rpc"))

        positions = await query_uniswap_v3_positions(
            wallet="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            web3=web3,
            chain="arbitrum",
        )

        for pos in positions:
            print(f"Position #{pos.token_id}:")
            print(f"  Tokens: {pos.token0} / {pos.token1}")
            print(f"  Fee tier: {pos.fee / 10000}%")
            print(f"  Tick range: [{pos.tick_lower}, {pos.tick_upper}]")
            print(f"  Liquidity: {pos.liquidity}")
    """
    # Get position manager address
    if position_manager is None:
        if chain not in UNISWAP_V3_POSITION_MANAGER:
            raise ValueError(
                f"Unsupported chain: {chain}. "
                f"Supported chains: {list(UNISWAP_V3_POSITION_MANAGER.keys())}. "
                "Provide position_manager address for other chains."
            )
        position_manager = UNISWAP_V3_POSITION_MANAGER[chain]

    # Normalize addresses
    wallet_checksum = web3.to_checksum_address(wallet)
    position_manager_checksum = web3.to_checksum_address(position_manager)

    positions: list[UniswapV3Position] = []

    # Step 1: Get the number of positions owned by the wallet
    balance = await _query_balance_of(web3, position_manager_checksum, wallet_checksum)
    if balance == 0:
        logger.debug(f"Wallet {wallet} has no Uniswap V3 positions on {chain}")
        return positions

    logger.info(f"Found {balance} Uniswap V3 position(s) for {wallet} on {chain}")

    # Step 2: Enumerate each position token ID
    for index in range(balance):
        token_id = await _query_token_of_owner_by_index(web3, position_manager_checksum, wallet_checksum, index)
        if token_id is None:
            logger.warning(f"Failed to query token ID at index {index}")
            continue

        # Step 3: Query position details for this token ID
        position = await _query_position(web3, position_manager_checksum, token_id)
        if position is not None:
            positions.append(position)
            logger.debug(
                f"Position #{token_id}: liquidity={position.liquidity}, "
                f"fee={position.fee}, range=[{position.tick_lower}, {position.tick_upper}]"
            )

    return positions


def query_uniswap_v3_positions_sync(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    position_manager: str | None = None,
) -> list[UniswapV3Position]:
    """Synchronous version of query_uniswap_v3_positions.

    For use in non-async contexts. See query_uniswap_v3_positions for full docs.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier
        position_manager: Optional custom position manager address

    Returns:
        List of UniswapV3Position objects
    """
    # Get position manager address
    if position_manager is None:
        if chain not in UNISWAP_V3_POSITION_MANAGER:
            raise ValueError(
                f"Unsupported chain: {chain}. "
                f"Supported chains: {list(UNISWAP_V3_POSITION_MANAGER.keys())}. "
                "Provide position_manager address for other chains."
            )
        position_manager = UNISWAP_V3_POSITION_MANAGER[chain]

    # Normalize addresses
    wallet_checksum = web3.to_checksum_address(wallet)
    position_manager_checksum = web3.to_checksum_address(position_manager)

    positions: list[UniswapV3Position] = []

    # Step 1: Get the number of positions owned by the wallet
    balance = _query_balance_of_sync(web3, position_manager_checksum, wallet_checksum)
    if balance == 0:
        logger.debug(f"Wallet {wallet} has no Uniswap V3 positions on {chain}")
        return positions

    logger.info(f"Found {balance} Uniswap V3 position(s) for {wallet} on {chain}")

    # Step 2: Enumerate each position token ID
    for index in range(balance):
        token_id = _query_token_of_owner_by_index_sync(web3, position_manager_checksum, wallet_checksum, index)
        if token_id is None:
            logger.warning(f"Failed to query token ID at index {index}")
            continue

        # Step 3: Query position details for this token ID
        position = _query_position_sync(web3, position_manager_checksum, token_id)
        if position is not None:
            positions.append(position)
            logger.debug(
                f"Position #{token_id}: liquidity={position.liquidity}, "
                f"fee={position.fee}, range=[{position.tick_lower}, {position.tick_upper}]"
            )

    return positions


# =============================================================================
# Internal Helper Functions
# =============================================================================


def _pad_address(addr: str) -> str:
    """Pad address to 32 bytes for ABI encoding."""
    return addr.lower().replace("0x", "").zfill(64)


def _pad_uint256(value: int) -> str:
    """Pad uint256 to 32 bytes for ABI encoding."""
    return hex(value)[2:].zfill(64)


def _decode_address(data: bytes, offset: int) -> str:
    """Decode an address from bytes at given offset."""
    return "0x" + data[offset + 12 : offset + 32].hex()


def _decode_uint256(data: bytes, offset: int) -> int:
    """Decode a uint256 from bytes at given offset."""
    return int.from_bytes(data[offset : offset + 32], byteorder="big")


def _decode_int24(data: bytes, offset: int) -> int:
    """Decode an int24 (tick) from bytes at given offset.

    Ticks are stored as int24 but padded to 32 bytes in ABI encoding.
    Need to handle sign extension for negative ticks.
    """
    value = int.from_bytes(data[offset : offset + 32], byteorder="big")
    # Check if the int24 is negative (bit 23 is set)
    if value >= 2**23:
        # Sign extend from 24 bits to full int
        value = value - 2**24
    return value


# Async helper functions


async def _query_balance_of(web3: Any, contract: str, owner: str) -> int:
    """Query ERC-721 balanceOf for owner."""
    calldata = BALANCE_OF_SELECTOR + _pad_address(owner)
    try:
        result = await web3.eth.call({"to": contract, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.error(f"Failed to query balanceOf: {e}")
        return 0


async def _query_token_of_owner_by_index(web3: Any, contract: str, owner: str, index: int) -> int | None:
    """Query tokenOfOwnerByIndex for enumerable ERC-721."""
    calldata = TOKEN_OF_OWNER_BY_INDEX_SELECTOR + _pad_address(owner) + _pad_uint256(index)
    try:
        result = await web3.eth.call({"to": contract, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.error(f"Failed to query tokenOfOwnerByIndex: {e}")
        return None


async def _query_position(web3: Any, contract: str, token_id: int) -> UniswapV3Position | None:
    """Query position details from NonfungiblePositionManager.

    The positions(uint256) function returns:
    - nonce (uint96)
    - operator (address)
    - token0 (address)
    - token1 (address)
    - fee (uint24)
    - tickLower (int24)
    - tickUpper (int24)
    - liquidity (uint128)
    - feeGrowthInside0LastX128 (uint256)
    - feeGrowthInside1LastX128 (uint256)
    - tokensOwed0 (uint128)
    - tokensOwed1 (uint128)
    """
    calldata = POSITIONS_SELECTOR + _pad_uint256(token_id)
    try:
        result = await web3.eth.call({"to": contract, "data": calldata})
        return _parse_position_result(result, token_id)
    except Exception as e:
        logger.error(f"Failed to query position #{token_id}: {e}")
        return None


# Sync helper functions


def _query_balance_of_sync(web3: Any, contract: str, owner: str) -> int:
    """Synchronous version of _query_balance_of."""
    calldata = BALANCE_OF_SELECTOR + _pad_address(owner)
    try:
        result = web3.eth.call({"to": contract, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.error(f"Failed to query balanceOf: {e}")
        return 0


def _query_token_of_owner_by_index_sync(web3: Any, contract: str, owner: str, index: int) -> int | None:
    """Synchronous version of _query_token_of_owner_by_index."""
    calldata = TOKEN_OF_OWNER_BY_INDEX_SELECTOR + _pad_address(owner) + _pad_uint256(index)
    try:
        result = web3.eth.call({"to": contract, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception as e:
        logger.error(f"Failed to query tokenOfOwnerByIndex: {e}")
        return None


def _query_position_sync(web3: Any, contract: str, token_id: int) -> UniswapV3Position | None:
    """Synchronous version of _query_position."""
    calldata = POSITIONS_SELECTOR + _pad_uint256(token_id)
    try:
        result = web3.eth.call({"to": contract, "data": calldata})
        return _parse_position_result(result, token_id)
    except Exception as e:
        logger.error(f"Failed to query position #{token_id}: {e}")
        return None


def _parse_position_result(result: bytes, token_id: int) -> UniswapV3Position | None:
    """Parse the result of a positions(uint256) call.

    Expected ABI-encoded struct (12 fields * 32 bytes = 384 bytes):
    - [0] nonce (uint96) - padded to 32 bytes
    - [1] operator (address) - padded to 32 bytes
    - [2] token0 (address) - padded to 32 bytes
    - [3] token1 (address) - padded to 32 bytes
    - [4] fee (uint24) - padded to 32 bytes
    - [5] tickLower (int24) - padded to 32 bytes
    - [6] tickUpper (int24) - padded to 32 bytes
    - [7] liquidity (uint128) - padded to 32 bytes
    - [8] feeGrowthInside0LastX128 (uint256)
    - [9] feeGrowthInside1LastX128 (uint256)
    - [10] tokensOwed0 (uint128) - padded to 32 bytes
    - [11] tokensOwed1 (uint128) - padded to 32 bytes
    """
    if len(result) < 384:
        logger.warning(f"Unexpected result length for position #{token_id}: {len(result)}")
        return None

    try:
        return UniswapV3Position(
            token_id=token_id,
            nonce=_decode_uint256(result, 0),
            operator=_decode_address(result, 32),
            token0=_decode_address(result, 64),
            token1=_decode_address(result, 96),
            fee=_decode_uint256(result, 128),
            tick_lower=_decode_int24(result, 160),
            tick_upper=_decode_int24(result, 192),
            liquidity=_decode_uint256(result, 224),
            fee_growth_inside0_last_x128=_decode_uint256(result, 256),
            fee_growth_inside1_last_x128=_decode_uint256(result, 288),
            tokens_owed0=_decode_uint256(result, 320),
            tokens_owed1=_decode_uint256(result, 352),
        )
    except Exception as e:
        logger.error(f"Failed to parse position #{token_id} data: {e}")
        return None


# =============================================================================
# GMX V2 Position Querying
# =============================================================================

# GMX V2 contract addresses per chain
# Source: https://github.com/gmx-io/gmx-interface/blob/master/sdk/src/configs/contracts.ts
GMX_V2_READER: dict[str, str] = {
    "arbitrum": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789",  # SyntheticsReader
}

GMX_V2_DATA_STORE: dict[str, str] = {
    "arbitrum": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
}

# GMX V2 markets (index token -> market address)
GMX_V2_MARKETS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "ETH/USD": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
        "BTC/USD": "0x47c031236e19d024b42f8AE6780E44A573170703",
        "LINK/USD": "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        "ARB/USD": "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        "SOL/USD": "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
    },
}

# Common collateral tokens for GMX V2
GMX_V2_COLLATERAL_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC.e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
    },
}

# GMX V2 index token decimals per market
# Most tokens have 18 decimals, but BTC uses 8
GMX_V2_INDEX_TOKEN_DECIMALS: dict[str, dict[str, int]] = {
    "arbitrum": {
        "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336": 18,  # ETH/USD
        "0x47c031236e19d024b42f8AE6780E44A573170703": 8,  # BTC/USD
        "0x7f1fa204bb700853D36994DA19F830b6Ad18455C": 18,  # LINK/USD
        "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407": 18,  # ARB/USD
        "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9": 9,  # SOL/USD
    },
}

# Function selector for getPositionInfo
# getPositionInfo(DataStore dataStore, IReferralStorage referralStorage, bytes32 positionKey,
#                 MarketUtils.MarketPrices memory prices, uint256 sizeDeltaUsd, address uiFeeReceiver,
#                 bool usePositionSizeAsSizeDeltaUsd) returns (PositionInfo memory)
# However, for simpler queries, we use getAccountPositions from Reader
# getAccountPositions(DataStore dataStore, address account, uint256 start, uint256 end) returns (PositionInfo[] memory)
GET_ACCOUNT_POSITIONS_SELECTOR = "0x7fa34c8b"  # getAccountPositions(address,address,uint256,uint256)


@dataclass
class GMXv2Position:
    """Data class representing a GMX V2 perpetual position.

    Attributes:
        position_key: Unique position identifier (keccak256 of account, market, collateral, isLong)
        account: Owner address
        market: Market address (e.g., ETH/USD market)
        collateral_token: Token used as collateral
        size_in_usd: Position size in USD (30 decimals precision)
        size_in_tokens: Position size in index tokens
        collateral_amount: Collateral amount in token decimals
        entry_price: Average entry price (30 decimals precision)
        is_long: True for long position, False for short
        realized_pnl_usd: Realized PnL (30 decimals)
        borrowing_factor: Accumulated borrowing factor
        funding_fee_amount_per_size: Funding fee per size
        long_token_claimable_funding: Claimable funding in long token
        short_token_claimable_funding: Claimable funding in short token
    """

    position_key: str
    account: str
    market: str
    collateral_token: str
    size_in_usd: int  # 30 decimals
    size_in_tokens: int  # Token decimals
    collateral_amount: int  # Token decimals
    entry_price: int  # Derived from size_in_usd / size_in_tokens (30 decimals)
    is_long: bool
    realized_pnl_usd: int = 0  # 30 decimals
    borrowing_factor: int = 0
    funding_fee_amount_per_size: int = 0
    long_token_claimable_funding: int = 0
    short_token_claimable_funding: int = 0

    @property
    def is_active(self) -> bool:
        """Check if the position has size."""
        return self.size_in_usd > 0

    @property
    def size_usd_decimal(self) -> float:
        """Get size in USD as a decimal (dividing by 10^30)."""
        return self.size_in_usd / 10**30

    @property
    def entry_price_decimal(self) -> float:
        """Get entry price as a decimal (dividing by 10^30)."""
        return self.entry_price / 10**30 if self.entry_price > 0 else 0.0

    @property
    def collateral_decimal(self) -> float:
        """Get collateral as a decimal (assumes 6 decimals for USDC)."""
        # Note: This is approximate - actual decimals depend on token
        return self.collateral_amount / 10**6

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "position_key": self.position_key,
            "account": self.account,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "entry_price": str(self.entry_price),
            "is_long": self.is_long,
            "is_active": self.is_active,
            "size_usd_decimal": self.size_usd_decimal,
            "entry_price_decimal": self.entry_price_decimal,
            "collateral_decimal": self.collateral_decimal,
            "realized_pnl_usd": str(self.realized_pnl_usd),
            "borrowing_factor": str(self.borrowing_factor),
            "funding_fee_amount_per_size": str(self.funding_fee_amount_per_size),
            "long_token_claimable_funding": str(self.long_token_claimable_funding),
            "short_token_claimable_funding": str(self.short_token_claimable_funding),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GMXv2Position":
        """Create from dictionary representation."""
        return cls(
            position_key=data["position_key"],
            account=data["account"],
            market=data["market"],
            collateral_token=data["collateral_token"],
            size_in_usd=int(data["size_in_usd"]),
            size_in_tokens=int(data["size_in_tokens"]),
            collateral_amount=int(data["collateral_amount"]),
            entry_price=int(data["entry_price"]),
            is_long=data["is_long"],
            realized_pnl_usd=int(data.get("realized_pnl_usd", "0")),
            borrowing_factor=int(data.get("borrowing_factor", "0")),
            funding_fee_amount_per_size=int(data.get("funding_fee_amount_per_size", "0")),
            long_token_claimable_funding=int(data.get("long_token_claimable_funding", "0")),
            short_token_claimable_funding=int(data.get("short_token_claimable_funding", "0")),
        )


def _compute_position_key(account: str, market: str, collateral_token: str, is_long: bool) -> str:
    """Compute GMX V2 position key.

    Position key = keccak256(abi.encode(account, market, collateralToken, isLong))

    Note: Uses Web3.keccak (Ethereum keccak256) NOT hashlib.sha3_256.
    These are different algorithms despite similar names.

    Args:
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction

    Returns:
        Position key as hex string
    """
    from web3 import Web3

    # ABI encode: address (32 bytes), address (32 bytes), address (32 bytes), bool (32 bytes)
    account_padded = account.lower().replace("0x", "").zfill(64)
    market_padded = market.lower().replace("0x", "").zfill(64)
    collateral_padded = collateral_token.lower().replace("0x", "").zfill(64)
    is_long_padded = "01".zfill(64) if is_long else "00".zfill(64)

    encoded = bytes.fromhex(account_padded + market_padded + collateral_padded + is_long_padded)
    return Web3.keccak(encoded).hex()


async def query_gmx_positions(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    markets: list[str] | None = None,
    collateral_tokens: list[str] | None = None,
) -> list[GMXv2Position]:
    """Query all GMX V2 perpetual positions for a wallet.

    This function queries the GMX V2 Reader contract to get position information
    for a wallet across specified markets and collateral tokens.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier (currently only arbitrum supported)
        markets: List of market addresses to check (defaults to all known markets)
        collateral_tokens: List of collateral token addresses to check (defaults to common tokens)

    Returns:
        List of GMXv2Position objects for each open position

    Raises:
        ValueError: If chain is not supported

    Example:
        from web3 import Web3
        web3 = Web3(Web3.HTTPProvider("https://arb1.arbitrum.io/rpc"))

        positions = await query_gmx_positions(
            wallet="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            web3=web3,
            chain="arbitrum",
        )

        for pos in positions:
            print(f"Position in {pos.market}:")
            print(f"  Size: ${pos.size_usd_decimal:.2f}")
            print(f"  Entry Price: ${pos.entry_price_decimal:.2f}")
            print(f"  Long: {pos.is_long}")
    """
    if chain not in GMX_V2_READER:
        raise ValueError(f"Unsupported chain: {chain}. Supported chains: {list(GMX_V2_READER.keys())}")

    # Use default markets if not specified
    if markets is None:
        markets = list(GMX_V2_MARKETS.get(chain, {}).values())

    # Use default collateral tokens if not specified
    if collateral_tokens is None:
        collateral_tokens = list(GMX_V2_COLLATERAL_TOKENS.get(chain, {}).values())

    positions: list[GMXv2Position] = []
    wallet_checksum = web3.to_checksum_address(wallet)
    data_store = web3.to_checksum_address(GMX_V2_DATA_STORE[chain])

    logger.debug(
        f"Querying GMX V2 positions for {wallet} on {chain}: "
        f"{len(markets)} markets x {len(collateral_tokens)} collaterals x 2 directions"
    )

    # Query each combination of market, collateral, and direction
    for market in markets:
        for collateral in collateral_tokens:
            for is_long in [True, False]:
                position = await _query_gmx_position(
                    web3=web3,
                    data_store=data_store,
                    account=wallet_checksum,
                    market=web3.to_checksum_address(market),
                    collateral_token=web3.to_checksum_address(collateral),
                    is_long=is_long,
                )
                if position is not None and position.is_active:
                    positions.append(position)
                    logger.info(
                        f"Found GMX position: market={market[:10]}..., "
                        f"size=${position.size_usd_decimal:.2f}, "
                        f"is_long={is_long}"
                    )

    if not positions:
        logger.debug(f"Wallet {wallet} has no GMX V2 positions on {chain}")

    return positions


def query_gmx_positions_sync(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    markets: list[str] | None = None,
    collateral_tokens: list[str] | None = None,
) -> list[GMXv2Position]:
    """Synchronous version of query_gmx_positions.

    For use in non-async contexts. See query_gmx_positions for full docs.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier
        markets: List of market addresses to check
        collateral_tokens: List of collateral token addresses to check

    Returns:
        List of GMXv2Position objects
    """
    if chain not in GMX_V2_READER:
        raise ValueError(f"Unsupported chain: {chain}. Supported chains: {list(GMX_V2_READER.keys())}")

    # Use default markets if not specified
    if markets is None:
        markets = list(GMX_V2_MARKETS.get(chain, {}).values())

    # Use default collateral tokens if not specified
    if collateral_tokens is None:
        collateral_tokens = list(GMX_V2_COLLATERAL_TOKENS.get(chain, {}).values())

    positions: list[GMXv2Position] = []
    wallet_checksum = web3.to_checksum_address(wallet)
    data_store = web3.to_checksum_address(GMX_V2_DATA_STORE[chain])

    logger.debug(
        f"Querying GMX V2 positions for {wallet} on {chain}: "
        f"{len(markets)} markets x {len(collateral_tokens)} collaterals x 2 directions"
    )

    # Query each combination of market, collateral, and direction
    for market in markets:
        for collateral in collateral_tokens:
            for is_long in [True, False]:
                position = _query_gmx_position_sync(
                    web3=web3,
                    data_store=data_store,
                    account=wallet_checksum,
                    market=web3.to_checksum_address(market),
                    collateral_token=web3.to_checksum_address(collateral),
                    is_long=is_long,
                )
                if position is not None and position.is_active:
                    positions.append(position)
                    logger.info(
                        f"Found GMX position: market={market[:10]}..., "
                        f"size=${position.size_usd_decimal:.2f}, "
                        f"is_long={is_long}"
                    )

    if not positions:
        logger.debug(f"Wallet {wallet} has no GMX V2 positions on {chain}")

    return positions


# GMX V2 DataStore position key calculation
# In GMX V2, positions are stored in DataStore under specific keys
# The position key format: keccak256(abi.encodePacked("POSITION", account, market, collateralToken, isLong))
# Reference: https://github.com/gmx-io/gmx-synthetics/blob/main/contracts/position/PositionStoreUtils.sol

# Function selector for reading position from DataStore
# getBytes32(bytes32 key) returns (bytes32)
GET_BYTES32_SELECTOR = "0x1d9771c4"  # getBytes32(bytes32)
# getUint(bytes32 key) returns (uint256)
GET_UINT_SELECTOR = "0xc2bc2efc"  # getUint(bytes32)
# getAddress(bytes32 key) returns (address)
GET_ADDRESS_SELECTOR = "0x21f8a721"  # getAddress(bytes32)
# getBool(bytes32 key) returns (bool)
GET_BOOL_SELECTOR = "0x7ae1cfca"  # getBool(bytes32)

# Position storage keys (from GMX V2 PositionStoreUtils.sol)
POSITION_KEY_PREFIX = "0x504f534954494f4e"  # "POSITION" in hex


def _compute_gmx_position_key(account: str, market: str, collateral_token: str, is_long: bool) -> str:
    """Compute GMX V2 position key using the exact GMX formula.

    GMX V2 uses: keccak256(abi.encode(account, market, collateralToken, isLong))

    Args:
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction

    Returns:
        Position key as hex string
    """
    from web3 import Web3

    # ABI encode parameters (each padded to 32 bytes)
    account_padded = account.lower().replace("0x", "").zfill(64)
    market_padded = market.lower().replace("0x", "").zfill(64)
    collateral_padded = collateral_token.lower().replace("0x", "").zfill(64)
    # Boolean is padded to 32 bytes with 0x01 for true, 0x00 for false
    is_long_padded = ("01" if is_long else "00").zfill(64)

    encoded = account_padded + market_padded + collateral_padded + is_long_padded
    position_key = Web3.keccak(hexstr=encoded).hex()
    return position_key


def _compute_position_field_key(position_key: str, field_hash: str) -> str:
    """Compute storage key for a position field.

    GMX V2 stores position fields at: keccak256(abi.encode(positionKey, fieldHash))

    Args:
        position_key: The position key
        field_hash: The field identifier hash

    Returns:
        Storage key for the field
    """
    from web3 import Web3

    # Position key is already 32 bytes, field hash is 32 bytes
    pos_key_clean = position_key.replace("0x", "").zfill(64)
    field_clean = field_hash.replace("0x", "").zfill(64)

    encoded = pos_key_clean + field_clean
    return Web3.keccak(hexstr=encoded).hex()


# GMX V2 position field hashes (from Keys.sol)
# These are keccak256 of the field names: keccak256("SIZE_IN_USD"), keccak256("SIZE_IN_TOKENS")
POSITION_SIZE_IN_USD_HASH = "0xb6993d111455d31cde889f96e4b6bca0ef8ead25ac8506e8ecdb7ef83d0c983d"
POSITION_SIZE_IN_TOKENS_HASH = "0x292fd1532086596b6e4cdb3da497a7e254ebd3d1b9b775c331cec646ba6d2e28"


async def _query_gmx_position(
    web3: Any,
    data_store: str,
    account: str,
    market: str,
    collateral_token: str,
    is_long: bool,
    chain: str = "arbitrum",
) -> GMXv2Position | None:
    """Query a single GMX V2 position from DataStore.

    GMX V2 stores position data in the DataStore contract. Each position field
    is stored separately under computed keys.

    Args:
        web3: Web3 instance
        data_store: DataStore contract address
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        chain: Chain name for decimal lookups (default: "arbitrum")

    Returns:
        GMXv2Position if found, None otherwise
    """
    position_key = _compute_gmx_position_key(account, market, collateral_token, is_long)

    try:
        # Query position size in USD (primary indicator of position existence)
        # In GMX V2, position data is accessed via Reader contract for gas efficiency
        # We'll query using a simpler approach: check position size
        size_in_usd = await _query_gmx_position_size(web3, data_store, position_key)

        if size_in_usd == 0:
            return None

        # Query remaining position fields
        size_in_tokens = await _query_gmx_position_tokens(web3, data_store, position_key)
        collateral_amount = await _query_gmx_position_collateral(web3, data_store, position_key)

        # Calculate entry price from size ratio using correct decimals for the index token
        # GMX V2 stores size_in_usd with 30 decimals and size_in_tokens with token decimals
        entry_price = 0
        if size_in_tokens > 0:
            # Look up the correct decimals for this market's index token
            index_decimals = GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {}).get(market, 18)
            entry_price = (size_in_usd * 10**index_decimals) // size_in_tokens

        return GMXv2Position(
            position_key=position_key,
            account=account,
            market=market,
            collateral_token=collateral_token,
            size_in_usd=size_in_usd,
            size_in_tokens=size_in_tokens,
            collateral_amount=collateral_amount,
            entry_price=entry_price,
            is_long=is_long,
        )

    except Exception as e:
        logger.debug(f"Failed to query GMX position {position_key[:10]}...: {e}")
        return None


def _query_gmx_position_sync(
    web3: Any,
    data_store: str,
    account: str,
    market: str,
    collateral_token: str,
    is_long: bool,
    chain: str = "arbitrum",
) -> GMXv2Position | None:
    """Synchronous version of _query_gmx_position.

    Args:
        web3: Web3 instance
        data_store: DataStore contract address
        account: Account address
        market: Market address
        collateral_token: Collateral token address
        is_long: Position direction
        chain: Chain name for decimal lookups (default: "arbitrum")

    Returns:
        GMXv2Position if found, None otherwise
    """
    position_key = _compute_gmx_position_key(account, market, collateral_token, is_long)

    try:
        # Query position size in USD
        size_in_usd = _query_gmx_position_size_sync(web3, data_store, position_key)

        if size_in_usd == 0:
            return None

        # Query remaining position fields
        size_in_tokens = _query_gmx_position_tokens_sync(web3, data_store, position_key)
        collateral_amount = _query_gmx_position_collateral_sync(web3, data_store, position_key)

        # Calculate entry price from size ratio using correct decimals for the index token
        # GMX V2 stores size_in_usd with 30 decimals and size_in_tokens with token decimals
        entry_price = 0
        if size_in_tokens > 0:
            # Look up the correct decimals for this market's index token
            index_decimals = GMX_V2_INDEX_TOKEN_DECIMALS.get(chain, {}).get(market, 18)
            entry_price = (size_in_usd * 10**index_decimals) // size_in_tokens

        return GMXv2Position(
            position_key=position_key,
            account=account,
            market=market,
            collateral_token=collateral_token,
            size_in_usd=size_in_usd,
            size_in_tokens=size_in_tokens,
            collateral_amount=collateral_amount,
            entry_price=entry_price,
            is_long=is_long,
        )

    except Exception as e:
        logger.debug(f"Failed to query GMX position {position_key[:10]}...: {e}")
        return None


# GMX V2 DataStore storage slot calculations
# Position data is stored at: keccak256(abi.encode(POSITION_LIST_KEY, positionKey, fieldKey))
# Reference: gmx-synthetics/contracts/data/Keys.sol


async def _query_gmx_position_size(web3: Any, data_store: str, position_key: str) -> int:
    """Query position size in USD from DataStore."""
    # SIZE_IN_USD key hash
    size_key = _compute_position_storage_key(position_key, "SIZE_IN_USD")
    calldata = GET_UINT_SELECTOR + size_key.replace("0x", "")

    try:
        result = await web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


def _query_gmx_position_size_sync(web3: Any, data_store: str, position_key: str) -> int:
    """Synchronous version of _query_gmx_position_size."""
    size_key = _compute_position_storage_key(position_key, "SIZE_IN_USD")
    calldata = GET_UINT_SELECTOR + size_key.replace("0x", "")

    try:
        result = web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


async def _query_gmx_position_tokens(web3: Any, data_store: str, position_key: str) -> int:
    """Query position size in tokens from DataStore."""
    tokens_key = _compute_position_storage_key(position_key, "SIZE_IN_TOKENS")
    calldata = GET_UINT_SELECTOR + tokens_key.replace("0x", "")

    try:
        result = await web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


def _query_gmx_position_tokens_sync(web3: Any, data_store: str, position_key: str) -> int:
    """Synchronous version of _query_gmx_position_tokens."""
    tokens_key = _compute_position_storage_key(position_key, "SIZE_IN_TOKENS")
    calldata = GET_UINT_SELECTOR + tokens_key.replace("0x", "")

    try:
        result = web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


async def _query_gmx_position_collateral(web3: Any, data_store: str, position_key: str) -> int:
    """Query position collateral amount from DataStore."""
    collateral_key = _compute_position_storage_key(position_key, "COLLATERAL_AMOUNT")
    calldata = GET_UINT_SELECTOR + collateral_key.replace("0x", "")

    try:
        result = await web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


def _query_gmx_position_collateral_sync(web3: Any, data_store: str, position_key: str) -> int:
    """Synchronous version of _query_gmx_position_collateral."""
    collateral_key = _compute_position_storage_key(position_key, "COLLATERAL_AMOUNT")
    calldata = GET_UINT_SELECTOR + collateral_key.replace("0x", "")

    try:
        result = web3.eth.call({"to": data_store, "data": calldata})
        return int.from_bytes(result, byteorder="big")
    except Exception:
        return 0


def _compute_position_storage_key(position_key: str, field_name: str) -> str:
    """Compute the DataStore storage key for a position field.

    GMX V2 DataStore stores position data at computed keys.
    The key is: keccak256(abi.encode(positionKey, keccak256(fieldName)))

    Args:
        position_key: The position key
        field_name: Name of the field (e.g., "SIZE_IN_USD")

    Returns:
        Storage key for the field
    """
    from web3 import Web3

    # First, compute the field hash
    field_hash = Web3.keccak(text=field_name).hex()

    # Then compute the storage key
    pos_key_clean = position_key.replace("0x", "").zfill(64)
    field_clean = field_hash.replace("0x", "").zfill(64)

    encoded = pos_key_clean + field_clean
    return Web3.keccak(hexstr=encoded).hex()


# =============================================================================
# Aave V3 Position Querying
# =============================================================================

# Aave V3 Pool Data Provider addresses per chain
# Source: https://docs.aave.com/developers/deployed-contracts/v3-mainnet
AAVE_V3_POOL_DATA_PROVIDER: dict[str, str] = {
    "ethereum": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
    "arbitrum": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    "optimism": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    "polygon": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    "base": "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
    "avalanche": "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
}

# Common tokens supported by Aave V3 per chain (asset address -> symbol)
AAVE_V3_TOKENS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": "WETH",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": "USDC",
        "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": "USDC.e",
        "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": "USDT",
        "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": "DAI",
        "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": "WBTC",
        "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": "LINK",
        "0x912CE59144191C1204E64559FE8253a0e49E6548": "ARB",
        "0x5979D7b546E38E414F7E9822514be443A4800529": "wstETH",
    },
    "ethereum": {
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2": "WETH",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48": "USDC",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7": "USDT",
        "0x6B175474E89094C44Da98b954EedeAC495271d0F": "DAI",
        "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599": "WBTC",
        "0x514910771AF9Ca656af840dff83E8264EcF986CA": "LINK",
        "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0": "wstETH",
    },
    "optimism": {
        "0x4200000000000000000000000000000000000006": "WETH",
        "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85": "USDC",
        "0x7F5c764cBc14f9669B88837ca1490cCa17c31607": "USDC.e",
        "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58": "USDT",
        "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": "DAI",
        "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb": "wstETH",
    },
    "polygon": {
        "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270": "WMATIC",
        "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619": "WETH",
        "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359": "USDC",
        "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174": "USDC.e",
        "0xc2132D05D31c914a87C6611C10748AEb04B58e8F": "USDT",
        "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063": "DAI",
        "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6": "WBTC",
    },
    "base": {
        "0x4200000000000000000000000000000000000006": "WETH",
        "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913": "USDC",
        "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22": "cbETH",
        "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452": "wstETH",
    },
    "avalanche": {
        "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7": "WAVAX",
        "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB": "WETH.e",
        "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E": "USDC",
        "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7": "USDT",
        "0xd586E7F844cEa2F87f50152665BCbc2C279D8d70": "DAI.e",
    },
}

# Token decimals for Aave V3 assets
AAVE_V3_TOKEN_DECIMALS: dict[str, int] = {
    "WETH": 18,
    "WETH.e": 18,
    "USDC": 6,
    "USDC.e": 6,
    "USDT": 6,
    "DAI": 18,
    "DAI.e": 18,
    "WBTC": 8,
    "LINK": 18,
    "ARB": 18,
    "wstETH": 18,
    "cbETH": 18,
    "WMATIC": 18,
    "WAVAX": 18,
}

# Function selector for getUserReserveData(address asset, address user)
GET_USER_RESERVE_DATA_SELECTOR = "0x28dd2d01"


@dataclass
class AaveV3LendingPosition:
    """Data class representing an Aave V3 lending position.

    This represents a user's position in a specific Aave V3 reserve (asset).
    A position can have both supply (aToken balance) and borrow (debt) components.

    Attributes:
        asset: Asset symbol (e.g., "WETH", "USDC")
        asset_address: Asset contract address
        current_atoken_balance: Current aToken balance (supplied amount + accrued interest)
        current_stable_debt: Current stable rate debt
        current_variable_debt: Current variable rate debt
        principal_stable_debt: Principal amount of stable debt
        scaled_variable_debt: Scaled variable debt (without interest)
        stable_borrow_rate: Current stable borrow rate (ray = 1e27)
        liquidity_rate: Current supply/liquidity rate (ray = 1e27)
        usage_as_collateral_enabled: Whether this asset is enabled as collateral
        decimals: Token decimals for human-readable conversion
    """

    asset: str
    asset_address: str
    current_atoken_balance: int  # In token's smallest unit (wei)
    current_stable_debt: int
    current_variable_debt: int
    principal_stable_debt: int
    scaled_variable_debt: int
    stable_borrow_rate: int  # Ray (1e27) precision
    liquidity_rate: int  # Ray (1e27) precision
    usage_as_collateral_enabled: bool
    decimals: int = 18

    @property
    def is_active(self) -> bool:
        """Check if the position has any supply or debt."""
        return self.current_atoken_balance > 0 or self.total_debt > 0

    @property
    def has_supply(self) -> bool:
        """Check if user has supply in this reserve."""
        return self.current_atoken_balance > 0

    @property
    def has_debt(self) -> bool:
        """Check if user has debt in this reserve."""
        return self.total_debt > 0

    @property
    def total_debt(self) -> int:
        """Get total debt (stable + variable)."""
        return self.current_stable_debt + self.current_variable_debt

    @property
    def atoken_balance_decimal(self) -> float:
        """Get aToken balance as a decimal number."""
        return self.current_atoken_balance / 10**self.decimals

    @property
    def stable_debt_decimal(self) -> float:
        """Get stable debt as a decimal number."""
        return self.current_stable_debt / 10**self.decimals

    @property
    def variable_debt_decimal(self) -> float:
        """Get variable debt as a decimal number."""
        return self.current_variable_debt / 10**self.decimals

    @property
    def total_debt_decimal(self) -> float:
        """Get total debt as a decimal number."""
        return self.total_debt / 10**self.decimals

    @property
    def liquidity_rate_percent(self) -> float:
        """Get liquidity rate as annual percentage."""
        # Ray (1e27) to percentage: rate / 1e27 * 100
        return self.liquidity_rate / 10**27 * 100

    @property
    def stable_borrow_rate_percent(self) -> float:
        """Get stable borrow rate as annual percentage."""
        return self.stable_borrow_rate / 10**27 * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "asset": self.asset,
            "asset_address": self.asset_address,
            "current_atoken_balance": str(self.current_atoken_balance),
            "current_stable_debt": str(self.current_stable_debt),
            "current_variable_debt": str(self.current_variable_debt),
            "principal_stable_debt": str(self.principal_stable_debt),
            "scaled_variable_debt": str(self.scaled_variable_debt),
            "stable_borrow_rate": str(self.stable_borrow_rate),
            "liquidity_rate": str(self.liquidity_rate),
            "usage_as_collateral_enabled": self.usage_as_collateral_enabled,
            "decimals": self.decimals,
            # Computed properties for convenience
            "is_active": self.is_active,
            "has_supply": self.has_supply,
            "has_debt": self.has_debt,
            "total_debt": str(self.total_debt),
            "atoken_balance_decimal": self.atoken_balance_decimal,
            "stable_debt_decimal": self.stable_debt_decimal,
            "variable_debt_decimal": self.variable_debt_decimal,
            "total_debt_decimal": self.total_debt_decimal,
            "liquidity_rate_percent": self.liquidity_rate_percent,
            "stable_borrow_rate_percent": self.stable_borrow_rate_percent,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AaveV3LendingPosition":
        """Create from dictionary representation."""
        return cls(
            asset=data["asset"],
            asset_address=data["asset_address"],
            current_atoken_balance=int(data["current_atoken_balance"]),
            current_stable_debt=int(data["current_stable_debt"]),
            current_variable_debt=int(data["current_variable_debt"]),
            principal_stable_debt=int(data["principal_stable_debt"]),
            scaled_variable_debt=int(data["scaled_variable_debt"]),
            stable_borrow_rate=int(data["stable_borrow_rate"]),
            liquidity_rate=int(data["liquidity_rate"]),
            usage_as_collateral_enabled=data["usage_as_collateral_enabled"],
            decimals=data.get("decimals", 18),
        )


async def query_aave_positions(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    assets: list[str] | None = None,
) -> list[AaveV3LendingPosition]:
    """Query all Aave V3 lending positions for a wallet.

    This function queries the Aave V3 Pool Data Provider contract to get
    the user's reserve data for each supported asset.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier (ethereum, arbitrum, optimism, polygon, base, avalanche)
        assets: List of asset addresses to check (defaults to all known tokens for chain)

    Returns:
        List of AaveV3LendingPosition objects for each position with non-zero balance

    Raises:
        ValueError: If chain is not supported

    Example:
        from web3 import Web3
        web3 = Web3(Web3.HTTPProvider("https://arb1.arbitrum.io/rpc"))

        positions = await query_aave_positions(
            wallet="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            web3=web3,
            chain="arbitrum",
        )

        for pos in positions:
            print(f"Position in {pos.asset}:")
            print(f"  Supply: {pos.atoken_balance_decimal:.4f}")
            print(f"  Debt: {pos.total_debt_decimal:.4f}")
            print(f"  Collateral enabled: {pos.usage_as_collateral_enabled}")
    """
    if chain not in AAVE_V3_POOL_DATA_PROVIDER:
        raise ValueError(f"Unsupported chain: {chain}. Supported chains: {list(AAVE_V3_POOL_DATA_PROVIDER.keys())}")

    data_provider = web3.to_checksum_address(AAVE_V3_POOL_DATA_PROVIDER[chain])
    wallet_checksum = web3.to_checksum_address(wallet)

    # Use provided assets or default to all known tokens for this chain
    if assets is None:
        token_map = AAVE_V3_TOKENS.get(chain, {})
        assets = list(token_map.keys())

    positions: list[AaveV3LendingPosition] = []

    logger.debug(f"Querying Aave V3 positions for {wallet} on {chain}: {len(assets)} assets to check")

    for asset_address in assets:
        position = await _query_aave_user_reserve_data(
            web3=web3,
            data_provider=data_provider,
            asset=web3.to_checksum_address(asset_address),
            user=wallet_checksum,
            chain=chain,
        )
        if position is not None and position.is_active:
            positions.append(position)
            logger.info(
                f"Found Aave position: {position.asset}, "
                f"supply={position.atoken_balance_decimal:.4f}, "
                f"debt={position.total_debt_decimal:.4f}"
            )

    if not positions:
        logger.debug(f"Wallet {wallet} has no Aave V3 positions on {chain}")

    return positions


def query_aave_positions_sync(
    wallet: str,
    web3: Any,
    chain: str = "arbitrum",
    assets: list[str] | None = None,
) -> list[AaveV3LendingPosition]:
    """Synchronous version of query_aave_positions.

    For use in non-async contexts. See query_aave_positions for full docs.

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier
        assets: List of asset addresses to check

    Returns:
        List of AaveV3LendingPosition objects
    """
    if chain not in AAVE_V3_POOL_DATA_PROVIDER:
        raise ValueError(f"Unsupported chain: {chain}. Supported chains: {list(AAVE_V3_POOL_DATA_PROVIDER.keys())}")

    data_provider = web3.to_checksum_address(AAVE_V3_POOL_DATA_PROVIDER[chain])
    wallet_checksum = web3.to_checksum_address(wallet)

    # Use provided assets or default to all known tokens for this chain
    if assets is None:
        token_map = AAVE_V3_TOKENS.get(chain, {})
        assets = list(token_map.keys())

    positions: list[AaveV3LendingPosition] = []

    logger.debug(f"Querying Aave V3 positions for {wallet} on {chain}: {len(assets)} assets to check")

    for asset_address in assets:
        position = _query_aave_user_reserve_data_sync(
            web3=web3,
            data_provider=data_provider,
            asset=web3.to_checksum_address(asset_address),
            user=wallet_checksum,
            chain=chain,
        )
        if position is not None and position.is_active:
            positions.append(position)
            logger.info(
                f"Found Aave position: {position.asset}, "
                f"supply={position.atoken_balance_decimal:.4f}, "
                f"debt={position.total_debt_decimal:.4f}"
            )

    if not positions:
        logger.debug(f"Wallet {wallet} has no Aave V3 positions on {chain}")

    return positions


async def _query_aave_user_reserve_data(
    web3: Any,
    data_provider: str,
    asset: str,
    user: str,
    chain: str,
) -> AaveV3LendingPosition | None:
    """Query user reserve data for a single asset from Aave V3.

    getUserReserveData returns:
    - currentATokenBalance (uint256)
    - currentStableDebt (uint256)
    - currentVariableDebt (uint256)
    - principalStableDebt (uint256)
    - scaledVariableDebt (uint256)
    - stableBorrowRate (uint256) - ray precision (1e27)
    - liquidityRate (uint256) - ray precision (1e27)
    - stableRateLastUpdated (uint40)
    - usageAsCollateralEnabled (bool)

    Args:
        web3: Web3 instance
        data_provider: Pool Data Provider address
        asset: Asset address
        user: User wallet address
        chain: Chain identifier for token symbol lookup

    Returns:
        AaveV3LendingPosition if data fetched successfully, None otherwise
    """
    # Build calldata: getUserReserveData(address asset, address user)
    calldata = GET_USER_RESERVE_DATA_SELECTOR + _pad_address(asset) + _pad_address(user)

    try:
        result = await web3.eth.call({"to": data_provider, "data": calldata})
        return _parse_aave_user_reserve_data(result, asset, chain)
    except Exception as e:
        logger.debug(f"Failed to query Aave user reserve data for {asset}: {e}")
        return None


def _query_aave_user_reserve_data_sync(
    web3: Any,
    data_provider: str,
    asset: str,
    user: str,
    chain: str,
) -> AaveV3LendingPosition | None:
    """Synchronous version of _query_aave_user_reserve_data."""
    # Build calldata: getUserReserveData(address asset, address user)
    calldata = GET_USER_RESERVE_DATA_SELECTOR + _pad_address(asset) + _pad_address(user)

    try:
        result = web3.eth.call({"to": data_provider, "data": calldata})
        return _parse_aave_user_reserve_data(result, asset, chain)
    except Exception as e:
        logger.debug(f"Failed to query Aave user reserve data for {asset}: {e}")
        return None


def _parse_aave_user_reserve_data(
    result: bytes,
    asset_address: str,
    chain: str,
) -> AaveV3LendingPosition | None:
    """Parse the result of a getUserReserveData call.

    Expected ABI-encoded response (9 fields * 32 bytes = 288 bytes):
    - [0] currentATokenBalance (uint256)
    - [1] currentStableDebt (uint256)
    - [2] currentVariableDebt (uint256)
    - [3] principalStableDebt (uint256)
    - [4] scaledVariableDebt (uint256)
    - [5] stableBorrowRate (uint256)
    - [6] liquidityRate (uint256)
    - [7] stableRateLastUpdated (uint40) - padded to 32 bytes
    - [8] usageAsCollateralEnabled (bool) - padded to 32 bytes

    Args:
        result: Raw bytes from eth_call
        asset_address: Asset contract address
        chain: Chain identifier for symbol lookup

    Returns:
        AaveV3LendingPosition if parsed successfully, None otherwise
    """
    if len(result) < 288:
        logger.warning(f"Unexpected result length for Aave user reserve data: {len(result)}")
        return None

    try:
        # Parse all fields
        current_atoken_balance = _decode_uint256(result, 0)
        current_stable_debt = _decode_uint256(result, 32)
        current_variable_debt = _decode_uint256(result, 64)
        principal_stable_debt = _decode_uint256(result, 96)
        scaled_variable_debt = _decode_uint256(result, 128)
        stable_borrow_rate = _decode_uint256(result, 160)
        liquidity_rate = _decode_uint256(result, 192)
        # stableRateLastUpdated at offset 224 - not used
        usage_as_collateral_enabled = _decode_uint256(result, 256) != 0

        # Look up asset symbol
        token_map = AAVE_V3_TOKENS.get(chain, {})
        asset_lower = asset_address.lower()
        # Try to find symbol - check both original and lowercase addresses
        asset_symbol = "UNKNOWN"
        for addr, symbol in token_map.items():
            if addr.lower() == asset_lower:
                asset_symbol = symbol
                break

        # Get decimals
        decimals = AAVE_V3_TOKEN_DECIMALS.get(asset_symbol, 18)

        return AaveV3LendingPosition(
            asset=asset_symbol,
            asset_address=asset_address,
            current_atoken_balance=current_atoken_balance,
            current_stable_debt=current_stable_debt,
            current_variable_debt=current_variable_debt,
            principal_stable_debt=principal_stable_debt,
            scaled_variable_debt=scaled_variable_debt,
            stable_borrow_rate=stable_borrow_rate,
            liquidity_rate=liquidity_rate,
            usage_as_collateral_enabled=usage_as_collateral_enabled,
            decimals=decimals,
        )

    except Exception as e:
        logger.error(f"Failed to parse Aave user reserve data: {e}")
        return None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Data classes
    "UniswapV3Position",
    "GMXv2Position",
    "AaveV3LendingPosition",
    # Uniswap V3 query functions
    "query_uniswap_v3_positions",
    "query_uniswap_v3_positions_sync",
    # GMX V2 query functions
    "query_gmx_positions",
    "query_gmx_positions_sync",
    # Aave V3 query functions
    "query_aave_positions",
    "query_aave_positions_sync",
    # Constants
    "UNISWAP_V3_POSITION_MANAGER",
    "GMX_V2_READER",
    "GMX_V2_DATA_STORE",
    "GMX_V2_MARKETS",
    "GMX_V2_COLLATERAL_TOKENS",
    "GMX_V2_INDEX_TOKEN_DECIMALS",
    "AAVE_V3_POOL_DATA_PROVIDER",
    "AAVE_V3_TOKENS",
    "AAVE_V3_TOKEN_DECIMALS",
]
