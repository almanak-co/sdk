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

from almanak.core.chains import DEFAULT_CHAIN
from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS
from almanak.framework.data.tokens.models import Token

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Contract addresses resolve through the strategy-side AddressRegistry
# (the W1 / VIB-4853 seam) instead of local per-chain dicts — VIB-4851
# Phase D, DEC-6: the local copies were duplicates of the connector-owned
# tables (a VIB-4874-class drift hazard).


def _contract_address(protocol: str, chain: str, kinds: tuple[str, ...] | str, *, hint: str = "") -> str:
    """Resolve a connector-owned contract address, raising on unsupported chains.

    Single registry lookup: the resolved address doubles as the chain-support
    check, so callers never pair a separate "is the chain supported?" probe
    with a second resolution the two could drift apart from.
    """
    from almanak.connectors._strategy_base.address_registry import AddressRegistry

    address = AddressRegistry.resolve_contract_address(protocol, chain, kinds)
    if address is None:
        supported = sorted(AddressRegistry.address_supported_chains(protocol))
        message = f"Unsupported chain: {chain}. Supported chains: {supported}"
        raise ValueError(f"{message}. {hint}" if hint else message)
    return address


def _address_table(protocol: str, chain: str) -> dict[str, str]:
    """Resolve a connector-owned address/asset table for ``(protocol, chain)``."""
    from almanak.connectors._strategy_base.address_registry import AddressRegistry

    return AddressRegistry.addresses_for(protocol, chain)


def _static_token(symbol: str, chain: str) -> Token:
    """Resolve static token metadata from the token catalogue."""
    symbol_upper = symbol.upper()
    for token in DEFAULT_TOKENS:
        if token.symbol.upper() != symbol_upper:
            continue
        if token.has_address_on(chain):
            return token
    raise RuntimeError(f"Missing static token metadata for {symbol} on {chain}")


def _static_token_by_address(address: str, chain: str) -> Token:
    """Resolve static token metadata by chain address."""
    address_lower = address.lower()
    for token in DEFAULT_TOKENS:
        token_address = token.get_address(chain)
        if token_address and token_address.lower() == address_lower:
            return token
    raise RuntimeError(f"Missing static token metadata for {address} on {chain}")


def _static_token_address(symbol: str, chain: str) -> str:
    """Resolve a static token address from the token catalogue."""
    address = _static_token(symbol, chain).get_address(chain)
    if address is None:
        raise RuntimeError(f"Missing static token metadata for {symbol} on {chain}")
    return address


def _static_token_decimals(symbol: str, chain: str) -> int:
    """Resolve token decimals from the token catalogue."""
    return _static_token(symbol, chain).get_decimals(chain)


def _static_asset_decimals(symbol: str, chain: str, address: str) -> int:
    """Resolve token decimals by symbol, falling back to connector address aliases."""
    try:
        return _static_token_decimals(symbol, chain)
    except RuntimeError:
        return _static_token_by_address(address, chain).get_decimals(chain)


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
# Reconciler coverage constants
# =============================================================================

# The set of protocols each reader lane can actually verify on-chain.
# The reconciler filters tracked positions to these sets so it never flags
# positions from other LP/perp/lending protocols as MISSING_ON_CHAIN.
# Matches the reader implementation exactly — do not broaden without a
# corresponding reader change.

LP_RECONCILER_PROTOCOLS: frozenset[str] = frozenset({"uniswap_v3"})
"""The set of protocols this reader can actually verify; the reconciler
filters tracked positions to this set."""

PERP_RECONCILER_PROTOCOLS: frozenset[str] = frozenset({"gmx_v2"})
"""The set of protocols this reader can actually verify; the reconciler
filters tracked positions to this set."""


def _lending_reconciler_protocols() -> frozenset[str]:
    """Derive the lending reconciler protocol set from the registry constant.

    Lazy so the registry import does not happen at module load time.
    """
    from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

    return frozenset({LendingReadRegistry.default_protocol()})


def __getattr__(name: str):  # noqa: ANN202 - PEP 562 lazy back-compat hook
    """Serve the derived lending coverage constant without import-time discovery."""
    if name == "LENDING_RECONCILER_PROTOCOLS":
        return _lending_reconciler_protocols()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# =============================================================================
# Position Querying Functions
# =============================================================================


async def query_uniswap_v3_positions(
    wallet: str,
    web3: Any,
    chain: str = DEFAULT_CHAIN,
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
        position_manager = _contract_address(
            "uniswap_v3",
            chain,
            ("position_manager", "nft"),
            hint="Provide position_manager address for other chains.",
        )

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
    chain: str = DEFAULT_CHAIN,
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
        position_manager = _contract_address(
            "uniswap_v3",
            chain,
            ("position_manager", "nft"),
            hint="Provide position_manager address for other chains.",
        )

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

# GMX V2 reader / data-store addresses resolve through AddressRegistry
# (roles "reader" / "data_store" on the gmx_v2 connector).


def _gmx_v2_index_token_decimals(chain: str, market: str) -> int | None:
    """Return the GMX index-token decimals for a verified market address.

    Served from the process's venue-verified catalog (populated by dynamic
    market resolution during this process's compiles) — there is no static
    table (address-first). ``None`` means unmeasured — callers already degrade
    (entry price 0 / strategy-value fallback) rather than guess.
    """
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

    meta = PerpsReadRegistry.market_metadata("gmx_v2", market, chain)
    return meta.index_token_decimals if meta is not None else None


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


class GmxPositionReadUnavailable(RuntimeError):
    """The GMX position book could not be MEASURED (RPC or decode failure).

    Empty≠Zero: a wallet with no positions returns an empty list from a
    successful read; an unavailable read raises this instead. The old
    per-combination brute-force degraded per key; the single range read is
    all-or-nothing, so silently returning ``[]`` here would make an outage
    indistinguishable from a flat book and let the paper reconciler treat
    tracked positions as gone.
    """


def _gmx_range_read_request(wallet_checksum: str, chain: str) -> tuple[Any, Any]:
    """Build the single ``Reader.getAccountPositions`` call for ``wallet``.

    Routed through ``PerpsReadRegistry.resolve_plan`` — the same seam the live
    valuer uses — so the paper plane needs NO market or collateral enumeration
    (the venue returns the whole book in one range read) and no concrete
    connector import (framework↔connector ratchet). This replaced the
    ``market x collateral x direction`` brute-force: a catalogued-market sweep
    misses every market the catalogue never listed (the XMR class).
    """
    from almanak.connectors._strategy_base.perps_read_base import PerpsPositionQuery
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

    plan = PerpsReadRegistry.resolve_plan("gmx_v2", PerpsPositionQuery(chain=chain, wallet_address=wallet_checksum))
    if plan is None:
        raise ValueError(f"Unsupported chain: {chain}. GMX V2 perp reads are not deployed there.")
    if len(plan.calls) != 1:
        raise RuntimeError(f"GMX range read planned {len(plan.calls)} calls; expected exactly 1")
    return plan, plan.calls[0]


def _gmx_positions_from_blob(
    plan: Any,
    blob: str | None,
    chain: str,
    markets: list[str] | None,
    collateral_tokens: list[str] | None,
) -> list[GMXv2Position]:
    """Decode a range-read return and map it onto paper ``GMXv2Position`` rows.

    ``markets`` / ``collateral_tokens`` act as optional POST-filters for
    callers that scope reconciliation to specific addresses; ``None`` means
    the whole book (strictly wider coverage than the old catalogued default).
    """
    result = plan.reduce(plan.query, [blob])
    if not result.ok:
        raise GmxPositionReadUnavailable(
            "GMX V2 range read returned an undecodable payload; the position book is unmeasured"
        )
    if result.truncated:
        # A full [0, 100) page may have been cut short (the reducer computes
        # this from the RAW page, pre-filter). Reasoning about ABSENCE from an
        # incomplete book is how a reconciler deletes live tracked positions —
        # refuse instead (Empty≠Zero: incomplete is unmeasured, not smaller).
        raise GmxPositionReadUnavailable(
            "GMX V2 range read returned a truncated page; the position book is incomplete "
            "and absence cannot be reasoned about"
        )
    market_filter = {m.lower() for m in markets} if markets is not None else None
    collateral_filter = {c.lower() for c in collateral_tokens} if collateral_tokens is not None else None

    positions: list[GMXv2Position] = []
    for pos in result.positions:
        if market_filter is not None and pos.market.lower() not in market_filter:
            continue
        if collateral_filter is not None and pos.collateral_token.lower() not in collateral_filter:
            continue
        entry_price = 0
        if pos.size_in_tokens > 0:
            index_decimals = _gmx_v2_index_token_decimals(chain, pos.market)
            if index_decimals is not None:
                entry_price = (pos.size_in_usd * 10**index_decimals) // pos.size_in_tokens
        positions.append(
            GMXv2Position(
                position_key=_compute_position_key(pos.account, pos.market, pos.collateral_token, pos.is_long),
                account=pos.account,
                market=pos.market,
                collateral_token=pos.collateral_token,
                size_in_usd=pos.size_in_usd,
                size_in_tokens=pos.size_in_tokens,
                collateral_amount=pos.collateral_amount,
                entry_price=entry_price,
                is_long=pos.is_long,
                borrowing_factor=pos.borrowing_factor,
                funding_fee_amount_per_size=pos.funding_fee_amount_per_size,
            )
        )
    return positions


async def query_gmx_positions(
    wallet: str,
    web3: Any,
    chain: str = DEFAULT_CHAIN,
    markets: list[str] | None = None,
    collateral_tokens: list[str] | None = None,
) -> list[GMXv2Position]:
    """Query all GMX V2 perpetual positions for a wallet.

    One ``Reader.getAccountPositions`` range read returns the wallet's whole
    position book — every market the venue lists, catalogued or not
    (address-first: there is no market catalogue to enumerate).

    Args:
        wallet: Wallet address to query positions for
        web3: Web3 instance connected to the target chain
        chain: Chain identifier (currently only arbitrum supported)
        markets: Optional market-address post-filter (``None`` = whole book)
        collateral_tokens: Optional collateral-address post-filter (``None`` = whole book)

    Returns:
        List of GMXv2Position objects for each open position

    Raises:
        ValueError: If chain is not supported
        GmxPositionReadUnavailable: If the read or its decode fails — an
            unavailable book is never reported as an empty one (Empty≠Zero)
    """
    wallet_checksum = web3.to_checksum_address(wallet)
    plan, call = _gmx_range_read_request(wallet_checksum, chain)
    try:
        raw = await web3.eth.call({"to": web3.to_checksum_address(call.to), "data": call.data})
        blob = raw.hex() if not isinstance(raw, str) else raw
        if blob and not blob.startswith("0x"):
            blob = "0x" + blob
    except Exception as exc:
        raise GmxPositionReadUnavailable(f"GMX V2 range read call failed on {chain}: {exc}") from exc

    positions = _gmx_positions_from_blob(plan, blob, chain, markets, collateral_tokens)
    if not positions:
        logger.debug(f"Wallet {wallet} has no GMX V2 positions on {chain}")
    return positions


def query_gmx_positions_sync(
    wallet: str,
    web3: Any,
    chain: str = DEFAULT_CHAIN,
    markets: list[str] | None = None,
    collateral_tokens: list[str] | None = None,
) -> list[GMXv2Position]:
    """Synchronous version of query_gmx_positions.

    For use in non-async contexts. See query_gmx_positions for full docs.
    """
    wallet_checksum = web3.to_checksum_address(wallet)
    plan, call = _gmx_range_read_request(wallet_checksum, chain)
    try:
        raw = web3.eth.call({"to": web3.to_checksum_address(call.to), "data": call.data})
        blob = raw.hex() if not isinstance(raw, str) else raw
        if blob and not blob.startswith("0x"):
            blob = "0x" + blob
    except Exception as exc:
        raise GmxPositionReadUnavailable(f"GMX V2 range read call failed on {chain}: {exc}") from exc

    positions = _gmx_positions_from_blob(plan, blob, chain, markets, collateral_tokens)
    if not positions:
        logger.debug(f"Wallet {wallet} has no GMX V2 positions on {chain}")
    return positions


# =============================================================================
# Aave V3 Position Querying
# =============================================================================

# Aave V3 pool-data-provider addresses resolve through AddressRegistry
# (role "pool_data_provider" on the aave_v3 connector).

_AAVE_V3_POSITION_QUERY_SYMBOLS: dict[str, tuple[str, ...]] = {
    "arbitrum": ("WETH", "USDC", "USDC.e", "USDT", "DAI", "WBTC", "LINK", "ARB", "wstETH"),
    "ethereum": ("WETH", "USDC", "USDT", "DAI", "WBTC", "LINK", "wstETH"),
    "optimism": ("WETH", "USDC", "USDC.e", "USDT", "DAI", "wstETH"),
    "polygon": ("WMATIC", "WETH", "USDC", "USDC.e", "USDT", "DAI", "WBTC"),
    "base": ("WETH", "USDC", "cbETH", "wstETH"),
    "avalanche": ("WAVAX", "WETH.e", "USDC", "USDT", "DAI.e"),
}


def _build_aave_v3_tokens() -> dict[str, dict[str, str]]:
    """Invert the connector-owned Aave token catalogue for reserve lookups."""
    tokens: dict[str, dict[str, str]] = {}
    for chain, symbols in _AAVE_V3_POSITION_QUERY_SYMBOLS.items():
        connector_tokens = _address_table("aave_v3_tokens", chain)
        missing = [symbol for symbol in symbols if symbol not in connector_tokens]
        if missing:
            raise RuntimeError(f"Aave V3 connector token catalogue missing {missing} on {chain}")
        tokens[chain] = {connector_tokens[symbol]: symbol for symbol in symbols}
    return tokens


def _build_aave_v3_token_decimals() -> dict[str, int]:
    """Derive Aave reserve decimals from the token catalogue."""
    decimals: dict[str, int] = {}
    for chain, symbols in _AAVE_V3_POSITION_QUERY_SYMBOLS.items():
        connector_tokens = _address_table("aave_v3_tokens", chain)
        for symbol in symbols:
            address = connector_tokens[symbol]
            symbol_decimals = _static_asset_decimals(symbol, chain, address)
            previous = decimals.setdefault(symbol, symbol_decimals)
            if previous != symbol_decimals:
                raise RuntimeError(
                    f"Aave V3 token {symbol} has inconsistent decimals: {previous} and {symbol_decimals}"
                )
    return decimals


AAVE_V3_TOKENS: dict[str, dict[str, str]] = _build_aave_v3_tokens()
AAVE_V3_TOKEN_DECIMALS: dict[str, int] = _build_aave_v3_token_decimals()

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
    chain: str = DEFAULT_CHAIN,
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
    data_provider = web3.to_checksum_address(_contract_address("aave_v3", chain, "pool_data_provider"))
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
    chain: str = DEFAULT_CHAIN,
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
    data_provider = web3.to_checksum_address(_contract_address("aave_v3", chain, "pool_data_provider"))
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
    "AAVE_V3_TOKENS",
    "AAVE_V3_TOKEN_DECIMALS",
]
