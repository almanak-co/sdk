"""MetaMorpho Vault SDK - Low-level interface for MetaMorpho vault operations via gateway RPC.

This module provides a low-level SDK for interacting with MetaMorpho vault contracts
(ERC-4626) through the gateway's RPC service. All reads go through eth_call via
gateway_client.rpc.Call().

MetaMorpho vaults are ERC-4626 compliant yield vaults that sit on top of Morpho Blue,
aggregating capital across multiple isolated lending markets.

Supported operations:
- Read vault info: asset, total assets, total supply, share price, fee, curator, timelock
- Read user positions: balance, max deposit, max redeem, preview deposit/redeem
- Read market configuration: supply queue, withdraw queue, market caps
- Build unsigned transactions for deposit, redeem, and approve

Example:
    from almanak.framework.connectors.morpho_vault.sdk import MetaMorphoSDK

    sdk = MetaMorphoSDK(gateway_client, chain="ethereum")
    info = sdk.get_vault_info("0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB")
"""

import json
import logging
from dataclasses import dataclass

from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# Supported chains for MetaMorpho
SUPPORTED_CHAINS = {"ethereum", "base"}

# ERC-4626 read selectors
ASSET_SELECTOR = "0x38d52e0f"  # asset()
TOTAL_ASSETS_SELECTOR = "0x01e1d114"  # totalAssets()
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"  # totalSupply()
CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"  # convertToAssets(uint256)
CONVERT_TO_SHARES_SELECTOR = "0xc6e6f592"  # convertToShares(uint256)
MAX_DEPOSIT_SELECTOR = "0x402d267d"  # maxDeposit(address)
MAX_REDEEM_SELECTOR = "0xd905777e"  # maxRedeem(address)
PREVIEW_DEPOSIT_SELECTOR = "0xef8b30f7"  # previewDeposit(uint256)
PREVIEW_REDEEM_SELECTOR = "0x4cdad506"  # previewRedeem(uint256)
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)
DECIMALS_SELECTOR = "0x313ce567"  # decimals()

# MetaMorpho-specific read selectors
CURATOR_SELECTOR = "0xe66f53b7"  # curator()
FEE_SELECTOR = "0xddca3f43"  # fee()
TIMELOCK_SELECTOR = "0xd33219b4"  # timelock()
SUPPLY_QUEUE_LENGTH_SELECTOR = "0xa17b3130"  # supplyQueueLength()
WITHDRAW_QUEUE_LENGTH_SELECTOR = "0x33f91ebb"  # withdrawQueueLength()
SUPPLY_QUEUE_SELECTOR = "0xf7d18521"  # supplyQueue(uint256)
WITHDRAW_QUEUE_SELECTOR = "0x62518ddf"  # withdrawQueue(uint256)
IS_ALLOCATOR_SELECTOR = "0x4dedf20e"  # isAllocator(address)

# ERC-4626 write selectors
DEPOSIT_SELECTOR = "0x6e553f65"  # deposit(uint256,address)
REDEEM_SELECTOR = "0xba087652"  # redeem(uint256,address,address)

# ERC-20
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)

# Max values
MAX_UINT256 = 2**256 - 1
MAX_QUEUE_LENGTH = 100  # Safety bound for supply/withdraw queue iteration

# Gas estimates
# MetaMorpho deposit/redeem delegate to Morpho Blue's underlying markets for capital
# reallocation, which adds ~150K gas on top of the base cost. Observed on-chain:
# - deposit(): actual ~357K, simulation returns ~340-361K
# - redeem(): actual ~341K, simulation returns ~309-344K
# Set to 450K with headroom to avoid first-attempt FailedInnerCall reverts (VIB-512).
DEFAULT_GAS_ESTIMATES: dict[str, int] = {
    "deposit": 450000,
    "redeem": 450000,
    "approve": 60000,
}


# =============================================================================
# Exceptions
# =============================================================================


class MetaMorphoSDKError(Exception):
    """Base exception for MetaMorpho SDK errors."""


class VaultNotFoundError(MetaMorphoSDKError):
    """Raised when vault contract does not exist or returns invalid data."""


class UnsupportedChainError(MetaMorphoSDKError):
    """Raised when chain is not supported."""


class RPCError(MetaMorphoSDKError):
    """Raised when an RPC call fails."""


class DepositExceedsCapError(MetaMorphoSDKError):
    """Raised when deposit amount exceeds vault's maxDeposit."""


class InsufficientSharesError(MetaMorphoSDKError):
    """Raised when redeem amount exceeds user's maxRedeem."""


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class VaultInfo:
    """Information about a MetaMorpho vault."""

    address: str
    asset: str  # Underlying token address
    total_assets: int
    total_supply: int
    share_price: int  # convertToAssets(1e18) -- assets per share in raw units
    decimals: int  # Share decimals (always 18 for MetaMorpho)
    curator: str
    fee: int  # WAD (1e18 = 100%)
    timelock: int  # seconds


@dataclass
class VaultPosition:
    """User position in a MetaMorpho vault."""

    vault_address: str
    user: str
    shares: int
    assets: int  # convertToAssets(shares)


@dataclass
class VaultMarketConfig:
    """Market configuration within a MetaMorpho vault (Phase 2)."""

    market_id: str
    cap: int
    enabled: bool
    removable_at: int


# =============================================================================
# Encoding / Decoding Helpers
# =============================================================================


def _encode_address(address: str) -> str:
    """ABI-encode an address as a 32-byte left-padded hex string."""
    addr = address.lower().removeprefix("0x")
    return addr.zfill(64)


def _encode_uint256(value: int) -> str:
    """ABI-encode a uint256 as a 32-byte big-endian hex string."""
    if value < 0:
        raise ValueError(f"Cannot ABI-encode negative value as uint256: {value}")
    return hex(value)[2:].zfill(64)


def _decode_uint256(hex_str: str) -> int:
    """Decode a hex string as a uint256. Returns 0 for empty/null responses."""
    clean = hex_str.strip()
    if not clean or clean == "0x":
        return 0
    return int(clean, 16)


def _decode_address(hex_str: str) -> str:
    """Decode a hex string as an address (last 20 bytes of a 32-byte word)."""
    clean = hex_str.removeprefix("0x")
    return "0x" + clean[-40:]


# =============================================================================
# SDK
# =============================================================================


class MetaMorphoSDK:
    """Low-level SDK for reading MetaMorpho vault state via gateway RPC calls.

    All RPC calls are routed through the gateway client's RPC service.
    This SDK handles ABI encoding/decoding and provides typed return values.

    Args:
        gateway_client: Connected gateway client with RPC service
        chain: Chain identifier (e.g., "ethereum", "base")
    """

    def __init__(self, gateway_client, chain: str):
        chain_lower = chain.lower()
        if chain_lower not in SUPPORTED_CHAINS:
            raise UnsupportedChainError(f"Chain '{chain}' not supported. Supported: {sorted(SUPPORTED_CHAINS)}")
        self._gateway_client = gateway_client
        self._chain = chain_lower

    # =========================================================================
    # RPC Helper
    # =========================================================================

    def _eth_call(self, to: str, data: str, request_id: str = "metamorpho_sdk") -> str:
        """Make an eth_call via the gateway and return the hex result."""
        request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method="eth_call",
            params=json.dumps([{"to": to, "data": data}, "latest"]),
            id=request_id,
        )
        response = self._gateway_client.rpc.Call(request, timeout=30.0)
        if not response.success:
            error_msg = response.error if response.error else "Unknown RPC error"
            raise RPCError(f"eth_call failed for {request_id}: {error_msg}")
        result = json.loads(response.result)
        if not result or result == "0x":
            raise VaultNotFoundError(f"Empty response from {request_id} - vault may not exist at target address")
        return result

    # =========================================================================
    # Read Methods - ERC-4626
    # =========================================================================

    def get_vault_asset(self, vault_address: str) -> str:
        """Read the vault's underlying asset address (asset())."""
        result = self._eth_call(to=vault_address, data=ASSET_SELECTOR, request_id="metamorpho_asset")
        return _decode_address(result)

    def get_total_assets(self, vault_address: str) -> int:
        """Read the vault's total assets (totalAssets())."""
        result = self._eth_call(to=vault_address, data=TOTAL_ASSETS_SELECTOR, request_id="metamorpho_total_assets")
        return _decode_uint256(result)

    def get_total_supply(self, vault_address: str) -> int:
        """Read the vault's total share supply (totalSupply())."""
        result = self._eth_call(to=vault_address, data=TOTAL_SUPPLY_SELECTOR, request_id="metamorpho_total_supply")
        return _decode_uint256(result)

    def get_share_price(self, vault_address: str) -> int:
        """Get share price as convertToAssets(one_share) in raw underlying units."""
        decimals = self.get_decimals(vault_address)
        one_share = 10**decimals
        calldata = CONVERT_TO_ASSETS_SELECTOR + _encode_uint256(one_share)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_share_price")
        return _decode_uint256(result)

    def get_decimals(self, vault_address: str) -> int:
        """Read the vault's share decimals (decimals()). Always 18 for MetaMorpho."""
        result = self._eth_call(to=vault_address, data=DECIMALS_SELECTOR, request_id="metamorpho_decimals")
        return _decode_uint256(result)

    def get_balance_of(self, vault_address: str, user: str) -> int:
        """Read user's share balance in the vault."""
        calldata = BALANCE_OF_SELECTOR + _encode_address(user)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_balance_of")
        return _decode_uint256(result)

    def get_max_deposit(self, vault_address: str, receiver: str) -> int:
        """Read maximum deposit amount allowed for a receiver."""
        calldata = MAX_DEPOSIT_SELECTOR + _encode_address(receiver)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_max_deposit")
        return _decode_uint256(result)

    def get_max_redeem(self, vault_address: str, owner: str) -> int:
        """Read maximum shares that can be redeemed by an owner."""
        calldata = MAX_REDEEM_SELECTOR + _encode_address(owner)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_max_redeem")
        return _decode_uint256(result)

    def preview_deposit(self, vault_address: str, assets: int) -> int:
        """Preview how many shares a deposit of `assets` would mint."""
        calldata = PREVIEW_DEPOSIT_SELECTOR + _encode_uint256(assets)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_preview_deposit")
        return _decode_uint256(result)

    def preview_redeem(self, vault_address: str, shares: int) -> int:
        """Preview how many assets a redemption of `shares` would return."""
        calldata = PREVIEW_REDEEM_SELECTOR + _encode_uint256(shares)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_preview_redeem")
        return _decode_uint256(result)

    def convert_to_assets(self, vault_address: str, shares: int) -> int:
        """Convert share amount to asset amount."""
        calldata = CONVERT_TO_ASSETS_SELECTOR + _encode_uint256(shares)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_convert_to_assets")
        return _decode_uint256(result)

    def convert_to_shares(self, vault_address: str, assets: int) -> int:
        """Convert asset amount to share amount."""
        calldata = CONVERT_TO_SHARES_SELECTOR + _encode_uint256(assets)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_convert_to_shares")
        return _decode_uint256(result)

    # =========================================================================
    # Read Methods - MetaMorpho-specific
    # =========================================================================

    def get_curator(self, vault_address: str) -> str:
        """Read the vault's curator address."""
        result = self._eth_call(to=vault_address, data=CURATOR_SELECTOR, request_id="metamorpho_curator")
        return _decode_address(result)

    def get_fee(self, vault_address: str) -> int:
        """Read the vault's performance fee (WAD scale, 1e18 = 100%)."""
        result = self._eth_call(to=vault_address, data=FEE_SELECTOR, request_id="metamorpho_fee")
        return _decode_uint256(result)

    def get_timelock(self, vault_address: str) -> int:
        """Read the vault's timelock duration in seconds."""
        result = self._eth_call(to=vault_address, data=TIMELOCK_SELECTOR, request_id="metamorpho_timelock")
        return _decode_uint256(result)

    def is_allocator(self, vault_address: str, address: str) -> bool:
        """Check if an address is an allocator for the vault."""
        calldata = IS_ALLOCATOR_SELECTOR + _encode_address(address)
        result = self._eth_call(to=vault_address, data=calldata, request_id="metamorpho_is_allocator")
        return _decode_uint256(result) != 0

    def get_supply_queue(self, vault_address: str) -> list[str]:
        """Read the vault's supply queue (list of market IDs)."""
        length_result = self._eth_call(
            to=vault_address, data=SUPPLY_QUEUE_LENGTH_SELECTOR, request_id="metamorpho_supply_queue_len"
        )
        length = _decode_uint256(length_result)
        if length > MAX_QUEUE_LENGTH:
            raise MetaMorphoSDKError(f"Supply queue length {length} exceeds maximum {MAX_QUEUE_LENGTH}")
        queue = []
        for i in range(length):
            calldata = SUPPLY_QUEUE_SELECTOR + _encode_uint256(i)
            result = self._eth_call(to=vault_address, data=calldata, request_id=f"metamorpho_supply_queue_{i}")
            queue.append(result.strip())
        return queue

    def get_withdraw_queue(self, vault_address: str) -> list[str]:
        """Read the vault's withdraw queue (list of market IDs)."""
        length_result = self._eth_call(
            to=vault_address, data=WITHDRAW_QUEUE_LENGTH_SELECTOR, request_id="metamorpho_withdraw_queue_len"
        )
        length = _decode_uint256(length_result)
        if length > MAX_QUEUE_LENGTH:
            raise MetaMorphoSDKError(f"Withdraw queue length {length} exceeds maximum {MAX_QUEUE_LENGTH}")
        queue = []
        for i in range(length):
            calldata = WITHDRAW_QUEUE_SELECTOR + _encode_uint256(i)
            result = self._eth_call(to=vault_address, data=calldata, request_id=f"metamorpho_withdraw_queue_{i}")
            queue.append(result.strip())
        return queue

    # =========================================================================
    # Composite Read Methods
    # =========================================================================

    def get_vault_info(self, vault_address: str) -> VaultInfo:
        """Read complete vault information in multiple RPC calls."""
        asset = self.get_vault_asset(vault_address)
        total_assets = self.get_total_assets(vault_address)
        total_supply = self.get_total_supply(vault_address)
        share_price = self.get_share_price(vault_address)
        decimals = self.get_decimals(vault_address)
        curator = self.get_curator(vault_address)
        fee = self.get_fee(vault_address)
        timelock = self.get_timelock(vault_address)

        return VaultInfo(
            address=vault_address,
            asset=asset,
            total_assets=total_assets,
            total_supply=total_supply,
            share_price=share_price,
            decimals=decimals,
            curator=curator,
            fee=fee,
            timelock=timelock,
        )

    def get_position(self, vault_address: str, user: str) -> VaultPosition:
        """Read a user's position in the vault."""
        shares = self.get_balance_of(vault_address, user)
        assets = self.convert_to_assets(vault_address, shares) if shares > 0 else 0

        return VaultPosition(
            vault_address=vault_address,
            user=user,
            shares=shares,
            assets=assets,
        )

    # =========================================================================
    # Write Methods (Build unsigned transactions)
    # =========================================================================

    def build_deposit_tx(self, vault_address: str, assets: int, receiver: str) -> dict:
        """Build an unsigned ERC-4626 deposit(uint256,address) transaction.

        Args:
            vault_address: The MetaMorpho vault address.
            assets: Amount of underlying assets to deposit (raw units).
            receiver: Address to receive vault shares.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        if assets <= 0:
            raise ValueError("Deposit amount must be positive")
        if assets > MAX_UINT256:
            raise ValueError("Deposit amount exceeds MAX_UINT256")

        calldata = DEPOSIT_SELECTOR + _encode_uint256(assets) + _encode_address(receiver)
        return {
            "to": vault_address,
            "from": receiver,
            "data": calldata,
            "value": "0",
            "gas_estimate": DEFAULT_GAS_ESTIMATES["deposit"],
        }

    def build_redeem_tx(self, vault_address: str, shares: int, receiver: str, owner: str) -> dict:
        """Build an unsigned ERC-4626 redeem(uint256,address,address) transaction.

        Args:
            vault_address: The MetaMorpho vault address.
            shares: Number of shares to redeem (raw units).
            receiver: Address to receive underlying assets.
            owner: Address that owns the shares being redeemed.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        if shares <= 0:
            raise ValueError("Redeem shares must be positive")
        if shares > MAX_UINT256:
            raise ValueError("Redeem shares exceed MAX_UINT256")

        calldata = REDEEM_SELECTOR + _encode_uint256(shares) + _encode_address(receiver) + _encode_address(owner)
        return {
            "to": vault_address,
            "from": owner,
            "data": calldata,
            "value": "0",
            "gas_estimate": DEFAULT_GAS_ESTIMATES["redeem"],
        }

    def build_approve_tx(self, token_address: str, spender: str, amount: int, owner: str) -> dict:
        """Build an ERC-20 approve transaction.

        Args:
            token_address: The ERC-20 token address.
            spender: The address to approve.
            amount: Amount to approve (raw units).
            owner: The address that owns the tokens (tx sender).

        Returns:
            Unsigned transaction dict.
        """
        if amount < 0:
            raise ValueError("Approve amount must be non-negative")
        if amount > MAX_UINT256:
            raise ValueError(f"Approve amount exceeds MAX_UINT256: {amount}")
        calldata = ERC20_APPROVE_SELECTOR + _encode_address(spender) + _encode_uint256(amount)
        return {
            "to": token_address,
            "from": owner,
            "data": calldata,
            "value": "0",
            "gas_estimate": DEFAULT_GAS_ESTIMATES["approve"],
        }
