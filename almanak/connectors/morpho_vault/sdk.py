"""MetaMorpho Vault SDK - Low-level interface for MetaMorpho vault operations via gateway RPC.

This module provides a low-level SDK for interacting with MetaMorpho vault contracts
(ERC-4626) through the gateway's RPC service. All reads go through eth_call via
gateway_client.rpc.Call().

MetaMorpho vaults are ERC-4626 compliant yield vaults that sit on top of Morpho Blue,
aggregating capital across multiple isolated lending markets.

Two vault generations share the ERC-4626 deposit/redeem interface but differ
everywhere else, and the SDK detects which one it is talking to ON-CHAIN
(``detect_vault_version``) — never from the symbol or a config label:

- **MetaMorpho v1**: allocation exposed as on-chain supply/withdraw queues;
  ``maxRedeem(owner)`` is a hair below ``balanceOf`` (round-trip rounding), so
  redeem-all sizes from ``maxRedeem``.
- **Morpho Vault V2**: allocation lives behind adapter contracts; the ERC-4626
  ``max*`` views return 0 BY DESIGN, so redeem-all sizes from ``balanceOf`` and
  is simulated before it is sent (withdrawals are served from idle assets plus
  one liquidity adapter and REVERT when those are short); optional gate
  contracts can restrict who may deposit or redeem; fees split into
  performance + management; ``fee()``/``timelock()``/queue reads revert.

Supported operations:
- Read vault info: asset, total assets, total supply, share price, fee, curator, timelock
- Read user positions: balance, max deposit, max redeem, preview deposit/redeem
- Read market configuration: supply queue, withdraw queue, market caps
- Build unsigned transactions for deposit, redeem, and approve

Example:
    from almanak.connectors.morpho_vault.sdk import MetaMorphoSDK

    sdk = MetaMorphoSDK(gateway_client, chain="ethereum")
    info = sdk.get_vault_info("0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB")
"""

import json
import logging
from dataclasses import dataclass, field

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

# Morpho Vault V2 read selectors (adapter-based generation). None of these
# exist on v1 and none of the v1 queue/fee/timelock reads exist on V2 — the
# two ``*_LENGTH`` reads double as the on-chain generation fingerprint.
V2_ADAPTERS_LENGTH_SELECTOR = "0x5aa22bc8"  # selector of the adaptersLength read
V2_ADAPTERS_SELECTOR = "0x4ef501ac"  # selector of the adapters(uint256) read
V2_LIQUIDITY_ADAPTER_SELECTOR = "0xad468d11"  # selector of the liquidityAdapter read
V2_PERFORMANCE_FEE_SELECTOR = "0x87788782"  # selector of the performanceFee read
V2_MANAGEMENT_FEE_SELECTOR = "0xa6f7f5d6"  # selector of the managementFee read
V2_FORCE_DEALLOCATE_PENALTY_SELECTOR = "0x99e99183"  # selector of the forceDeallocatePenalty(address) read
V2_RECEIVE_SHARES_GATE_SELECTOR = "0x7e729ac4"  # selector of the receiveSharesGate read
V2_SEND_SHARES_GATE_SELECTOR = "0x93ab2ab7"  # selector of the sendSharesGate read
V2_RECEIVE_ASSETS_GATE_SELECTOR = "0x54cde13e"  # selector of the receiveAssetsGate read
V2_SEND_ASSETS_GATE_SELECTOR = "0x8eede801"  # selector of the sendAssetsGate read
# Gate-contract reads (called on the gate address, not the vault).
GATE_CAN_RECEIVE_SHARES_SELECTOR = "0x98c9b49c"  # selector of the canReceiveShares(address) read
GATE_CAN_SEND_SHARES_SELECTOR = "0x8e511e4d"  # selector of the canSendShares(address) read
GATE_CAN_RECEIVE_ASSETS_SELECTOR = "0x0d326b18"  # selector of the canReceiveAssets(address) read
GATE_CAN_SEND_ASSETS_SELECTOR = "0x20fe8d58"  # selector of canSendAssets(address)

VAULT_VERSION_V1 = "v1"  # MetaMorpho (queue-based)
VAULT_VERSION_V2 = "v2"  # Morpho Vault V2 (adapter-based)
ZERO_ADDRESS = "0x" + "00" * 20
MAX_ADAPTER_COUNT = 32  # Safety bound for V2 adapter enumeration

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
    """Raised when redeem amount exceeds user's redeemable shares."""


class UnsupportedVaultError(MetaMorphoSDKError):
    """Raised when a vault answers neither the v1 nor the V2 generation fingerprint."""


class VaultGatedError(MetaMorphoSDKError):
    """Raised when a Morpho Vault V2 gate contract refuses this wallet for the operation."""


class VaultIlliquidError(MetaMorphoSDKError):
    """Raised when a Morpho Vault V2 redeem simulation reverts.

    V2 serves withdrawals from idle assets plus one liquidity adapter and
    reverts when those cannot cover the request; the depositor's escape hatch
    is ``forceDeallocate`` (penalised), which this connector does not issue.
    """


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
    fee: int  # WAD (1e18 = 100%); v1 ``fee()`` / V2 ``performanceFee()``
    timelock: int  # seconds; v1 only (V2 timelocks are per-selector) — 0 on V2
    vault_version: str = VAULT_VERSION_V1
    management_fee: int = 0  # WAD; V2 only (v1 has no management fee)
    liquidity_adapter: str | None = None  # V2 only: adapter that serves withdrawals
    adapters: list[str] = field(default_factory=list)  # V2 only
    force_deallocate_penalty: int | None = None  # WAD; V2 only, read on the liquidity adapter


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


_REVERT_MARKERS = ("revert", "execution reverted", "invalid opcode", "invalid jump", '"code": 3', "'code': 3")


def _is_contract_revert(message: str) -> bool:
    """True when an eth_call error text describes the EVM rejecting the call (not the transport failing)."""
    lowered = message.lower()
    return any(marker in lowered for marker in _REVERT_MARKERS)


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
        # Generation is immutable per address — one fingerprint probe per vault.
        self._version_cache: dict[str, str] = {}

    # =========================================================================
    # RPC Helper
    # =========================================================================

    def _eth_call(
        self,
        to: str,
        data: str,
        request_id: str = "metamorpho_sdk",
        *,
        from_address: str | None = None,
    ) -> str:
        """Make an eth_call via the gateway and return the hex result.

        ``from_address`` sets the simulated caller — required for state-changing
        selectors simulated as reads (e.g. a V2 ``redeem`` dry-run, which checks
        the caller's share balance and gates).
        """
        call_obj: dict[str, str] = {"to": to, "data": data}
        if from_address:
            call_obj["from"] = from_address
        request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method="eth_call",
            params=json.dumps([call_obj, "latest"]),
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

    def _try_eth_call(self, to: str, data: str, request_id: str) -> str | None:
        """``_eth_call`` that returns ``None`` when the CONTRACT rejects the call.

        Used for generation fingerprinting and gate reads, where "this selector
        reverts" / "no data" is the answer, not an error. A transport-level
        failure (timeout, node unavailable, rate limit) is NOT an answer about
        the contract and is re-raised as ``RPCError`` — mapping it to ``None``
        would let a flaky RPC masquerade as "not a v1 vault" and flip the
        generation verdict.
        """
        try:
            return self._eth_call(to=to, data=data, request_id=request_id)
        except VaultNotFoundError:
            return None
        except RPCError as exc:
            if _is_contract_revert(str(exc)):
                return None
            raise

    # Generation detection

    def detect_vault_version(self, vault_address: str) -> str:
        """Fingerprint the vault generation on-chain (cached per address).

        ``withdrawQueueLength()`` answers only on MetaMorpho v1;
        ``adaptersLength()`` answers only on Morpho Vault V2. A contract that
        answers neither is not a Morpho vault this connector knows how to exit
        safely — ``UnsupportedVaultError`` rather than a guess.
        """
        key = vault_address.lower()
        cached = self._version_cache.get(key)
        if cached is not None:
            return cached
        if self._try_eth_call(vault_address, WITHDRAW_QUEUE_LENGTH_SELECTOR, "metamorpho_version_probe_v1") is not None:
            version = VAULT_VERSION_V1
        elif self._try_eth_call(vault_address, V2_ADAPTERS_LENGTH_SELECTOR, "metamorpho_version_probe_v2") is not None:
            version = VAULT_VERSION_V2
        else:
            raise UnsupportedVaultError(
                f"Vault {vault_address} on {self._chain} answers neither the MetaMorpho v1 "
                "(withdrawQueueLength) nor the Morpho Vault V2 (adaptersLength) fingerprint; "
                "refusing to guess its redeem semantics."
            )
        self._version_cache[key] = version
        return version

    def is_vault_v2(self, vault_address: str) -> bool:
        return self.detect_vault_version(vault_address) == VAULT_VERSION_V2

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

    def get_redeemable_shares(self, vault_address: str, owner: str) -> int:
        """Shares a redeem-all should request, by generation.

        v1: ``maxRedeem(owner)`` — a hair below ``balanceOf`` because of the
        ERC-4626 round-trip rounding; ``redeem(balanceOf)`` reverts there.
        V2: ``balanceOf(owner)`` — V2 returns 0 from every ``max*`` view by
        design, so ``maxRedeem`` would turn every exit into "No shares to
        redeem". Liquidity is checked by ``simulate_redeem``.
        """
        if self.is_vault_v2(vault_address):
            return self.get_balance_of(vault_address, owner)
        return self.get_max_redeem(vault_address, owner)

    def simulate_redeem(self, vault_address: str, shares: int, receiver: str, owner: str) -> int:
        """Dry-run ``redeem(shares, receiver, owner)`` as an eth_call from ``owner``.

        Returns the assets the redeem would return. Raises ``VaultIlliquidError``
        when the call reverts — on V2 that is the "idle + liquidity adapter
        cannot cover this" signal (or a gate refusal); the tx must not be sent.
        """
        calldata = REDEEM_SELECTOR + _encode_uint256(shares) + _encode_address(receiver) + _encode_address(owner)
        try:
            result = self._eth_call(
                to=vault_address, data=calldata, request_id="metamorpho_simulate_redeem", from_address=owner
            )
        except (RPCError, VaultNotFoundError) as exc:
            if isinstance(exc, RPCError) and not _is_contract_revert(str(exc)):
                # A timeout / unavailable node says nothing about the vault's
                # liquidity; surfacing it as "illiquid" would send an operator
                # off to wait for liquidity that was never short.
                raise
            raise VaultIlliquidError(
                f"redeem({shares} shares) simulation reverted for vault {vault_address} on {self._chain}: {exc}. "
                "On Morpho Vault V2 this means idle assets + the liquidity adapter cannot cover the withdrawal "
                "right now (or a gate refuses the wallet). Wait for liquidity or use forceDeallocate manually; "
                "the transaction was NOT sent."
            ) from exc
        return _decode_uint256(result)

    # Morpho Vault V2 - gates

    def _gate_address(self, vault_address: str, selector: str, request_id: str) -> str | None:
        """Return the configured gate, or ``None`` when the vault has none.

        ``None`` is only a successfully decoded zero-address or a contract
        revert (selector absent). Transport failures re-raise from
        ``_try_eth_call``. Malformed return data fails closed — it is not
        "no gate".
        """
        raw = self._try_eth_call(vault_address, selector, request_id)
        if raw is None:
            return None
        clean = raw.removeprefix("0x")
        if len(clean) < 40:
            raise RPCError(f"malformed gate address from {request_id}: {raw!r}")
        gate = _decode_address(raw)
        return None if gate == ZERO_ADDRESS else gate

    def _gate_allows(self, gate: str, selector: str, account: str, request_id: str) -> bool:
        raw = self._try_eth_call(gate, selector + _encode_address(account), request_id)
        # A gate that does not answer cannot be assumed permissive: fail closed.
        return raw is not None and _decode_uint256(raw) != 0

    def check_deposit_gate(self, vault_address: str, sender: str, receiver: str | None = None) -> None:
        """V2 only: raise ``VaultGatedError`` if the deposit is gated.

        A V2 vault can configure ``sendAssetsGate`` independently of
        ``receiveSharesGate``: the depositing wallet (``sender``) must be
        allowed to supply the underlying, and ``receiver`` must be allowed
        to receive shares. ``receiver`` defaults to ``sender``.
        """
        if not self.is_vault_v2(vault_address):
            return
        receiver = receiver or sender
        send_gate = self._gate_address(vault_address, V2_SEND_ASSETS_GATE_SELECTOR, "metamorpho_v2_send_assets_gate")
        if send_gate is not None and not self._gate_allows(
            send_gate, GATE_CAN_SEND_ASSETS_SELECTOR, sender, "metamorpho_v2_can_send_assets"
        ):
            raise VaultGatedError(
                f"Vault {vault_address} on {self._chain} has a send-assets gate ({send_gate}) that refuses {sender}; "
                "deposits from this wallet are not permitted."
            )
        receive_gate = self._gate_address(
            vault_address, V2_RECEIVE_SHARES_GATE_SELECTOR, "metamorpho_v2_receive_shares_gate"
        )
        if receive_gate is not None and not self._gate_allows(
            receive_gate, GATE_CAN_RECEIVE_SHARES_SELECTOR, receiver, "metamorpho_v2_can_receive_shares"
        ):
            raise VaultGatedError(
                f"Vault {vault_address} on {self._chain} has a receive-shares gate ({receive_gate}) that refuses {receiver}; "
                "deposits from this wallet are not permitted."
            )

    def check_redeem_gates(self, vault_address: str, owner: str, receiver: str) -> None:
        """V2 only: raise ``VaultGatedError`` if ``owner`` may not send shares or ``receiver`` receive assets."""
        if not self.is_vault_v2(vault_address):
            return
        send_gate = self._gate_address(vault_address, V2_SEND_SHARES_GATE_SELECTOR, "metamorpho_v2_send_shares_gate")
        if send_gate is not None and not self._gate_allows(
            send_gate, GATE_CAN_SEND_SHARES_SELECTOR, owner, "metamorpho_v2_can_send_shares"
        ):
            raise VaultGatedError(
                f"Vault {vault_address} on {self._chain} has a send-shares gate ({send_gate}) that refuses {owner}."
            )
        receive_gate = self._gate_address(
            vault_address, V2_RECEIVE_ASSETS_GATE_SELECTOR, "metamorpho_v2_receive_assets_gate"
        )
        if receive_gate is not None and not self._gate_allows(
            receive_gate, GATE_CAN_RECEIVE_ASSETS_SELECTOR, receiver, "metamorpho_v2_can_receive_assets"
        ):
            raise VaultGatedError(
                f"Vault {vault_address} on {self._chain} has a receive-assets gate ({receive_gate}) that refuses {receiver}."
            )

    # Read Methods - Morpho Vault V2-specific

    def get_v2_performance_fee(self, vault_address: str) -> int:
        result = self._eth_call(
            to=vault_address, data=V2_PERFORMANCE_FEE_SELECTOR, request_id="metamorpho_v2_performance_fee"
        )
        return _decode_uint256(result)

    def get_v2_management_fee(self, vault_address: str) -> int:
        result = self._eth_call(
            to=vault_address, data=V2_MANAGEMENT_FEE_SELECTOR, request_id="metamorpho_v2_management_fee"
        )
        return _decode_uint256(result)

    def get_v2_liquidity_adapter(self, vault_address: str) -> str | None:
        raw = self._try_eth_call(vault_address, V2_LIQUIDITY_ADAPTER_SELECTOR, "metamorpho_v2_liquidity_adapter")
        if raw is None:
            return None
        adapter = _decode_address(raw)
        return None if adapter == ZERO_ADDRESS else adapter

    def get_v2_adapters(self, vault_address: str) -> list[str]:
        length_result = self._eth_call(
            to=vault_address, data=V2_ADAPTERS_LENGTH_SELECTOR, request_id="metamorpho_v2_adapters_len"
        )
        length = _decode_uint256(length_result)
        if length > MAX_ADAPTER_COUNT:
            raise MetaMorphoSDKError(f"Adapter count {length} exceeds maximum {MAX_ADAPTER_COUNT}")
        adapters = []
        for i in range(length):
            result = self._eth_call(
                to=vault_address,
                data=V2_ADAPTERS_SELECTOR + _encode_uint256(i),
                request_id=f"metamorpho_v2_adapter_{i}",
            )
            adapters.append(_decode_address(result))
        return adapters

    def get_v2_force_deallocate_penalty(self, vault_address: str, adapter: str) -> int | None:
        raw = self._try_eth_call(
            vault_address,
            V2_FORCE_DEALLOCATE_PENALTY_SELECTOR + _encode_address(adapter),
            "metamorpho_v2_force_deallocate_penalty",
        )
        return None if raw is None else _decode_uint256(raw)

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
        """Read complete vault information in multiple RPC calls (generation-aware).

        The ERC-4626 block is shared; the generation-specific block reads the
        selectors that exist on that generation only (v1 ``fee``/``timelock``
        revert on V2, V2 fee/adapter reads revert on v1).
        """
        version = self.detect_vault_version(vault_address)
        asset = self.get_vault_asset(vault_address)
        total_assets = self.get_total_assets(vault_address)
        total_supply = self.get_total_supply(vault_address)
        share_price = self.get_share_price(vault_address)
        decimals = self.get_decimals(vault_address)
        curator = self.get_curator(vault_address)
        if version == VAULT_VERSION_V2:
            liquidity_adapter = self.get_v2_liquidity_adapter(vault_address)
            return VaultInfo(
                address=vault_address,
                asset=asset,
                total_assets=total_assets,
                total_supply=total_supply,
                share_price=share_price,
                decimals=decimals,
                curator=curator,
                fee=self.get_v2_performance_fee(vault_address),
                timelock=0,
                vault_version=VAULT_VERSION_V2,
                management_fee=self.get_v2_management_fee(vault_address),
                liquidity_adapter=liquidity_adapter,
                adapters=self.get_v2_adapters(vault_address),
                force_deallocate_penalty=(
                    self.get_v2_force_deallocate_penalty(vault_address, liquidity_adapter)
                    if liquidity_adapter
                    else None
                ),
            )
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
            vault_version=VAULT_VERSION_V1,
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
