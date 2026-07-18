"""Lagoon Vault SDK - Low-level interface for Lagoon vault operations via gateway RPC.

This module provides a low-level SDK for interacting with Lagoon vault contracts
(ERC-7540) through the gateway's RPC service. All reads go through eth_call or
eth_getStorageAt via gateway_client.rpc.Call().

Supported operations:
- Read total assets, pending deposits/redemptions, share price
- Read underlying token balance for a wallet
- Read proposed total assets and silo address via storage slots
- Verify vault contract version
- Build unsigned transactions for vault write operations (propose, settle)

Example:
    from almanak.connectors.lagoon.sdk import LagoonVaultSDK

    sdk = LagoonVaultSDK(gateway_client, chain="ethereum")
    total = sdk.get_total_assets("0xVaultAddress")
"""

import json
import logging
from decimal import Decimal

from almanak.core.models.config import VaultVersion
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)

# Function selectors (keccak256 of canonical signatures, first 4 bytes)
TOTAL_ASSETS_SELECTOR = "0x01e1d114"  # totalAssets()
PENDING_DEPOSIT_REQUEST_SELECTOR = "0x26c6f96c"  # pendingDepositRequest(uint256,address)
PENDING_REDEEM_REQUEST_SELECTOR = "0xf5a23d8d"  # pendingRedeemRequest(uint256,address)
CONVERT_TO_ASSETS_SELECTOR = "0x07a2d13a"  # convertToAssets(uint256)
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"  # totalSupply()
VERSION_SELECTOR = "0x54fd4d50"  # version()
GET_ROLES_STORAGE_SELECTOR = "0x937147e3"  # getRolesStorage()
ASSET_SELECTOR = "0x38d52e0f"  # asset()
OWNER_SELECTOR = "0x8da5cb5b"  # owner()

# Write operation selectors (keccak256 of canonical signatures, first 4 bytes)
UPDATE_NEW_TOTAL_ASSETS_SELECTOR = "0xbcd1bf34"  # updateNewTotalAssets(uint256)
SETTLE_DEPOSIT_SELECTOR = "0xd24ca58a"  # settleDeposit(uint256)
SETTLE_REDEEM_SELECTOR = "0xa627df66"  # settleRedeem(uint256)

# Vault-release (teardown) selectors — Lagoon v0.5.0 Open->Closing->Closed (VIB-5667).
INITIATE_CLOSING_SELECTOR = "0xa4393915"  # initiateClosing()
CLOSE_SELECTOR = "0x0aebeb4e"  # close(uint256)
REDEEM_SELECTOR = "0xba087652"  # redeem(uint256,address,address)

# Lagoon v0.5.0 ``State`` enum (Enums.sol): Open=0, Closing=1, Closed=2.
VAULT_STATE_OPEN = "Open"
VAULT_STATE_CLOSING = "Closing"
VAULT_STATE_CLOSED = "Closed"
_VAULT_STATE_BY_ORDINAL = {0: VAULT_STATE_OPEN, 1: VAULT_STATE_CLOSING, 2: VAULT_STATE_CLOSED}

# ERC-7201 storage slot of the Vault ``state`` field.
# Namespace: keccak256(abi.encode(uint256(keccak256("hopper.storage.vault")) - 1)) & ~0xff.
# Lagoon v0.5.0 does NOT expose a public ``state()`` getter (the selector is
# absent from the deployed implementation bytecode), so the lifecycle state is
# read directly from storage. Verified on a Base fork: this slot holds 0 (Open)
# and flips to 1 (Closing) after ``initiateClosing()`` (emitting StateUpdated),
# then 2 (Closed) after ``close()``. The ordinal is the low byte of the word.
VAULT_STATE_SLOT = 0x0E6B3200A60A991C539F47DDDACA04A18EB4BCF2B53906FB44751D827F001400

# ERC-7540 deposit selectors
REQUEST_DEPOSIT_SELECTOR = "0x85b77f45"  # requestDeposit(uint256,address,address)
ERC20_APPROVE_SELECTOR = "0x095ea7b3"  # approve(address,uint256)
MAX_UINT256 = (1 << 256) - 1

# Lagoon v0.5.0 resets ``newTotalAssets`` to ``type(uint256).max`` after a valuation
# proposal is consumed by a settle call. Reading this sentinel from the proposal slot
# means "no live proposal" -- a fresh ``updateNewTotalAssets`` is required before the
# next settle, otherwise the settle reverts ``NewTotalAssetsMissing()`` (0x87d895da).
PROPOSAL_CONSUMED = MAX_UINT256

# ERC-7201 namespaced storage slots (Lagoon v0.5.0, hopperlabsxyz/lagoon-v0)
# Namespace: keccak256(abi.encode(uint256(keccak256("hopper.storage.ERC7540")) - 1)) & ~bytes32(uint256(0xff))
# Base slot: 0x5c74d456014b1c0eb4368d944667a568313858a3029a650ff0cb7b56f8b57a00
# Struct ERC7540Storage { totalAssets(+0), newTotalAssets(+1), ..., pendingSilo(+8) }
PROPOSED_TOTAL_ASSETS_SLOT = 0x5C74D456014B1C0EB4368D944667A568313858A3029A650FF0CB7B56F8B57A01  # base+1
SILO_ADDRESS_SLOT = 0x5C74D456014B1C0EB4368D944667A568313858A3029A650FF0CB7B56F8B57A08  # base+8


def _encode_address(address: str) -> str:
    """ABI-encode an address as a 32-byte left-padded hex string."""
    addr = address.lower().removeprefix("0x")
    return addr.zfill(64)


def _encode_uint256(value: int) -> str:
    """ABI-encode a uint256 as a 32-byte big-endian hex string."""
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
    # Take last 40 hex chars (20 bytes)
    return "0x" + clean[-40:]


class LagoonVaultSDK:
    """Low-level SDK for reading Lagoon vault state via gateway RPC calls.

    All RPC calls are routed through the gateway client's RPC service.
    This SDK handles ABI encoding/decoding and provides typed return values.

    Args:
        gateway_client: Connected gateway client with RPC service
        chain: Chain identifier (e.g., "ethereum", "base")
    """

    def __init__(self, gateway_client, chain: str):
        self._gateway_client = gateway_client
        self._chain = chain.lower()

    def _ensure_gateway_client(self) -> None:
        """Fail fast with a clear cause when the gateway client is unavailable.

        The vault reads (including the release-lifecycle reads used during teardown)
        route through ``self._gateway_client.rpc``. A missing / unwired client should
        surface an actionable error BEFORE an RPC is issued, not an opaque
        ``AttributeError`` deep in the call — otherwise a disconnected client during a
        teardown release reads as an inscrutable failure rather than "no gateway".
        """
        client = self._gateway_client
        if client is None or getattr(client, "rpc", None) is None:
            raise RuntimeError(
                "Lagoon SDK gateway client is unavailable (no RPC channel) — cannot issue on-chain reads. "
                "The strategy container reaches chain state only through a connected gateway."
            )

    def _eth_call(self, to: str, data: str, request_id: str = "lagoon_sdk") -> str:
        """Make an eth_call via the gateway and return the hex result."""
        self._ensure_gateway_client()
        request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method="eth_call",
            params=json.dumps([{"to": to, "data": data}, "latest"]),
            id=request_id,
        )
        response = self._gateway_client.rpc.Call(request, timeout=30.0)
        if not response.success:
            error_msg = response.error if response.error else "Unknown RPC error"
            raise RuntimeError(f"eth_call failed for {request_id}: {error_msg}")
        return json.loads(response.result)

    def _eth_get_storage_at(self, address: str, slot: int, request_id: str = "lagoon_sdk") -> str:
        """Read a storage slot via eth_getStorageAt."""
        self._ensure_gateway_client()
        request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method="eth_getStorageAt",
            params=json.dumps([address, hex(slot), "latest"]),
            id=request_id,
        )
        response = self._gateway_client.rpc.Call(request, timeout=30.0)
        if not response.success:
            error_msg = response.error if response.error else "Unknown RPC error"
            raise RuntimeError(f"eth_getStorageAt failed for {request_id}: {error_msg}")
        return json.loads(response.result)

    def get_total_assets(self, vault_address: str) -> int:
        """Read the vault's total assets (totalAssets()).

        Returns:
            Total assets in underlying token units (raw integer).
        """
        result = self._eth_call(
            to=vault_address,
            data=TOTAL_ASSETS_SELECTOR,
            request_id="lagoon_total_assets",
        )
        return _decode_uint256(result)

    def get_pending_deposits(self, vault_address: str, request_id_num: int = 0) -> int:
        """Read pending deposit requests for the vault.

        Uses ERC-7540 pendingDepositRequest(uint256,address) with requestId=0
        and the vault address as the controller.

        Returns:
            Pending deposits in underlying token units (raw integer).
        """
        calldata = PENDING_DEPOSIT_REQUEST_SELECTOR + _encode_uint256(request_id_num) + _encode_address(vault_address)
        result = self._eth_call(
            to=vault_address,
            data=calldata,
            request_id="lagoon_pending_deposits",
        )
        return _decode_uint256(result)

    def get_pending_redemptions(self, vault_address: str, request_id_num: int = 0) -> int:
        """Read pending redemption requests for the vault.

        Uses ERC-7540 pendingRedeemRequest(uint256,address) with requestId=0
        and the vault address as the controller.

        Returns:
            Pending redemptions in share units (raw integer).
        """
        calldata = PENDING_REDEEM_REQUEST_SELECTOR + _encode_uint256(request_id_num) + _encode_address(vault_address)
        result = self._eth_call(
            to=vault_address,
            data=calldata,
            request_id="lagoon_pending_redemptions",
        )
        return _decode_uint256(result)

    def get_share_price(self, vault_address: str) -> Decimal:
        """Get the current share price by converting 1 share to assets.

        Uses convertToAssets(1e18) to get the value of one full share
        in underlying token units.

        Returns:
            Share price as a Decimal (assets per share, normalized to 18 decimals).
        """
        one_share = 10**18
        calldata = CONVERT_TO_ASSETS_SELECTOR + _encode_uint256(one_share)
        result = self._eth_call(
            to=vault_address,
            data=calldata,
            request_id="lagoon_share_price",
        )
        assets_per_share = _decode_uint256(result)
        return Decimal(assets_per_share) / Decimal(one_share)

    def convert_to_assets(self, vault_address: str, shares: int) -> int:
        """Convert a share amount to underlying asset units via on-chain call.

        Uses the ERC-4626 convertToAssets(uint256) function directly with the
        given share amount, avoiding precision loss from intermediate division.

        Args:
            vault_address: Vault contract address.
            shares: Share amount in raw units (18 decimals).

        Returns:
            Equivalent underlying asset amount in raw units (underlying decimals).
        """
        calldata = CONVERT_TO_ASSETS_SELECTOR + _encode_uint256(shares)
        result = self._eth_call(
            to=vault_address,
            data=calldata,
            request_id="lagoon_convert_to_assets",
        )
        return _decode_uint256(result)

    def get_underlying_balance(self, vault_address: str, wallet_address: str) -> int:
        """Read the underlying token balance of a wallet in the vault context.

        Uses balanceOf(address) on the vault to read share balance, then could
        be converted to underlying. Returns raw share balance.

        Returns:
            Share balance (raw integer).
        """
        calldata = BALANCE_OF_SELECTOR + _encode_address(wallet_address)
        result = self._eth_call(
            to=vault_address,
            data=calldata,
            request_id="lagoon_underlying_balance",
        )
        return _decode_uint256(result)

    def get_proposed_total_assets(self, vault_address: str) -> int:
        """Read the proposed total assets via direct storage slot read.

        This value is set during the propose phase of settlement and
        represents the valuator's proposed total asset value.

        Returns:
            Proposed total assets in underlying token units (raw integer).
        """
        result = self._eth_get_storage_at(
            address=vault_address,
            slot=PROPOSED_TOTAL_ASSETS_SLOT,
            request_id="lagoon_proposed_total_assets",
        )
        return _decode_uint256(result)

    def has_live_proposal(self, vault_address: str, expected: int | None = None) -> bool:
        """Return whether the vault currently holds a live (unconsumed) valuation proposal.

        Lagoon v0.5.0 makes ``updateNewTotalAssets`` single-use: a settle call consumes
        the proposal and resets the ``newTotalAssets`` slot to ``type(uint256).max``
        (``PROPOSAL_CONSUMED``). This read distinguishes "proposal still pending" from
        "proposal already spent", which the settlement state machine needs to avoid
        re-issuing a settle against a consumed proposal (which would revert
        ``NewTotalAssetsMissing()``).

        Args:
            vault_address: The vault contract address.
            expected: If provided, additionally require the live proposal to equal this
                value (guards against confirming a stale or unrelated proposal).

        Returns:
            True if a live proposal exists (and matches ``expected`` when given).
        """
        proposed = self.get_proposed_total_assets(vault_address)
        if proposed == PROPOSAL_CONSUMED:
            return False
        if expected is not None:
            return proposed == expected
        return True

    def get_silo_address(self, vault_address: str) -> str:
        """Read the silo contract address via direct storage slot read.

        The silo is a helper contract that holds deposited assets during
        the settlement process.

        Returns:
            Silo contract address as a checksummed hex string.
        """
        result = self._eth_get_storage_at(
            address=vault_address,
            slot=SILO_ADDRESS_SLOT,
            request_id="lagoon_silo_address",
        )
        return _decode_address(result)

    def get_new_total_assets(self, vault_address: str) -> int:
        """Read the live proposed ``newTotalAssets()`` slot (VIB-5667).

        Alias of :meth:`get_proposed_total_assets` — Lagoon's ``newTotalAssets``
        lives at ``PROPOSED_TOTAL_ASSETS_SLOT`` (base+1). The vault-release path
        reads this back from chain AFTER ``updateNewTotalAssets`` and passes the
        exact value to ``close(uint256)`` so the call cannot revert
        ``WrongNewTotalAssets``. Returns ``PROPOSAL_CONSUMED`` (``type(uint256).max``)
        when no live proposal exists.

        Returns:
            The proposed total assets in underlying units (raw integer).
        """
        return self.get_proposed_total_assets(vault_address)

    def get_vault_state(self, vault_address: str) -> str:
        """Read the vault's lifecycle state: Open / Closing / Closed.

        Lagoon v0.5.0 does NOT expose a public ``state()`` getter, so the ``State``
        enum (Open=0, Closing=1, Closed=2) is read directly from its ERC-7201
        storage slot (``VAULT_STATE_SLOT``). Used by the vault-release state machine
        for idempotent resume (skip already-completed phases after a crash).

        Returns:
            One of ``"Open"``, ``"Closing"``, ``"Closed"``.

        Raises:
            RuntimeError: If the stored ordinal is not a known state (fail loud
                rather than silently treating an unknown state as Open).
        """
        result = self._eth_get_storage_at(
            address=vault_address,
            slot=VAULT_STATE_SLOT,
            request_id="lagoon_vault_state",
        )
        # The State enum ordinal is the low byte of the slot word; mask it so a
        # future packing of adjacent fields into this ERC-7201 slot can't turn a
        # valid 0/1/2 into a spurious "unknown ordinal" (or a chance misread).
        ordinal = _decode_uint256(result) & 0xFF
        state = _VAULT_STATE_BY_ORDINAL.get(ordinal)
        if state is None:
            raise RuntimeError(f"Unknown Lagoon vault state ordinal {ordinal} for {vault_address}")
        return state

    def get_underlying_token_address(self, vault_address: str) -> str:
        """Read the vault's underlying token address (ERC-4626 asset()).

        Returns:
            Underlying token contract address as a hex string.
        """
        result = self._eth_call(
            to=vault_address,
            data=ASSET_SELECTOR,
            request_id="lagoon_asset",
        )
        return _decode_address(result)

    def get_roles_storage(self, vault_address: str) -> dict:
        """Read the vault's RolesStorage via getRolesStorage().

        Returns a dict with the vault's role addresses, matching Lagoon v0.5.0:
            whitelistManager, feeReceiver, safe, feeRegistry, valuationManager

        Returns:
            Dict with keys: whitelistManager, feeReceiver, safe, feeRegistry, valuationManager.
        """
        result = self._eth_call(
            to=vault_address,
            data=GET_ROLES_STORAGE_SELECTOR,
            request_id="lagoon_get_roles",
        )
        # Returns 5 ABI-encoded addresses (5 * 64 hex chars = 320 chars)
        raw = result.removeprefix("0x")
        if len(raw) < 320:
            raise RuntimeError(f"getRolesStorage returned unexpected data length: {len(raw)}")
        return {
            "whitelistManager": _decode_address(raw[0:64]),
            "feeReceiver": _decode_address(raw[64:128]),
            "safe": _decode_address(raw[128:192]),
            "feeRegistry": _decode_address(raw[192:256]),
            "valuationManager": _decode_address(raw[256:320]),
        }

    def get_owner(self, vault_address: str) -> str:
        """Read the vault owner (``owner()`` — the ``init.admin``, Ownable).

        The owner is the only address authorised to call ``initiateClosing()``.
        The vault-release single-signer preflight asserts owner == safe == valuator
        == the gateway's signing key before attempting a close (VIB-5667).

        Returns:
            Owner address as a hex string.
        """
        result = self._eth_call(
            to=vault_address,
            data=OWNER_SELECTOR,
            request_id="lagoon_owner",
        )
        return _decode_address(result)

    def get_valuation_manager(self, vault_address: str) -> str:
        """Read the vault's valuation manager address.

        Convenience method that calls getRolesStorage() and extracts the
        valuationManager. This is the address authorized to call
        updateNewTotalAssets().

        Returns:
            Valuation manager address as a hex string.
        """
        roles = self.get_roles_storage(vault_address)
        return roles["valuationManager"]

    def get_curator(self, vault_address: str) -> str:
        """Read the vault's curator (Safe) address.

        Convenience method that calls getRolesStorage() and extracts the Safe address.
        This is the address that owns the vault and can call settleDeposit/settleRedeem.

        Returns:
            Curator (Safe) address as a hex string.
        """
        roles = self.get_roles_storage(vault_address)
        return roles["safe"]

    def verify_version(self, vault_address: str, expected_version: VaultVersion) -> None:
        """Verify the on-chain vault version matches the expected version.

        Reads the version() string from the vault contract and compares
        it to the expected VaultVersion. Raises ValueError on mismatch.

        Args:
            vault_address: The vault contract address.
            expected_version: The expected VaultVersion enum value.

        Raises:
            ValueError: If the on-chain version does not match.
        """
        result = self._eth_call(
            to=vault_address,
            data=VERSION_SELECTOR,
            request_id="lagoon_verify_version",
        )
        # version() returns an ABI-encoded string: offset (32 bytes) + length (32 bytes) + data
        raw = result.removeprefix("0x")
        if len(raw) < 128:
            raise ValueError(f"Unexpected version response length: {len(raw)} hex chars")
        # Skip offset (first 32 bytes = 64 hex chars), read length
        length = int(raw[64:128], 16)
        # Read the string data
        version_hex = raw[128 : 128 + length * 2]
        on_chain_version = bytes.fromhex(version_hex).decode("utf-8")

        # Lagoon vault contracts return a 'v'-prefixed semver string (e.g.
        # "v0.5.0"), while the VaultVersion enum is canonical bare semver
        # ("0.5.0"). Normalise the on-chain value (trim whitespace, strip a
        # single leading 'v'/'V') before comparison so a genuinely deployed
        # v0.5.0 vault matches its enum member instead of failing preflight
        # on every settlement.
        normalized = on_chain_version.strip()
        if normalized[:1] in ("v", "V"):
            normalized = normalized[1:]

        if normalized != expected_version.value:
            raise ValueError(
                f"Vault version mismatch: on-chain '{on_chain_version}' != expected '{expected_version.value}'"
            )
        logger.info("Vault version verified: %s", on_chain_version)

    # --- Write Operations (build unsigned transactions) ---

    def build_update_total_assets_tx(self, vault_address: str, valuator_address: str, new_total_assets: int) -> dict:
        """Build an unsigned transaction for updateNewTotalAssets(uint256).

        This is called by the valuator to propose a new total asset valuation
        during the settlement process.

        Args:
            vault_address: The vault contract address.
            valuator_address: The valuator's address (tx sender).
            new_total_assets: The proposed total assets in underlying token units.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        calldata = UPDATE_NEW_TOTAL_ASSETS_SELECTOR + _encode_uint256(new_total_assets)
        return {
            "to": vault_address,
            "from": valuator_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": 100_000,
        }

    def build_settle_deposit_tx(self, vault_address: str, safe_address: str, total_assets: int) -> dict:
        """Build an unsigned transaction for settleDeposit(uint256).

        This is called by the safe (vault owner) to settle pending deposits
        after the valuator has proposed a new total asset value.

        Args:
            vault_address: The vault contract address.
            safe_address: The safe wallet address (tx sender).
            total_assets: The total assets value for settlement.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        calldata = SETTLE_DEPOSIT_SELECTOR + _encode_uint256(total_assets)
        return {
            "to": vault_address,
            "from": safe_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": 200_000,
        }

    def build_settle_redeem_tx(self, vault_address: str, safe_address: str, total_assets: int) -> dict:
        """Build an unsigned transaction for settleRedeem(uint256).

        This is called by the safe (vault owner) to settle pending redemptions
        after deposits have been settled.

        Args:
            vault_address: The vault contract address.
            safe_address: The safe wallet address (tx sender).
            total_assets: The total assets value for settlement.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        calldata = SETTLE_REDEEM_SELECTOR + _encode_uint256(total_assets)
        return {
            "to": vault_address,
            "from": safe_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": 200_000,
        }

    def build_initiate_closing_tx(self, vault_address: str, owner_address: str) -> dict:
        """Build an unsigned transaction for ``initiateClosing()`` (VIB-5667).

        Called by the vault owner (``init.admin``) to transition Open->Closing.
        Re-proposes the pending NAV if one is defined. No calldata arguments.

        Args:
            vault_address: The vault contract address.
            owner_address: The vault owner's address (tx sender).

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        return {
            "to": vault_address,
            "from": owner_address,
            "data": INITIATE_CLOSING_SELECTOR,
            "value": "0",
            "gas_estimate": 200_000,
        }

    def build_close_tx(self, vault_address: str, safe_address: str, new_total_assets: int) -> dict:
        """Build an unsigned transaction for ``close(uint256)`` (VIB-5667).

        Called by the Safe (Closing->Closed). Atomically takes fees, settles
        deposits + redeems, sets ``state=Closed`` and pulls ``new_total_assets``
        underlying from the Safe into the vault. ``new_total_assets`` MUST equal
        the on-chain ``newTotalAssets()`` slot exactly (read it back via
        :meth:`get_new_total_assets` between propose and close), otherwise the
        call reverts ``WrongNewTotalAssets``.

        Args:
            vault_address: The vault contract address.
            safe_address: The Safe wallet address (tx sender).
            new_total_assets: Final NAV; must equal the live ``newTotalAssets()``.

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        calldata = CLOSE_SELECTOR + _encode_uint256(new_total_assets)
        return {
            "to": vault_address,
            "from": safe_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": 500_000,
        }

    def build_redeem_tx(self, vault_address: str, controller_address: str, shares: int) -> dict:
        """Build an unsigned tx for ERC-4626 ``redeem(uint256,address,address)``.

        Post-close synchronous redemption: burns ``shares`` and transfers the
        underlying to ``controller_address``. Calls
        ``redeem(shares, receiver=controller, owner=controller)`` — used to sweep
        the manager's own residual shares after the vault is Closed (VIB-5667).

        Args:
            vault_address: The vault contract address.
            controller_address: The share owner/receiver (tx sender).
            shares: Number of shares to redeem (raw 18-decimal units).

        Returns:
            Unsigned transaction dict with keys: to, from, data, value, gas_estimate.
        """
        calldata = (
            REDEEM_SELECTOR
            + _encode_uint256(shares)
            + _encode_address(controller_address)
            + _encode_address(controller_address)
        )
        return {
            "to": vault_address,
            "from": controller_address,
            "data": calldata,
            "value": "0",
            "gas_estimate": 250_000,
        }

    # --- ERC-7540 Deposit Operations ---

    def build_approve_deposit_tx(self, underlying_token: str, vault_address: str, depositor: str, amount: int) -> dict:
        """Build an ERC20 approve tx so the vault can pull underlying tokens.

        The depositor must approve the vault to spend `amount` of the underlying
        token before calling requestDeposit.

        Args:
            underlying_token: Address of the underlying ERC20 token.
            vault_address: The vault contract address (spender).
            depositor: The depositor address (tx sender / token owner).
            amount: Amount in raw underlying units to approve.

        Returns:
            Unsigned transaction dict.
        """
        calldata = ERC20_APPROVE_SELECTOR + _encode_address(vault_address) + _encode_uint256(amount)
        return {
            "to": underlying_token,
            "from": depositor,
            "data": calldata,
            "value": "0",
            "gas_estimate": 60_000,
        }

    def build_request_deposit_tx(self, vault_address: str, depositor: str, amount: int) -> dict:
        """Build an ERC-7540 requestDeposit(uint256,address,address) tx.

        Calls vault.requestDeposit(assets, controller=depositor, owner=depositor).
        The depositor must have approved the vault for `amount` of underlying first.

        Args:
            vault_address: The vault contract address.
            depositor: The depositor address (controller and owner).
            amount: Amount of underlying tokens to deposit (raw units).

        Returns:
            Unsigned transaction dict.
        """
        calldata = (
            REQUEST_DEPOSIT_SELECTOR + _encode_uint256(amount) + _encode_address(depositor) + _encode_address(depositor)
        )
        return {
            "to": vault_address,
            "from": depositor,
            "data": calldata,
            "value": "0",
            "gas_estimate": 150_000,
        }
