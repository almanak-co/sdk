"""Tests for LagoonVaultSDK read and write operations."""

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.core.models.config import VaultVersion
from almanak.framework.connectors.lagoon.sdk import (
    BALANCE_OF_SELECTOR,
    CONVERT_TO_ASSETS_SELECTOR,
    PENDING_DEPOSIT_REQUEST_SELECTOR,
    PENDING_REDEEM_REQUEST_SELECTOR,
    PROPOSED_TOTAL_ASSETS_SLOT,
    SETTLE_DEPOSIT_SELECTOR,
    SETTLE_REDEEM_SELECTOR,
    SILO_ADDRESS_SLOT,
    TOTAL_ASSETS_SELECTOR,
    UPDATE_NEW_TOTAL_ASSETS_SELECTOR,
    VERSION_SELECTOR,
    LagoonVaultSDK,
    _decode_address,
    _decode_uint256,
    _encode_address,
    _encode_uint256,
)

VAULT_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
WALLET_ADDRESS = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


@pytest.fixture
def mock_gateway_client():
    """Create a mock gateway client with an RPC stub."""
    client = MagicMock()
    client.rpc = MagicMock()
    return client


@pytest.fixture
def sdk(mock_gateway_client):
    """Create a LagoonVaultSDK instance with mocked gateway."""
    return LagoonVaultSDK(mock_gateway_client, chain="ethereum")


def _make_rpc_response(result_hex: str, success: bool = True, error: str = ""):
    """Create a mock RPC response."""
    response = MagicMock()
    response.success = success
    response.result = json.dumps(result_hex)
    response.error = error
    return response


# --- Encoding/Decoding Helpers ---


class TestEncodingHelpers:
    def test_encode_address(self):
        addr = "0xAbCdEf1234567890abcdef1234567890AbCdEf12"
        result = _encode_address(addr)
        assert len(result) == 64
        assert result == "000000000000000000000000abcdef1234567890abcdef1234567890abcdef12"

    def test_encode_address_no_prefix(self):
        addr = "AbCdEf1234567890abcdef1234567890AbCdEf12"
        result = _encode_address(addr)
        assert len(result) == 64

    def test_encode_uint256_zero(self):
        result = _encode_uint256(0)
        assert result == "0" * 64

    def test_encode_uint256_one(self):
        result = _encode_uint256(1)
        assert result == "0" * 63 + "1"

    def test_encode_uint256_large(self):
        val = 10**18
        result = _encode_uint256(val)
        assert len(result) == 64
        assert int(result, 16) == val

    def test_decode_uint256(self):
        hex_str = "0x" + "0" * 63 + "a"
        assert _decode_uint256(hex_str) == 10

    def test_decode_uint256_large(self):
        val = 123456789012345678
        hex_str = "0x" + hex(val)[2:].zfill(64)
        assert _decode_uint256(hex_str) == val

    def test_decode_address(self):
        raw = "0x" + "0" * 24 + "abcdef1234567890abcdef1234567890abcdef12"
        result = _decode_address(raw)
        assert result == "0xabcdef1234567890abcdef1234567890abcdef12"


# --- Read Methods ---


class TestGetTotalAssets:
    def test_returns_decoded_uint256(self, sdk, mock_gateway_client):
        total_assets = 1_000_000 * 10**6  # 1M USDC (6 decimals)
        hex_result = "0x" + _encode_uint256(total_assets)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_total_assets(VAULT_ADDRESS)

        assert result == total_assets

    def test_calls_gateway_with_correct_params(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_total_assets(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        assert request.chain == "ethereum"
        assert request.method == "eth_call"
        params = json.loads(request.params)
        assert params[0]["to"] == VAULT_ADDRESS
        assert params[0]["data"] == TOTAL_ASSETS_SELECTOR
        assert params[1] == "latest"

    def test_raises_on_rpc_failure(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("", success=False, error="revert")

        with pytest.raises(RuntimeError, match="eth_call failed"):
            sdk.get_total_assets(VAULT_ADDRESS)


class TestGetPendingDeposits:
    def test_returns_decoded_uint256(self, sdk, mock_gateway_client):
        pending = 500_000 * 10**6  # 500K USDC
        hex_result = "0x" + _encode_uint256(pending)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_pending_deposits(VAULT_ADDRESS)

        assert result == pending

    def test_encodes_vault_address_as_param(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_pending_deposits(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        params = json.loads(request.params)
        calldata = params[0]["data"]
        assert calldata.startswith(PENDING_DEPOSIT_REQUEST_SELECTOR)
        # ERC-7540: pendingDepositRequest(uint256,address) encodes requestId + address
        encoded_params = calldata[len(PENDING_DEPOSIT_REQUEST_SELECTOR) :]
        assert len(encoded_params) == 128  # uint256 (64) + address (64)
        assert VAULT_ADDRESS.lower()[2:] in encoded_params


class TestGetPendingRedemptions:
    def test_returns_decoded_uint256(self, sdk, mock_gateway_client):
        pending = 100 * 10**18  # 100 shares
        hex_result = "0x" + _encode_uint256(pending)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_pending_redemptions(VAULT_ADDRESS)

        assert result == pending

    def test_encodes_vault_address_as_param(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_pending_redemptions(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        params = json.loads(request.params)
        calldata = params[0]["data"]
        assert calldata.startswith(PENDING_REDEEM_REQUEST_SELECTOR)


class TestGetSharePrice:
    def test_returns_decimal_ratio(self, sdk, mock_gateway_client):
        # convertToAssets(1e18) returns 1.05e18 (5% gain)
        assets_per_share = int(1.05 * 10**18)
        hex_result = "0x" + _encode_uint256(assets_per_share)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_share_price(VAULT_ADDRESS)

        assert isinstance(result, Decimal)
        # Should be approximately 1.05
        assert result == Decimal(assets_per_share) / Decimal(10**18)

    def test_encodes_one_share(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + _encode_uint256(10**18))

        sdk.get_share_price(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        params = json.loads(request.params)
        calldata = params[0]["data"]
        assert calldata.startswith(CONVERT_TO_ASSETS_SELECTOR)
        # Verify 1e18 is encoded
        encoded_amount = calldata[len(CONVERT_TO_ASSETS_SELECTOR) :]
        assert int(encoded_amount, 16) == 10**18

    def test_par_value(self, sdk, mock_gateway_client):
        """Share price should be 1.0 when assets == shares."""
        hex_result = "0x" + _encode_uint256(10**18)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_share_price(VAULT_ADDRESS)

        assert result == Decimal(1)


class TestGetUnderlyingBalance:
    def test_returns_decoded_uint256(self, sdk, mock_gateway_client):
        balance = 50 * 10**18  # 50 shares
        hex_result = "0x" + _encode_uint256(balance)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_underlying_balance(VAULT_ADDRESS, WALLET_ADDRESS)

        assert result == balance

    def test_encodes_wallet_address(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_underlying_balance(VAULT_ADDRESS, WALLET_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        params = json.loads(request.params)
        calldata = params[0]["data"]
        assert calldata.startswith(BALANCE_OF_SELECTOR)
        encoded_addr = calldata[len(BALANCE_OF_SELECTOR) :]
        assert WALLET_ADDRESS.lower()[2:] in encoded_addr


# --- Storage Slot Reads ---


class TestGetProposedTotalAssets:
    def test_returns_decoded_uint256(self, sdk, mock_gateway_client):
        proposed = 2_000_000 * 10**6  # 2M USDC
        hex_result = "0x" + _encode_uint256(proposed)
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_proposed_total_assets(VAULT_ADDRESS)

        assert result == proposed

    def test_uses_eth_get_storage_at(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_proposed_total_assets(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        assert request.method == "eth_getStorageAt"
        params = json.loads(request.params)
        assert params[0] == VAULT_ADDRESS
        assert params[1] == hex(PROPOSED_TOTAL_ASSETS_SLOT)
        assert params[2] == "latest"

    def test_raises_on_rpc_failure(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("", success=False, error="timeout")

        with pytest.raises(RuntimeError, match="eth_getStorageAt failed"):
            sdk.get_proposed_total_assets(VAULT_ADDRESS)


class TestGetSiloAddress:
    def test_returns_decoded_address(self, sdk, mock_gateway_client):
        silo = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        hex_result = "0x" + "0" * 24 + silo[2:]
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        result = sdk.get_silo_address(VAULT_ADDRESS)

        assert result.lower() == silo.lower()

    def test_uses_correct_storage_slot(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x" + "0" * 64)

        sdk.get_silo_address(VAULT_ADDRESS)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        assert request.method == "eth_getStorageAt"
        params = json.loads(request.params)
        assert params[1] == hex(SILO_ADDRESS_SLOT)


# --- Version Verification ---


class TestVerifyVersion:
    def _encode_abi_string(self, text: str) -> str:
        """Encode a string as ABI-encoded dynamic bytes."""
        text_bytes = text.encode("utf-8")
        # offset (32 bytes pointing to 0x20 = 32)
        offset = "0" * 62 + "20"
        # length (32 bytes)
        length = _encode_uint256(len(text_bytes))
        # data (padded to 32 bytes)
        data_hex = text_bytes.hex()
        padding = "0" * (64 - len(data_hex)) if len(data_hex) < 64 else ""
        return "0x" + offset + length + data_hex + padding

    def test_succeeds_on_matching_version(self, sdk, mock_gateway_client):
        hex_result = self._encode_abi_string("0.5.0")
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        # Should not raise
        sdk.verify_version(VAULT_ADDRESS, VaultVersion.V0_5_0)

    def test_raises_on_version_mismatch(self, sdk, mock_gateway_client):
        hex_result = self._encode_abi_string("0.3.0")
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        with pytest.raises(ValueError, match="Vault version mismatch"):
            sdk.verify_version(VAULT_ADDRESS, VaultVersion.V0_5_0)

    def test_calls_version_selector(self, sdk, mock_gateway_client):
        hex_result = self._encode_abi_string("0.5.0")
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        sdk.verify_version(VAULT_ADDRESS, VaultVersion.V0_5_0)

        call_args = mock_gateway_client.rpc.Call.call_args
        request = call_args[0][0]
        params = json.loads(request.params)
        assert params[0]["data"] == VERSION_SELECTOR

    def test_v1_0_0_version(self, sdk, mock_gateway_client):
        hex_result = self._encode_abi_string("1.0.0")
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response(hex_result)

        sdk.verify_version(VAULT_ADDRESS, VaultVersion.V1_0_0)

    def test_raises_on_short_response(self, sdk, mock_gateway_client):
        mock_gateway_client.rpc.Call.return_value = _make_rpc_response("0x1234")

        with pytest.raises(ValueError, match="Unexpected version response length"):
            sdk.verify_version(VAULT_ADDRESS, VaultVersion.V0_5_0)


# --- Constructor ---


class TestConstructor:
    def test_stores_chain_lowercase(self, mock_gateway_client):
        sdk = LagoonVaultSDK(mock_gateway_client, chain="Ethereum")
        assert sdk._chain == "ethereum"

    def test_stores_gateway_client(self, mock_gateway_client):
        sdk = LagoonVaultSDK(mock_gateway_client, chain="ethereum")
        assert sdk._gateway_client is mock_gateway_client


# --- Write Operations (build unsigned transactions) ---

VALUATOR_ADDRESS = "0x1111111111111111111111111111111111111111"
SAFE_ADDRESS = "0x2222222222222222222222222222222222222222"


class TestBuildUpdateTotalAssetsTx:
    def test_returns_correct_structure(self, sdk):
        tx = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 1_000_000)

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == VALUATOR_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 100_000
        assert "data" in tx

    def test_encodes_correct_selector(self, sdk):
        tx = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 0)

        assert tx["data"].startswith(UPDATE_NEW_TOTAL_ASSETS_SELECTOR)

    def test_encodes_uint256_argument(self, sdk):
        total_assets = 1_000_000 * 10**6  # 1M USDC
        tx = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, total_assets)

        calldata = tx["data"]
        encoded_arg = calldata[len(UPDATE_NEW_TOTAL_ASSETS_SELECTOR):]
        assert len(encoded_arg) == 64
        assert int(encoded_arg, 16) == total_assets

    def test_zero_total_assets(self, sdk):
        tx = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 0)

        calldata = tx["data"]
        encoded_arg = calldata[len(UPDATE_NEW_TOTAL_ASSETS_SELECTOR):]
        assert int(encoded_arg, 16) == 0

    def test_large_total_assets(self, sdk):
        large_value = 10**30  # Very large number
        tx = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, large_value)

        calldata = tx["data"]
        encoded_arg = calldata[len(UPDATE_NEW_TOTAL_ASSETS_SELECTOR):]
        assert int(encoded_arg, 16) == large_value

    def test_does_not_call_rpc(self, sdk, mock_gateway_client):
        """Write methods build transactions locally, no RPC calls."""
        sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 1_000_000)

        mock_gateway_client.rpc.Call.assert_not_called()


class TestBuildSettleDepositTx:
    def test_returns_correct_structure(self, sdk):
        tx = sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 1_000_000)

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == SAFE_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 200_000
        assert "data" in tx

    def test_encodes_correct_selector(self, sdk):
        tx = sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 0)

        assert tx["data"].startswith(SETTLE_DEPOSIT_SELECTOR)

    def test_encodes_uint256_argument(self, sdk):
        total_assets = 2_000_000 * 10**6  # 2M USDC
        tx = sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, total_assets)

        calldata = tx["data"]
        encoded_arg = calldata[len(SETTLE_DEPOSIT_SELECTOR):]
        assert len(encoded_arg) == 64
        assert int(encoded_arg, 16) == total_assets

    def test_zero_total_assets(self, sdk):
        tx = sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 0)

        calldata = tx["data"]
        encoded_arg = calldata[len(SETTLE_DEPOSIT_SELECTOR):]
        assert int(encoded_arg, 16) == 0

    def test_does_not_call_rpc(self, sdk, mock_gateway_client):
        """Write methods build transactions locally, no RPC calls."""
        sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 1_000_000)

        mock_gateway_client.rpc.Call.assert_not_called()


class TestBuildSettleRedeemTx:
    def test_returns_correct_structure(self, sdk):
        tx = sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 1_000_000)

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == SAFE_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 200_000
        assert "data" in tx

    def test_encodes_correct_selector(self, sdk):
        tx = sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 0)

        assert tx["data"].startswith(SETTLE_REDEEM_SELECTOR)

    def test_encodes_uint256_argument(self, sdk):
        total_assets = 500_000 * 10**6  # 500K USDC
        tx = sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, total_assets)

        calldata = tx["data"]
        encoded_arg = calldata[len(SETTLE_REDEEM_SELECTOR):]
        assert len(encoded_arg) == 64
        assert int(encoded_arg, 16) == total_assets

    def test_zero_total_assets(self, sdk):
        tx = sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 0)

        calldata = tx["data"]
        encoded_arg = calldata[len(SETTLE_REDEEM_SELECTOR):]
        assert int(encoded_arg, 16) == 0

    def test_does_not_call_rpc(self, sdk, mock_gateway_client):
        """Write methods build transactions locally, no RPC calls."""
        sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 1_000_000)

        mock_gateway_client.rpc.Call.assert_not_called()


class TestWriteMethodCalldata:
    """Cross-cutting tests for calldata format consistency across all write methods."""

    def test_all_write_methods_produce_correct_length_calldata(self, sdk):
        """All write methods should produce selector (10 chars with 0x, or 8+2) + 64 char arg."""
        methods = [
            sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 42),
            sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 42),
            sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 42),
        ]
        for tx in methods:
            calldata = tx["data"]
            # Selector is "0x" + 8 hex chars, argument is 64 hex chars
            assert calldata.startswith("0x")
            # Total: "0x" prefix + 8 selector + 64 arg = 74 chars
            assert len(calldata) == 74, f"Unexpected calldata length: {len(calldata)} for {calldata[:12]}..."

    def test_different_selectors_for_each_method(self, sdk):
        tx_propose = sdk.build_update_total_assets_tx(VAULT_ADDRESS, VALUATOR_ADDRESS, 100)
        tx_deposit = sdk.build_settle_deposit_tx(VAULT_ADDRESS, SAFE_ADDRESS, 100)
        tx_redeem = sdk.build_settle_redeem_tx(VAULT_ADDRESS, SAFE_ADDRESS, 100)

        selectors = {
            tx_propose["data"][:10],
            tx_deposit["data"][:10],
            tx_redeem["data"][:10],
        }
        assert len(selectors) == 3, "All three write methods should have distinct selectors"
