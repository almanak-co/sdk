"""Tests for LagoonVaultAdapter ActionBundle building."""

from unittest.mock import MagicMock

import pytest

from almanak.core.models.params import (
    SettleDepositParams,
    SettleRedeemParams,
    UpdateTotalAssetsParams,
)
from almanak.framework.connectors.lagoon.adapter import LagoonVaultAdapter
from almanak.framework.connectors.lagoon.sdk import (
    SETTLE_DEPOSIT_SELECTOR,
    SETTLE_REDEEM_SELECTOR,
    UPDATE_NEW_TOTAL_ASSETS_SELECTOR,
    LagoonVaultSDK,
    _encode_uint256,
)

VAULT_ADDRESS = "0x1234567890abcdef1234567890abcdef12345678"
VALUATOR_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SAFE_ADDRESS = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


@pytest.fixture
def mock_sdk():
    """Create a LagoonVaultSDK with a mocked gateway client."""
    gateway_client = MagicMock()
    return LagoonVaultSDK(gateway_client, chain="ethereum")


@pytest.fixture
def adapter(mock_sdk):
    """Create a LagoonVaultAdapter with real SDK (no RPC calls needed for writes)."""
    return LagoonVaultAdapter(mock_sdk)


# --- Propose Valuation Bundle ---


class TestBuildProposeValuationBundle:
    def test_returns_action_bundle(self, adapter):
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=5_000_000,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)

        assert bundle.intent_type == "PROPOSE_VAULT_VALUATION"
        assert len(bundle.transactions) == 1

    def test_transaction_fields(self, adapter):
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=1_000_000,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)
        tx = bundle.transactions[0]

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == VALUATOR_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 100_000

    def test_calldata_encoding(self, adapter):
        total_assets = 123_456_789
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=total_assets,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)
        tx = bundle.transactions[0]

        expected_data = UPDATE_NEW_TOTAL_ASSETS_SELECTOR + _encode_uint256(total_assets)
        assert tx["data"] == expected_data

    def test_metadata(self, adapter):
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=999,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)

        assert bundle.metadata["vault_address"] == VAULT_ADDRESS
        assert bundle.metadata["new_total_assets"] == 999

    def test_zero_total_assets(self, adapter):
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=0,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)
        tx = bundle.transactions[0]

        expected_data = UPDATE_NEW_TOTAL_ASSETS_SELECTOR + _encode_uint256(0)
        assert tx["data"] == expected_data

    def test_large_total_assets(self, adapter):
        large_value = 10**18  # 1e18 in raw units
        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=large_value,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)
        tx = bundle.transactions[0]

        expected_data = UPDATE_NEW_TOTAL_ASSETS_SELECTOR + _encode_uint256(large_value)
        assert tx["data"] == expected_data


# --- Settle Deposit Bundle ---


class TestBuildSettleDepositBundle:
    def test_returns_action_bundle(self, adapter):
        params = SettleDepositParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=5_000_000,
        )
        bundle = adapter.build_settle_deposit_bundle(params)

        assert bundle.intent_type == "SETTLE_VAULT_DEPOSIT"
        assert len(bundle.transactions) == 1

    def test_transaction_fields(self, adapter):
        params = SettleDepositParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=1_000_000,
        )
        bundle = adapter.build_settle_deposit_bundle(params)
        tx = bundle.transactions[0]

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == SAFE_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 200_000

    def test_calldata_encoding(self, adapter):
        total_assets = 999_888_777
        params = SettleDepositParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=total_assets,
        )
        bundle = adapter.build_settle_deposit_bundle(params)
        tx = bundle.transactions[0]

        expected_data = SETTLE_DEPOSIT_SELECTOR + _encode_uint256(total_assets)
        assert tx["data"] == expected_data

    def test_metadata(self, adapter):
        params = SettleDepositParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=42,
        )
        bundle = adapter.build_settle_deposit_bundle(params)

        assert bundle.metadata["vault_address"] == VAULT_ADDRESS
        assert bundle.metadata["total_assets"] == 42

    def test_zero_total_assets(self, adapter):
        params = SettleDepositParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=0,
        )
        bundle = adapter.build_settle_deposit_bundle(params)
        tx = bundle.transactions[0]

        expected_data = SETTLE_DEPOSIT_SELECTOR + _encode_uint256(0)
        assert tx["data"] == expected_data


# --- Settle Redeem Bundle ---


class TestBuildSettleRedeemBundle:
    def test_returns_action_bundle(self, adapter):
        params = SettleRedeemParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=5_000_000,
        )
        bundle = adapter.build_settle_redeem_bundle(params)

        assert bundle.intent_type == "SETTLE_VAULT_REDEEM"
        assert len(bundle.transactions) == 1

    def test_transaction_fields(self, adapter):
        params = SettleRedeemParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=1_000_000,
        )
        bundle = adapter.build_settle_redeem_bundle(params)
        tx = bundle.transactions[0]

        assert tx["to"] == VAULT_ADDRESS
        assert tx["from"] == SAFE_ADDRESS
        assert tx["value"] == "0"
        assert tx["gas_estimate"] == 200_000

    def test_calldata_encoding(self, adapter):
        total_assets = 555_666_777
        params = SettleRedeemParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=total_assets,
        )
        bundle = adapter.build_settle_redeem_bundle(params)
        tx = bundle.transactions[0]

        expected_data = SETTLE_REDEEM_SELECTOR + _encode_uint256(total_assets)
        assert tx["data"] == expected_data

    def test_metadata(self, adapter):
        params = SettleRedeemParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=12345,
        )
        bundle = adapter.build_settle_redeem_bundle(params)

        assert bundle.metadata["vault_address"] == VAULT_ADDRESS
        assert bundle.metadata["total_assets"] == 12345

    def test_zero_total_assets(self, adapter):
        params = SettleRedeemParams(
            vault_address=VAULT_ADDRESS,
            safe_address=SAFE_ADDRESS,
            total_assets=0,
        )
        bundle = adapter.build_settle_redeem_bundle(params)
        tx = bundle.transactions[0]

        expected_data = SETTLE_REDEEM_SELECTOR + _encode_uint256(0)
        assert tx["data"] == expected_data


# --- Adapter Construction ---


class TestAdapterConstruction:
    def test_accepts_sdk(self):
        sdk = MagicMock(spec=LagoonVaultSDK)
        adapter = LagoonVaultAdapter(sdk)
        assert adapter._sdk is sdk

    def test_delegates_to_sdk(self):
        """Verify the adapter delegates to SDK methods."""
        sdk = MagicMock(spec=LagoonVaultSDK)
        sdk.build_update_total_assets_tx.return_value = {
            "to": VAULT_ADDRESS,
            "from": VALUATOR_ADDRESS,
            "data": "0xtest",
            "value": "0",
            "gas_estimate": 100_000,
        }
        adapter = LagoonVaultAdapter(sdk)

        params = UpdateTotalAssetsParams(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=100,
            pending_deposits=0,
        )
        bundle = adapter.build_propose_valuation_bundle(params)

        sdk.build_update_total_assets_tx.assert_called_once_with(
            vault_address=VAULT_ADDRESS,
            valuator_address=VALUATOR_ADDRESS,
            new_total_assets=100,
        )
        assert bundle.transactions[0]["data"] == "0xtest"
