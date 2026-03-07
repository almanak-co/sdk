"""Tests for MetaMorpho Vault Adapter."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_vault.adapter import (
    MetaMorphoAdapter,
    MetaMorphoConfig,
    TransactionResult,
    create_test_adapter,
)
from almanak.framework.connectors.morpho_vault.sdk import (
    DepositExceedsCapError,
    InsufficientSharesError,
    VaultInfo,
    VaultPosition,
    _encode_uint256,
)

VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
WALLET_ADDR = "0x1234567890123456789012345678901234567890"
ASSET_ADDR = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


# =============================================================================
# Config Validation
# =============================================================================


class TestMetaMorphoConfig:
    def test_valid_config(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        assert config.chain == "ethereum"

    def test_invalid_chain_raises(self):
        with pytest.raises(ValueError, match="Invalid chain"):
            MetaMorphoConfig(chain="polygon", wallet_address=WALLET_ADDR)

    def test_invalid_wallet_raises(self):
        with pytest.raises(ValueError, match="Invalid wallet address"):
            MetaMorphoConfig(chain="ethereum", wallet_address="invalid")

    def test_base_chain(self):
        config = MetaMorphoConfig(chain="base", wallet_address=WALLET_ADDR)
        assert config.chain == "base"


# =============================================================================
# Adapter Construction
# =============================================================================


class TestAdapterConstruction:
    def test_create_with_gateway_client(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        adapter = MetaMorphoAdapter(config, gateway_client=gw)
        assert adapter.chain == "ethereum"
        assert adapter.wallet_address == WALLET_ADDR

    def test_sdk_lazy_init(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        adapter = MetaMorphoAdapter(config)
        assert adapter._sdk is None

    def test_sdk_requires_gateway(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        adapter = MetaMorphoAdapter(config)
        with pytest.raises(RuntimeError, match="gateway_client"):
            _ = adapter.sdk

    def test_create_test_adapter(self):
        adapter = create_test_adapter()
        assert adapter.chain == "ethereum"
        assert adapter._gateway_client is None

    def test_create_test_adapter_custom_chain(self):
        adapter = create_test_adapter(chain="base")
        assert adapter.chain == "base"


# =============================================================================
# Deposit
# =============================================================================


class TestDeposit:
    def _make_adapter_with_mock_sdk(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        resolver = MagicMock()

        # Token resolver mock
        resolved = MagicMock()
        resolved.address = ASSET_ADDR
        resolved.decimals = 6
        resolver.resolve.return_value = resolved

        adapter = MetaMorphoAdapter(config, gateway_client=gw, token_resolver=resolver)

        # Mock SDK methods
        mock_sdk = MagicMock()
        mock_sdk.get_vault_asset.return_value = ASSET_ADDR
        mock_sdk.get_max_deposit.return_value = 10**18  # Very large
        mock_sdk.build_approve_tx.return_value = {
            "to": ASSET_ADDR,
            "data": "0xapprove",
            "value": "0",
            "gas_estimate": 60000,
        }
        mock_sdk.build_deposit_tx.return_value = {
            "to": VAULT_ADDR,
            "data": "0xdeposit",
            "value": "0",
            "gas_estimate": 450000,
        }
        adapter._sdk = mock_sdk

        return adapter, mock_sdk

    def test_deposit_success(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        result = adapter.deposit(VAULT_ADDR, Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert "approve" in result.tx_data
        assert "deposit" in result.tx_data
        assert result.gas_estimate == 510000

    def test_deposit_zero_amount(self):
        adapter, _ = self._make_adapter_with_mock_sdk()
        result = adapter.deposit(VAULT_ADDR, Decimal("0"))
        assert result.success is False
        assert "positive" in result.error

    def test_deposit_invalid_address(self):
        adapter, _ = self._make_adapter_with_mock_sdk()
        result = adapter.deposit("invalid", Decimal("1000"))
        assert result.success is False
        assert "Invalid address" in result.error

    def test_deposit_exceeds_cap(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        mock_sdk.get_max_deposit.return_value = 1  # Very small
        result = adapter.deposit(VAULT_ADDR, Decimal("1000"))
        assert result.success is False

    def test_deposit_calls_sdk_correctly(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        adapter.deposit(VAULT_ADDR, Decimal("1000"))

        mock_sdk.get_vault_asset.assert_called_once_with(VAULT_ADDR)
        mock_sdk.build_deposit_tx.assert_called_once()
        call_args = mock_sdk.build_deposit_tx.call_args
        assert call_args.kwargs["vault_address"] == VAULT_ADDR
        assert call_args.kwargs["receiver"] == WALLET_ADDR


# =============================================================================
# Redeem
# =============================================================================


class TestRedeem:
    def _make_adapter_with_mock_sdk(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        adapter = MetaMorphoAdapter(config, gateway_client=gw)

        mock_sdk = MagicMock()
        mock_sdk.get_max_redeem.return_value = 500 * 10**18
        mock_sdk.get_decimals.return_value = 18
        mock_sdk.build_redeem_tx.return_value = {
            "to": VAULT_ADDR,
            "data": "0xredeem",
            "value": "0",
            "gas_estimate": 450000,
        }
        adapter._sdk = mock_sdk
        return adapter, mock_sdk

    def test_redeem_with_shares(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        result = adapter.redeem(VAULT_ADDR, Decimal("100"))

        assert result.success is True
        assert result.tx_data is not None
        assert "redeem" in result.tx_data
        assert result.gas_estimate == 450000

    def test_redeem_all(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        result = adapter.redeem(VAULT_ADDR, "all")

        assert result.success is True
        # "all" path should call get_max_redeem exactly once (no double RPC)
        mock_sdk.get_max_redeem.assert_called_once()

    def test_redeem_all_no_shares(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        mock_sdk.get_max_redeem.return_value = 0
        result = adapter.redeem(VAULT_ADDR, "all")
        assert result.success is False
        assert "No shares" in result.error

    def test_redeem_exceeds_max(self):
        adapter, mock_sdk = self._make_adapter_with_mock_sdk()
        mock_sdk.get_max_redeem.return_value = 1  # Very small
        result = adapter.redeem(VAULT_ADDR, Decimal("1000"))
        assert result.success is False

    def test_redeem_invalid_address(self):
        adapter, _ = self._make_adapter_with_mock_sdk()
        result = adapter.redeem("bad", Decimal("100"))
        assert result.success is False
        assert "Invalid address" in result.error


# =============================================================================
# Vault Info / Position
# =============================================================================


class TestVaultInfo:
    def test_get_vault_info(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        adapter = MetaMorphoAdapter(config, gateway_client=gw)

        mock_sdk = MagicMock()
        mock_sdk.get_vault_info.return_value = VaultInfo(
            address=VAULT_ADDR,
            asset=ASSET_ADDR,
            total_assets=1_000_000_000,
            total_supply=999 * 10**18,
            share_price=1_001_000,
            decimals=18,
            curator=WALLET_ADDR,
            fee=50_000_000_000_000_000,
            timelock=86400,
        )
        adapter._sdk = mock_sdk

        info = adapter.get_vault_info(VAULT_ADDR)
        assert info.address == VAULT_ADDR
        assert info.total_assets == 1_000_000_000

    def test_get_position(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        adapter = MetaMorphoAdapter(config, gateway_client=gw)

        mock_sdk = MagicMock()
        mock_sdk.get_position.return_value = VaultPosition(
            vault_address=VAULT_ADDR,
            user=WALLET_ADDR,
            shares=100 * 10**18,
            assets=100_500_000,
        )
        adapter._sdk = mock_sdk

        pos = adapter.get_position(VAULT_ADDR)
        assert pos.shares == 100 * 10**18
        mock_sdk.get_position.assert_called_with(VAULT_ADDR, WALLET_ADDR)


# =============================================================================
# Approve
# =============================================================================


class TestApprove:
    def test_build_approve_transaction(self):
        config = MetaMorphoConfig(chain="ethereum", wallet_address=WALLET_ADDR)
        gw = MagicMock()
        resolver = MagicMock()
        resolved = MagicMock()
        resolved.address = ASSET_ADDR
        resolved.decimals = 6
        resolver.resolve.return_value = resolved

        adapter = MetaMorphoAdapter(config, gateway_client=gw, token_resolver=resolver)
        mock_sdk = MagicMock()
        mock_sdk.build_approve_tx.return_value = {
            "to": ASSET_ADDR,
            "data": "0xapprove",
            "value": "0",
            "gas_estimate": 60000,
        }
        adapter._sdk = mock_sdk

        result = adapter.build_approve_transaction("USDC", Decimal("1000"), VAULT_ADDR)
        assert result.success is True
        assert result.gas_estimate == 60000


# =============================================================================
# TransactionResult
# =============================================================================


class TestTransactionResult:
    def test_to_dict(self):
        result = TransactionResult(
            success=True,
            tx_data={"approve": {}, "deposit": {}},
            gas_estimate=510000,
            description="Deposit 1000 USDC",
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["gas_estimate"] == 510000

    def test_failed_result(self):
        result = TransactionResult(success=False, error="Something went wrong")
        assert result.tx_data is None
        assert result.error == "Something went wrong"
