"""Tests for Portfolio Manager tools: check_protocol_support, get_wallet_overview, wrap_native.

Covers schema validation, catalog registration, and check_protocol_support logic.
The wallet_overview and wrap_native execution paths require a gateway mock and are
covered at the integration level.
"""

import pytest
from pydantic import ValidationError

from almanak.framework.agent_tools.catalog import ToolCategory, get_default_catalog
from almanak.framework.agent_tools.schemas import (
    CheckProtocolSupportRequest,
    GetWalletOverviewRequest,
    WrapNativeRequest,
)


class TestWrapNativeSchema:
    """WrapNativeRequest validation."""

    def test_valid_request(self):
        req = WrapNativeRequest(token="WETH", amount="0.5", chain="arbitrum")
        assert req.token == "WETH"
        assert req.amount == "0.5"

    def test_amount_all(self):
        req = WrapNativeRequest(token="WETH", amount="all")
        assert req.amount == "all"

    def test_zero_amount_rejected(self):
        with pytest.raises(ValidationError):
            WrapNativeRequest(token="WETH", amount="0")

    def test_negative_amount_rejected(self):
        with pytest.raises(ValidationError):
            WrapNativeRequest(token="WETH", amount="-1")

    def test_error_message_says_amount(self):
        """Validator error should reference 'amount', not the method name."""
        with pytest.raises(ValidationError) as exc_info:
            WrapNativeRequest(token="WETH", amount="-1")
        assert "amount" in str(exc_info.value).lower()


class TestGetWalletOverviewSchema:
    """GetWalletOverviewRequest validation."""

    def test_defaults(self):
        req = GetWalletOverviewRequest()
        assert req.chain == "arbitrum"
        assert req.min_balance_usd == 0.01
        assert req.extra_tokens == []

    def test_custom_params(self):
        req = GetWalletOverviewRequest(
            chain="base",
            wallet_address="0x1234",
            min_balance_usd=1.0,
            extra_tokens=["DEGEN"],
        )
        assert req.chain == "base"
        assert req.extra_tokens == ["DEGEN"]

    def test_negative_min_balance_rejected(self):
        with pytest.raises(ValidationError):
            GetWalletOverviewRequest(min_balance_usd=-1.0)

    def test_zero_min_balance_accepted(self):
        req = GetWalletOverviewRequest(min_balance_usd=0)
        assert req.min_balance_usd == 0


class TestCheckProtocolSupportSchema:
    """CheckProtocolSupportRequest validation."""

    def test_valid_request(self):
        req = CheckProtocolSupportRequest(protocol="uniswap_v3", chain="arbitrum")
        assert req.protocol == "uniswap_v3"

    def test_no_chain_means_all(self):
        req = CheckProtocolSupportRequest(protocol="aave_v3")
        assert req.chain == ""


class TestCheckProtocolSupportLogic:
    """Test check_protocol_support using the executor's method directly."""

    @pytest.fixture()
    def executor_method(self):
        """Get the _execute_check_protocol_support method with minimal mocking."""
        from unittest.mock import MagicMock

        from almanak.framework.agent_tools.executor import ToolExecutor

        # Create a minimal executor with mocked gateway client
        mock_client = MagicMock()
        executor = object.__new__(ToolExecutor)
        executor._client = mock_client
        executor._default_chain = "arbitrum"
        return executor._execute_check_protocol_support

    def test_known_protocol_exact_match(self, executor_method):
        result = executor_method({"protocol": "uniswap_v3", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["supported"] is True
        assert "arbitrum" in result.data["supported_chains"]
        assert result.data["sdk_template"] is not None

    def test_known_protocol_wrong_chain(self, executor_method):
        result = executor_method({"protocol": "aerodrome", "chain": "ethereum"})
        assert result.status == "success"
        assert result.data["supported"] is False
        assert "base" in result.data["supported_chains"]

    def test_unknown_protocol(self, executor_method):
        result = executor_method({"protocol": "fluid-dex", "chain": "arbitrum"})
        assert result.status == "success"
        assert result.data["supported"] is False
        assert result.data["supported_chains"] == []

    def test_case_insensitive(self, executor_method):
        result = executor_method({"protocol": "Uniswap_V3", "chain": "Arbitrum"})
        assert result.data["supported"] is True

    def test_dash_underscore_normalization(self, executor_method):
        result = executor_method({"protocol": "uniswap-v3", "chain": "arbitrum"})
        assert result.data["supported"] is True

    def test_no_chain_returns_all(self, executor_method):
        result = executor_method({"protocol": "uniswap_v3", "chain": ""})
        assert result.data["supported"] is True
        assert len(result.data["supported_chains"]) > 1

    def test_no_partial_match(self, executor_method):
        """Partial protocol names should NOT match (exact only)."""
        result = executor_method({"protocol": "v3", "chain": ""})
        assert result.data["supported"] is False


class TestCatalogRegistration:
    """Verify all 3 new tools are properly registered."""

    def test_wrap_native_registered(self):
        catalog = get_default_catalog()
        tool = catalog.get("wrap_native")
        assert tool is not None
        assert tool.category == ToolCategory.ACTION
        assert not tool.idempotent

    def test_get_wallet_overview_registered(self):
        catalog = get_default_catalog()
        tool = catalog.get("get_wallet_overview")
        assert tool is not None
        assert tool.category == ToolCategory.DATA

    def test_check_protocol_support_registered(self):
        catalog = get_default_catalog()
        tool = catalog.get("check_protocol_support")
        assert tool is not None
        assert tool.category == ToolCategory.DATA
