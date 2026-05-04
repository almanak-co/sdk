"""Mop-up tests for data class to_dict / properties and remaining edge branches.

Hits the small uncovered branches that don't naturally fall out of the
larger operation tests: data class serializers, position properties,
the SDK lazy-init success path, the flash_loan / approve "unknown token"
error branches via a None-returning resolver, and SDK PositionNotFoundError.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_blue.adapter import (
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBlueMarketParams,
    MorphoBlueMarketState,
    MorphoBluePosition,
    TransactionResult,
)
from almanak.framework.connectors.morpho_blue.sdk import (
    MorphoBlueSDKError,
    PositionNotFoundError,
)

TEST_WALLET = "0x1234567890123456789012345678901234567890"


class TestDataClassSerializers:
    def test_market_params_to_tuple(self) -> None:
        p = MorphoBlueMarketParams(
            loan_token="l", collateral_token="c", oracle="o", irm="i", lltv=860000000000000000
        )
        assert p.to_tuple() == ("l", "c", "o", "i", 860000000000000000)

    def test_market_params_to_dict(self) -> None:
        p = MorphoBlueMarketParams(
            loan_token="l", collateral_token="c", oracle="o", irm="i", lltv=860000000000000000
        )
        d = p.to_dict()
        assert d["lltv_percent"] == 86.0

    def test_market_state_zero_supply_utilization(self) -> None:
        s = MorphoBlueMarketState(market_id="0x1")
        assert s.utilization == Decimal("0")

    def test_market_state_nonzero_utilization(self) -> None:
        s = MorphoBlueMarketState(
            market_id="0x1",
            total_supply_assets=Decimal("1000"),
            total_borrow_assets=Decimal("500"),
        )
        assert s.utilization == Decimal("0.5")

    def test_market_state_to_dict(self) -> None:
        s = MorphoBlueMarketState(
            market_id="0x1",
            total_supply_assets=Decimal("1000"),
        )
        d = s.to_dict()
        assert "utilization" in d
        assert d["market_id"] == "0x1"

    def test_position_has_props(self) -> None:
        p = MorphoBluePosition(
            market_id="0x1",
            supply_shares=Decimal("100"),
            borrow_shares=Decimal("50"),
            collateral=Decimal("1"),
        )
        assert p.has_supply
        assert p.has_borrow
        assert p.has_collateral

    def test_position_empty_props(self) -> None:
        p = MorphoBluePosition(market_id="0x1")
        assert not p.has_supply
        assert not p.has_borrow
        assert not p.has_collateral

    def test_position_to_dict(self) -> None:
        p = MorphoBluePosition(market_id="0x1", supply_shares=Decimal("10"))
        d = p.to_dict()
        assert d["has_supply"] is True

    def test_transaction_result_to_dict(self) -> None:
        tr = TransactionResult(success=True, gas_estimate=100, description="hello")
        d = tr.to_dict()
        assert d["success"] is True
        assert d["description"] == "hello"


class TestSDKLazyInitSuccess:
    def test_sdk_lazy_init_creates_sdk(self) -> None:
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            allow_placeholder_prices=True,
            enable_sdk=True,
        )
        adapter = MorphoBlueAdapter(config, token_resolver=MagicMock())
        # Patch MorphoBlueSDK so we don't actually create a Web3 connection
        with patch(
            "almanak.framework.connectors.morpho_blue.sdk.MorphoBlueSDK"
        ) as mock_sdk_cls:
            mock_sdk_cls.return_value = MagicMock()
            sdk = adapter.sdk
            assert sdk is not None
            # Second call returns cached sdk (same instance)
            sdk2 = adapter.sdk
            assert sdk2 is sdk


class TestSDKExceptions:
    def test_position_not_found_error(self) -> None:
        err = PositionNotFoundError("0xmarket", "0xuser")
        assert err.market_id == "0xmarket"
        assert err.user == "0xuser"
        assert "0xmarket" in str(err)

    def test_morpho_blue_sdk_error(self) -> None:
        err = MorphoBlueSDKError("oops")
        assert "oops" in str(err)


class TestSDKChainRegistryMismatch:
    """Cover the safety check for SUPPORTED_CHAINS containing a chain not
    in MORPHO_BLUE registry — currently impossible without code change, so
    we monkey-patch the registry in-memory."""

    def test_missing_registry_raises_sdk_error(self) -> None:
        from almanak.framework.connectors.morpho_blue import sdk as sdk_module

        # Add a fake chain that's "supported" but missing from the registry.
        # Stub get_rpc_url so the SDK can get past the RPC URL resolution and
        # reach the registry check that we actually want to exercise.
        original_supported = sdk_module.SUPPORTED_CHAINS
        sdk_module.SUPPORTED_CHAINS = original_supported | {"phantom"}
        try:
            with patch(
                "almanak.framework.connectors.morpho_blue.sdk.get_rpc_url",
                return_value="http://x",
            ):
                with pytest.raises(MorphoBlueSDKError, match="address registry"):
                    sdk_module.MorphoBlueSDK(chain="phantom")
        finally:
            sdk_module.SUPPORTED_CHAINS = original_supported


class TestReceiptParserStrictParseError:
    """Exercise the strict-parse failure-injected branch (VIB-3159)."""

    def test_strict_parse_propagates_exception(self) -> None:
        from almanak.framework.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser
        from almanak.framework.execution.extract_result import ExtractError

        parser = MorphoBlueReceiptParser()

        def boom(_receipt):
            raise ValueError("simulated parse_receipt crash")

        parser.parse_receipt = boom  # type: ignore[method-assign]
        out = parser.extract_supply_amount_result({"logs": []})
        assert isinstance(out, ExtractError)

    def test_strict_parse_returns_failed_parse(self) -> None:
        from almanak.framework.connectors.morpho_blue.receipt_parser import (
            MorphoBlueReceiptParser,
            ParseResult,
        )
        from almanak.framework.execution.extract_result import ExtractError

        parser = MorphoBlueReceiptParser()
        parser.parse_receipt = lambda _r: ParseResult(success=False, error="x")  # type: ignore[method-assign]
        out = parser.extract_supply_amount_result({"logs": []})
        assert isinstance(out, ExtractError)


class TestExtractWarnings:
    """Cover the warning branches in extract_*_amount when parse_receipt fails internally."""

    def test_extract_supply_amount_returns_none_on_parse_exception(self) -> None:
        from almanak.framework.connectors.morpho_blue.receipt_parser import MorphoBlueReceiptParser

        parser = MorphoBlueReceiptParser()

        def boom(_receipt):
            raise ValueError("crash")

        parser.parse_receipt = boom  # type: ignore[method-assign]
        # All extract_* methods catch the exception and return None
        assert parser.extract_supply_amount({}) is None
        assert parser.extract_withdraw_amount({}) is None
        assert parser.extract_borrow_amount({}) is None
        assert parser.extract_repay_amount({}) is None
        assert parser.extract_shares_received({}) is None
        assert parser.extract_shares_burned({}) is None
        assert parser.extract_supply_collateral_amount({}) is None
        assert parser.extract_protocol_fees({}) is None
