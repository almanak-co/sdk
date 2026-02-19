"""Tests for agent tool Pydantic schemas."""

import pytest
from pydantic import ValidationError

from almanak.framework.agent_tools.schemas import (
    BatchGetBalancesRequest,
    CloseLPPositionRequest,
    CompileIntentRequest,
    GetBalanceRequest,
    GetIndicatorRequest,
    GetPriceRequest,
    OpenLPPositionRequest,
    SaveAgentStateRequest,
    SwapTokensRequest,
    ToolResponse,
)


class TestToolResponse:
    def test_success_envelope(self):
        r = ToolResponse(status="success", data={"price": 3000})
        assert r.status == "success"
        assert r.data == {"price": 3000}
        assert r.error is None

    def test_error_envelope(self):
        r = ToolResponse(status="error", error={"code": "fail", "message": "bad"})
        assert r.status == "error"
        assert r.data is None

    def test_with_hints(self):
        r = ToolResponse(
            status="success",
            data={},
            decision_hints={"volatility": "high"},
            explanation="Market is volatile.",
        )
        assert r.decision_hints == {"volatility": "high"}
        assert r.explanation == "Market is volatile."


class TestReadToolSchemas:
    def test_get_price_defaults(self):
        req = GetPriceRequest(token="ETH")
        assert req.chain == "arbitrum"

    def test_get_price_custom_chain(self):
        req = GetPriceRequest(token="ETH", chain="base")
        assert req.chain == "base"

    def test_get_balance(self):
        req = GetBalanceRequest(token="USDC")
        assert req.token == "USDC"

    def test_batch_get_balances(self):
        req = BatchGetBalancesRequest(tokens=["ETH", "USDC"])
        assert req.tokens == ["ETH", "USDC"]

    def test_batch_get_balances_no_tokens_fails(self):
        with pytest.raises(ValidationError):
            BatchGetBalancesRequest()  # tokens is now required

    def test_get_indicator(self):
        req = GetIndicatorRequest(token="ETH", indicator="rsi", period=14)
        assert req.indicator == "rsi"
        assert req.period == 14


class TestActionToolSchemas:
    def test_swap_tokens_defaults(self):
        req = SwapTokensRequest(token_in="USDC", token_out="ETH", amount="1000")
        assert req.slippage_bps == 50
        assert req.dry_run is False
        assert req.protocol is None

    def test_swap_tokens_dry_run(self):
        req = SwapTokensRequest(token_in="USDC", token_out="ETH", amount="1000", dry_run=True)
        assert req.dry_run is True

    def test_open_lp_position(self):
        req = OpenLPPositionRequest(
            token_a="WETH",
            token_b="USDC",
            amount_a="1.0",
            amount_b="3200",
            fee_tier=500,
            price_lower="2800",
            price_upper="3600",
        )
        assert req.fee_tier == 500
        assert req.price_lower == "2800"

    def test_open_lp_position_requires_range(self):
        with pytest.raises(ValidationError):
            OpenLPPositionRequest(
                token_a="WETH",
                token_b="USDC",
                amount_a="1.0",
                amount_b="3200",
            )

    def test_close_lp_position_defaults(self):
        req = CloseLPPositionRequest(position_id="12345")
        assert req.amount == "all"
        assert req.collect_fees is True

    def test_swap_tokens_missing_required(self):
        with pytest.raises(ValidationError):
            SwapTokensRequest(token_in="USDC")  # missing token_out and amount

    def test_borrow_requires_collateral(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        with pytest.raises(ValidationError):
            BorrowLendingRequest(token="USDC", amount="5000")  # missing collateral fields

    def test_borrow_with_collateral(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        req = BorrowLendingRequest(
            token="USDC", amount="5000", collateral_token="WETH", collateral_amount="2.0"
        )
        assert req.collateral_token == "WETH"
        assert req.collateral_amount == "2.0"


class TestPlanningToolSchemas:
    def test_compile_intent(self):
        req = CompileIntentRequest(
            intent_type="swap",
            params={"from_token": "USDC", "to_token": "ETH", "amount": "1000"},
        )
        assert req.intent_type == "swap"


class TestStateToolSchemas:
    def test_save_state(self):
        req = SaveAgentStateRequest(state={"position_id": 12345})
        assert req.state == {"position_id": 12345}
        assert req.strategy_id == ""


class TestSchemaValidation:
    """WS3: Input validation hardening."""

    # -- SwapTokensRequest --
    def test_swap_positive_amount(self):
        req = SwapTokensRequest(token_in="USDC", token_out="ETH", amount="100.5")
        assert req.amount == "100.5"

    def test_swap_zero_amount_rejected(self):
        with pytest.raises(ValidationError, match="positive"):
            SwapTokensRequest(token_in="USDC", token_out="ETH", amount="0")

    def test_swap_negative_amount_rejected(self):
        with pytest.raises(ValidationError, match="positive"):
            SwapTokensRequest(token_in="USDC", token_out="ETH", amount="-100")

    def test_swap_non_numeric_amount_rejected(self):
        with pytest.raises(ValidationError, match="decimal"):
            SwapTokensRequest(token_in="USDC", token_out="ETH", amount="abc")

    def test_swap_slippage_bps_lower_bound(self):
        req = SwapTokensRequest(token_in="USDC", token_out="ETH", amount="100", slippage_bps=1)
        assert req.slippage_bps == 1

    def test_swap_slippage_bps_upper_bound(self):
        req = SwapTokensRequest(token_in="USDC", token_out="ETH", amount="100", slippage_bps=1000)
        assert req.slippage_bps == 1000

    def test_swap_slippage_bps_zero_rejected(self):
        with pytest.raises(ValidationError):
            SwapTokensRequest(token_in="USDC", token_out="ETH", amount="100", slippage_bps=0)

    def test_swap_slippage_bps_over_1000_rejected(self):
        with pytest.raises(ValidationError):
            SwapTokensRequest(token_in="USDC", token_out="ETH", amount="100", slippage_bps=1001)

    # -- OpenLPPositionRequest --
    def test_lp_open_positive_amounts(self):
        req = OpenLPPositionRequest(
            token_a="WETH", token_b="USDC",
            amount_a="1.0", amount_b="3200",
            price_lower="2800", price_upper="3600",
        )
        assert req.amount_a == "1.0"

    def test_lp_open_zero_amount_rejected(self):
        with pytest.raises(ValidationError, match="positive"):
            OpenLPPositionRequest(
                token_a="WETH", token_b="USDC",
                amount_a="0", amount_b="3200",
                price_lower="2800", price_upper="3600",
            )

    def test_lp_open_price_lower_gte_upper_rejected(self):
        with pytest.raises(ValidationError, match="price_lower"):
            OpenLPPositionRequest(
                token_a="WETH", token_b="USDC",
                amount_a="1.0", amount_b="3200",
                price_lower="3600", price_upper="2800",
            )

    def test_lp_open_price_equal_rejected(self):
        with pytest.raises(ValidationError, match="price_lower"):
            OpenLPPositionRequest(
                token_a="WETH", token_b="USDC",
                amount_a="1.0", amount_b="3200",
                price_lower="3000", price_upper="3000",
            )

    def test_lp_open_fee_tier_bounds(self):
        req = OpenLPPositionRequest(
            token_a="WETH", token_b="USDC",
            amount_a="1.0", amount_b="3200",
            fee_tier=100, price_lower="2800", price_upper="3600",
        )
        assert req.fee_tier == 100

    def test_lp_open_fee_tier_too_low_rejected(self):
        with pytest.raises(ValidationError):
            OpenLPPositionRequest(
                token_a="WETH", token_b="USDC",
                amount_a="1.0", amount_b="3200",
                fee_tier=50, price_lower="2800", price_upper="3600",
            )

    def test_lp_open_fee_tier_too_high_rejected(self):
        with pytest.raises(ValidationError):
            OpenLPPositionRequest(
                token_a="WETH", token_b="USDC",
                amount_a="1.0", amount_b="3200",
                fee_tier=100001, price_lower="2800", price_upper="3600",
            )

    # -- SupplyLendingRequest --
    def test_supply_positive_amount(self):
        from almanak.framework.agent_tools.schemas import SupplyLendingRequest

        req = SupplyLendingRequest(token="USDC", amount="1000")
        assert req.amount == "1000"

    def test_supply_zero_amount_rejected(self):
        from almanak.framework.agent_tools.schemas import SupplyLendingRequest

        with pytest.raises(ValidationError, match="positive"):
            SupplyLendingRequest(token="USDC", amount="0")

    # -- BorrowLendingRequest --
    def test_borrow_positive_amount(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        req = BorrowLendingRequest(token="USDC", amount="5000", collateral_token="WETH", collateral_amount="2.0")
        assert req.amount == "5000"

    def test_borrow_zero_amount_rejected(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        with pytest.raises(ValidationError, match="positive"):
            BorrowLendingRequest(token="USDC", amount="0", collateral_token="WETH", collateral_amount="2.0")

    def test_borrow_collateral_all_accepted(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        req = BorrowLendingRequest(token="USDC", amount="5000", collateral_token="WETH", collateral_amount="all")
        assert req.collateral_amount == "all"

    def test_borrow_collateral_zero_rejected(self):
        from almanak.framework.agent_tools.schemas import BorrowLendingRequest

        with pytest.raises(ValidationError, match="positive"):
            BorrowLendingRequest(token="USDC", amount="5000", collateral_token="WETH", collateral_amount="0")

    # -- RepayLendingRequest --
    def test_repay_amount_all_accepted(self):
        from almanak.framework.agent_tools.schemas import RepayLendingRequest

        req = RepayLendingRequest(token="USDC", amount="all")
        assert req.amount == "all"

    def test_repay_positive_amount(self):
        from almanak.framework.agent_tools.schemas import RepayLendingRequest

        req = RepayLendingRequest(token="USDC", amount="500")
        assert req.amount == "500"

    def test_repay_zero_rejected(self):
        from almanak.framework.agent_tools.schemas import RepayLendingRequest

        with pytest.raises(ValidationError, match="positive"):
            RepayLendingRequest(token="USDC", amount="0")

    # -- DeployVaultRequest --
    def test_deploy_vault_valid_addresses(self):
        from almanak.framework.agent_tools.schemas import DeployVaultRequest

        req = DeployVaultRequest(
            chain="base",
            name="Test",
            symbol="TST",
            underlying_token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            safe_address="0x1234567890abcdef1234567890abcdef12345678",
            admin_address="0x1234567890abcdef1234567890abcdef12345678",
            fee_receiver_address="0x1234567890abcdef1234567890abcdef12345678",
            deployer_address="0x1234567890abcdef1234567890abcdef12345678",
        )
        assert req.name == "Test"

    def test_deploy_vault_invalid_address_rejected(self):
        from almanak.framework.agent_tools.schemas import DeployVaultRequest

        with pytest.raises(ValidationError, match="Invalid Ethereum address"):
            DeployVaultRequest(
                chain="base",
                name="Test",
                symbol="TST",
                underlying_token_address="not-an-address",
                safe_address="0x1234567890abcdef1234567890abcdef12345678",
                admin_address="0x1234567890abcdef1234567890abcdef12345678",
                fee_receiver_address="0x1234567890abcdef1234567890abcdef12345678",
                deployer_address="0x1234567890abcdef1234567890abcdef12345678",
            )

    def test_deploy_vault_short_address_rejected(self):
        from almanak.framework.agent_tools.schemas import DeployVaultRequest

        with pytest.raises(ValidationError, match="Invalid Ethereum address"):
            DeployVaultRequest(
                chain="base",
                name="Test",
                symbol="TST",
                underlying_token_address="0x1234",
                safe_address="0x1234567890abcdef1234567890abcdef12345678",
                admin_address="0x1234567890abcdef1234567890abcdef12345678",
                fee_receiver_address="0x1234567890abcdef1234567890abcdef12345678",
                deployer_address="0x1234567890abcdef1234567890abcdef12345678",
            )

    def test_deploy_vault_optional_valuation_manager_valid(self):
        from almanak.framework.agent_tools.schemas import DeployVaultRequest

        req = DeployVaultRequest(
            chain="base",
            name="Test",
            symbol="TST",
            underlying_token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            safe_address="0x1234567890abcdef1234567890abcdef12345678",
            admin_address="0x1234567890abcdef1234567890abcdef12345678",
            fee_receiver_address="0x1234567890abcdef1234567890abcdef12345678",
            deployer_address="0x1234567890abcdef1234567890abcdef12345678",
            valuation_manager_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        )
        assert req.valuation_manager_address is not None

    def test_deploy_vault_optional_valuation_manager_invalid_rejected(self):
        from almanak.framework.agent_tools.schemas import DeployVaultRequest

        with pytest.raises(ValidationError, match="Invalid Ethereum address"):
            DeployVaultRequest(
                chain="base",
                name="Test",
                symbol="TST",
                underlying_token_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                safe_address="0x1234567890abcdef1234567890abcdef12345678",
                admin_address="0x1234567890abcdef1234567890abcdef12345678",
                fee_receiver_address="0x1234567890abcdef1234567890abcdef12345678",
                deployer_address="0x1234567890abcdef1234567890abcdef12345678",
                valuation_manager_address="bad",
            )

    # -- SettleVaultRequest --
    def test_settle_vault_valid_new_total_assets(self):
        from almanak.framework.agent_tools.schemas import SettleVaultRequest

        req = SettleVaultRequest(
            vault_address="0x1234",
            safe_address="0xabc",
            valuator_address="0xdef",
            new_total_assets="100000",
        )
        assert req.new_total_assets == "100000"

    def test_settle_vault_new_total_assets_none_ok(self):
        from almanak.framework.agent_tools.schemas import SettleVaultRequest

        req = SettleVaultRequest(
            vault_address="0x1234",
            safe_address="0xabc",
            valuator_address="0xdef",
        )
        assert req.new_total_assets is None

    def test_settle_vault_negative_total_assets_rejected(self):
        from almanak.framework.agent_tools.schemas import SettleVaultRequest

        with pytest.raises(ValidationError, match="non-negative"):
            SettleVaultRequest(
                vault_address="0x1234",
                safe_address="0xabc",
                valuator_address="0xdef",
                new_total_assets="-100",
            )

    def test_settle_vault_non_int_total_assets_rejected(self):
        from almanak.framework.agent_tools.schemas import SettleVaultRequest

        with pytest.raises(ValidationError, match="integer"):
            SettleVaultRequest(
                vault_address="0x1234",
                safe_address="0xabc",
                valuator_address="0xdef",
                new_total_assets="abc",
            )

    # -- DepositVaultRequest --
    def test_deposit_vault_valid_amount(self):
        from almanak.framework.agent_tools.schemas import DepositVaultRequest

        req = DepositVaultRequest(
            vault_address="0x1234",
            underlying_token="0xabc",
            amount="10000000",
        )
        assert req.amount == "10000000"

    def test_deposit_vault_negative_amount_rejected(self):
        from almanak.framework.agent_tools.schemas import DepositVaultRequest

        with pytest.raises(ValidationError, match="non-negative"):
            DepositVaultRequest(
                vault_address="0x1234",
                underlying_token="0xabc",
                amount="-1",
            )

    def test_deposit_vault_zero_amount_rejected(self):
        from almanak.framework.agent_tools.schemas import DepositVaultRequest

        with pytest.raises(ValidationError, match="positive"):
            DepositVaultRequest(
                vault_address="0x1234",
                underlying_token="0xabc",
                amount="0",
            )


class TestJsonSchemaGeneration:
    def test_get_price_json_schema(self):
        schema = GetPriceRequest.model_json_schema()
        assert "properties" in schema
        assert "token" in schema["properties"]
        assert "chain" in schema["properties"]

    def test_swap_tokens_json_schema(self):
        schema = SwapTokensRequest.model_json_schema()
        assert "token_in" in schema["properties"]
        assert "dry_run" in schema["properties"]
