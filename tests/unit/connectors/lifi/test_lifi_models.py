"""Unit tests for LiFi Models -- edge cases, serialization, and completeness.

These tests verify model parsing handles missing fields, partial data,
and edge cases that may occur with real API responses.
"""

from almanak.framework.connectors.lifi.models import (
    LiFiAction,
    LiFiEstimate,
    LiFiFeeCost,
    LiFiGasCost,
    LiFiOrderStrategy,
    LiFiStatusResponse,
    LiFiStep,
    LiFiStepType,
    LiFiToken,
    LiFiTransactionRequest,
    LiFiTransferStatus,
    LiFiTransferSubstatus,
)


# ============================================================================
# Enum Tests
# ============================================================================


class TestEnums:
    """Test enum values match LiFi API constants."""

    def test_order_strategy_values(self):
        assert LiFiOrderStrategy.FASTEST == "FASTEST"
        assert LiFiOrderStrategy.CHEAPEST == "CHEAPEST"
        assert LiFiOrderStrategy.SAFEST == "SAFEST"
        assert LiFiOrderStrategy.RECOMMENDED == "RECOMMENDED"

    def test_transfer_status_values(self):
        assert LiFiTransferStatus.NOT_FOUND == "NOT_FOUND"
        assert LiFiTransferStatus.PENDING == "PENDING"
        assert LiFiTransferStatus.DONE == "DONE"
        assert LiFiTransferStatus.FAILED == "FAILED"

    def test_transfer_substatus_values(self):
        assert LiFiTransferSubstatus.COMPLETED == "COMPLETED"
        assert LiFiTransferSubstatus.PARTIAL == "PARTIAL"
        assert LiFiTransferSubstatus.REFUNDED == "REFUNDED"

    def test_step_type_values(self):
        assert LiFiStepType.SWAP == "swap"
        assert LiFiStepType.CROSS == "cross"
        assert LiFiStepType.LIFI == "lifi"
        assert LiFiStepType.PROTOCOL == "protocol"


# ============================================================================
# Token Model Edge Cases
# ============================================================================


class TestLiFiToken:
    """Test LiFiToken parsing edge cases."""

    def test_from_empty_response(self):
        """Parsing empty dict returns sensible defaults."""
        token = LiFiToken.from_api_response({})
        assert token.address == ""
        assert token.chain_id == 0
        assert token.symbol == ""
        assert token.decimals == 18  # LiFi default
        assert token.name == ""
        assert token.price_usd is None

    def test_from_complete_response(self):
        token = LiFiToken.from_api_response({
            "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "chainId": 42161,
            "symbol": "USDC",
            "decimals": 6,
            "name": "USD Coin",
            "priceUSD": "1.0001",
        })
        assert token.address == "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        assert token.chain_id == 42161
        assert token.symbol == "USDC"
        assert token.decimals == 6
        assert token.name == "USD Coin"
        assert token.price_usd == 1.0001

    def test_price_usd_none_when_missing(self):
        token = LiFiToken.from_api_response({"address": "0x1", "chainId": 1, "symbol": "X", "decimals": 18})
        assert token.price_usd is None


# ============================================================================
# Action Model Edge Cases
# ============================================================================


class TestLiFiAction:
    """Test LiFiAction parsing edge cases."""

    def test_from_empty_response(self):
        action = LiFiAction.from_api_response({})
        assert action.from_chain_id == 0
        assert action.to_chain_id == 0
        assert action.from_amount == "0"
        assert action.slippage == 0.005  # default

    def test_from_complete_response(self):
        action = LiFiAction.from_api_response({
            "fromChainId": 42161,
            "toChainId": 8453,
            "fromToken": {"address": "0xusdc", "chainId": 42161, "symbol": "USDC", "decimals": 6},
            "toToken": {"address": "0xusdc_base", "chainId": 8453, "symbol": "USDC", "decimals": 6},
            "fromAmount": "1000000000",
            "fromAddress": "0xwallet",
            "toAddress": "0xwallet",
            "slippage": 0.01,
        })
        assert action.from_chain_id == 42161
        assert action.to_chain_id == 8453
        assert action.from_token.symbol == "USDC"
        assert action.to_token.symbol == "USDC"
        assert action.from_amount == "1000000000"
        assert action.slippage == 0.01


# ============================================================================
# Estimate Model Edge Cases
# ============================================================================


class TestLiFiEstimate:
    """Test LiFiEstimate parsing and calculations."""

    def test_from_empty_response(self):
        estimate = LiFiEstimate.from_api_response({})
        assert estimate.from_amount == "0"
        assert estimate.to_amount == "0"
        assert estimate.to_amount_min == "0"
        assert estimate.approval_address == ""
        assert estimate.execution_duration == 0
        assert estimate.fee_costs == []
        assert estimate.gas_costs == []

    def test_total_gas_with_multiple_costs(self):
        estimate = LiFiEstimate(
            gas_costs=[
                LiFiGasCost(estimate="100000"),
                LiFiGasCost(estimate="50000"),
                LiFiGasCost(estimate="25000"),
            ]
        )
        assert estimate.total_gas_estimate == 175000

    def test_total_gas_with_invalid_values(self):
        """Invalid gas values should be skipped, not crash."""
        estimate = LiFiEstimate(
            gas_costs=[
                LiFiGasCost(estimate="100000"),
                LiFiGasCost(estimate="not_a_number"),
                LiFiGasCost(estimate=""),
            ]
        )
        assert estimate.total_gas_estimate == 100000

    def test_total_fee_usd_with_multiple_fees(self):
        estimate = LiFiEstimate(
            fee_costs=[
                LiFiFeeCost(amount_usd="5.00"),
                LiFiFeeCost(amount_usd="2.50"),
            ]
        )
        assert estimate.total_fee_usd == 7.5

    def test_total_fee_usd_with_invalid_values(self):
        """Invalid fee values should be skipped."""
        estimate = LiFiEstimate(
            fee_costs=[
                LiFiFeeCost(amount_usd="5.00"),
                LiFiFeeCost(amount_usd="N/A"),
            ]
        )
        assert estimate.total_fee_usd == 5.0

    def test_no_costs_returns_zero(self):
        estimate = LiFiEstimate()
        assert estimate.total_gas_estimate == 0
        assert estimate.total_fee_usd == 0.0


# ============================================================================
# Gas & Fee Cost Edge Cases
# ============================================================================


class TestLiFiGasCost:
    """Test gas cost model parsing."""

    def test_from_empty_response(self):
        cost = LiFiGasCost.from_api_response({})
        assert cost.type == ""
        assert cost.estimate == "0"
        assert cost.token is None

    def test_with_token(self):
        cost = LiFiGasCost.from_api_response({
            "type": "SUM",
            "estimate": "250000",
            "limit": "350000",
            "amount": "2500000000000000",
            "amountUSD": "0.50",
            "token": {"address": "0xeth", "chainId": 42161, "symbol": "ETH", "decimals": 18},
        })
        assert cost.type == "SUM"
        assert cost.estimate == "250000"
        assert cost.token is not None
        assert cost.token.symbol == "ETH"


class TestLiFiFeeCost:
    """Test fee cost model parsing."""

    def test_from_empty_response(self):
        cost = LiFiFeeCost.from_api_response({})
        assert cost.name == ""
        assert cost.included is True  # default

    def test_included_false(self):
        cost = LiFiFeeCost.from_api_response({"included": False, "name": "Bridge Fee"})
        assert cost.included is False


# ============================================================================
# Transaction Request Edge Cases
# ============================================================================


class TestLiFiTransactionRequest:
    """Test transaction request parsing."""

    def test_from_empty_response(self):
        tx = LiFiTransactionRequest.from_api_response({})
        assert tx.from_address == ""
        assert tx.to == ""
        assert tx.data == ""
        assert tx.value == "0"

    def test_from_complete_response(self):
        tx = LiFiTransactionRequest.from_api_response({
            "from": "0xwallet",
            "to": "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE",
            "chainId": 42161,
            "data": "0xabcdef",
            "value": "1000000000000000000",
            "gasPrice": "100000000",
            "gasLimit": "350000",
        })
        assert tx.from_address == "0xwallet"
        assert tx.to == "0x1231DEB6f5749EF6cE6943a275A1D3E7486F4EaE"
        assert tx.chain_id == 42161
        assert tx.value == "1000000000000000000"


# ============================================================================
# Step Model Edge Cases
# ============================================================================


class TestLiFiStep:
    """Test step model parsing edge cases."""

    def test_from_empty_response(self):
        step = LiFiStep.from_api_response({})
        assert step.id == ""
        assert step.type == ""
        assert step.tool == ""
        assert step.action is None
        assert step.estimate is None
        assert step.transaction_request is None
        assert step.included_steps == []

    def test_is_cross_chain_by_action(self):
        """Cross-chain detection from action chain IDs."""
        step = LiFiStep(
            type="lifi",
            action=LiFiAction(from_chain_id=42161, to_chain_id=8453,
                              from_token=LiFiToken(address="", chain_id=42161, symbol="", decimals=18),
                              to_token=LiFiToken(address="", chain_id=8453, symbol="", decimals=18),
                              from_amount="0"),
        )
        assert step.is_cross_chain

    def test_is_same_chain_by_action(self):
        step = LiFiStep(
            type="swap",
            action=LiFiAction(from_chain_id=42161, to_chain_id=42161,
                              from_token=LiFiToken(address="", chain_id=42161, symbol="", decimals=18),
                              to_token=LiFiToken(address="", chain_id=42161, symbol="", decimals=18),
                              from_amount="0"),
        )
        assert not step.is_cross_chain

    def test_is_cross_chain_by_type_fallback(self):
        """When no action, fall back to step type."""
        step = LiFiStep(type="cross")
        assert step.is_cross_chain

        step_lifi = LiFiStep(type="lifi")
        assert step_lifi.is_cross_chain

        step_swap = LiFiStep(type="swap")
        assert not step_swap.is_cross_chain

    def test_get_to_amount_no_estimate(self):
        step = LiFiStep()
        assert step.get_to_amount() == 0

    def test_get_to_amount_min_no_estimate(self):
        step = LiFiStep()
        assert step.get_to_amount_min() == 0

    def test_get_to_amount_invalid_string(self):
        step = LiFiStep(estimate=LiFiEstimate(to_amount="invalid"))
        assert step.get_to_amount() == 0

    def test_nested_included_steps(self):
        """Multi-hop routes have nested included_steps."""
        response = {
            "id": "outer",
            "type": "lifi",
            "tool": "lifi",
            "includedSteps": [
                {"id": "step1", "type": "swap", "tool": "1inch"},
                {"id": "step2", "type": "cross", "tool": "across"},
                {"id": "step3", "type": "swap", "tool": "paraswap"},
            ],
        }
        step = LiFiStep.from_api_response(response)
        assert len(step.included_steps) == 3
        assert step.included_steps[0].tool == "1inch"
        assert step.included_steps[1].tool == "across"
        assert step.included_steps[2].tool == "paraswap"


# ============================================================================
# Status Response Edge Cases
# ============================================================================


class TestLiFiStatusResponse:
    """Test status response parsing edge cases."""

    def test_from_empty_response(self):
        status = LiFiStatusResponse.from_api_response({})
        assert status.transaction_id == ""
        assert status.status == ""
        assert not status.is_complete
        assert not status.is_failed
        assert not status.is_pending

    def test_complete_status(self):
        status = LiFiStatusResponse(status=LiFiTransferStatus.DONE)
        assert status.is_complete
        assert not status.is_failed
        assert not status.is_pending

    def test_failed_status(self):
        status = LiFiStatusResponse(status=LiFiTransferStatus.FAILED)
        assert status.is_failed
        assert not status.is_complete

    def test_pending_status(self):
        status = LiFiStatusResponse(status=LiFiTransferStatus.PENDING)
        assert status.is_pending
        assert not status.is_complete

    def test_tx_hash_from_nested_sending(self):
        """TX hash extraction from nested sending/receiving objects."""
        status = LiFiStatusResponse.from_api_response({
            "sending": {"txHash": "0xsend123", "chainId": 42161},
            "receiving": {"txHash": "0xrecv456", "chainId": 8453},
            "status": "DONE",
        })
        assert status.sending_tx_hash == "0xsend123"
        assert status.receiving_tx_hash == "0xrecv456"
        assert status.from_chain_id == 42161
        assert status.to_chain_id == 8453

    def test_tx_hash_from_flat_fields(self):
        """TX hash extraction from flat sendingTxHash field (older API format)."""
        status = LiFiStatusResponse.from_api_response({
            "sendingTxHash": "0xsend_flat",
            "receivingTxHash": "0xrecv_flat",
            "fromChainId": 1,
            "toChainId": 42161,
            "status": "PENDING",
        })
        assert status.sending_tx_hash == "0xsend_flat"
        assert status.receiving_tx_hash == "0xrecv_flat"

    def test_bridge_name_from_tool_fallback(self):
        """Bridge name falls back to 'tool' field."""
        status = LiFiStatusResponse.from_api_response({
            "tool": "stargate",
            "status": "DONE",
        })
        assert status.bridge_name == "stargate"

    def test_bridge_name_from_bridge_field(self):
        """Bridge name from 'bridge' field takes priority."""
        status = LiFiStatusResponse.from_api_response({
            "bridge": "across",
            "tool": "stargate",
            "status": "DONE",
        })
        assert status.bridge_name == "across"
