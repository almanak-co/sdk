"""Branch coverage for ToolExecutor action-response builders.

Covers ``_build_action_response_from_enriched`` (EnrichedExecutionResult
shape) and ``_infer_check_name``. Pure dispatch logic — no gateway, no policy.
"""

from types import SimpleNamespace

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor


@pytest.fixture
def executor() -> ToolExecutor:
    executor = object.__new__(ToolExecutor)
    executor._default_chain = "ethereum"
    return executor


def _enriched(**overrides):
    defaults = {
        "tx_hash": "0xenr",
        "swap_amounts": None,
        "lp_close_data": None,
        "position_id": None,
        "extracted_data": {},
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestBuildActionResponseFromEnriched:
    def test_swap_with_enrichment(self, executor):
        swap = SimpleNamespace(
            amount_in_decimal="1.5",
            amount_out_decimal="2900.1",
            effective_price="1933.4",
            slippage_bps=12,
            token_in="WETH",
            token_out="USDC",
        )
        resp = executor._build_action_response_from_enriched(
            "swap_tokens", _enriched(swap_amounts=swap), {}
        )
        assert resp["amount_in"] == "1.5"
        assert resp["amount_out"] == "2900.1"
        assert resp["effective_price"] == "1933.4"
        assert resp["slippage_bps"] == 12
        assert resp["token_in"] == "WETH"

    def test_swap_without_enrichment_falls_back_to_args(self, executor):
        resp = executor._build_action_response_from_enriched(
            "swap_tokens",
            _enriched(),
            {"amount": "10", "token_in": "USDC", "token_out": "WETH"},
        )
        assert resp["amount_in"] == "10"
        assert resp["amount_out"] == ""
        assert resp["slippage_bps"] is None
        assert resp["token_out"] == "WETH"

    def test_open_lp_position_uses_enriched_fields(self, executor):
        enriched = _enriched(
            position_id=77,
            extracted_data={"liquidity": "999", "tick_lower": -100, "tick_upper": 100},
        )
        resp = executor._build_action_response_from_enriched("open_lp_position", enriched, {})
        assert resp["position_id"] == 77
        assert resp["liquidity"] == "999"
        assert resp["tick_lower"] == -100

    def test_close_lp_position_with_data(self, executor):
        lp = SimpleNamespace(amount0_collected=5, amount1_collected=6, fees0=1, fees1=2)
        resp = executor._build_action_response_from_enriched(
            "close_lp_position", _enriched(lp_close_data=lp), {}
        )
        assert resp["token_a_received"] == "5"
        assert resp["fees_collected_b"] == "2"

    def test_close_lp_position_without_data(self, executor):
        resp = executor._build_action_response_from_enriched(
            "close_lp_position", _enriched(), {}
        )
        assert resp["token_a_received"] == ""
        assert resp["fees_collected_a"] == ""

    def test_lending_tools(self, executor):
        supply = executor._build_action_response_from_enriched(
            "supply_lending", _enriched(), {"amount": "5"}
        )
        borrow = executor._build_action_response_from_enriched(
            "borrow_lending", _enriched(), {"amount": "7"}
        )
        assert supply["amount_supplied"] == "5"
        assert borrow["amount_borrowed"] == "7"

    @pytest.mark.parametrize(
        ("tool", "field"),
        [("repay_lending", "amount_repaid"), ("withdraw_lending", "amount_withdrawn")],
    )
    def test_all_sentinel_not_leaked(self, executor, tool, field):
        resp = executor._build_action_response_from_enriched(
            tool, _enriched(), {"amount": "all"}
        )
        assert resp[field] == ""

    def test_bridge_prefers_extracted_metadata(self, executor):
        enriched = _enriched(
            extracted_data={
                "amount": 9,
                "from_chain": "base",
                "to_chain": "arbitrum",
                "bridge": "across",
                "estimated_time": 120,
            }
        )
        resp = executor._build_action_response_from_enriched(
            "bridge_tokens", enriched, {"amount": "ignored"}
        )
        assert resp["amount_bridged"] == "9"
        assert resp["bridge_used"] == "across"
        assert resp["estimated_arrival_seconds"] == 120

    def test_bridge_falls_back_to_args(self, executor):
        resp = executor._build_action_response_from_enriched(
            "bridge_tokens",
            _enriched(extracted_data=None),
            {"amount": "3", "from_chain": "base", "to_chain": "optimism"},
        )
        assert resp["amount_bridged"] == "3"
        assert resp["to_chain"] == "optimism"

    def test_wrap_native_prefers_metadata(self, executor):
        enriched = _enriched(
            extracted_data={"amount_wrapped": 2, "token": "WETH", "chain": "base"}
        )
        resp = executor._build_action_response_from_enriched("wrap_native", enriched, {})
        assert resp["amount_wrapped"] == "2"
        assert resp["chain"] == "base"

    def test_unwrap_native_uses_args_and_default_chain(self, executor):
        resp = executor._build_action_response_from_enriched(
            "unwrap_native", _enriched(), {"amount": "4", "token": "WETH"}
        )
        assert resp["amount_unwrapped"] == "4"
        assert resp["chain"] == "ethereum"

    def test_unknown_tool_returns_base(self, executor):
        resp = executor._build_action_response_from_enriched("get_price", _enriched(), {})
        assert resp == {"tx_hash": "0xenr", "gas_usd": ""}


class TestInferCheckName:
    @pytest.mark.parametrize(
        ("violation", "expected"),
        [
            ("Tool 'x' is not in the allowed set", "tool_not_allowed"),
            ("Chain 'foo' not allowed by policy", "chain_not_allowed"),
            ("Protocol 'bar' not allowed", "protocol_not_allowed"),
            ("Token 'XYZ' is not in the allowed set", "token_not_allowed"),
            ("Intent type SWAP not allowed", "intent_type_not_allowed"),
            ("Trade exceeds single-trade limit of $10000", "single_trade_limit"),
            ("Daily spend limit exceeded", "daily_spend_limit"),
            ("Rate limit hit: too many tool calls", "tool_rate_limit"),
            ("Rate limit reached for trades this hour", "trade_rate_limit"),
            ("Circuit breaker engaged after losses", "circuit_breaker"),
            ("Cooldown active until next window", "cooldown"),
            ("Stop-loss triggered on drawdown", "stop_loss"),
            ("Position size exceeds configured cap", "position_size_limit"),
            ("Amount above approval threshold", "approval_gate"),
            ("Rebalance not permitted yet", "rebalance_gate"),
            ("Wallet 0xdead is not in the allowed set", "execution_wallet_not_allowed"),
            ("Some brand new violation text", "policy_violation"),
        ],
    )
    def test_mapping(self, violation, expected):
        assert ToolExecutor._infer_check_name(violation) == expected
