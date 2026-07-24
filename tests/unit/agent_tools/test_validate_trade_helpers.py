"""Branch coverage for ToolExecutor validate-trade helpers.

Covers ``_build_synthetic_args`` (per-intent-type arg mapping) and
``_generate_risk_warnings`` (near-limit advisory generation). Pure logic —
no gateway; the PolicyEngine is real but never persists state.

Construction seam follows test_action_response_builders.py:
``object.__new__(ToolExecutor)`` with only the attributes the helpers read.
"""

import time
from decimal import Decimal

from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine

# =============================================================================
# _build_synthetic_args
# =============================================================================


class TestBuildSyntheticArgs:
    def test_swap_maps_from_to_tokens_and_amount(self):
        args = ToolExecutor._build_synthetic_args(
            "swap",
            {"from_token": "USDC", "to_token": "WETH", "amount": "100", "protocol": "uniswap_v3"},
            "base",
        )
        assert args["token_in"] == "USDC"
        assert args["token_out"] == "WETH"
        assert args["amount"] == "100"
        assert args["protocol"] == "uniswap_v3"
        assert args["chain"] == "base"
        assert args["intent_type"] == "swap"

    def test_swap_falls_back_to_token_in_out_and_amount_usd(self):
        args = ToolExecutor._build_synthetic_args(
            "SWAP",
            {"token_in": "USDC", "token_out": "WETH", "amount_usd": "250"},
            "arbitrum",
        )
        assert args["token_in"] == "USDC"
        assert args["token_out"] == "WETH"
        assert args["amount"] == "250"
        assert args["protocol"] == ""

    def test_lp_open_splits_slash_pool(self):
        args = ToolExecutor._build_synthetic_args(
            "lp_open",
            {"pool": "WETH/USDC", "amount0": "1", "amount1": "3000"},
            "base",
        )
        assert args["token_a"] == "WETH"
        assert args["token_b"] == "USDC"
        assert args["amount_a"] == "1"
        assert args["amount_b"] == "3000"

    def test_lp_open_without_slash_uses_explicit_token_params(self):
        args = ToolExecutor._build_synthetic_args(
            "lp_open",
            {"token_a": "WETH", "token_b": "USDC", "amount_a": "1", "amount_b": "3000"},
            "base",
        )
        assert args["token_a"] == "WETH"
        assert args["token_b"] == "USDC"
        assert args["amount_a"] == "1"
        assert args["amount_b"] == "3000"

    def test_lp_close_carries_position_id(self):
        args = ToolExecutor._build_synthetic_args(
            "lp_close",
            {"pool": "WETH/USDC", "position_id": 42},
            "base",
        )
        assert args["position_id"] == 42
        assert args["token_a"] == "WETH"

    def test_supply_and_repay_map_token_amount_protocol(self):
        for intent_type in ("supply", "repay"):
            args = ToolExecutor._build_synthetic_args(
                intent_type,
                {"token": "USDC", "amount": "500", "protocol": "aave_v3"},
                "base",
            )
            assert args["token"] == "USDC"
            assert args["amount"] == "500"
            assert args["protocol"] == "aave_v3"
            assert args["intent_type"] == intent_type

    def test_borrow_maps_borrow_and_collateral_fields(self):
        args = ToolExecutor._build_synthetic_args(
            "borrow",
            {
                "borrow_token": "USDC",
                "borrow_amount": "1000",
                "collateral_token": "WETH",
                "collateral_amount": "1",
                "protocol": "aave_v3",
            },
            "base",
        )
        assert args["token"] == "USDC"
        assert args["amount"] == "1000"
        assert args["collateral_token"] == "WETH"
        assert args["collateral_amount"] == "1"

    def test_borrow_falls_back_to_generic_token_amount(self):
        args = ToolExecutor._build_synthetic_args(
            "borrow",
            {"token": "USDC", "amount": "1000"},
            "base",
        )
        assert args["token"] == "USDC"
        assert args["amount"] == "1000"
        assert args["collateral_token"] == ""

    def test_unknown_intent_type_passes_params_through(self):
        args = ToolExecutor._build_synthetic_args(
            "bridge",
            {"from_chain": "base", "to_chain": "arbitrum", "amount": "5"},
            "base",
        )
        assert args["from_chain"] == "base"
        assert args["to_chain"] == "arbitrum"
        assert args["amount"] == "5"
        assert args["intent_type"] == "bridge"
        assert args["chain"] == "base"


# =============================================================================
# _generate_risk_warnings
# =============================================================================


def _executor(policy: AgentPolicy | None = None) -> ToolExecutor:
    executor = object.__new__(ToolExecutor)
    executor._policy_engine = PolicyEngine(policy or AgentPolicy())
    return executor


class TestGenerateRiskWarnings:
    def test_no_state_no_amount_yields_no_warnings(self):
        executor = _executor()
        assert executor._generate_risk_warnings({"chain": "base"}) == []

    def test_single_trade_above_80_pct_of_limit_warns(self):
        executor = _executor()
        # 9000 of 10000 default limit -> 90%
        warnings = executor._generate_risk_warnings({"amount": "9000"})
        checks = [w["check"] for w in warnings]
        assert "single_trade_near_limit" in checks
        [w] = [w for w in warnings if w["check"] == "single_trade_near_limit"]
        assert w["severity"] == "warning"
        assert "90%" in w["message"]

    def test_single_trade_below_80_pct_no_warning(self):
        executor = _executor()
        warnings = executor._generate_risk_warnings({"amount": "5000"})
        assert all(w["check"] != "single_trade_near_limit" for w in warnings)

    def test_single_trade_exceeding_limit_is_not_a_warning(self):
        """Exceeding the limit is a hard violation elsewhere, not an advisory."""
        executor = _executor()
        warnings = executor._generate_risk_warnings({"amount": "15000"})
        assert all(w["check"] != "single_trade_near_limit" for w in warnings)

    def test_daily_spend_above_80_pct_warns(self):
        executor = _executor()
        # 45000 of 50000 default daily limit -> 90%
        executor._policy_engine._daily_spend_usd = Decimal("45000")
        warnings = executor._generate_risk_warnings({"chain": "base"})
        checks = [w["check"] for w in warnings]
        assert "daily_spend_near_limit" in checks

    def test_daily_spend_projection_over_limit_is_not_a_warning(self):
        executor = _executor()
        executor._policy_engine._daily_spend_usd = Decimal("48000")
        warnings = executor._generate_risk_warnings({"amount": "5000"})
        assert all(w["check"] != "daily_spend_near_limit" for w in warnings)

    def test_cooldown_partially_elapsed_warns_with_remaining_seconds(self):
        executor = _executor()
        # 200s elapsed of a 300s cooldown -> past half, still blocking
        executor._policy_engine._last_trade_timestamp = time.time() - 200
        warnings = executor._generate_risk_warnings({"chain": "base"})
        [w] = [w for w in warnings if w["check"] == "cooldown_partial"]
        assert "remaining" in w["message"]

    def test_cooldown_fully_elapsed_no_warning(self):
        executor = _executor()
        executor._policy_engine._last_trade_timestamp = time.time() - 400
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "cooldown_partial" for w in warnings)

    def test_cooldown_early_phase_no_warning(self):
        executor = _executor()
        # 60s elapsed of a 300s cooldown -> below the half-way advisory threshold
        executor._policy_engine._last_trade_timestamp = time.time() - 60
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "cooldown_partial" for w in warnings)

    def test_trade_rate_one_below_limit_warns(self):
        executor = _executor()
        now = time.time()
        # 9 recent trades against the default 10/hour limit
        executor._policy_engine._trades_this_hour = [now - i for i in range(9)]
        warnings = executor._generate_risk_warnings({"chain": "base"})
        [w] = [w for w in warnings if w["check"] == "trade_rate_near_limit"]
        assert "9/10" in w["message"]

    def test_trade_rate_at_limit_no_advisory(self):
        """At the limit the hard rate check fires; the advisory stays silent."""
        executor = _executor()
        now = time.time()
        executor._policy_engine._trades_this_hour = [now - i for i in range(10)]
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "trade_rate_near_limit" for w in warnings)

    def test_stale_trades_outside_window_ignored(self):
        executor = _executor()
        # All trades older than an hour -> pruned from the advisory count
        executor._policy_engine._trades_this_hour = [time.time() - 7200] * 9
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "trade_rate_near_limit" for w in warnings)

    def test_consecutive_failures_one_below_breaker_warns(self):
        executor = _executor()
        # 2 of the default 3 -> one more failure trips the breaker
        executor._policy_engine._consecutive_failures = 2
        warnings = executor._generate_risk_warnings({"chain": "base"})
        [w] = [w for w in warnings if w["check"] == "circuit_breaker_near"]
        assert "2/3" in w["message"]

    def test_breaker_already_tripped_no_advisory(self):
        executor = _executor()
        executor._policy_engine._consecutive_failures = 3
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "circuit_breaker_near" for w in warnings)

    def test_zero_failures_no_advisory(self):
        executor = _executor()
        warnings = executor._generate_risk_warnings({"chain": "base"})
        assert all(w["check"] != "circuit_breaker_near" for w in warnings)

    def test_multiple_near_limit_conditions_stack(self):
        executor = _executor()
        engine = executor._policy_engine
        engine._daily_spend_usd = Decimal("36500")
        engine._consecutive_failures = 2
        warnings = executor._generate_risk_warnings({"amount": "8500"})
        checks = {w["check"] for w in warnings}
        assert {"single_trade_near_limit", "daily_spend_near_limit", "circuit_breaker_near"} <= checks

    def test_disabled_limits_produce_no_spend_warnings(self):
        policy = AgentPolicy(
            max_single_trade_usd=Decimal("0"),
            max_daily_spend_usd=Decimal("0"),
        )
        executor = _executor(policy)
        warnings = executor._generate_risk_warnings({"amount": "9000"})
        checks = {w["check"] for w in warnings}
        assert "single_trade_near_limit" not in checks
        assert "daily_spend_near_limit" not in checks
