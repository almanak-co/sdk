"""Tests for agent policy engine."""

import time
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.agent_tools.catalog import RiskTier, ToolCategory, ToolDefinition, get_default_catalog
from almanak.framework.agent_tools.errors import RiskBlockedError
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyDecision, PolicyEngine
from almanak.framework.agent_tools.schemas import GetPriceRequest, GetPriceResponse, SwapTokensRequest, SwapTokensResponse


def _make_tool(name: str, category: ToolCategory = ToolCategory.DATA, risk_tier: RiskTier = RiskTier.NONE):
    return ToolDefinition(
        name=name,
        description="test",
        category=category,
        risk_tier=risk_tier,
        request_schema=GetPriceRequest,
        response_schema=GetPriceResponse,
    )


class TestAgentPolicyDefaults:
    def test_defaults(self):
        p = AgentPolicy()
        assert p.max_single_trade_usd == Decimal("10000")
        assert p.max_daily_spend_usd == Decimal("50000")
        assert "arbitrum" in p.allowed_chains
        assert p.require_simulation_before_execution is True
        assert p.max_trades_per_hour == 10
        assert p.cooldown_seconds == 300


class TestPolicyDecision:
    def test_allowed(self):
        d = PolicyDecision(allowed=True)
        d.raise_if_denied("test")  # Should not raise

    def test_denied_raises(self):
        d = PolicyDecision(allowed=False, violations=["limit exceeded"])
        with pytest.raises(RiskBlockedError):
            d.raise_if_denied("swap_tokens")


class TestPolicyEngineToolAllowed:
    def test_all_tools_allowed_by_default(self):
        engine = PolicyEngine(AgentPolicy())
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_tool_not_in_allowed_set(self):
        policy = AgentPolicy(allowed_tools={"get_price", "get_balance"})
        engine = PolicyEngine(policy)
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"chain": "arbitrum"})
        assert decision.allowed is False
        assert "not in the allowed set" in decision.violations[0]

    def test_tool_in_allowed_set(self):
        policy = AgentPolicy(allowed_tools={"get_price"})
        engine = PolicyEngine(policy)
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True


class TestPolicyEngineChainAllowed:
    def test_allowed_chain(self):
        engine = PolicyEngine(AgentPolicy(allowed_chains={"arbitrum", "base"}))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH", "chain": "base"})
        assert decision.allowed is True

    def test_disallowed_chain(self):
        engine = PolicyEngine(AgentPolicy(allowed_chains={"arbitrum"}))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH", "chain": "ethereum"})
        assert decision.allowed is False
        assert "Chain 'ethereum' is not allowed" in decision.violations[0]

    def test_no_chain_in_args(self):
        engine = PolicyEngine(AgentPolicy(allowed_chains={"arbitrum"}))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH"})
        assert decision.allowed is True  # No chain = no chain check


class TestPolicyEngineTokenAllowed:
    def test_tokens_none_allows_all(self):
        engine = PolicyEngine(AgentPolicy(allowed_tokens=None))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"token_in": "WBTC", "token_out": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_token_in_allowed_set(self):
        engine = PolicyEngine(AgentPolicy(allowed_tokens={"ETH", "USDC"}))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_token_not_in_allowed_set(self):
        engine = PolicyEngine(AgentPolicy(allowed_tokens={"ETH", "USDC"}))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"token": "WBTC", "chain": "arbitrum"})
        assert decision.allowed is False


class TestPolicyEngineSpendLimits:
    def test_under_single_trade_limit(self):
        engine = PolicyEngine(AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "3000", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_over_single_trade_limit(self):
        engine = PolicyEngine(AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "6000", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    def test_daily_spend_accumulation(self):
        engine = PolicyEngine(AgentPolicy(
            max_single_trade_usd=Decimal("10000"),
            max_daily_spend_usd=Decimal("5000"),
            cooldown_seconds=0,
        ))
        # Record some prior spending
        engine.record_trade(Decimal("4000"), success=True)

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "2000", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "daily limit" in decision.violations[0]

    @patch.object(PolicyEngine, "_resolve_token_decimals", return_value=6)
    def test_deposit_vault_subject_to_spend_limits(self, _mock_decimals):
        """P0-3: deposit_vault moves real funds and must check spend limits."""
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=lambda t: Decimal("1.0") if t.upper() == "USDC" else None,
        )
        tool = _make_tool("deposit_vault", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 6000 USDC in raw units (6 decimals) = 6000000000 raw
        # 6000000000 / 10^6 = 6000 USDC * $1 = $6000 > $5000 limit
        decision = engine.check(tool, {"amount": "6000000000", "underlying_token": "USDC", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    @patch.object(PolicyEngine, "_resolve_token_decimals", return_value=6)
    def test_deposit_vault_uses_underlying_token_for_price_lookup(self, _mock_decimals):
        """P0-3 regression fix: deposit_vault uses underlying_token, not token_in.

        Without this fix, raw amounts like 10000000 (10 USDC) would be treated as
        $10M USD because the token field isn't matched, and the raw amount fallback
        would be used.
        """
        def _mock_price(token: str) -> Decimal | None:
            if token.upper() == "USDC" or token.startswith("0x"):
                return Decimal("1.0")  # stablecoin
            return None

        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("50"), cooldown_seconds=0),
            price_lookup=_mock_price,
        )
        tool = _make_tool("deposit_vault", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 10000000 raw units / 10^6 = 10 USDC * $1 = $10 USD -- under $50 limit
        decision = engine.check(tool, {"amount": "10000000", "underlying_token": "USDC", "chain": "arbitrum"})
        assert decision.allowed is True

    @patch.object(PolicyEngine, "_resolve_token_decimals", return_value=6)
    def test_deposit_vault_normalizes_raw_amounts_by_decimals(self, _mock_decimals):
        """Vault deposits use raw token units (e.g. 10000000 = 10 USDC with 6 decimals).

        The policy engine must normalize by token decimals before price conversion,
        otherwise 10000000 raw units would be treated as $10M.
        """
        def _mock_price(token: str) -> Decimal | None:
            if token.upper() == "USDC":
                return Decimal("1.0")
            return None

        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("50"), cooldown_seconds=0),
            price_lookup=_mock_price,
        )
        tool = _make_tool("deposit_vault", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 10000000 raw units / 10^6 = 10 USDC * $1 = $10 USD -- under $50 limit
        decision = engine.check(tool, {"amount": "10000000", "underlying_token": "USDC", "chain": "arbitrum"})
        assert decision.allowed is True

    @patch.object(PolicyEngine, "_resolve_token_decimals", return_value=6)
    def test_deposit_vault_raw_amount_exceeds_limit_after_normalization(self, _mock_decimals):
        """Large raw amount that still exceeds limit after decimals normalization."""
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("50"), cooldown_seconds=0),
            price_lookup=lambda t: Decimal("1.0") if t.upper() == "USDC" else None,
        )
        tool = _make_tool("deposit_vault", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 100000000 raw / 10^6 = 100 USDC * $1 = $100 > $50 limit
        decision = engine.check(tool, {"amount": "100000000", "underlying_token": "USDC", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    def test_deploy_vault_exempt_from_spend_limits(self):
        """deploy_vault doesn't transfer user funds; should skip spend checks."""
        catalog = get_default_catalog()
        tool_def = catalog.get("deploy_vault")
        engine = PolicyEngine(AgentPolicy(max_single_trade_usd=Decimal("100"), cooldown_seconds=0))
        # deploy_vault has amount fields that shouldn't trigger spend limits
        decision = engine.check(tool_def, {"amount": "999999", "chain": "arbitrum"})
        # Should pass because deploy_vault is in _VAULT_LIFECYCLE_TOOLS
        spend_violations = [v for v in decision.violations if "trade" in v.lower() or "spend" in v.lower()]
        assert len(spend_violations) == 0

    def test_teardown_vault_not_exempt_from_spend_checks(self):
        """VIB-101: teardown_vault moves real funds and must not bypass spend checks."""
        from almanak.framework.agent_tools.policy import _VAULT_LIFECYCLE_TOOLS

        assert "teardown_vault" not in _VAULT_LIFECYCLE_TOOLS

    def test_teardown_vault_subject_to_stop_loss(self):
        """VIB-101: teardown_vault is HIGH risk, so stop-loss applies."""
        catalog = get_default_catalog()
        tool_def = catalog.get("teardown_vault")
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        engine.update_portfolio_value(Decimal("100000"))
        engine.update_portfolio_value(Decimal("90000"))  # 10% drawdown
        decision = engine.check(tool_def, {"chain": "arbitrum"})
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 1

    def test_teardown_vault_subject_to_cooldown(self):
        """VIB-101: teardown_vault is HIGH risk, so cooldown applies."""
        catalog = get_default_catalog()
        tool_def = catalog.get("teardown_vault")
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=60))
        engine.record_trade(Decimal("100"), success=True)
        decision = engine.check(tool_def, {"chain": "arbitrum"})
        cooldown_violations = [v for v in decision.violations if "Cooldown" in v]
        assert len(cooldown_violations) == 1


class TestPolicyEngineRateLimits:
    def test_tool_call_rate_limit(self):
        engine = PolicyEngine(AgentPolicy(max_tool_calls_per_minute=3))
        tool = _make_tool("get_price")

        for _ in range(3):
            engine.record_tool_call()

        decision = engine.check(tool, {"token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "rate limit" in decision.violations[0].lower()

    def test_trade_rate_limit(self):
        engine = PolicyEngine(AgentPolicy(max_trades_per_hour=2, cooldown_seconds=0))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)

        engine.record_trade(Decimal("100"), success=True)
        engine.record_trade(Decimal("100"), success=True)

        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "Trade rate limit" in decision.violations[0]


class TestPolicyEngineCircuitBreaker:
    def test_consecutive_failures_trigger(self):
        engine = PolicyEngine(AgentPolicy(max_consecutive_failures=2, cooldown_seconds=0))
        engine.record_trade(Decimal("100"), success=False)
        engine.record_trade(Decimal("100"), success=False)

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "Circuit breaker" in decision.violations[0]

    def test_success_resets_failures(self):
        engine = PolicyEngine(AgentPolicy(max_consecutive_failures=2, cooldown_seconds=0))
        engine.record_trade(Decimal("100"), success=False)
        engine.record_trade(Decimal("100"), success=True)  # Resets counter
        engine.record_trade(Decimal("100"), success=False)

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        # Only 1 consecutive failure, threshold is 2
        assert decision.allowed is True


class TestPolicyEngineCooldown:
    def test_cooldown_blocks_trade(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=60))
        engine.record_trade(Decimal("100"), success=True)

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "Cooldown" in decision.violations[0]

    def test_cooldown_expired(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=1))
        engine._last_trade_timestamp = time.time() - 2  # 2 seconds ago

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        # Cooldown expired, should pass (no other violations)
        cooldown_violations = [v for v in decision.violations if "Cooldown" in v]
        assert len(cooldown_violations) == 0

    def test_data_tools_skip_cooldown(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=300))
        engine.record_trade(Decimal("100"), success=True)

        tool = _make_tool("get_price")  # DATA, NONE risk
        decision = engine.check(tool, {"token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True  # Data tools don't check cooldown


class TestPolicyEnginePriceAwareSpendLimits:
    """Tests for price-aware pre-check in _check_spend_limits (P0-1)."""

    def test_high_value_token_blocked_with_price_lookup(self):
        """100 ETH at $3000 = $300k which exceeds $10k limit."""
        price_lookup = lambda token: Decimal("3000") if token == "ETH" else None  # noqa: E731
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("10000"), cooldown_seconds=0),
            price_lookup=price_lookup,
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "token_in": "ETH", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    def test_high_value_token_passes_without_price_lookup(self):
        """Without price lookup, 100 (raw) < 10000 limit, so it passes (old behavior)."""
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("10000"), cooldown_seconds=0),
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "token_in": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_price_lookup_failure_falls_back_to_raw(self):
        """If price lookup raises, fall back to raw amount (backward compatible)."""
        def failing_lookup(token):
            raise RuntimeError("gateway down")

        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=failing_lookup,
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 3000 raw < 5000 limit, so should pass even though lookup fails
        decision = engine.check(tool, {"amount": "3000", "token_in": "ETH", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_price_lookup_returns_none_falls_back(self):
        """If price lookup returns None, fall back to raw amount."""
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=lambda _: None,
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "3000", "chain": "arbitrum"})
        assert decision.allowed is True

    def test_lp_open_sums_both_tokens(self):
        """LP open with two tokens: both should be converted to USD."""
        prices = {"USDC": Decimal("1"), "ETH": Decimal("3000")}
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=lambda t: prices.get(t),
        )
        tool = _make_tool("open_lp", category=ToolCategory.ACTION, risk_tier=RiskTier.HIGH)
        # 1 ETH ($3000) + 1000 USDC ($1000) = $4000, under $5000
        decision = engine.check(tool, {
            "amount_a": "1", "token_a": "ETH",
            "amount_b": "1000", "token_b": "USDC",
            "chain": "arbitrum",
        })
        assert decision.allowed is True

    def test_lp_open_sums_both_tokens_exceeds(self):
        """LP open where combined value exceeds limit."""
        prices = {"USDC": Decimal("1"), "ETH": Decimal("3000")}
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=lambda t: prices.get(t),
        )
        tool = _make_tool("open_lp", category=ToolCategory.ACTION, risk_tier=RiskTier.HIGH)
        # 2 ETH ($6000) + 1000 USDC ($1000) = $7000, exceeds $5000
        decision = engine.check(tool, {
            "amount_a": "2", "token_a": "ETH",
            "amount_b": "1000", "token_b": "USDC",
            "chain": "arbitrum",
        })
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    def test_from_token_field_recognized_in_spend_check(self):
        """from_token (intent vocabulary) is recognized by pre-check for action tools."""
        prices = {"ETH": Decimal("3000"), "USDC": Decimal("1")}
        engine = PolicyEngine(
            AgentPolicy(max_single_trade_usd=Decimal("5000"), cooldown_seconds=0),
            price_lookup=lambda t: prices.get(t),
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 2 ETH at $3000 = $6000, exceeds $5000 limit -- using from_token field
        decision = engine.check(tool, {"amount": "2", "from_token": "ETH", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "single-trade limit" in decision.violations[0]

    def test_daily_limit_with_price_lookup(self):
        """Price-aware daily limit tracking."""
        engine = PolicyEngine(
            AgentPolicy(
                max_single_trade_usd=Decimal("50000"),
                max_daily_spend_usd=Decimal("10000"),
                cooldown_seconds=0,
            ),
            price_lookup=lambda t: Decimal("3000") if t == "ETH" else Decimal("1"),
        )
        engine.record_trade(Decimal("8000"), success=True)

        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # 1 ETH = $3000, projected = $8000 + $3000 = $11000 > $10000
        decision = engine.check(tool, {"amount": "1", "token_in": "ETH", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "daily limit" in decision.violations[0]


class TestPolicyEngineResetDaily:
    def test_reset_clears_spend(self):
        engine = PolicyEngine(AgentPolicy())
        engine.record_trade(Decimal("5000"), success=True)
        assert engine._daily_spend_usd == Decimal("5000")

        engine.reset_daily()
        assert engine._daily_spend_usd == Decimal("0")
        assert len(engine._trades_this_hour) == 0


class TestPolicyEngineAutoDailyReset:
    """WS1a: auto daily reset when 24h has elapsed."""

    def test_auto_reset_after_24h(self):
        engine = PolicyEngine(AgentPolicy(
            max_single_trade_usd=Decimal("100000"),
            max_daily_spend_usd=Decimal("5000"),
            cooldown_seconds=0,
        ))
        engine.record_trade(Decimal("4000"), success=True)
        # Simulate 25 hours elapsed
        engine._day_start = time.time() - 90001
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        # $2000 projected = $4000 + $2000 = $6000 > $5000 if not reset
        # But auto-reset should clear $4000, so $2000 < $5000 passes
        decision = engine.check(tool, {"amount": "2000", "chain": "arbitrum"})
        daily_violations = [v for v in decision.violations if "daily limit" in v]
        assert len(daily_violations) == 0

    def test_no_reset_before_24h(self):
        engine = PolicyEngine(AgentPolicy(
            max_single_trade_usd=Decimal("100000"),
            max_daily_spend_usd=Decimal("5000"),
            cooldown_seconds=0,
        ))
        engine.record_trade(Decimal("4000"), success=True)
        # Only 1 hour elapsed -- no reset
        engine._day_start = time.time() - 3600
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "2000", "chain": "arbitrum"})
        assert decision.allowed is False
        assert "daily limit" in decision.violations[0]


class TestPolicyEnginePositionSize:
    """WS1b: enforce max_position_size_usd."""

    def test_under_position_size_limit(self):
        engine = PolicyEngine(AgentPolicy(
            max_position_size_usd=Decimal("50000"),
            max_single_trade_usd=Decimal("100000"),
            cooldown_seconds=0,
        ))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "30000", "chain": "arbitrum"})
        position_violations = [v for v in decision.violations if "position size" in v.lower()]
        assert len(position_violations) == 0

    def test_over_position_size_limit(self):
        engine = PolicyEngine(AgentPolicy(
            max_position_size_usd=Decimal("50000"),
            max_single_trade_usd=Decimal("200000"),
            max_daily_spend_usd=Decimal("200000"),
            cooldown_seconds=0,
        ))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "60000", "chain": "arbitrum"})
        assert decision.allowed is False
        position_violations = [v for v in decision.violations if "position size" in v.lower()]
        assert len(position_violations) == 1

    def test_position_size_with_price_lookup(self):
        """Price-aware position size: 10 ETH at $3000 = $30k < $50k limit."""
        engine = PolicyEngine(
            AgentPolicy(
                max_position_size_usd=Decimal("50000"),
                max_single_trade_usd=Decimal("100000"),
                cooldown_seconds=0,
            ),
            price_lookup=lambda t: Decimal("3000") if t == "ETH" else None,
        )
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "10", "token_in": "ETH", "chain": "arbitrum"})
        position_violations = [v for v in decision.violations if "position size" in v.lower()]
        assert len(position_violations) == 0

    def test_data_tools_skip_position_size_check(self):
        """DATA tools should not check position size."""
        engine = PolicyEngine(AgentPolicy(max_position_size_usd=Decimal("100")))
        tool = _make_tool("get_price")  # DATA, NONE risk
        decision = engine.check(tool, {"amount": "999999", "chain": "arbitrum"})
        assert decision.allowed is True


class TestPolicyEngineApprovalGate:
    """WS1c: enforce require_human_approval_above_usd."""

    def test_under_approval_threshold(self):
        engine = PolicyEngine(AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            cooldown_seconds=0,
        ))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "5000", "chain": "arbitrum"})
        approval_violations = [v for v in decision.violations if "approval threshold" in v.lower()]
        assert len(approval_violations) == 0

    def test_over_approval_threshold(self):
        engine = PolicyEngine(AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            max_daily_spend_usd=Decimal("100000"),
            max_position_size_usd=Decimal("100000"),
            cooldown_seconds=0,
        ))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "15000", "chain": "arbitrum"})
        assert decision.allowed is False
        approval_violations = [v for v in decision.violations if "approval threshold" in v.lower()]
        assert len(approval_violations) == 1

    def test_per_tool_threshold_is_more_restrictive(self):
        """When per-tool threshold is lower than policy threshold, use per-tool."""
        engine = PolicyEngine(AgentPolicy(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            max_daily_spend_usd=Decimal("100000"),
            max_position_size_usd=Decimal("100000"),
            cooldown_seconds=0,
        ))
        tool = ToolDefinition(
            name="swap_tokens",
            description="test",
            category=ToolCategory.ACTION,
            risk_tier=RiskTier.MEDIUM,
            request_schema=SwapTokensRequest,
            response_schema=SwapTokensResponse,
            requires_approval_above_usd=5000.0,
        )
        decision = engine.check(tool, {"amount": "7000", "chain": "arbitrum"})
        assert decision.allowed is False
        approval_violations = [v for v in decision.violations if "approval threshold" in v.lower()]
        assert len(approval_violations) == 1

    def test_data_tools_skip_approval_gate(self):
        engine = PolicyEngine(AgentPolicy(require_human_approval_above_usd=Decimal("100")))
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"amount": "999999", "chain": "arbitrum"})
        assert decision.allowed is True


class TestPolicyEngineStopLoss:
    """WS1d: enforce stop_loss_pct via high-water mark drawdown.

    Stop-loss only blocks MEDIUM/HIGH risk tools. DATA tools are never blocked.
    """

    def test_no_drawdown_passes(self):
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        engine.update_portfolio_value(Decimal("100000"))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 0

    def test_drawdown_exceeds_stop_loss(self):
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        engine.update_portfolio_value(Decimal("100000"))  # peak
        engine.update_portfolio_value(Decimal("94000"))   # 6% drawdown
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        assert decision.allowed is False
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 1

    def test_drawdown_under_stop_loss(self):
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        engine.update_portfolio_value(Decimal("100000"))
        engine.update_portfolio_value(Decimal("96000"))  # 4% drawdown, under 5%
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 0

    def test_no_portfolio_value_skips_check(self):
        """If portfolio value was never set, stop-loss check is skipped."""
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 0

    def test_high_water_mark_ratchets_up(self):
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0"), cooldown_seconds=0))
        engine.update_portfolio_value(Decimal("100000"))
        engine.update_portfolio_value(Decimal("120000"))  # new peak
        engine.update_portfolio_value(Decimal("115000"))  # 4.2% from new peak
        assert engine._peak_portfolio_usd == Decimal("120000")
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        stop_violations = [v for v in decision.violations if "Stop-loss" in v]
        assert len(stop_violations) == 0  # 4.2% < 5%

    def test_data_tools_not_blocked_by_stop_loss(self):
        """DATA tools should never be blocked by stop-loss."""
        engine = PolicyEngine(AgentPolicy(stop_loss_pct=Decimal("5.0")))
        engine.update_portfolio_value(Decimal("100000"))
        engine.update_portfolio_value(Decimal("90000"))  # 10% drawdown
        tool = _make_tool("get_price")  # DATA, NONE risk
        decision = engine.check(tool, {"chain": "arbitrum"})
        assert decision.allowed is True


class TestPolicyEngineRebalanceGate:
    """WS1e: rebalance gate suggestion for LP tools."""

    def test_lp_open_without_rebalance_gets_suggestion(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=0))
        tool = _make_tool("open_lp_position", category=ToolCategory.ACTION, risk_tier=RiskTier.HIGH)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        # Should have a suggestion but NOT a violation
        rebalance_suggestions = [s for s in decision.suggestions if "compute_rebalance_candidate" in s]
        assert len(rebalance_suggestions) == 1

    def test_lp_open_with_rebalance_approved_no_suggestion(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=0))
        engine.set_rebalance_approved(True)
        tool = _make_tool("open_lp_position", category=ToolCategory.ACTION, risk_tier=RiskTier.HIGH)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        rebalance_suggestions = [s for s in decision.suggestions if "compute_rebalance_candidate" in s]
        assert len(rebalance_suggestions) == 0

    def test_non_lp_tool_skips_rebalance_gate(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=0))
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        rebalance_suggestions = [s for s in decision.suggestions if "compute_rebalance_candidate" in s]
        assert len(rebalance_suggestions) == 0

    def test_set_rebalance_approved_false_resets(self):
        engine = PolicyEngine(AgentPolicy(cooldown_seconds=0))
        engine.set_rebalance_approved(True)
        engine.set_rebalance_approved(False)
        tool = _make_tool("close_lp_position", category=ToolCategory.ACTION, risk_tier=RiskTier.HIGH)
        decision = engine.check(tool, {"amount": "100", "chain": "arbitrum"})
        rebalance_suggestions = [s for s in decision.suggestions if "compute_rebalance_candidate" in s]
        assert len(rebalance_suggestions) == 1


class TestExecutionWalletValidation:
    """A3: validate execution_wallet against configured allowlist."""

    def test_execution_wallet_in_allowlist_passes(self):
        policy = AgentPolicy(
            allowed_execution_wallets={"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"execution_wallet": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "chain": "arbitrum"})
        assert decision.allowed

    def test_execution_wallet_not_in_allowlist_blocked(self):
        policy = AgentPolicy(
            allowed_execution_wallets={"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"execution_wallet": "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "chain": "arbitrum"})
        assert not decision.allowed
        assert any("not in the allowed set" in v for v in decision.violations)

    def test_execution_wallet_none_defaults_ok(self):
        policy = AgentPolicy(cooldown_seconds=0)  # no allowed_execution_wallets
        engine = PolicyEngine(policy)
        tool = _make_tool("get_price")
        decision = engine.check(tool, {"execution_wallet": "0xANYTHING", "chain": "arbitrum"})
        assert decision.allowed

    def test_execution_wallet_case_insensitive(self):
        policy = AgentPolicy(
            allowed_execution_wallets={"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            cooldown_seconds=0,
        )
        engine = PolicyEngine(policy)
        tool = _make_tool("swap_tokens", category=ToolCategory.ACTION, risk_tier=RiskTier.MEDIUM)
        decision = engine.check(tool, {"execution_wallet": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "chain": "arbitrum"})
        assert decision.allowed
