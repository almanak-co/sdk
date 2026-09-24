"""An unpriceable token must not be valued by pretending its amount is USD.

A token count used as dollars both overstates and understates. Excluding the
leg and then returning 0 lets spend and approval checks treat the trade as
free. An unmeasured leg is refused until the gateway returns a price for it.
"""

from decimal import Decimal

from almanak.framework.agent_tools.catalog import RiskTier, ToolCategory, ToolDefinition
from almanak.framework.agent_tools.policy import AgentPolicy, PolicyEngine
from almanak.framework.agent_tools.schemas import SwapTokensRequest, SwapTokensResponse

MEME = "0xea169512f81d60f9d76b306878ce99808b66b030"  # SHINYHUNTERS, no price source


def _tool() -> ToolDefinition:
    return ToolDefinition(
        name="swap_tokens",
        description="test",
        category=ToolCategory.ACTION,
        risk_tier=RiskTier.MEDIUM,
        request_schema=SwapTokensRequest,
        response_schema=SwapTokensResponse,
    )


def _engine(**kw) -> PolicyEngine:
    return PolicyEngine(AgentPolicy(cooldown_seconds=0, **kw))


class TestUnpricedTokensAreNotFabricated:
    def test_token_amount_is_not_treated_as_usd(self):
        """The exact incident: 16240.32 tokens must not be valued at $16,240.32."""
        engine = _engine()
        value = engine._estimate_usd_value(
            {"amount": "16240.321598567055201355", "token_in": MEME, "chain": "robinhood"}
        )
        assert value != Decimal("16240.321598567055201355"), (
            "token count was used as a USD value -- this is the defect that made "
            "a $2.96 position unexitable behind a $10,000 approval gate"
        )
        assert value == Decimal("0"), "an unpriceable leg is unmeasured, not guessed"

    def test_unpriced_leg_is_recorded_for_the_caller(self):
        engine = _engine()
        engine._estimate_usd_value({"amount": "1000", "token_in": MEME, "chain": "robinhood"})
        assert MEME in engine._unpriced_legs, "callers must be able to tell 'no value' from 'zero value'"

    def test_unpriced_sell_is_refused_instead_of_priced_at_zero(self):
        """A missing price must not skip the cap by looking like a $0 trade."""
        engine = _engine(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            allowed_chains={"robinhood"},
        )
        decision = engine.check(
            _tool(),
            {"amount": "16240.321598567055201355", "token_in": MEME, "token_out": "ETH", "chain": "robinhood"},
        )
        assert decision.allowed is False
        assert decision.requires_approval is False
        assert "unmeasured" in decision.violations[0]

    def test_a_gateway_price_makes_the_sell_measurable(self):
        """With a price (live, or the gateway's opt-in operator override) the sell passes."""
        engine = _engine(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
            allowed_chains={"robinhood"},
        )
        engine._price_lookup = lambda token, chain=None: Decimal("0.0001820988") if token == MEME else None
        decision = engine.check(
            _tool(),
            {"amount": "16240.321598567055201355", "token_in": MEME, "token_out": "ETH", "chain": "robinhood"},
        )
        assert decision.allowed is True
        assert decision.requires_approval is False

    def test_pegged_stablecoin_still_uses_amount_as_usd(self):
        """The legitimate case must keep working: 15000 USDC really is ~$15,000."""
        engine = _engine(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
        )
        assert engine._estimate_usd_value({"amount": "15000", "token_in": "USDC", "chain": "arbitrum"}) == Decimal(
            "15000"
        )
        decision = engine.check(_tool(), {"amount": "15000", "token_in": "USDC", "chain": "arbitrum"})
        assert decision.requires_approval is True

    def test_a_real_price_is_always_preferred(self):
        engine = _engine()
        engine._price_lookup = lambda t: Decimal("0.0001820988") if t == MEME else None
        value = engine._estimate_usd_value(
            {"amount": "16240.321598567055201355", "token_in": MEME, "chain": "robinhood"}
        )
        assert Decimal("2.9") < value < Decimal("3.0"), f"expected ~$2.96, got {value}"

    def test_susds_without_a_price_is_not_valued_at_one_dollar(self):
        """Savings USDS trades above $1, so a missing price must not pass as amount == USD."""
        engine = _engine(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("100000"),
        )
        decision = engine.check(
            _tool(),
            {"amount": "15000", "token_in": "SUSDS", "chain": "arbitrum"},
        )
        assert decision.allowed is False
        assert decision.requires_approval is False
        assert "unmeasured" in decision.violations[0]

    def test_measured_susds_price_above_par_reaches_the_approval_gate(self):
        engine = _engine(
            require_human_approval_above_usd=Decimal("10000"),
            max_single_trade_usd=Decimal("20000"),
        )
        engine._price_lookup = lambda token, chain=None: Decimal("1.08") if token == "SUSDS" else None
        decision = engine.check(
            _tool(),
            {"amount": "10000", "token_in": "SUSDS", "chain": "arbitrum"},
        )
        assert decision.allowed is True
        assert decision.requires_approval is True

    def test_pegged_check_never_trusts_a_bare_address(self):
        """An address we cannot price is exactly what must not be assumed to be $1."""
        from almanak.framework.agent_tools.policy import is_usd_pegged

        assert is_usd_pegged("USDC") is True
        assert is_usd_pegged("usdc") is True
        assert is_usd_pegged(MEME) is False
        assert is_usd_pegged("0xdeadbeef") is False
        assert is_usd_pegged(None) is False


class TestPricingChainAndFiniteness:
    """The chain a leg is priced on, and what counts as a price at all."""

    def test_a_bridge_leg_is_priced_on_the_chain_it_is_spent_from(self):
        """A bridge spends on ``from_chain``; the executor sets ``chain`` later.

        The same address is a different asset on another chain, so pricing the
        source token on the default chain values one token at another's price
        and the spend limit is compared against the wrong number.
        """
        seen: list[object] = []

        def lookup(token: object, chain: object = None) -> Decimal:
            seen.append(chain)
            return Decimal("1")

        engine = _engine()
        engine._price_lookup = lookup
        engine._estimate_usd_value(
            {"amount": "5", "from_token": MEME, "from_chain": "arbitrum", "to_chain": "base"}
        )
        assert seen == ["arbitrum"], f"bridge leg priced on {seen!r}, not its source chain"

    def test_a_non_bridge_leg_still_prices_on_its_own_chain(self):
        """from_chain is absent off the bridge path, so nothing else moves."""
        seen: list[object] = []

        def lookup(token: object, chain: object = None) -> Decimal:
            seen.append(chain)
            return Decimal("1")

        engine = _engine()
        engine._price_lookup = lookup
        engine._estimate_usd_value({"amount": "5", "token_in": MEME, "chain": "robinhood"})
        assert seen == ["robinhood"]

    def test_an_infinite_price_is_unmeasured_not_infinite(self):
        """Infinity passes a bare ``> 0`` and would make the notional infinite."""
        engine = _engine()
        engine._price_lookup = lambda token, chain=None: Decimal("Infinity")
        value = engine._estimate_usd_value({"amount": "5", "token_in": MEME, "chain": "robinhood"})
        assert value == Decimal("0") and MEME in engine._unpriced_legs
