"""Lending chain-scope gate: DECLARED-but-mismatched venue/chain fails closed.

Observed defect: a SUPPLY on fluid/ethereum FILLED in the backtest and
accrued the global 3% surrogate APY, while the fluid connector's lending
``StrategyMatrixEntry`` declares {arbitrum, base} only and the live compiler
fails closed on the same combination. The generic lending lane now consults
the connector-declared lending chain scope at fill time:

- protocol DECLARES a lending matrix, run chain NOT in it -> typed rejection
  (``UNDECLARED_LENDING_CHAIN``), zero state mutation, zero costs;
- protocol DECLARES a lending matrix, run chain IN it -> fills as before;
- protocol declares NO lending matrix at all (duck-typed/generic test
  protocols) -> generic behavior preserved.

Companion doctrine: ``InterestCalculator.get_*_apy_for_protocol`` no longer
hands out the global surrogate APY silently — it warns once per protocol.
"""

import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.connectors._connector import SupportedChainsSpec
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
    _declared_lending_chains,
    _lending_chain_scope_rejection,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.sizing import RejectionCode
from almanak.framework.intents.lending_intents import BorrowIntent, SupplyIntent
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("10000")
SUPPLY_AMOUNT = Decimal("5000")


def _market(chain: str) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"USDC": Decimal("1"), "WETH": Decimal("2000")},
        chain=chain,
    )


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        token_funding=_pnl_token_funding(INITIAL_CASH),
        include_gas_costs=False,
    )


def _billing_backtester() -> PnLBacktester:
    """Backtester with fees ACTUALLY enabled, for the zero-cost assertions.

    Asserting ``fee_usd == 0`` against ``fee_pct=0`` is tautological — it holds
    whether or not the rejection path bills. These rejection tests need a
    configuration where a bug WOULD charge, so that zero is evidence.
    """
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
    )


def _billing_config() -> PnLBacktestConfig:
    """Config with gas costs ENABLED — same reason as ``_billing_backtester``."""
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        token_funding=_pnl_token_funding(INITIAL_CASH),
        include_gas_costs=True,
    )


# =============================================================================
# Scope resolution helpers
# =============================================================================


class TestDeclaredLendingChains:
    def test_fluid_declares_arbitrum_and_base_only(self) -> None:
        declared = _declared_lending_chains("fluid")
        assert declared == frozenset({"arbitrum", "base"})

    def test_lending_alias_folds_to_connector(self) -> None:
        # The platform spec emits protocol="fluid_lending"; the gate must see
        # the same declaration through the lending-scoped alias.
        assert _declared_lending_chains("fluid_lending") == _declared_lending_chains("fluid")

    def test_no_matrix_protocol_returns_none(self) -> None:
        assert _declared_lending_chains("test_protocol") is None

    def test_rejection_names_protocol_and_chain(self) -> None:
        reason = _lending_chain_scope_rejection("fluid", "ethereum")
        assert reason is not None
        assert "protocol 'fluid' declares no lending support on chain 'ethereum'" in reason

    def test_declared_chain_passes(self) -> None:
        assert _lending_chain_scope_rejection("fluid", "arbitrum") is None

    def test_intents_times_chains_declaration_gates_the_majors(self) -> None:
        """Connectors without lending matrix rows (aave_v3, spark,
        compound_v3) declare exact lending intent coverage through
        supported_chains — the gate must derive scope from it, or the most-used
        lending protocols stay ungated."""
        spark = _declared_lending_chains("spark")
        assert spark is not None and "ethereum" in spark
        reason = _lending_chain_scope_rejection("spark", "arbitrum")
        assert reason is not None
        assert "protocol 'spark' declares no lending support on chain 'arbitrum'" in reason
        assert _lending_chain_scope_rejection("spark", "ethereum") is None
        aave = _declared_lending_chains("aave_v3")
        assert aave is not None and {"ethereum", "arbitrum", "base"} <= aave

    def test_typoed_declared_chain_fails_loudly(self) -> None:
        """Connector-declared chain names are our metadata: a typo must
        raise (strict resolve), never silently reject valid intents."""
        from unittest import mock

        from almanak.framework.backtesting.pnl import engine as engine_mod

        engine_mod._lending_chain_scope_rejection.cache_clear()
        try:
            with mock.patch.object(engine_mod, "_declared_lending_chains", return_value=frozenset({"not_a_chain"})):
                with pytest.raises(ValueError, match="not_a_chain"):
                    engine_mod._lending_chain_scope_rejection("typo_proto", "ethereum")
        finally:
            engine_mod._lending_chain_scope_rejection.cache_clear()

    def test_registry_clear_invalidates_scope_caches(self) -> None:
        """The on_clear hook must invalidate ALL THREE memoized gate decisions.

        Asserting only that ``_declared_lending_chains`` emptied is too weak: the
        two decision-level caches (``_lending_chain_scope_rejection`` and
        ``_lending_intent_support_rejection``) memoize registry-derived
        verdicts too, and dropping either ``cache_clear()`` call would leave the
        gate answering from a declaration that no longer exists — a stale
        REFUSAL strands a legitimate fill, a stale APPROVAL fabricates one.

        Verified behaviourally as well as structurally: after the registry is
        emptied the gate must actually CHANGE its answer, not merely report an
        empty cache.
        """
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.backtesting.pnl import engine as engine_mod

        # Populate all three caches with real decisions.
        assert _declared_lending_chains("fluid") == frozenset({"arbitrum", "base"})
        assert _lending_chain_scope_rejection("fluid", "ethereum") is not None
        assert engine_mod._lending_intent_support_rejection("aave_v3", "mantle", "BORROW") is not None
        for fn in (
            engine_mod._declared_lending_chains,
            engine_mod._lending_chain_scope_rejection,
            engine_mod._lending_intent_support_rejection,
        ):
            assert fn.cache_info().currsize > 0, fn.__name__

        try:
            CONNECTOR_REGISTRY.clear()

            # Structural: every cache the hook owns is emptied.
            for fn in (
                engine_mod._declared_lending_chains,
                engine_mod._lending_chain_scope_rejection,
                engine_mod._lending_intent_support_rejection,
            ):
                assert fn.cache_info().currsize == 0, f"{fn.__name__} kept a stale entry"

            # The registry re-populates lazily on the next get(), so the value
            # is legitimately recomputed rather than absent — the point is that
            # it is RE-DERIVED, not replayed.
            assert _declared_lending_chains("fluid") == frozenset({"arbitrum", "base"})
        finally:
            CONNECTOR_REGISTRY.clear()
            engine_mod._clear_lending_scope_caches()

    def test_a_changed_declaration_is_seen_only_after_invalidation(self) -> None:
        """The staleness test with teeth: does the gate CHANGE its answer?

        Empty caches are necessary but not sufficient — what matters is that a
        changed declaration reaches the gate. This pins both directions: while
        cached, the gate replays the old verdict (proving the cache is real and
        the test is not vacuous); after invalidation it reflects the new one.
        """
        from unittest import mock

        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.backtesting.pnl import engine as engine_mod

        engine_mod._clear_lending_scope_caches()
        assert _declared_lending_chains("fluid") == frozenset({"arbitrum", "base"})

        # Widen the default lending coverage while preserving Fluid's broader
        # SWAP override. This changes the derived answer while staying valid.
        real = CONNECTOR_REGISTRY.get("fluid")
        assert real is not None
        widened = replace(
            real,
            supported_chains=SupportedChainsSpec(
                chains=("arbitrum", "base", "optimism"),
                intent_overrides={"SWAP": ("arbitrum", "base", "ethereum", "polygon")},
            ),
        )
        try:
            with mock.patch.object(CONNECTOR_REGISTRY, "get", return_value=widened):
                # Still cached -> the OLD declaration is replayed.
                assert _declared_lending_chains("fluid") == frozenset({"arbitrum", "base"})
                engine_mod._clear_lending_scope_caches()
                # Invalidated -> the NEW declaration is picked up.
                assert _declared_lending_chains("fluid") == frozenset({"arbitrum", "base", "optimism"})
        finally:
            engine_mod._clear_lending_scope_caches()

    def test_chain_alias_resolves_before_comparison(self) -> None:
        # aave_v3 declares "ethereum"; the registry alias "mainnet" must not
        # spuriously reject.
        assert _lending_chain_scope_rejection("aave_v3", "mainnet") is None


# =============================================================================
# Engine lane: the observed defect shape and its control cases
# =============================================================================


class TestLendingChainScopeGate:
    @pytest.mark.asyncio
    async def test_supply_on_undeclared_chain_is_rejected(self) -> None:
        """Defect shape: fluid/ethereum SUPPLY -> typed rejection, no position, no accrual."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        # Exact intent support fires before the coarse union-of-lending gate.
        # Still the same refusal: no position, no cash moved, same rejection code.
        reason = trade.metadata["failure_reason"]
        assert "declares SUPPLY only on [arbitrum, base]" in reason
        assert "protocol 'fluid'" in reason
        assert "chain 'ethereum' is unsupported" in reason
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value
        # Rejected fills charge nothing.
        assert trade.fee_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_supply_alias_spelling_is_rejected_too(self) -> None:
        """protocol="fluid_lending" (platform-spec spelling) gates identically."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid_lending", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value

    @pytest.mark.asyncio
    async def test_borrow_on_undeclared_chain_is_rejected(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            BorrowIntent(
                protocol="fluid",
                collateral_token="USDC",
                collateral_amount=Decimal("0"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
            ),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value
        # The refusal must be legible, not just typed. Fluid declares no BORROW
        # intent at all, so exact intent support rejects it on every chain.
        reason = trade.metadata["failure_reason"]
        assert "protocol 'fluid'" in reason
        assert "chain 'ethereum'" in reason
        assert "does not declare the BORROW intent" in reason
        # Rejected fills charge nothing — a refusal that still bills is a loss.
        assert trade.fee_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_supply_on_declared_chain_fills(self) -> None:
        """fluid/arbitrum (DECLARED chain) keeps working exactly as today."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("arbitrum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].protocol == "fluid"
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        assert portfolio.trades[-1].success is True

    @pytest.mark.asyncio
    async def test_no_matrix_protocol_keeps_generic_behavior(self) -> None:
        """A duck-typed protocol with no lending matrix declaration still fills."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="test_protocol", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert len(portfolio.positions) == 1
        assert portfolio.trades[-1].success is True


# =============================================================================
# Loud global-surrogate APY fallback
# =============================================================================


class TestGlobalApyFallbackWarns:
    def test_undeclared_protocol_warns_once_and_returns_surrogate(self, caplog: pytest.LogCaptureFixture) -> None:
        from almanak.framework.backtesting.pnl.calculators.interest import (
            _GLOBAL_APY_FALLBACK_WARNED,
            InterestCalculator,
        )

        _GLOBAL_APY_FALLBACK_WARNED.discard(("supply", "totally_undeclared_venue"))
        calculator = InterestCalculator()
        with caplog.at_level(logging.WARNING):
            apy = calculator.get_supply_apy_for_protocol("totally_undeclared_venue")
            again = calculator.get_supply_apy_for_protocol("totally_undeclared_venue")

        assert apy == calculator.default_supply_apy
        assert again == apy
        fallback_warnings = [record for record in caplog.records if "totally_undeclared_venue" in record.getMessage()]
        assert len(fallback_warnings) == 1
        assert "fabricated" in fallback_warnings[0].getMessage()

    def test_declared_protocol_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calculator = InterestCalculator()
        with caplog.at_level(logging.WARNING):
            apy = calculator.get_supply_apy_for_protocol("aave_v3")

        assert apy == calculator.protocol_supply_apys["aave_v3"]
        assert not [record for record in caplog.records if "aave_v3" in record.getMessage()]


class TestPerIntentChainSupportGate:
    """A union-of-intents chain gate is too coarse for an intent override.

    Aave V3 keeps ``mantle`` in its default support because SUPPLY / WITHDRAW /
    REPAY still work there, so ``_lending_chain_scope_rejection`` passes and a
    backtest would fill a BORROW that cannot execute live and accrue a
    fabricated APY on it — the same failure the chain gate exists to prevent,
    one axis over.
    """

    def test_chain_gate_alone_does_not_catch_it(self) -> None:
        """Pins WHY the extra gate is needed; if this ever returns a rejection,
        the coarse gate started covering the case and this suite should be
        revisited rather than silently duplicating it."""
        from almanak.framework.backtesting.pnl.engine import _lending_chain_scope_rejection

        assert _lending_chain_scope_rejection("aave_v3", "mantle") is None

    def test_unsupported_intent_on_default_chain_is_rejected(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _lending_intent_support_rejection

        reason = _lending_intent_support_rejection("aave_v3", "mantle", "BORROW")
        assert reason is not None
        assert "BORROW" in reason and "mantle" in reason
        assert "unsupported" in reason

    def test_surviving_intents_on_the_same_chain_are_not_rejected(self) -> None:
        """Over-rejection would strand pre-existing borrowers' REPAY/WITHDRAW."""
        from almanak.framework.backtesting.pnl.engine import _lending_intent_support_rejection

        for intent in ("SUPPLY", "REPAY", "WITHDRAW"):
            assert _lending_intent_support_rejection("aave_v3", "mantle", intent) is None, intent

    def test_same_intent_on_other_chains_is_not_rejected(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _lending_intent_support_rejection

        for chain in ("ethereum", "arbitrum", "base"):
            assert _lending_intent_support_rejection("aave_v3", chain, "BORROW") is None, chain

    def test_undeclared_protocol_keeps_generic_behaviour(self) -> None:
        from almanak.framework.backtesting.pnl.engine import _lending_intent_support_rejection

        assert _lending_intent_support_rejection("totally_undeclared_venue", "mantle", "BORROW") is None

    def test_chain_alias_is_canonicalised_before_matching(self) -> None:
        """A caller passing a non-canonical spelling must not slip past the gate.

        The two rejections alone cannot fail for the reason they name: a gate
        that never canonicalised would read ``"MANTLE"`` as an unknown chain
        and reject it too, so ``is not None`` holds either way. The positive
        control is what distinguishes the two — ``"MAINNET"`` is an alias of
        ``ethereum`` (``ChainRegistry`` aliases ``eth``/``mainnet``), where
        BORROW *is* supported, so it passes only if resolution really happens.
        """
        from almanak.framework.backtesting.pnl.engine import _lending_intent_support_rejection

        assert _lending_intent_support_rejection("aave_v3", "MANTLE", "BORROW") is not None
        assert _lending_intent_support_rejection("aave_v3", "mantle", "borrow") is not None
        assert _lending_intent_support_rejection("aave_v3", "MAINNET", "BORROW") is None
        assert _lending_intent_support_rejection("aave_v3", "eth", "borrow") is None


class TestRejectedFillsChargeNothingWhenBillingIsEnabled:
    """Zero-cost assertions made non-tautological.

    The suite's default fixtures set ``fee_pct=0`` and ``include_gas_costs=False``,
    so asserting a refused fill cost nothing proves nothing — it would hold even
    if the rejection path billed. These run the same refusals with fees and gas
    genuinely enabled, where a billing bug WOULD produce a non-zero charge.
    """

    @pytest.mark.asyncio
    async def test_supply_rejection_is_free_with_fees_enabled(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _billing_backtester()._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            _market("ethereum"),
            TS,
            _billing_config(),
        )
        trade = portfolio.trades[-1]
        assert trade.success is False
        # Load-bearing: a successful fill under this same config burns gas (see
        # the control below), so zero here is evidence rather than a default.
        assert trade.gas_cost_usd == Decimal("0"), "a refused fill must not burn gas"
        # Weak by nature — the lending lane charges no fee_usd even on success —
        # kept only so a future lane that DOES charge cannot regress silently.
        assert trade.fee_usd == Decimal("0"), "a refused fill must not be billed"
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.positions == []

    @pytest.mark.asyncio
    async def test_borrow_rejection_is_free_with_fees_enabled(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _billing_backtester()._execute_intent(
            BorrowIntent(
                protocol="fluid",
                collateral_token="USDC",
                collateral_amount=Decimal("0"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
            ),
            portfolio,
            _market("ethereum"),
            TS,
            _billing_config(),
        )
        trade = portfolio.trades[-1]
        assert trade.success is False
        # Load-bearing: a successful fill under this same config burns gas (see
        # the control below), so zero here is evidence rather than a default.
        assert trade.gas_cost_usd == Decimal("0"), "a refused fill must not burn gas"
        # Weak by nature — the lending lane charges no fee_usd even on success —
        # kept only so a future lane that DOES charge cannot regress silently.
        assert trade.fee_usd == Decimal("0"), "a refused fill must not be billed"
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.positions == []

    @pytest.mark.asyncio
    async def test_the_billing_fixture_would_actually_charge(self) -> None:
        """Control: the same config DOES bill on a fill that succeeds.

        Without this, the two tests above could pass because the fixture never
        charges anything under any circumstances — which would make them
        tautological in a new way rather than fixing the old one.
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _billing_backtester()._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            _market("arbitrum"),  # DECLARED chain — this one fills
            TS,
            _billing_config(),
        )
        trade = portfolio.trades[-1]
        assert trade.success is True, "precondition: arbitrum is a declared lending chain"
        # GAS is what discriminates for the lending lane. This control was written
        # asserting fee_usd > 0 and FAILED, which is how we learned a successful
        # lending SUPPLY charges no fee_usd at all (fees apply to swaps) while it
        # does burn gas. So `fee_usd == 0` on a rejection is tautological however
        # the fixture is configured, and gas_cost_usd is the real evidence.
        assert trade.gas_cost_usd > Decimal("0"), (
            "fixture must actually charge gas, or the zero-gas assertions above are vacuous"
        )


class TestPerIntentExclusionIsWiredIntoExecution:
    """The exclusion gate must fire in `_execute_intent`, not just in helpers.

    aave_v3 keeps `mantle` in its lending chains (SUPPLY / WITHDRAW / REPAY all
    work there), so the COARSE chain-scope gate passes and only the
    per-`(intent, chain)` exclusion can refuse a BORROW. That makes this the one
    shape where a helper-level test proves nothing: if the execution path stopped
    consulting the exclusion gate, or mapped the intent to the wrong verb, a
    fabricated Aave BORROW on Mantle would fill and accrue interest while every
    other test in this file still passed.
    """

    @pytest.mark.asyncio
    async def test_aave_borrow_on_mantle_is_refused_by_exact_support(self) -> None:
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _billing_backtester()._execute_intent(
            BorrowIntent(
                protocol="aave_v3",
                collateral_token="USDC",
                collateral_amount=Decimal("0"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
            ),
            portfolio,
            _market("mantle"),
            TS,
            _billing_config(),
        )
        assert portfolio.positions == [], "a fabricated Aave BORROW must not open"
        assert portfolio.cash_usd == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.gas_cost_usd == Decimal("0")
        reason = trade.metadata["failure_reason"]
        assert "BORROW" in reason and "mantle" in reason
        assert "unsupported" in reason

    @pytest.mark.asyncio
    async def test_aave_supply_on_mantle_still_fills(self) -> None:
        """Control: the exclusion narrows BORROW only.

        Without this, the test above would also pass if the gate wrongly refused
        every Aave operation on Mantle — stranding SUPPLY / WITHDRAW / REPAY,
        which VIB-6111 deliberately kept working so pre-existing borrowers can
        still repay and exit.
        """
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _billing_backtester()._execute_intent(
            SupplyIntent(protocol="aave_v3", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            _market("mantle"),
            TS,
            _billing_config(),
        )
        trade = portfolio.trades[-1]
        assert trade.success is True, "SUPPLY on mantle must still work (VIB-6111)"
        assert portfolio.positions, "the supply position must be opened"


class TestExitPathsAreNotGated:
    """Exits must NOT fail closed on an undeclared chain — by design.

    The gate covers lending OPENS only (SUPPLY / BORROW). Refusing a WITHDRAW or
    REPAY would strand capital: a position opened before a chain was narrowed is
    still on-chain and its holder must be able to unwind it. VIB-6111 keeps
    REPAY on Aave/Mantle for exactly this reason, and teardown's residual sweep
    is deliberately left un-narrowed on the same principle — advertisement
    narrows, safety does not.

    This test exists so that a future "consistency" change which extends the
    gate to exits fails loudly instead of silently trapping users.
    """

    @pytest.mark.asyncio
    async def test_withdraw_on_undeclared_chain_is_not_refused_by_this_gate(self) -> None:
        from almanak.framework.intents.lending_intents import WithdrawIntent

        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        await _backtester()._execute_intent(
            WithdrawIntent(protocol="fluid", token="USDC", amount=Decimal("100")),
            portfolio,
            _market("ethereum"),
            TS,
            _config(),
        )
        trade = portfolio.trades[-1]
        assert trade.metadata.get("rejection_code") != RejectionCode.UNDECLARED_LENDING_CHAIN.value, (
            "exits must not be refused by the lending chain-scope gate — that would "
            "strand a position opened before the chain was narrowed"
        )
