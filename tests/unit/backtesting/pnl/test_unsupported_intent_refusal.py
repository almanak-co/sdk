"""Unsupported intents are refused outright by the PnL engine.

Design decision (2026-07-02): an intent type outside the simulated envelope
is a fatal, run-stopping error — never a costed no-op. Before this change the
generic lane recorded a trade with fees/slippage/gas for ANY intent type while
moving zero tokens and creating no position, silently diverging the backtest
from live behaviour (~15 vocabulary intent types were affected, e.g.
BRIDGE, STAKE, PERP_CANCEL_ORDER; WRAP_NATIVE/UNWRAP_NATIVE, LP_COLLECT_FEES
and DELEVERAGE have since gained real simulation lanes and left this set).
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest

from almanak.core.intent_types import IntentType
from almanak.framework.backtesting.exceptions import UnsupportedIntentError
from almanak.framework.backtesting.pnl._engine_helpers import (
    _SIMPLE_FLOW_HANDLERS,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.error_handling import classify_error
from almanak.framework.backtesting.pnl.intent_support import (
    BACKTEST_INTENT_DISPOSITIONS,
    GENERIC_SIMULATED_INTENT_TYPES,
    BacktestIntentDisposition,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding


class _EmptyDataProvider:
    provider_name = "mock_empty"

    async def iterate(self, config):  # pragma: no cover - direct engine helper tests only
        if False:
            yield


class _StakeDuck:
    """Canonical intent type with no simulation lane."""

    intent_type = "STAKE"
    token = "ETH"
    amount = Decimal("1")


class _BridgeDuck:
    """BRIDGE is in the backtesting IntentType enum but has no simulation lane."""

    intent_type = "BRIDGE"
    from_chain = "ethereum"
    to_chain = "arbitrum"
    token = "USDC"
    amount_usd = Decimal("100")


class _UnrecognizedDuck:
    """Intent integration value that is not in the canonical vocabulary."""

    intent_type = "NOT_A_REAL_TYPE"


class _SwapDuck:
    intent_type = "SWAP"
    from_token = "USDC"
    to_token = "WETH"
    amount_usd = Decimal("100")
    protocol = "uniswap_v3"


def _engine() -> PnLBacktester:
    return PnLBacktester(
        data_provider=_EmptyDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 2),
        token_funding=_pnl_token_funding(Decimal("10000"), chain="ethereum"),
        chain="ethereum",
        tokens=["WETH", "USDC"],
        include_gas_costs=False,
    )


def _market_state() -> MarketState:
    return MarketState(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        chain="ethereum",
        prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
    )


class TestUnsupportedIntentRefusal:
    """The generic lane refuses what it cannot simulate."""

    @pytest.mark.asyncio
    async def test_unknown_intent_type_is_refused(self):
        """A STAKE-style intent (UNKNOWN to the engine) raises, names the intent."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()

        with pytest.raises(UnsupportedIntentError, match=r"STAKE \(_StakeDuck\)"):
            await engine._execute_intent(_StakeDuck(), portfolio, state, state.timestamp, _config())

    @pytest.mark.asyncio
    async def test_bridge_intent_is_refused(self):
        """BRIDGE exists in the enum but has no lane — refused, not a $0 trade."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()

        with pytest.raises(UnsupportedIntentError, match="BRIDGE"):
            await engine._execute_intent(_BridgeDuck(), portfolio, state, state.timestamp, _config())

    @pytest.mark.asyncio
    async def test_unrecognized_intent_is_refused_before_sizing_or_adapter_dispatch(self, monkeypatch):
        """Unknown integration values preserve the public refusal contract at ingress."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()
        resolve_all_sizing = Mock(side_effect=AssertionError("sizing must not run"))
        adapter_dispatch = AsyncMock(side_effect=AssertionError("adapter dispatch must not run"))
        monkeypatch.setattr(
            "almanak.framework.backtesting.pnl.sizing.resolve_all_sizing",
            resolve_all_sizing,
        )
        monkeypatch.setattr(engine, "_execute_with_adapter_if_available", adapter_dispatch)

        with pytest.raises(UnsupportedIntentError, match="NOT_A_REAL_TYPE"):
            await engine._execute_intent(_UnrecognizedDuck(), portfolio, state, state.timestamp, _config())

        resolve_all_sizing.assert_not_called()
        adapter_dispatch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_intent_list_is_refused_with_multi_intent_hint(self):
        """A bare list[Intent] from decide() is refused loudly with the VIB-5094 hint."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()

        with pytest.raises(UnsupportedIntentError, match="single intent per tick"):
            await engine._execute_intent([_SwapDuck(), _SwapDuck()], portfolio, state, state.timestamp, _config())

    @pytest.mark.asyncio
    async def test_refusal_mutates_no_portfolio_state(self):
        """Refusal happens before cost/flow computation: zero state mutation."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()
        cash_before = portfolio.cash_usd
        tokens_before = dict(portfolio.tokens)

        with pytest.raises(UnsupportedIntentError):
            await engine._execute_intent(_StakeDuck(), portfolio, state, state.timestamp, _config())

        assert portfolio.cash_usd == cash_before
        assert portfolio.tokens == tokens_before
        assert portfolio.positions == []
        assert portfolio.trades == []

    @pytest.mark.asyncio
    async def test_supported_intent_still_executes(self):
        """The refusal gate does not touch envelope intents: SWAP still trades."""
        engine = _engine()
        portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("10000"), chain="ethereum")
        state = _market_state()

        record = await engine._execute_intent(_SwapDuck(), portfolio, state, state.timestamp, _config())

        assert record.success


class TestEnvelopeDefinition:
    """The simulated envelope is explicit, not implied by dict keys."""

    def test_envelope_is_flow_handlers_plus_lifecycle_types(self):
        assert GENERIC_SIMULATED_INTENT_TYPES == frozenset(_SIMPLE_FLOW_HANDLERS) | {
            IntentType.SWAP,
            IntentType.HOLD,
            IntentType.PERP_OPEN,
            IntentType.PERP_CLOSE,
        }

    def test_bridge_is_explicitly_refused_outside_the_envelope(self):
        assert IntentType.BRIDGE not in GENERIC_SIMULATED_INTENT_TYPES
        assert BACKTEST_INTENT_DISPOSITIONS[IntentType.BRIDGE] is BacktestIntentDisposition.REFUSED

    def test_every_canonical_intent_has_an_explicit_disposition(self):
        assert set(BACKTEST_INTENT_DISPOSITIONS) == set(IntentType)

    def test_refused_intents_are_an_explicit_reviewed_set(self):
        refused = {
            intent_type
            for intent_type, disposition in BACKTEST_INTENT_DISPOSITIONS.items()
            if disposition is BacktestIntentDisposition.REFUSED
        }
        assert refused == {
            IntentType.PERP_CANCEL_ORDER,
            IntentType.PERP_WITHDRAW,
            IntentType.BRIDGE,
            IntentType.ENSURE_BALANCE,
            IntentType.FLASH_LOAN,
            IntentType.STAKE,
            IntentType.UNSTAKE,
            IntentType.PREDICTION_BUY,
            IntentType.PREDICTION_SELL,
            IntentType.PREDICTION_REDEEM,
            IntentType.VAULT_REALLOCATE,
            IntentType.VAULT_MANAGE,
        }

    def test_connectorless_placeholders_are_explicitly_not_applicable(self):
        placeholders = {
            intent_type
            for intent_type, disposition in BACKTEST_INTENT_DISPOSITIONS.items()
            if disposition is BacktestIntentDisposition.PLACEHOLDER_NOT_APPLICABLE
        }
        assert placeholders == {
            IntentType.LIQUIDATE,
            IntentType.OPEN_CDP,
            IntentType.MINT_STABLE,
            IntentType.REPAY_STABLE,
            IntentType.CLOSE_CDP,
        }


class TestClassification:
    """UnsupportedIntentError is fatal: the run stops, it is never retried."""

    def test_classified_fatal(self):
        classification = classify_error(UnsupportedIntentError("STAKE (StakeDuck)", ("SWAP",)))
        assert classification.is_fatal
        assert not classification.is_recoverable
