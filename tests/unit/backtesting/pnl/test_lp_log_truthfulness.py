"""Log truthfulness for adapter-lane LP fills.

The LP adapter builds a fill and logs it BEFORE portfolio.apply_fill runs the
cash check, so a cash-rejected LP_OPEN used to emit "LP_OPEN executed" and
then "Rejected LP_OPEN fill" for the same intent — observed on staging: an
"executed" line for a trade that never happened (accounting was correct
throughout; only the log lied).

Contract pinned here:
- Adapter lines say "fill simulated (pending portfolio acceptance)" — they
  may fire for fills that later reject.
- The ONLY line entitled to say "Executed intent" is the engine's
  post-acceptance log, and it fires exactly once per accepted fill.
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.adapters.lp_adapter import LPBacktestAdapter
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.intents.vocabulary import LPOpenIntent
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2026, 4, 21, 2, 0, tzinfo=UTC)
SEED = Decimal("5")


def market(ts: datetime) -> MarketState:
    return MarketState(
        timestamp=ts,
        prices={"CBETH": Decimal("1730"), "WETH": Decimal("1600"), "USDC": Decimal("1")},
        chain="base",
    )


def _backtester() -> PnLBacktester:
    bt = PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )
    adapter = LPBacktestAdapter()
    # Shadow the bound method so _prewarm_after_open's getattr sees None and
    # skips the (network-touching) history prewarm.
    adapter.prewarm_history = None  # type: ignore[assignment]
    bt._adapter = adapter
    return bt


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=6),
        token_funding=_pnl_token_funding(SEED),
        chain="base",
        include_gas_costs=False,
    )


def open_intent() -> LPOpenIntent:
    # ~$4.9994 notional against the $5 seed: an all-in open whose second
    # attempt must reject on cash.
    return LPOpenIntent(
        pool="CBETH/WETH",
        amount0=Decimal("0.001445"),
        amount1=Decimal("0.00156222"),
        range_lower=Decimal("0.95"),
        range_upper=Decimal("1.20"),
        protocol="aerodrome_slipstream",
        chain="base",
    )


@pytest.mark.asyncio
async def test_rejected_lp_open_never_claims_executed(caplog) -> None:
    backtester = _backtester()
    portfolio = SimulatedPortfolio(initial_capital_usd=SEED, chain="base")

    with caplog.at_level(logging.INFO):
        rec1 = await backtester._execute_intent(open_intent(), portfolio, market(TS), TS, _config())
        ts2 = TS + timedelta(hours=1)
        rec2 = await backtester._execute_intent(open_intent(), portfolio, market(ts2), ts2, _config())

    assert rec1.success is True
    assert rec2.success is False
    assert "insufficient cash" in rec2.metadata.get("failure_reason", "")
    assert len(portfolio.positions) == 1

    messages = [r.getMessage() for r in caplog.records]
    # No line may claim execution on this path: the direct-execute lane has
    # no post-acceptance logger (the pending lane's
    # _log_pending_trade_outcome owns "Executed intent", pinned below), and
    # the legacy pre-acceptance "LP_OPEN executed" wording must be gone.
    assert not [m for m in messages if "executed" in m.lower() and "LP_OPEN" in m]
    # The adapter's per-fill detail line survives, truthfully labeled, and
    # fires for both attempts (it is a simulation log, not an execution log).
    pending_lines = [m for m in messages if "LP_OPEN fill simulated (pending portfolio acceptance)" in m]
    assert len(pending_lines) == 2


@pytest.mark.asyncio
async def test_lp_close_log_is_pending_labeled(caplog) -> None:
    backtester = _backtester()
    portfolio = SimulatedPortfolio(initial_capital_usd=Decimal("20"), chain="base")

    rec_open = await backtester._execute_intent(open_intent(), portfolio, market(TS), TS, _config())
    assert rec_open.success is True
    position_id = portfolio.positions[0].position_id

    from almanak.framework.intents.vocabulary import LPCloseIntent

    ts2 = TS + timedelta(hours=1)
    with caplog.at_level(logging.INFO):
        rec_close = await backtester._execute_intent(
            LPCloseIntent(position_id=position_id, protocol="aerodrome_slipstream", chain="base"),
            portfolio,
            market(ts2),
            ts2,
            _config(),
        )

    assert rec_close.success is True
    messages = [r.getMessage() for r in caplog.records]
    assert not [m for m in messages if "LP_CLOSE executed" in m]
    assert [m for m in messages if "LP_CLOSE fill simulated (pending portfolio acceptance)" in m]


def test_pending_outcome_success_logs_executed_at_info(caplog) -> None:
    """The pending lane's post-acceptance log is the ONE authoritative
    "Executed intent" line, and it must be INFO (it was DEBUG, so runs showed
    the adapter's pre-acceptance line as the only "executed" signal)."""
    from almanak.framework.backtesting.models import IntentType
    from almanak.framework.backtesting.pnl.engine import TradeRecord

    backtester = _backtester()
    record = TradeRecord(
        timestamp=TS,
        intent_type=IntentType.LP_OPEN,
        protocol="aerodrome_slipstream",
        tokens=["CBETH", "WETH"],
        amount_usd=Decimal("5"),
        fee_usd=Decimal("0"),
        slippage_usd=Decimal("0"),
        gas_cost_usd=Decimal("0"),
        executed_price=Decimal("1"),
        pnl_usd=None,
        success=True,
    )
    with caplog.at_level(logging.INFO):
        backtester._log_pending_trade_outcome(record, TS - timedelta(hours=2), TS)

    executed = [r for r in caplog.records if "Executed intent" in r.getMessage()]
    assert len(executed) == 1
    assert executed[0].levelno == logging.INFO
    assert "type=LP_OPEN" in executed[0].getMessage()
