"""Loop-parity meta-test: the fixture factory can never drift from the engine.

Runs the REAL PnL engine loop (preflight -> initialize -> iteration, the same
sequence ``PnLBacktester._run_backtest`` drives) over synthetic data, then
asserts the loop-produced portfolio key shapes equal the factory's. If the
engine's key discipline changes and ``engine_plane`` doesn't, THIS fails —
by design. That is the whole point: ALM-2960 survived a large unit suite
because fixtures were hand-built in a symbol-keyed world the engine had left;
this test makes that drift a visible failure instead of a silent one.

This file intentionally reaches into ``_engine_helpers`` internals: it is the
one place allowed to, so every other unit test can use the factory instead.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.data_provider import normalize_token_key
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.logging_utils import BacktestLogger
from almanak.framework.backtesting.pnl.position_models import SimulatedFill
from tests.unit.backtesting.support.engine_plane import (
    DEFAULT_TOKEN_ADDRESSES,
    START,
    USDC_ARBITRUM,
    WETH_ARBITRUM,
    make_run_market_state,
    make_run_portfolio,
)
from tests.validation.backtesting.trust_matrix import (
    TICK_SECONDS,
    ScriptedStrategy,
    SwapDuck,
    SyntheticPriceProvider,
    flat_series,
)

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig


def _run_real_loop(intents: list, hours: int):
    """Drive the real engine sequence and return the live BacktestState."""
    price_series = flat_series(hours + 3)
    provider_series = dict(price_series)
    provider_series.setdefault(("arbitrum", USDC_ARBITRUM), [Decimal("1")] * (hours + 3))
    config = PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=hours),
        interval_seconds=TICK_SECONDS,
        token_funding=[
            {
                "symbol": "USDC",
                "address": USDC_ARBITRUM,
                "chain": "arbitrum",
                "amount": "10000",
                "amount_type": "token",
            }
        ],
        tokens=list(provider_series),
        include_gas_costs=False,
        inclusion_delay_blocks=0,
    )
    provider = SyntheticPriceProvider(provider_series)
    # Production ingress attaches the run's identity map on the provider
    # (CoinGeckoDataProvider(token_addresses=...) in backtest_runner); the
    # engine reads it back via _registered_token_addresses.
    provider._token_addresses = dict(DEFAULT_TOKEN_ADDRESSES)
    backtester = PnLBacktester(
        data_provider=provider,
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )
    strategy = ScriptedStrategy(intents)
    bt_logger = BacktestLogger(backtest_id="engine-plane-parity", json_format=False, logger=logging.getLogger(__name__))
    backtester._current_backtest_id = "engine-plane-parity"

    async def _drive():
        await _engine_helpers.run_preflight(
            backtester=backtester, config=config, bt_logger=bt_logger, strategy=strategy
        )
        state = _engine_helpers.initialize_backtest(
            backtester=backtester, strategy=strategy, config=config, bt_logger=bt_logger
        )
        await _engine_helpers.execute_iteration_loop(
            backtester=backtester, strategy=strategy, config=config, bt_logger=bt_logger, state=state
        )
        return state

    try:
        return asyncio.run(_drive())
    finally:
        asyncio.run(backtester.close())


class TestSeedPlaneParity:
    def test_factory_seed_keys_match_real_loop(self) -> None:
        state = _run_real_loop([None, None], hours=2)
        factory = make_run_portfolio()

        assert set(factory.tokens) == set(state.portfolio.tokens)
        assert set(factory._cost_basis) == set(state.portfolio._cost_basis)
        assert factory.initial_capital_usd == state.portfolio.initial_capital_usd
        # The funded balance is address-native in BOTH worlds — a bare-symbol
        # key on either side is the drift this test exists to catch.
        usdc_key = normalize_token_key("arbitrum", USDC_ARBITRUM)
        assert usdc_key in state.portfolio.tokens
        assert usdc_key in factory.tokens
        assert "USDC" not in state.portfolio.tokens
        assert "USDC" not in factory.tokens

    def test_factory_identity_registrations_match_real_loop(self) -> None:
        state = _run_real_loop([None, None], hours=2)
        factory = make_run_portfolio()
        loop_identities = {sym: ident.key for sym, ident in state.portfolio._identity_table.items()}
        factory_identities = {sym: ident.key for sym, ident in factory._identity_table.items()}
        # The loop registers every provider-registered token; the factory
        # registers its declared map. Parity on the shared symbols is the
        # contract (the factory's map is a subset by construction).
        for symbol, key in factory_identities.items():
            assert loop_identities.get(symbol) == key, (symbol, key, loop_identities)
        assert "USDC" in factory_identities and "WETH" in factory_identities


class TestCreditPlaneParity:
    def test_loop_credit_key_matches_factory_prediction(self) -> None:
        # A swap executed by the REAL loop credits WETH under some key; the
        # factory-built portfolio must predict exactly that key. This is the
        # 2960 class as a permanent parity assertion.
        state = _run_real_loop([SwapDuck(amount_usd=Decimal("1000")), None, None], hours=3)
        swaps = [t for t in state.portfolio.trades if t.intent_type == IntentType.SWAP and t.success]
        assert swaps, "parity harness bug: scripted swap did not fill"

        weth_key = normalize_token_key("arbitrum", WETH_ARBITRUM)
        assert state.portfolio.tokens.get(weth_key, Decimal("0")) > 0
        assert "WETH" not in state.portfolio.tokens

        factory = make_run_portfolio()
        assert factory._resolve_key("WETH") == weth_key

    def test_factory_round_trip_lands_on_one_key(self) -> None:
        # The audit's pre-fix demonstration, kept as a regression pin: a
        # symbol-shaped credit applied to a factory portfolio must land on the
        # address-native key — never open a second, parallel symbol balance.
        factory = make_run_portfolio()
        market_state = make_run_market_state()
        fill = SimulatedFill(
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            intent_type=IntentType.SWAP,
            protocol="uniswap_v3",
            tokens=["USDC", "WETH"],
            executed_price=Decimal("2000"),
            amount_usd=Decimal("1000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={"WETH": Decimal("0.5")},
            tokens_out={"USDC": Decimal("1000")},
        )
        assert factory.apply_fill(fill, market_state=market_state)

        weth_key = normalize_token_key("arbitrum", WETH_ARBITRUM)
        assert factory.tokens.get(weth_key) == Decimal("0.5")
        assert "WETH" not in factory.tokens
        weth_keys = [k for k in factory.tokens if k == weth_key or k == "WETH"]
        assert len(weth_keys) == 1
