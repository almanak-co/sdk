"""decide() sees one consistent post-fill wallet, by address AND by symbol.

Two regressions on the per-tick snapshot the PnL engine hands to ``decide()``:

ALM-3394 — the snapshot seeded wallet balances EAGERLY before the tick's
    pending fills ran, while position reads (``lp_position_value``) are LAZY
    over the live portfolio. On the tick an LP_OPEN landed, decide() saw the
    PRE-fill wallet next to the POST-fill position: 2 WETH in the wallet AND
    1.25 WETH inside the LP — capital counted twice for one tick.

ALM-3398 — the symbol alias map fed the snapshot from provider / caller
    registrations only, never from ``token_funding``. Address-keyed funding
    seeded under the address key, so ``balance("WETH")`` raised ``Cannot
    determine balance`` and — worse — the cash lane wrote ``balance("USDC")``
    as ``cash_usd``, a MEASURED ``0`` shadowing 10,000 funded USDC
    (Empty != Zero).

Both tests drive the real ``PnLBacktester`` over a flat-price provider with a
single LP_OPEN on the first decide().
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.initial_portfolio import TokenFundingInitializationError

ARB_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
ARB_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
START = datetime(2024, 1, 1, tzinfo=UTC)
NUM_TICKS = 5


class _FlatProvider:
    """Symbol-keyed constant prices; no provider-side token-address map."""

    provider_name = "flat"

    async def iterate(self, config: Any):
        for i in range(NUM_TICKS):
            timestamp = START + timedelta(hours=i)
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
                    chain="arbitrum",
                    block_number=1_000_000 + i,
                    gas_price_gwei=Decimal("30"),
                ),
            )


@dataclass
class _LPOpenDuck:
    intent_type: str = "LP_OPEN"
    token0: str = "WETH"
    token1: str = "USDC"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "uniswap_v3"
    tick_lower: int = -887272
    tick_upper: int = 887272
    fee_tier: Decimal = Decimal("0.003")


@dataclass
class _Observation:
    tick: int
    weth: Any
    usdc: Any
    lp_weth: Decimal | None


class _LPOnceStrategy:
    """Opens one full-range LP on the first decide(); records what decide() sees."""

    deployment_id = "snapshot_seeding_regression"

    def __init__(self, weth_key: str, usdc_key: str) -> None:
        self._weth_key = weth_key
        self._usdc_key = usdc_key
        self.observations: list[_Observation] = []
        self.position_id: str | None = None
        self.balance_errors: list[Exception] = []

    def _balance(self, market: Any, key: str) -> Any:
        try:
            return market.balance(key).balance
        except Exception as exc:  # noqa: BLE001 — the test asserts on the refusal shape
            self.balance_errors.append(exc)
            return exc

    def decide(self, market: Any) -> Any:
        tick = len(self.observations) + 1
        weth = self._balance(market, self._weth_key)
        usdc = self._balance(market, self._usdc_key)
        lp_weth = None
        if self.position_id is not None:
            lp_weth = market.lp_position_value(self.position_id, protocol="uniswap_v3").amount0
        self.observations.append(_Observation(tick=tick, weth=weth, usdc=usdc, lp_weth=lp_weth))
        return _LPOpenDuck() if tick == 1 else None

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        if success and getattr(result, "position_id", None) is not None:
            self.position_id = str(result.position_id)

    def get_metadata(self) -> None:
        return None


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=START,
        end_time=START + timedelta(hours=NUM_TICKS),
        interval_seconds=3600,
        tokens=["WETH", "USDC"],
        chain="arbitrum",
        include_gas_costs=False,
        inclusion_delay_blocks=0,
        token_funding=[
            {"symbol": "WETH", "address": ARB_WETH, "chain": "arbitrum", "amount": "2", "amount_type": "token"},
            {"symbol": "USDC", "address": ARB_USDC, "chain": "arbitrum", "amount": "10000", "amount_type": "token"},
        ],
    )


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=_FlatProvider(),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


def _first_lp_tick(strategy: _LPOnceStrategy) -> _Observation:
    lp_ticks = [obs for obs in strategy.observations if obs.lp_weth is not None]
    assert lp_ticks, "the LP_OPEN never became visible to decide()"
    return lp_ticks[0]


async def _run(strategy: _LPOnceStrategy) -> Any:
    result = await _backtester().backtest(strategy, _config())
    assert result.success, result.error
    assert strategy.position_id is not None, "LP_OPEN did not fill"
    assert len(strategy.observations) == NUM_TICKS
    return result


class TestFillsPrecedeSnapshotSeeding:
    """ALM-3394: the tick the LP first appears already shows the debited wallet."""

    @pytest.mark.asyncio
    async def test_decide_sees_post_fill_wallet_on_lp_appearance_tick(self):
        strategy = _LPOnceStrategy(ARB_WETH, ARB_USDC)
        await _run(strategy)

        pre_fill = strategy.observations[0]
        assert (pre_fill.weth, pre_fill.usdc, pre_fill.lp_weth) == (Decimal("2"), Decimal("10000"), None)

        first_lp = _first_lp_tick(strategy)
        # $5,000 full-range LP at WETH=$2,000: 1.25 WETH + 2,500 USDC leave
        # the wallet ON the tick the position becomes readable — never a
        # 2-WETH wallet beside a 1.25-WETH position.
        assert first_lp.lp_weth == pytest.approx(Decimal("1.25"))
        assert first_lp.weth == pytest.approx(Decimal("2") - first_lp.lp_weth)
        assert first_lp.usdc < Decimal("10000")
        assert first_lp.weth < Decimal("2")

    @pytest.mark.asyncio
    async def test_wallet_plus_lp_never_exceeds_funded_capital(self):
        strategy = _LPOnceStrategy(ARB_WETH, ARB_USDC)
        await _run(strategy)

        for obs in strategy.observations:
            inside_lp = obs.lp_weth or Decimal("0")
            assert obs.weth + inside_lp <= Decimal("2"), f"tick {obs.tick}: WETH double counted ({obs})"


class TestFundedSymbolsResolve:
    """ALM-3398: symbol reads resolve to the address-native funded balances."""

    @pytest.mark.asyncio
    async def test_symbol_reads_serve_funded_balances_pre_fill(self, caplog):
        strategy = _LPOnceStrategy("WETH", "USDC")
        with caplog.at_level(logging.WARNING, logger="almanak.framework.backtesting.pnl.engine"):
            result = await _run(strategy)

        assert strategy.balance_errors == []
        pre_fill = strategy.observations[0]
        # 2 / 10,000, not a refusal and NEVER a measured 0 for USDC.
        assert (pre_fill.weth, pre_fill.usdc) == (Decimal("2"), Decimal("10000"))

        first_lp = _first_lp_tick(strategy)
        assert first_lp.weth == pytest.approx(Decimal("2") - first_lp.lp_weth)
        assert first_lp.usdc < Decimal("10000")

        assert not any("STARVED" in record.message for record in caplog.records)
        balance_failures = [entry for entry in (result.decision_input_failures or []) if entry["source"] == "balance"]
        assert balance_failures == []

    @pytest.mark.asyncio
    async def test_symbol_and_address_reads_agree_every_tick(self):
        by_symbol = _LPOnceStrategy("WETH", "USDC")
        by_address = _LPOnceStrategy(ARB_WETH, ARB_USDC)
        await _run(by_symbol)
        await _run(by_address)

        assert [(o.weth, o.usdc, o.lp_weth) for o in by_symbol.observations] == [
            (o.weth, o.usdc, o.lp_weth) for o in by_address.observations
        ]


class TestFundingTokenAddressMap:
    """The funded basket joins the run's identity plane; contradictions refuse."""

    # Imported per test (not at module level) so the engine-level regressions
    # above still collect — and FAIL — on a checkout that lacks the fix.
    @staticmethod
    def _map(*args: Any, **kwargs: Any) -> dict[str, tuple[str, str]]:
        from almanak.framework.backtesting.pnl._engine_helpers import _funding_token_addresses

        return _funding_token_addresses(*args, **kwargs)

    def _entries(self, usdc_address: str = ARB_USDC) -> list[dict[str, Any]]:
        return [
            {"symbol": "WETH", "address": ARB_WETH, "chain": "arbitrum", "amount": "2", "amount_type": "token"},
            {"symbol": "USDC", "address": usdc_address, "chain": "arbitrum", "amount": "1", "amount_type": "token"},
        ]

    def test_funded_symbols_map_to_their_addresses(self):
        funded = self._map(self._entries(), chain="arbitrum", registered={})
        assert funded == {"WETH": ("arbitrum", ARB_WETH), "USDC": ("arbitrum", ARB_USDC)}

    def test_agreeing_registration_is_accepted(self):
        funded = self._map(
            self._entries(),
            chain="arbitrum",
            registered={"USDC": ("arbitrum", ARB_USDC.upper())},
        )
        assert funded["USDC"] == ("arbitrum", ARB_USDC)

    def test_foreign_chain_registration_does_not_conflict(self):
        eth_usdc = ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        funded = self._map(self._entries(), chain="arbitrum", registered={"USDC": eth_usdc})
        assert funded["USDC"] == ("arbitrum", ARB_USDC)

    def test_same_chain_disagreement_refuses_at_init(self):
        bridged = "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"
        with pytest.raises(TokenFundingInitializationError, match="USDC"):
            self._map(self._entries(), chain="arbitrum", registered={"USDC": ("arbitrum", bridged)})

    def test_duplicate_funded_symbol_with_two_addresses_refuses(self):
        bridged = "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"
        entries = self._entries() + [
            {"symbol": "USDC", "address": bridged, "chain": "arbitrum", "amount": "1", "amount_type": "token"}
        ]
        with pytest.raises(TokenFundingInitializationError, match="USDC"):
            self._map(entries, chain="arbitrum", registered={})

    def test_engine_registers_funded_symbols_even_without_provider_map(self):
        backtester = _backtester()
        assert backtester._registered_token_addresses() == {}
        backtester._funding_token_addresses = self._map(self._entries(), chain="arbitrum", registered={})
        assert backtester._registered_token_addresses() == {
            "WETH": ("arbitrum", ARB_WETH),
            "USDC": ("arbitrum", ARB_USDC),
        }
