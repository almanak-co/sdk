"""Regression tests for VIB-3738 / VIB-3739 — teardown verification on demo strategies.

These tests reproduce the exact failure mode reported by QA (and confirmed by
static analysis): `get_open_positions()` returning a non-empty summary after a
clean teardown succeeds on-chain. The framework then logs "positions still open"
and refuses to mark teardown complete.

Each test exercises the strategy through the same lifecycle the framework drives:
  1. Strategy opens a position.
  2. Teardown intents run, calling `on_intent_executed` on success (or the
     wallet ends up flat in lido's case).
  3. Framework re-queries `get_open_positions()` for verification.
  4. Expected: positions list is empty.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.demo_strategies.balancer_flash_arb.strategy import BalancerFlashArbStrategy
from almanak.demo_strategies.lido_staker.strategy import LidoStakerStrategy
from almanak.demo_strategies.morpho_univ3_leveraged_lp.strategy import (
    MorphoUniV3LeveragedLPStrategy,
)
from almanak.framework.intents.vocabulary import (
    IntentType,
)

# ---------------------------------------------------------------------------
# Helpers — bypass IntentStrategy.__init__ so tests can exercise pure logic
# without booting a gateway.
# ---------------------------------------------------------------------------


def _make_morpho_strategy() -> MorphoUniV3LeveragedLPStrategy:
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = MorphoUniV3LeveragedLPStrategy.__new__(MorphoUniV3LeveragedLPStrategy)
        s._strategy_id = "test_morpho"
        s.STRATEGY_NAME = "demo_morpho_univ3_leveraged_lp"
        s.name = "demo_morpho_univ3_leveraged_lp"
        s._chain = "ethereum"
        s.market_id = "0x" + "ab" * 32
        s.collateral_token = "wstETH"
        s.borrow_token = "USDC"
        s.collateral_amount = Decimal("0.014")
        s.target_ltv = Decimal("0.50")
        s.min_health_factor = Decimal("1.5")
        s.lp_pool = "WETH/USDC/500"
        s.lp_range_width_pct = Decimal("0.20")
        s.swap_slippage = Decimal("0.005")
        s._state = "active"
        s._collateral_supplied = Decimal("0.014")
        s._borrowed_amount = Decimal("26.60")
        s._lp_position_id = 12345
        s._force_action = ""
        s.create_market_snapshot = MagicMock(return_value=MagicMock(price=lambda _t: Decimal("3800")))
        return s


def _make_balancer_strategy() -> BalancerFlashArbStrategy:
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = BalancerFlashArbStrategy.__new__(BalancerFlashArbStrategy)
        s._strategy_id = "test_balancer"
        s.STRATEGY_NAME = "demo_balancer_flash_arb"
        s.name = "demo_balancer_flash_arb"
        s._chain = "arbitrum"
        s.flash_loan_amount_usd = Decimal("1000")
        s.max_slippage_pct = 1.0
        s.base_token = "WETH"
        s.quote_token = "USDC"
        s.teardown_protocol = "uniswap_v3"
        s.force_action = "swap"
        s._trades_executed = 1
        s._fell_back_to_swap = False
        return s


def _make_lido_strategy(receive_wrapped: bool = True) -> LidoStakerStrategy:
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        s = LidoStakerStrategy.__new__(LidoStakerStrategy)
        s._strategy_id = "test_lido"
        s.STRATEGY_NAME = "lido_staker"
        s.name = "lido_staker"
        s._chain = "ethereum"
        s.min_stake_amount = Decimal("0.1")
        s.gas_reserve = Decimal("0.01")
        s.receive_wrapped = receive_wrapped
        s.force_action = ""
        s._staked = True
        s._staked_amount = Decimal("1.0")
        return s


def _balance_obj(amount: Decimal | str | int, usd: Decimal | str | int = "0"):
    obj = MagicMock()
    obj.balance = Decimal(str(amount))
    obj.balance_usd = Decimal(str(usd))
    return obj


# ---------------------------------------------------------------------------
# VIB-3738: morpho_univ3_leveraged_lp — teardown must clear cached counters
# ---------------------------------------------------------------------------


class TestMorphoTeardownVerification:
    """get_open_positions() returns empty after the unwind intents succeed."""

    def test_lp_close_clears_cached_position_id(self):
        s = _make_morpho_strategy()
        assert s._lp_position_id == 12345

        intent = MagicMock()
        intent.intent_type = IntentType.LP_CLOSE
        s.on_intent_executed(intent, success=True, result=MagicMock())

        assert s._lp_position_id is None

    def test_repay_clears_borrow_counter(self):
        s = _make_morpho_strategy()
        assert s._borrowed_amount == Decimal("26.60")

        intent = MagicMock()
        intent.intent_type = IntentType.REPAY
        s.on_intent_executed(intent, success=True, result=MagicMock())

        assert s._borrowed_amount == Decimal("0")

    def test_withdraw_clears_collateral_counter(self):
        s = _make_morpho_strategy()
        assert s._collateral_supplied == Decimal("0.014")

        intent = MagicMock()
        intent.intent_type = IntentType.WITHDRAW
        s.on_intent_executed(intent, success=True, result=MagicMock())

        assert s._collateral_supplied == Decimal("0")

    def test_full_teardown_zeroes_get_open_positions(self):
        """Reproduces the QA bug: after a successful unwind, no positions remain."""
        s = _make_morpho_strategy()

        # Sanity: pre-teardown reports all 3 positions (LP + supply + borrow).
        before = s.get_open_positions()
        assert len(before.positions) == 3

        # Drive the framework's teardown success callbacks in order.
        for intent_type in (IntentType.LP_CLOSE, IntentType.REPAY, IntentType.WITHDRAW):
            intent = MagicMock()
            intent.intent_type = intent_type
            s.on_intent_executed(intent, success=True, result=MagicMock())

        after = s.get_open_positions()
        assert after.positions == []

    def test_failed_teardown_intent_does_not_clear_state(self):
        s = _make_morpho_strategy()
        intent = MagicMock()
        intent.intent_type = IntentType.LP_CLOSE
        s.on_intent_executed(intent, success=False, result=MagicMock())

        # Failure path must not silently zero the cache; operator needs to see
        # that the LP position is still tracked so it can be retried.
        assert s._lp_position_id == 12345

    def test_retry_after_lp_close_still_emits_proceeds_swap(self):
        """Codex P2 / Claude #7: after LP_CLOSE clears `_lp_position_id` but
        REPAY fails, a retry of generate_teardown_intents must still emit the
        WETH→USDC proceeds swap (gated on wallet balance, not LP id)."""
        from almanak.framework.intents.vocabulary import RepayIntent, SwapIntent, WithdrawIntent
        from almanak.framework.teardown import TeardownMode

        s = _make_morpho_strategy()

        # Drive successful LP_CLOSE → _lp_position_id is None.
        lp_close = MagicMock()
        lp_close.intent_type = IntentType.LP_CLOSE
        s.on_intent_executed(lp_close, success=True, result=MagicMock())
        assert s._lp_position_id is None

        # Wallet now holds the WETH proceeds; market reports a real balance.
        market = MagicMock()
        market.balance.return_value = _balance_obj("0.005", "17")

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=market)

        # Retry should NOT re-emit LP_CLOSE (already done) but MUST emit the
        # WETH→USDC swap, then REPAY, then WITHDRAW.
        intent_types = [type(i).__name__ for i in intents]
        assert intent_types == ["SwapIntent", "RepayIntent", "WithdrawIntent"]
        assert isinstance(intents[0], SwapIntent)
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"
        assert isinstance(intents[1], RepayIntent)
        assert isinstance(intents[2], WithdrawIntent)

    def test_retry_with_no_proceeds_skips_swap(self):
        """If LP closed and proceeds already swapped (zero WETH), retry skips swap."""
        from almanak.framework.intents.vocabulary import RepayIntent, WithdrawIntent
        from almanak.framework.teardown import TeardownMode

        s = _make_morpho_strategy()
        s._lp_position_id = None  # already closed in prior teardown round

        market = MagicMock()
        market.balance.return_value = _balance_obj("0", "0")

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=market)
        intent_types = [type(i).__name__ for i in intents]
        # No LP_CLOSE, no SWAP — just REPAY and WITHDRAW remaining.
        assert intent_types == ["RepayIntent", "WithdrawIntent"]
        assert isinstance(intents[0], RepayIntent)
        assert isinstance(intents[1], WithdrawIntent)

    def test_retry_with_no_market_falls_back_to_create_market_snapshot(self):
        """If the runner doesn't pass a market kwarg, the strategy must still
        check WETH balance via create_market_snapshot() before deciding to skip
        the proceeds swap. CodeRabbit / Gemini PR #1964 feedback."""
        from almanak.framework.intents.vocabulary import SwapIntent
        from almanak.framework.teardown import TeardownMode

        s = _make_morpho_strategy()
        s._lp_position_id = None  # LP closed, retry path

        # Strategy will build its own market snapshot.
        snapshot = MagicMock()
        snapshot.balance.return_value = _balance_obj("0.005", "17")
        s.create_market_snapshot = MagicMock(return_value=snapshot)

        # market=None simulates the runner not passing one.
        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=None)

        s.create_market_snapshot.assert_called_once()
        intent_types = [type(i).__name__ for i in intents]
        assert "SwapIntent" in intent_types
        swap = next(i for i in intents if isinstance(i, SwapIntent))
        assert swap.from_token == "WETH"
        assert swap.to_token == "USDC"

    def test_retry_with_no_market_and_snapshot_failure_emits_swap(self):
        """If neither the runner nor `create_market_snapshot()` provide a usable
        market, conservatively emit the swap rather than silently skipping."""
        from almanak.framework.teardown import TeardownMode

        s = _make_morpho_strategy()
        s._lp_position_id = None
        s.create_market_snapshot = MagicMock(side_effect=RuntimeError("gateway offline"))

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=None)
        intent_types = [type(i).__name__ for i in intents]
        assert "SwapIntent" in intent_types  # fail-safe emits the swap


# ---------------------------------------------------------------------------
# VIB-3738: balancer_flash_arb — get_open_positions reads on-chain balance
# ---------------------------------------------------------------------------


class TestBalancerTeardownVerification:
    """The strategy now reports based on on-chain wallet balance, not flags."""

    def test_post_swap_zero_balance_returns_empty(self):
        """After teardown swap drains base_token, no position is reported."""
        s = _make_balancer_strategy()

        market = MagicMock()
        market.balance.return_value = _balance_obj("0", "0")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert summary.positions == []

    def test_dust_below_threshold_returns_empty(self):
        """1 wei of WETH is dust, not a position."""
        s = _make_balancer_strategy()

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.0000000001", "0")  # 1e-10 WETH
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert summary.positions == []

    def test_real_balance_above_threshold_reports_position(self):
        """A real wallet balance still reports as an open position."""
        s = _make_balancer_strategy()

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.5", "1700")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert len(summary.positions) == 1
        position = summary.positions[0]
        assert position.value_usd == Decimal("1700")
        assert position.protocol == "uniswap_v3"
        assert position.details["asset"] == "WETH"
        assert position.details["balance"] == "0.5"
        assert position.details["source_protocol"] == "enso"
        assert position.details["teardown_protocol"] == "uniswap_v3"

    def test_query_failure_with_cached_trade_reports_position(self):
        """RPC failure + recorded trade → fail closed and report position.

        Codex P1 / Claude #4: a silent under-report on RPC outage would let
        teardown succeed while funds are still on-chain. Strategy must err on
        the side of reporting a position so the framework retries.
        """
        s = _make_balancer_strategy()
        # _trades_executed=1 and force_action="swap" come from the fixture, so
        # _has_likely_open_position() returns True.

        market = MagicMock()
        market.balance.side_effect = ValueError("rpc unavailable")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert len(summary.positions) == 1
        position = summary.positions[0]
        assert position.details["valuation_source"] == "cached_fallback_estimate"
        assert position.protocol == "uniswap_v3"
        assert position.details["source_protocol"] == "enso"
        assert position.details["teardown_protocol"] == "uniswap_v3"
        # value_usd must be non-zero so safety_guard derives a sensible
        # acceptable-loss floor (CodeRabbit PR #1964).
        assert position.value_usd > Decimal("0")

    def test_query_failure_with_no_cached_trade_reports_no_position(self):
        """RPC failure + no trade executed → no false-positive teardown."""
        s = _make_balancer_strategy()
        s._trades_executed = 0
        s._fell_back_to_swap = False
        s.force_action = None  # not a swap path

        market = MagicMock()
        market.balance.side_effect = ValueError("rpc unavailable")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert summary.positions == []

    def test_teardown_intents_skipped_when_no_balance(self):
        """generate_teardown_intents returns [] when wallet has no base_token."""
        from almanak.framework.teardown import TeardownMode

        s = _make_balancer_strategy()

        market = MagicMock()
        market.balance.return_value = _balance_obj("0", "0")

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=market)
        assert intents == []

    def test_teardown_intents_emitted_on_query_failure_with_cached_trade(self):
        """RPC failure during teardown intent generation must still emit the swap
        if the strategy recorded an executed trade (Codex P1)."""
        from almanak.framework.teardown import TeardownMode

        s = _make_balancer_strategy()  # fixture: _trades_executed=1, force_action="swap"

        market = MagicMock()
        market.balance.side_effect = ValueError("rpc unavailable")

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=market)
        assert len(intents) == 1
        assert intents[0].from_token == "WETH"
        assert intents[0].protocol == "uniswap_v3"

    def test_teardown_intents_emitted_when_balance_present(self):
        from almanak.framework.teardown import TeardownMode

        s = _make_balancer_strategy()

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.5", "1700")

        intents = s.generate_teardown_intents(TeardownMode.SOFT, market=market)
        assert len(intents) == 1
        assert intents[0].from_token == "WETH"
        assert intents[0].to_token == "USDC"
        assert intents[0].protocol == "uniswap_v3"


# ---------------------------------------------------------------------------
# VIB-3739: lido_staker — wei dust must not trip the post-teardown check
# ---------------------------------------------------------------------------


class TestLidoDustThreshold:
    """1-2 wei stETH dust after a rebasing-token swap is not an open position."""

    @pytest.mark.parametrize("dust_wei", [1, 2, 100, 100_000])
    def test_wei_dust_returns_no_position(self, dust_wei):
        """Below the dust threshold (0.0001 stETH ≈ <$0.40), no position reported."""
        s = _make_lido_strategy(receive_wrapped=False)

        # 100_000 wei of stETH = 1e-13 stETH — still well under the 0.0001 floor.
        amount = Decimal(dust_wei) / Decimal(10) ** 18

        market = MagicMock()
        market.balance.return_value = _balance_obj(amount, "0")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert summary.positions == []

    def test_real_balance_returns_position(self):
        """A real stETH balance (e.g. 0.5 stETH) still reports as open."""
        s = _make_lido_strategy(receive_wrapped=True)

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.5", "1700")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert len(summary.positions) == 1
        assert summary.positions[0].details["asset"] == "wstETH"

    def test_threshold_boundary_at_one_decimal_above(self):
        """Just above the threshold (0.0001 + epsilon) reports a position."""
        s = _make_lido_strategy(receive_wrapped=False)

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.0002", "0.6")
        s.create_market_snapshot = MagicMock(return_value=market)

        summary = s.get_open_positions()
        assert len(summary.positions) == 1

    def test_teardown_intents_skipped_at_dust_level(self):
        s = _make_lido_strategy(receive_wrapped=False)

        market = MagicMock()
        market.balance.return_value = _balance_obj("0.00001", "0")  # 1e-5 stETH

        intents = s.generate_teardown_intents(market=market)
        assert intents == []
