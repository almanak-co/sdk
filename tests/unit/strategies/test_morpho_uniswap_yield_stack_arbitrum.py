"""Tests for the Morpho Blue + Uniswap V3 yield stack strategy on Arbitrum.

Validates the T2 composition state machine: BORROW -> SWAP -> LP_OPEN entry path,
LP_CLOSE -> SWAP -> REPAY -> WITHDRAW teardown path, on_intent_executed callbacks,
and Morpho Blue-specific constraints (amount-based repay/withdraw).

Kitchen Loop iteration 145, VIB-2168.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def strategy():
    from strategies.incubating.morpho_uniswap_yield_stack_arbitrum.strategy import (
        MorphoUniswapYieldStackArbitrumStrategy,
    )

    strat = MorphoUniswapYieldStackArbitrumStrategy.__new__(MorphoUniswapYieldStackArbitrumStrategy)
    strat.config = {}
    strat._chain = "arbitrum"
    strat._wallet_address = "0x" + "0" * 40
    strat._deployment_id = "test-morpho-univ3-arb"
    strat.collateral_token = "WETH"
    strat.collateral_amount = Decimal("0.05")
    strat.borrow_token = "USDC"
    strat.ltv_target = Decimal("0.3")
    strat.market_id = "0x" + "ab" * 32  # Fake market_id
    strat.lp_pool = "WETH/USDC/500"
    strat.lp_range_width_pct = Decimal("0.2")
    strat._loop_state = "idle"
    strat._previous_stable_state = "idle"
    strat._collateral_supplied = Decimal("0")
    strat._borrowed_amount = Decimal("0")
    strat._swapped_weth_amount = Decimal("0")
    strat._lp_usdc_amount = Decimal("0")
    strat._lp_position_id = None
    return strat


def _mock_market(
    collateral_price: float = 3000.0,
    borrow_price: float = 1.0,
) -> MagicMock:
    market = MagicMock()

    def price_fn(token):
        if token == "WETH":
            return Decimal(str(collateral_price))
        if token == "USDC":
            return Decimal(str(borrow_price))
        raise ValueError(f"Unknown token: {token}")

    market.price = MagicMock(side_effect=price_fn)
    return market


class TestEntryPath:
    """Test the entry path: SUPPLY -> BORROW -> SWAP -> LP_OPEN."""

    def test_idle_creates_supply_intent(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "SUPPLY"
        assert intent.protocol == "morpho_blue"
        assert intent.use_as_collateral is True
        assert strategy._loop_state == "supplying"

    def test_supply_intent_includes_market_id(self, strategy):
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SUPPLY"
        assert intent.market_id == strategy.market_id
        assert intent.token == "WETH"
        assert intent.amount == Decimal("0.05")

    def test_supplied_creates_borrow_intent(self, strategy):
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent is not None
        assert intent.intent_type.value == "BORROW"
        assert intent.protocol == "morpho_blue"
        assert strategy._loop_state == "borrowing"

    def test_borrow_intent_uses_morpho_protocol(self, strategy):
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.protocol == "morpho_blue"

    def test_borrow_intent_excludes_bundled_collateral(self, strategy):
        """Collateral is supplied by the standalone SUPPLY intent (VIB-3586)."""
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.collateral_amount == Decimal("0")

    def test_borrow_intent_calculates_amount(self, strategy):
        """Borrow amount = collateral_value * ltv / borrow_price."""
        strategy._loop_state = "supplied"
        market = _mock_market(collateral_price=3000.0, borrow_price=1.0)
        intent = strategy.decide(market)
        # 0.05 WETH * $3000 * 0.3 LTV / $1 = $45
        assert intent.borrow_amount == Decimal("45.00")

    def test_borrow_intent_includes_market_id(self, strategy):
        strategy._loop_state = "supplied"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.market_id == strategy.market_id

    def test_borrowed_state_creates_swap_intent(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "SWAP"
        assert intent.from_token == "USDC"
        assert intent.to_token == "WETH"
        assert intent.protocol == "uniswap_v3"
        # Swaps half: 45/2 = 22.50
        assert intent.amount == Decimal("22.50")

    def test_swapped_state_creates_lp_open_intent(self, strategy):
        strategy._loop_state = "swapped"
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_weth_amount = Decimal("0.0075")
        market = _mock_market(collateral_price=3000.0)
        intent = strategy.decide(market)
        assert intent.intent_type.value == "LP_OPEN"
        assert intent.protocol == "uniswap_v3"

    def test_lp_open_range_width(self, strategy):
        strategy._loop_state = "swapped"
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_weth_amount = Decimal("0.0075")
        market = _mock_market(collateral_price=3000.0)
        intent = strategy.decide(market)
        # Range: 3000 * (1 - 0.1) to 3000 * (1 + 0.1) = 2700-3300
        assert intent.range_lower == Decimal("2700.0")
        assert intent.range_upper == Decimal("3300.0")

    def test_complete_state_holds(self, strategy):
        strategy._loop_state = "complete"
        market = _mock_market()
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestOnIntentExecuted:
    """Test state machine transitions via on_intent_executed."""

    def test_supply_success_transitions(self, strategy):
        strategy._loop_state = "supplying"
        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.amount = Decimal("0.05")
        strategy.on_intent_executed(intent, True, None)
        assert strategy._loop_state == "supplied"
        assert strategy._collateral_supplied == Decimal("0.05")

    def test_borrow_success_transitions(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._collateral_supplied = Decimal("0.05")  # Set by prior SUPPLY
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.borrow_amount = Decimal("45.00")
        strategy.on_intent_executed(intent, True, None)
        assert strategy._loop_state == "borrowed"
        assert strategy._borrowed_amount == Decimal("45.00")
        assert strategy._collateral_supplied == Decimal("0.05")

    def test_swap_success_transitions(self, strategy):
        strategy._loop_state = "swapping"
        strategy._borrowed_amount = Decimal("45.00")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"

        result = MagicMock()
        result.swap_amounts.amount_out_decimal = Decimal("0.0075")

        strategy.on_intent_executed(intent, True, result)
        assert strategy._loop_state == "swapped"
        assert strategy._swapped_weth_amount == Decimal("0.0075")
        assert strategy._lp_usdc_amount == Decimal("22.50")

    def test_swap_success_without_enrichment(self, strategy):
        strategy._loop_state = "swapping"
        strategy._borrowed_amount = Decimal("45.00")
        intent = MagicMock()
        intent.intent_type.value = "SWAP"
        strategy.on_intent_executed(intent, True, None)
        assert strategy._loop_state == "swapped"
        # Falls back to estimate
        assert strategy._swapped_weth_amount == Decimal("0.001")

    def test_lp_open_success_transitions(self, strategy):
        strategy._loop_state = "lp_opening"
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        result = MagicMock()
        result.position_id = 12345
        strategy.on_intent_executed(intent, True, result)
        assert strategy._loop_state == "complete"
        assert strategy._lp_position_id == "12345"

    def test_failure_reverts_state(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "idle"
        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        strategy.on_intent_executed(intent, False, None)
        assert strategy._loop_state == "idle"


class TestTransitionalStateRecovery:
    """Test that stuck transitional states revert properly.

    The strategy reverts to the previous stable state and returns HOLD.
    On the *next* call, the stable state handler fires the correct intent.
    """

    def test_borrowing_reverts_to_supplied(self, strategy):
        strategy._loop_state = "borrowing"
        strategy._previous_stable_state = "supplied"
        market = _mock_market()
        # First call: revert + HOLD
        intent = strategy.decide(market)
        assert strategy._loop_state == "supplied"
        assert intent.intent_type.value == "HOLD"
        # Second call: supplied -> BORROW
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "BORROW"

    def test_supplying_reverts_to_idle(self, strategy):
        strategy._loop_state = "supplying"
        strategy._previous_stable_state = "idle"
        market = _mock_market()
        # First call: revert + HOLD
        intent = strategy.decide(market)
        assert strategy._loop_state == "idle"
        assert intent.intent_type.value == "HOLD"
        # Second call: idle -> SUPPLY
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "SUPPLY"

    def test_swapping_reverts_to_borrowed(self, strategy):
        strategy._loop_state = "swapping"
        strategy._previous_stable_state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        market = _mock_market()
        # First call: revert + HOLD
        intent = strategy.decide(market)
        assert strategy._loop_state == "borrowed"
        assert intent.intent_type.value == "HOLD"
        # Second call: borrowed -> SWAP
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "SWAP"

    def test_lp_opening_reverts_to_swapped(self, strategy):
        strategy._loop_state = "lp_opening"
        strategy._previous_stable_state = "swapped"
        strategy._borrowed_amount = Decimal("45.00")
        strategy._swapped_weth_amount = Decimal("0.0075")
        market = _mock_market()
        # First call: revert + HOLD
        intent = strategy.decide(market)
        assert strategy._loop_state == "swapped"
        assert intent.intent_type.value == "HOLD"
        # Second call: swapped -> LP_OPEN
        intent2 = strategy.decide(market)
        assert intent2.intent_type.value == "LP_OPEN"


class TestTeardown:
    """Test teardown intent generation."""

    def test_teardown_full_position(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = "12345"
        strategy._swapped_weth_amount = Decimal("0.0075")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._collateral_supplied = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)

        # LP_CLOSE, SWAP weth->usdc, REPAY, WITHDRAW = 4 intents
        assert len(intents) == 4
        assert intents[0].intent_type.value == "LP_CLOSE"
        assert intents[1].intent_type.value == "SWAP"
        assert intents[2].intent_type.value == "REPAY"
        assert intents[3].intent_type.value == "WITHDRAW"

    def test_teardown_repay_uses_morpho_protocol(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._borrowed_amount = Decimal("45.00")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        repay = [i for i in intents if i.intent_type.value == "REPAY"][0]
        assert repay.protocol == "morpho_blue"
        assert repay.repay_full is False  # Amount-based to avoid Morpho overflow

    def test_teardown_withdraw_uses_morpho_protocol(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._collateral_supplied = Decimal("0.05")
        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        withdraw = [i for i in intents if i.intent_type.value == "WITHDRAW"][0]
        assert withdraw.protocol == "morpho_blue"
        assert withdraw.withdraw_all is False  # Amount-based

    def test_teardown_hard_mode_higher_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = "12345"
        strategy._swapped_weth_amount = Decimal("0.0075")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._collateral_supplied = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.HARD)
        swap = [i for i in intents if i.intent_type.value == "SWAP"][0]
        assert swap.max_slippage == Decimal("0.03")

    def test_teardown_soft_mode_normal_slippage(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = "12345"
        strategy._swapped_weth_amount = Decimal("0.0075")
        strategy._borrowed_amount = Decimal("45.00")
        strategy._collateral_supplied = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        swap = [i for i in intents if i.intent_type.value == "SWAP"][0]
        assert swap.max_slippage == Decimal("0.005")

    def test_teardown_no_lp_skips_close(self, strategy):
        from almanak.framework.teardown import TeardownMode

        strategy._lp_position_id = None
        strategy._borrowed_amount = Decimal("45.00")
        strategy._collateral_supplied = Decimal("0.05")

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        types = [i.intent_type.value for i in intents]
        assert "LP_CLOSE" not in types
        assert "REPAY" in types
        assert "WITHDRAW" in types

    def test_teardown_empty_positions(self, strategy):
        from almanak.framework.teardown import TeardownMode

        intents = strategy.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 0


class TestGetOpenPositions:
    """Test position reporting for teardown preview."""

    def test_reports_all_positions(self, strategy):
        strategy._lp_position_id = "12345"
        strategy._lp_usdc_amount = Decimal("22.50")
        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")
        strategy.market_id = "0x" + "ab" * 32

        # Mock create_market_snapshot to avoid gateway dependency
        mock_market = MagicMock()
        mock_market.price.return_value = Decimal("3000")
        strategy.create_market_snapshot = MagicMock(return_value=mock_market)

        summary = strategy.get_open_positions()
        assert len(summary.positions) == 3  # LP + SUPPLY + BORROW
        types = [p.position_type.value for p in summary.positions]
        assert "LP" in types
        assert "SUPPLY" in types
        assert "BORROW" in types

    def test_reports_no_positions_when_empty(self, strategy):
        strategy.create_market_snapshot = MagicMock(side_effect=Exception("no gateway"))
        summary = strategy.get_open_positions()
        assert len(summary.positions) == 0


class TestStatePersistence:
    """Test state save/restore."""

    def test_get_persistent_state(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._previous_stable_state = "idle"
        strategy._collateral_supplied = Decimal("0.05")
        strategy._borrowed_amount = Decimal("45.00")
        strategy.market_id = "0xtest"

        state = strategy.get_persistent_state()
        assert state["loop_state"] == "borrowed"
        assert state["borrowed_amount"] == "45.00"
        assert state["market_id"] == "0xtest"

    def test_load_persistent_state(self, strategy):
        state = {
            "loop_state": "swapped",
            "previous_stable_state": "borrowed",
            "collateral_supplied": "0.05",
            "borrowed_amount": "45.00",
            "swapped_weth_amount": "0.0075",
            "lp_usdc_amount": "22.50",
            "lp_position_id": "12345",
            "market_id": "0xrestored",
        }
        strategy.load_persistent_state(state)
        assert strategy._loop_state == "swapped"
        assert strategy._borrowed_amount == Decimal("45.00")
        assert strategy._swapped_weth_amount == Decimal("0.0075")
        assert strategy._lp_position_id == "12345"
        assert strategy.market_id == "0xrestored"


class TestGetStatus:
    def test_status_format(self, strategy):
        strategy._loop_state = "borrowed"
        strategy._borrowed_amount = Decimal("45.00")
        status = strategy.get_status()
        assert status["strategy"] == "morpho_uniswap_yield_stack_arbitrum"
        assert status["chain"] == "arbitrum"
        assert status["state"]["loop_state"] == "borrowed"
        assert status["state"]["borrowed"] == "45.00"


class TestPriceUnavailable:
    def test_holds_when_prices_unavailable(self, strategy):
        market = MagicMock()
        market.price = MagicMock(side_effect=ValueError("No price"))
        intent = strategy.decide(market)
        assert intent.intent_type.value == "HOLD"


class TestMorphoBlueArbitrumConfig:
    """Test that Morpho Blue contracts are configured for Arbitrum."""

    def test_morpho_blue_arbitrum_in_contracts(self):
        from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE

        assert "arbitrum" in MORPHO_BLUE
        # Arbitrum deploys Morpho Blue at a chain-specific address (NOT the universal
        # 0xBBBB...FFCb vanity address used on Ethereum/Base). Registry corrected in
        # VIB-2969 after iter-173 discovered the previously-registered universal
        # address had 0 bytes of code on Arbitrum.
        assert MORPHO_BLUE["arbitrum"]["morpho"] == "0x6c247b1F6182318877311737BaC0844bAa518F5e"

    def test_morpho_blue_arbitrum_tokens(self):
        from almanak.connectors.morpho_blue.addresses import MORPHO_BLUE_TOKENS

        assert "arbitrum" in MORPHO_BLUE_TOKENS
        assert "WETH" in MORPHO_BLUE_TOKENS["arbitrum"]
        assert "USDC" in MORPHO_BLUE_TOKENS["arbitrum"]

    def test_adapter_accepts_arbitrum(self):
        from almanak.connectors.morpho_blue.adapter import MORPHO_BLUE_ADDRESSES

        assert "arbitrum" in MORPHO_BLUE_ADDRESSES
