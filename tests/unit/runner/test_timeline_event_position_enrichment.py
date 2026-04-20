"""Tests for timeline-event enrichment with position data.

Regression test for Bug 4 of the 0G DogFooding report (2026-04-16):
when the SDK opens an LP position, the timeline event written to
``gateway.db:timeline_events.details_json`` previously contained only
``{intent_type, success, gas_used}`` — the NFT position ID, tick range,
and liquidity extracted by ResultEnricher were dropped before
persistence. Downstream consumers (teardown, PM dashboard, audits)
could not recover the position ID without reparsing receipts.

This test asserts that ``_emit_execution_timeline_event`` now forwards
the enriched fields (``position_id``, ``tick_lower``, ``tick_upper``,
``liquidity``) into the timeline event's details dict.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.framework.runner.strategy_runner import StrategyRunner


def _make_runner() -> StrategyRunner:
    return StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.strategy_id = "test_strat"
    strategy.chain = "zerog"
    return strategy


def _make_lp_open_intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="LP_OPEN")
    return intent


def _make_swap_intent() -> MagicMock:
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="SWAP")
    return intent


class TestTimelineEventPositionEnrichment:
    """Bug 4 fix — position data flows into timeline_events.details_json."""

    def test_lp_open_timeline_event_carries_position_id(self):
        """LP_OPEN timeline event includes position_id, ticks, and liquidity."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_lp_open_intent()

        result = SimpleNamespace(
            position_id=2359,
            transaction_results=[SimpleNamespace(tx_hash="0xdeadbeef")],
            total_gas_used=580_800,
            extracted_data={
                "tick_lower": 343_800,
                "tick_upper": 349_800,
                "liquidity": 700_417_431_525,
            },
            lp_close_data=None,
            swap_amounts=None,
        )

        captured = []

        def fake_add_event(event):
            captured.append(event)

        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=fake_add_event):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        assert len(captured) == 1
        details = captured[0].details
        assert details["intent_type"] == "LP_OPEN"
        assert details["success"] is True
        assert details["gas_used"] == 580_800
        assert details["position_id"] == 2359
        assert details["tick_lower"] == 343_800
        assert details["tick_upper"] == 349_800
        # liquidity is stringified because it exceeds JSON's safe integer range
        assert details["liquidity"] == "700417431525" or details["liquidity"] == 700_417_431_525

    def test_lp_open_large_liquidity_is_stringified(self):
        """Liquidity values >= 2**53 are stringified to preserve precision."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_lp_open_intent()

        huge_liquidity = 2**60  # ~1.15e18, well beyond 2**53
        result = SimpleNamespace(
            position_id=42,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=100_000,
            extracted_data={
                "tick_lower": 0,
                "tick_upper": 10,
                "liquidity": huge_liquidity,
            },
            lp_close_data=None,
            swap_amounts=None,
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        assert captured[0].details["liquidity"] == str(huge_liquidity)

    def test_failed_intent_does_not_enrich(self):
        """Failed executions keep the minimal details set (no position data)."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_lp_open_intent()

        result = SimpleNamespace(
            position_id=None,
            transaction_results=[],
            total_gas_used=0,
            error="reverted",
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=False, result=result)

        details = captured[0].details
        assert details["success"] is False
        assert "position_id" not in details
        assert "tick_lower" not in details

    def test_intent_without_enriched_data_keeps_minimal_details(self):
        """When ResultEnricher produced nothing, legacy details are preserved."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_lp_open_intent()

        # Unenriched result: no position_id, empty extracted_data
        result = SimpleNamespace(
            position_id=None,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=1234,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=None,
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        details = captured[0].details
        assert details == {"intent_type": "LP_OPEN", "success": True, "gas_used": 1234}

    def test_swap_timeline_event_carries_swap_amounts(self):
        """SWAP timeline event includes swap_amounts dict when present."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_swap_intent()

        swap_amounts = SimpleNamespace(
            to_dict=MagicMock(return_value={"amount_in": "100", "amount_out": "0.042"}),
        )
        result = SimpleNamespace(
            position_id=None,
            transaction_results=[SimpleNamespace(tx_hash="0xdef")],
            total_gas_used=200_000,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=swap_amounts,
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        assert captured[0].details["swap"] == {"amount_in": "100", "amount_out": "0.042"}

    def test_lp_close_timeline_event_carries_lp_close_data(self):
        """LP_CLOSE timeline event forwards lp_close_data dict into details.

        Bug 4 of the 0G DogFooding report added lp_close_data to the
        persisted timeline alongside position_id / swap_amounts. This test
        locks in that the close-side amounts (amount0/1 collected, fees0/1)
        actually reach details_json so PM dashboards can reconstruct the
        close without reparsing receipts.
        """
        runner = _make_runner()
        strategy = _make_strategy()

        intent = MagicMock()
        intent.intent_type = SimpleNamespace(value="LP_CLOSE")

        lp_close_payload = {
            "amount0_collected": "1000000",  # 1 USDC
            "amount1_collected": "500000000000000000",  # 0.5 WETH
            "fees0": "100",
            "fees1": "200",
        }
        lp_close_data = SimpleNamespace(to_dict=MagicMock(return_value=lp_close_payload))

        result = SimpleNamespace(
            position_id=2359,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=250_000,
            extracted_data={},
            lp_close_data=lp_close_data,
            swap_amounts=None,
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        assert captured[0].details["lp_close"] == lp_close_payload
        assert captured[0].details["position_id"] == 2359

    def test_oversized_position_id_is_stringified(self):
        """Position IDs >= 2**53 are stringified so JSON consumers don't
        lose precision (CodeRabbit review, PR #1522)."""
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_lp_open_intent()

        huge_id = 2**60  # well beyond JS safe-integer range
        result = SimpleNamespace(
            position_id=huge_id,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=100_000,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=None,
        )

        captured = []
        with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
            runner._emit_execution_timeline_event(strategy, intent, success=True, result=result)

        assert captured[0].details["position_id"] == str(huge_id)
        assert isinstance(captured[0].details["position_id"], str)
