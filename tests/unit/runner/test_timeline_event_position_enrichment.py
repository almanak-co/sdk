"""Tests for the runner's `_emit_execution_timeline_event` PR4 contract.

History: this file originally locked in Bug 4 of the 0G DogFooding report
(2026-04-16) — the SDK was supposed to copy position metadata
(``position_id``, tick range, liquidity, swap_amounts, lp_close_data) into
``timeline_events.details_json`` so downstream consumers could reconstruct
the executed intent without re-reading the chain.

VIB-4043 / PR4 inverts that contract. PRD-TimelineEvents §6.1 forbids
money-shaped data (token amounts, gas, prices, slippage, position
attribution, receipt-parser payloads) from timeline events. The financial
truth lives in `transaction_ledger`; the timeline event becomes a UX
breadcrumb that points at it via the typed
``related_ledger_entry_id`` column.

This file is now the contract test for that new behavior:
  * Successful execution timeline events carry the minimal lifecycle
    payload — ``{"intent_type", "success"}`` — and nothing else.
  * Money-shaped keys are explicitly absent.
  * ``related_ledger_entry_id`` propagates onto the event when the runner
    threads it through.
  * Failure events follow the same shape (no money in details).

The producer-side static check
(``tests/static/test_timeline_payload_keys.py``) prevents new violations
from sneaking into the codebase. This unit test pins the runtime shape
of the runner's primary emit path.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from almanak.framework.runner.strategy_runner import StrategyRunner

FORBIDDEN_KEYS_IN_DETAILS = {
    "gas_used",
    "amount",
    "amount_in",
    "amount_out",
    "amount0",
    "amount1",
    "tick_lower",
    "tick_upper",
    "liquidity",
    "lp_close",
    "swap",
    "position_id",
    "effective_price",
    "slippage_bps",
    "extracted_data",
}


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


def _make_intent(intent_type: str) -> MagicMock:
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value=intent_type)
    return intent


def _capture_event(runner, strategy, intent, *, success: bool, result, related_ledger_entry_id: str = ""):
    captured: list = []
    with patch("almanak.framework.runner.strategy_runner.add_event", side_effect=captured.append):
        runner._emit_execution_timeline_event(
            strategy,
            intent,
            success=success,
            result=result,
            related_ledger_entry_id=related_ledger_entry_id,
        )
    assert len(captured) == 1
    return captured[0]


class TestPR4ScopeContract:
    """Timeline events MUST NOT carry money-shaped keys (PRD-TimelineEvents §6.1)."""

    def test_lp_open_success_emits_minimal_details(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN")

        # Even when ResultEnricher fills in all the metadata historically
        # forwarded into details, none of it must reach the timeline event.
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

        event = _capture_event(runner, strategy, intent, success=True, result=result)

        # Lifecycle markers only.
        assert event.details == {"intent_type": "LP_OPEN", "success": True}
        for key in FORBIDDEN_KEYS_IN_DETAILS:
            assert key not in event.details, f"PR4 violation — `{key}` leaked into timeline details"
        # tx_hash IS allowed (UX deep-link to the explorer); description is the UX summary.
        assert event.tx_hash == "0xdeadbeef"
        assert "LP_OPEN" in event.description

    def test_swap_success_does_not_carry_swap_amounts(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("SWAP")

        swap_amounts = SimpleNamespace(
            to_dict=MagicMock(
                return_value={"amount_in": "100", "amount_out": "0.042", "effective_price": "0.00042"}
            ),
        )
        result = SimpleNamespace(
            position_id=None,
            transaction_results=[SimpleNamespace(tx_hash="0xdef")],
            total_gas_used=200_000,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=swap_amounts,
        )

        event = _capture_event(runner, strategy, intent, success=True, result=result)
        # Pin the EXACT shape — so a future field that happens to not be in
        # FORBIDDEN_KEYS can't slip through. (CodeRabbit review.)
        assert event.details == {"intent_type": "SWAP", "success": True}
        for key in FORBIDDEN_KEYS_IN_DETAILS:
            assert key not in event.details, f"PR4 violation — `{key}` leaked into SWAP details"

    def test_lp_close_success_does_not_carry_lp_close_payload(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("LP_CLOSE")

        lp_close_data = SimpleNamespace(
            to_dict=MagicMock(return_value={"amount0_collected": "1000000"}),
        )
        result = SimpleNamespace(
            position_id=2359,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=250_000,
            extracted_data={},
            lp_close_data=lp_close_data,
            swap_amounts=None,
        )

        event = _capture_event(runner, strategy, intent, success=True, result=result)
        assert "lp_close" not in event.details
        assert event.details == {"intent_type": "LP_CLOSE", "success": True}

    def test_failure_event_minimal_details(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("SWAP")

        result = SimpleNamespace(
            position_id=None,
            transaction_results=[],
            total_gas_used=0,
            error="reverted: out of gas",
        )

        event = _capture_event(runner, strategy, intent, success=False, result=result)
        assert event.details == {"intent_type": "SWAP", "success": False}
        assert "reverted" in event.description


class TestPR4FailureDescriptionSanitization:
    """The failure description must NOT carry money-shaped data.

    CodeRabbit review on PR #2116: ``result.error`` is set to strings like the
    slippage-breach message and the reconciliation delta summary, both of
    which contain bps and token deltas. Embedding the raw error in the
    timeline description would re-introduce the leak that PR4 closed in
    ``details``. The runner must classify into a generic bucket; the full
    error stays in ``transaction_ledger.error`` for renderers to drill into
    via ``related_ledger_entry_id``.
    """

    def _make_failed_result(self, error: str):
        return SimpleNamespace(
            position_id=None,
            transaction_results=[],
            total_gas_used=0,
            error=error,
        )

    def test_slippage_breach_description_is_generic(self):
        runner = _make_runner()
        intent = _make_intent("SWAP")
        result = self._make_failed_result(
            "Slippage circuit breaker: actual slippage 250 bps exceeds limit 100 bps "
            "(swap: USDC -> ETH)"
        )
        event = _capture_event(runner, _make_strategy(), intent, success=False, result=result)
        # Money-shaped data MUST NOT reach the timeline description.
        assert "250" not in event.description
        assert "bps" not in event.description
        assert "USDC" not in event.description
        assert "->" not in event.description
        assert "slippage breach" in event.description

    def test_reconciliation_description_is_generic(self):
        runner = _make_runner()
        intent = _make_intent("LP_OPEN")
        result = self._make_failed_result(
            "Balance reconciliation incident: USDC delta=0.123 expected=[0.0,0.05]; ETH delta=-0.001 expected=[0.0,0.0]"
        )
        event = _capture_event(runner, _make_strategy(), intent, success=False, result=result)
        for token in ("USDC", "ETH"):
            assert token not in event.description, "token name leak in failure description"
        for digit in ("0.123", "0.05", "-0.001"):
            assert digit not in event.description, "delta number leak in failure description"
        assert "reconciliation incident" in event.description

    def test_revert_description_is_generic(self):
        runner = _make_runner()
        intent = _make_intent("SWAP")
        result = self._make_failed_result("reverted: out of gas (used 580800/600000)")
        event = _capture_event(runner, _make_strategy(), intent, success=False, result=result)
        # Even raw revert strings often carry gas metrics; bucket to the class.
        assert "580800" not in event.description
        assert "execution reverted" in event.description

    def test_unknown_error_falls_back_to_generic_bucket(self):
        runner = _make_runner()
        intent = _make_intent("SWAP")
        result = self._make_failed_result("")
        event = _capture_event(runner, _make_strategy(), intent, success=False, result=result)
        assert "unknown error" in event.description

    def test_backfilled_error_msg_routes_to_correct_bucket(self):
        """CodeRabbit review: when ``last_execution_result.error`` is empty
        but the state-machine reason carries the real failure text, the
        runner backfills BEFORE emitting so the timeline gets bucketed
        correctly instead of falling back to "unknown error".

        This unit test pins the classifier's behavior on the realistic
        backfilled string. The full producer-side fix (moving the backfill
        before ``_emit_execution_timeline_event``) is in
        ``_handle_failed_state`` of strategy_runner.py.
        """
        # Empty error → "unknown error" (the symptom Pre-fix)
        assert StrategyRunner._classify_failure_reason("") == "unknown error"
        # Backfilled state-machine reason → routes correctly post-fix
        assert (
            StrategyRunner._classify_failure_reason(
                "Slippage circuit breaker: actual slippage 250 bps exceeds limit"
            )
            == "slippage breach"
        )
        assert (
            StrategyRunner._classify_failure_reason("reverted: out of gas")
            == "execution reverted"
        )

    def test_classifier_resilient_to_non_string_error_payloads(self):
        """CodeRabbit (PR #2117): the classifier is on the failure-emission
        hot path. A non-string ``error`` value must NOT raise ``AttributeError``
        and silently kill the timeline event.

        Coerce defensively (None / Exception / bytes / arbitrary object) and
        pin the bucket so a future refactor can't reintroduce the type-strict
        crash.
        """
        cls = StrategyRunner._classify_failure_reason
        # None and empty-after-coercion inputs bucket as "unknown error".
        assert cls(None) == "unknown error"
        assert cls(b"") == "unknown error"
        assert cls("") == "unknown error"
        # Exception object — common failure-path leak; ``str(exc)`` is the message.
        assert cls(ValueError("Slippage breach 400 bps")) == "slippage breach"
        assert cls(RuntimeError("transaction reverted")) == "execution reverted"
        # Bytes input — decoded to str.
        assert cls(b"reverted: oog") == "execution reverted"
        # Arbitrary object falling back through ``str()``.

        class _SurrogateError:
            def __str__(self) -> str:
                return "circuit breaker open"

        assert cls(_SurrogateError()) == "circuit breaker open"
        # Object whose ``str()`` carries no signal still routes to a stable
        # bucket — never raises, never returns ``""``.
        assert cls(object()) == "execution failed"


class TestPR4LedgerCorrelation:
    """`related_ledger_entry_id` propagates onto the timeline event."""

    def test_id_threaded_when_provided(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN")
        result = SimpleNamespace(
            position_id=1,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=100,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=None,
        )
        event = _capture_event(
            runner, strategy, intent, success=True, result=result, related_ledger_entry_id="ledger-77"
        )
        assert event.related_ledger_entry_id == "ledger-77"

    def test_id_empty_when_not_provided(self):
        runner = _make_runner()
        strategy = _make_strategy()
        intent = _make_intent("LP_OPEN")
        result = SimpleNamespace(
            position_id=1,
            transaction_results=[SimpleNamespace(tx_hash="0xabc")],
            total_gas_used=100,
            extracted_data={},
            lp_close_data=None,
            swap_amounts=None,
        )
        event = _capture_event(runner, strategy, intent, success=True, result=result)
        assert event.related_ledger_entry_id == ""
