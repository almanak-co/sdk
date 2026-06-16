"""VIB-5140: ``_execute_intents`` tracks the MAX close-tx receipt block.

A multi-intent teardown closes several positions whose txs can land in
DIFFERENT blocks, and intents may complete non-monotonically (slippage
retries / reordering). The post-teardown closure verifier pins its on-chain
reads to ``TeardownResult.last_receipt_block``; if that were the
LAST-PROCESSED intent's block and that block were EARLIER than another
close's block, a position closed in the LATER block would read as still-open
under-pinned → false-negative → STRATEGY_ERROR.

Reading at the HIGHEST close block makes every close visible (close state
only moves forward), so MAX is the correct anchor. These tests prove
``_execute_intents`` folds the per-intent receipt blocks with ``max`` —
specifically that a later-processed intent with an EARLIER block does NOT
lower the anchor.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _exec_result_with_block(block: int) -> SimpleNamespace:
    """A successful execution result whose single receipt landed at ``block``.

    Shaped for ``strategy_runner._last_receipt_block``: a
    ``transaction_results`` list of objects with ``success`` + ``receipt``
    (``receipt.block_number``).
    """
    receipt = SimpleNamespace(block_number=block)
    tx = SimpleNamespace(success=True, receipt=receipt, tx_hash="0xabc")
    return SimpleNamespace(
        success=True,
        final_slippage=Decimal("0.005"),
        total_gas_used=21000,
        transaction_results=[tx],
        status="success",
        error=None,
        approval_request=None,
    )


def _make_state(n_intents: int) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="teardown-test",
        deployment_id="deployment:abc123",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=n_intents,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _make_positions() -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:abc123",
        timestamp=datetime.now(UTC),
        positions=[],
    )


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc123"
    strategy.name = "Test"
    strategy.chain = "arbitrum"
    # No optional hooks: drop the framework/user intent-execution callbacks so
    # the success path stays minimal (the success branch guards each on
    # ``hasattr``).
    del strategy._framework_record_intent_execution
    del strategy.on_intent_executed
    del strategy.save_state
    del strategy.flush_pending_saves
    return strategy


async def _run(block_sequence: list[int]) -> int | None:
    """Drive ``_execute_intents`` with ``execute_with_escalation`` returning, in
    processing order, a successful result per ``block_sequence`` entry. Returns
    the resulting ``TeardownResult.last_receipt_block``.
    """
    mgr = TeardownManager()
    mgr.state_manager = None  # skip persistence
    # Bypass the escalation/orchestrator path: hand back crafted exec results
    # in processing order (one per intent).
    results = [_exec_result_with_block(b) for b in block_sequence]
    mgr.slippage_manager.execute_with_escalation = AsyncMock(side_effect=results)

    intents = [
        SimpleNamespace(max_slippage=None, intent_type="SWAP") for _ in block_sequence
    ]
    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_make_strategy(),
        intents=intents,
        positions=_make_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_make_state(len(block_sequence)),
    )
    return result.last_receipt_block


@pytest.mark.asyncio
async def test_last_receipt_block_is_max_when_processing_order_descends():
    """Later-processed intent has an EARLIER block (200 then 100): the anchor
    MUST stay at the MAX (200), not drop to the last-processed 100."""
    assert await _run([200, 100]) == 200


@pytest.mark.asyncio
async def test_last_receipt_block_is_max_non_monotonic():
    """Non-monotonic blocks across three closes (150, 90, 175): anchor = 175."""
    assert await _run([150, 90, 175]) == 175


@pytest.mark.asyncio
async def test_last_receipt_block_single_intent():
    """Single close → its own block."""
    assert await _run([123]) == 123


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
