"""TeardownManager._verify_closure dispatches to registered post-conditions.

VIB-3742: the verifier must run protocol-specific on-chain post-conditions
on the pre-execution position snapshot, not just re-read the strategy's
in-memory state. This test replaces the registered TJ V2 post-condition
with a stub so we can drive the closed/residual paths without a live fork.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.teardown_post_condition import (
    # Registration is framework-internal (manifest-driven via
    # CONNECTOR.teardown_post_condition); tests reach the private seam to
    # swap/restore hooks without building a whole connector manifest.
    _register_teardown_post_condition,
)
from almanak.framework.teardown.post_conditions import (
    ClosureCheckResult,
    get_teardown_post_condition,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


@pytest.fixture
def _restore_traderjoe_v2_hook():
    """Snapshot + restore the TJ V2 hook so test mutations don't leak."""
    original = get_teardown_post_condition("traderjoe_v2")
    yield
    if original is not None:
        _register_teardown_post_condition("traderjoe_v2", original)


def _make_position_snapshot(*positions) -> SimpleNamespace:
    return SimpleNamespace(positions=list(positions))


def _make_strategy(open_positions: list | None = None) -> MagicMock:
    strategy = MagicMock()
    strategy.wallet_address = "0xabc"
    strategy.get_open_positions.return_value = SimpleNamespace(
        positions=open_positions or []
    )
    return strategy


@pytest.mark.asyncio
async def test_verify_closure_passes_when_post_condition_returns_closed(
    _restore_traderjoe_v2_hook,
):
    hook = MagicMock(return_value=ClosureCheckResult(closed=True, protocol="traderjoe_v2"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool", "bin_ids": [1, 2, 3]},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is True
    hook.assert_called_once()


@pytest.mark.asyncio
async def test_verify_closure_fails_when_post_condition_returns_residual(
    _restore_traderjoe_v2_hook,
):
    hook = MagicMock(
        return_value=ClosureCheckResult(
            closed=False,
            protocol="traderjoe_v2",
            position_id="pos-1",
            residual={"bin_balances": {100: 4567}, "total_lb_tokens": 4567},
        )
    )
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool", "bin_ids": [100]},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is False


@pytest.mark.asyncio
async def test_verify_closure_fails_closed_when_post_condition_raises(
    _restore_traderjoe_v2_hook,
):
    hook = MagicMock(side_effect=RuntimeError("boom"))
    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xpool"},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is False  # fail-closed


@pytest.mark.asyncio
async def test_verify_closure_falls_back_to_in_memory_when_no_snapshot():
    """Legacy path (no pre_execution_positions) — uses get_open_positions()."""
    mgr = TeardownManager()
    # Strategy still reports open positions -> fail.
    strategy_with_residual = _make_strategy(open_positions=[object()])
    assert (
        await mgr._verify_closure(strategy=strategy_with_residual)
    ) is False

    # Strategy reports nothing open -> pass.
    strategy_clean = _make_strategy(open_positions=[])
    assert (await mgr._verify_closure(strategy=strategy_clean)) is True


@pytest.mark.asyncio
async def test_verify_closure_detailed_marks_in_memory_fallback_as_no_breakdown():
    """VIB-5085: the in-memory fallback (no pre-execution snapshot) returns a
    ClosureVerification with ``has_position_breakdown=False`` so lifecycle
    callers DON'T trust ``positions_closed=0`` and fall back to the intent
    count — otherwise a balance-driven teardown that closed real positions but
    exposes no PositionInfo rows would persist ``positions_closed=0`` on
    success (the inverse of the bug)."""
    mgr = TeardownManager()

    clean = await mgr._verify_closure_detailed(strategy=_make_strategy(open_positions=[]))
    assert clean.all_closed is True
    assert clean.has_position_breakdown is False
    assert clean.positions_total == 0

    # The snapshot path, by contrast, DOES carry a trustworthy breakdown.
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="some_unregistered_protocol",
            position_id="pos-x",
            chain="ethereum",
            details={},
        )
    )
    detailed = await mgr._verify_closure_detailed(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )
    assert detailed.all_closed is True
    assert detailed.has_position_breakdown is True
    assert detailed.positions_total == 1
    assert detailed.positions_closed == 1


@pytest.mark.asyncio
async def test_verify_closure_skips_protocols_without_post_condition(
    _restore_traderjoe_v2_hook,
):
    """A protocol with no registered hook does NOT block closure (logged only).

    The pre-existing in-memory check is still authoritative for those.
    """
    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="some_unregistered_protocol",
            position_id="pos-x",
            chain="ethereum",
            details={},
        )
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(open_positions=[]),
        pre_execution_positions=snapshot,
    )

    # No registered hook AND in-memory state is empty AND no failures recorded
    # -> verifier accepts. (Documents the current behaviour: protocols
    # without a registered post-condition rely on the in-memory check.)
    assert result is True


@pytest.mark.asyncio
async def test_verify_closure_aggregates_multiple_position_failures(
    _restore_traderjoe_v2_hook,
):
    """If two TJ V2 positions both have residuals, verify reports False."""
    calls = []

    def hook(*, position, wallet_address, gateway_client=None, rpc_url=None):
        calls.append(position.position_id)
        return ClosureCheckResult(
            closed=False,
            protocol="traderjoe_v2",
            position_id=position.position_id,
            residual={"bin_balances": {1: 1}},
        )

    _register_teardown_post_condition("traderjoe_v2", hook)

    mgr = TeardownManager()
    snapshot = _make_position_snapshot(
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-1",
            chain="avalanche",
            details={"pool_address": "0xa"},
        ),
        SimpleNamespace(
            protocol="traderjoe_v2",
            position_id="pos-2",
            chain="avalanche",
            details={"pool_address": "0xb"},
        ),
    )

    result = await mgr._verify_closure(
        strategy=_make_strategy(),
        pre_execution_positions=snapshot,
    )

    assert result is False
    assert calls == ["pos-1", "pos-2"]
