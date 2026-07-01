"""VIB-5568: teardown recovery of stranded pending-order collateral.

The recovery half of VIB-5116. Two layers:

1. ``full_close_intents`` maps a ``kind="pending_order"`` residual ``PositionInfo``
   to an ``Intent.perp_cancel_order`` (routed to the venue compiler), and
   FAIL-CLOSES (skips) any residual without a real bytes32 order key or the
   ``residual_unverified`` sentinel — never a mis-targeted cancel.
2. ``_recover_pending_order_intents`` turns the framework-discovered residuals
   into cancel intents appended to the teardown intent list (so they flow through
   the committed ``_execute_intents`` pipeline), and reports ``incomplete`` when
   the residual read was UNMEASURED (Empty != Zero, fail-closed).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.intents import Intent, IntentType
from almanak.framework.teardown import PositionInfo, PositionType, full_close_intents
from almanak.framework.teardown.completeness import check_intent_coverage

_KEY = "0x" + "abcd1234" * 8  # full bytes32


def _pending_residual(order_key: str = _KEY, *, cancellable: bool = True) -> PositionInfo:
    details = {
        "source": "teardown_residual_discovery",
        "kind": "pending_order",
        "order_key": order_key,
        "venue": "gmx_v2",
        "cancellable": cancellable,
    }
    if not cancellable:
        details["seconds_until_cancellable"] = 210
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id=order_key,
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details=details,
    )


def _unmeasured_sentinel() -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.PERP,
        position_id="gmx_v2-residual-unverified-arbitrum",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={"source": "teardown_residual_discovery", "kind": "residual_unverified", "error": "rpc blip"},
    )


# --------------------------------------------------------------------------- #
# Layer 1: full_close_intents residual -> cancel mapping
# --------------------------------------------------------------------------- #


def test_pending_order_residual_maps_to_cancel():
    out = full_close_intents([_pending_residual()])
    assert len(out) == 1
    cancel = out[0]
    assert cancel.intent_type == IntentType.PERP_CANCEL_ORDER
    assert cancel.order_key == _KEY
    assert cancel.protocol == "gmx_v2"
    assert cancel.chain == "arbitrum"


def test_unmeasured_sentinel_produces_no_cancel():
    """A residual_unverified sentinel is NOT a cancellable order — it must stay a
    loud uncovered residual (completeness fails), never fabricate a cancel."""
    out = full_close_intents([_unmeasured_sentinel()])
    assert out == []


@pytest.mark.parametrize("bad_key", ["", "0xdead", "gmx-order-arbitrum-0"])
def test_pending_order_without_bytes32_key_is_skipped(bad_key):
    """Fail-closed: no real bytes32 key -> skip (stays surfaced), never a
    mis-targeted cancel from a zero-padded truncated key."""
    residual = _pending_residual(order_key=bad_key)
    # position_id also carries the bad key so there is no valid fallback.
    residual = PositionInfo(
        position_type=PositionType.PERP,
        position_id=bad_key or "gmx-order-arbitrum-0",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
        details={"kind": "pending_order", "order_key": bad_key, "venue": "gmx_v2"},
    )
    assert full_close_intents([residual]) == []


def test_only_measured_pending_order_becomes_cancel_in_mixed_set():
    out = full_close_intents([_pending_residual(), _unmeasured_sentinel()])
    assert len(out) == 1
    assert out[0].intent_type == IntentType.PERP_CANCEL_ORDER


def test_not_yet_cancellable_pending_order_is_deferred_not_cancelled():
    """Age-gate (VIB-5568): an order still inside GMX's ~300s cancel window is NOT
    turned into a doomed cancel — full_close skips it (recovery lane defers loud)."""
    assert full_close_intents([_pending_residual(cancellable=False)]) == []


# --------------------------------------------------------------------------- #
# Layer 2: _recover_pending_order_intents runner lane
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_recover_appends_cancel_intents():
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    strategy = object()
    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=[_pending_residual()],
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, strategy, [], None)

    assert len(intents) == 1
    assert intents[0].intent_type == IntentType.PERP_CANCEL_ORDER
    assert incomplete is False
    assert warning is None


@pytest.mark.asyncio
async def test_recover_preserves_existing_intents_and_appends():
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    sentinel_existing = object()  # a pre-existing strategy close intent
    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=[_pending_residual()],
    ):
        intents, _, _ = await _recover_pending_order_intents(None, object(), [sentinel_existing], None)

    assert intents[0] is sentinel_existing
    assert intents[1].intent_type == IntentType.PERP_CANCEL_ORDER


@pytest.mark.asyncio
async def test_recover_defers_not_yet_cancellable_order():
    """A fresh (not-yet-cancellable) pending order is deferred LOUD: no cancel is
    built (it would revert), teardown is not certified clean, and the warning tells
    the operator when it becomes recoverable."""
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=[_pending_residual(cancellable=False)],
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, object(), [], None)

    assert intents == []  # no doomed cancel built
    assert incomplete is True
    assert warning is not None and "not yet cancellable" in warning


@pytest.mark.asyncio
async def test_recover_mixed_cancellable_and_deferred():
    """A cancellable order is recovered while a fresh one is deferred — cancels for
    the former, incomplete+warning for the latter."""
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    old_key = _KEY
    young_key = "0x" + "77" * 32
    residuals = [
        _pending_residual(order_key=old_key, cancellable=True),
        _pending_residual(order_key=young_key, cancellable=False),
    ]
    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=residuals,
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, object(), [], None)

    assert [i.intent_type for i in intents] == [IntentType.PERP_CANCEL_ORDER]
    assert intents[0].order_key == old_key
    assert incomplete is True  # the young one is not recovered
    assert warning is not None and "not yet cancellable" in warning


@pytest.mark.asyncio
async def test_recover_unmeasured_read_marks_incomplete():
    """An UNMEASURED residual read (sentinel) yields incomplete=True so teardown
    is not certified clean while a strand may remain (Empty != Zero)."""
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=[_unmeasured_sentinel()],
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, object(), [], None)

    assert intents == []  # nothing cancellable
    assert incomplete is True
    assert warning is not None and "UNMEASURED" in warning


@pytest.mark.asyncio
async def test_recover_no_residuals_is_noop():
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        return_value=[],
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, object(), [], None)

    assert intents == []
    assert incomplete is False
    assert warning is None


@pytest.mark.asyncio
async def test_recover_never_raises_on_discovery_failure():
    """Discovery raising must NOT raise (never blocks risk reduction) but fails
    CLOSED: we could not enumerate residuals, so teardown is not certified clean
    (incomplete=True + a manual-check warning). Existing intents are untouched."""
    from almanak.framework.runner.runner_teardown import _recover_pending_order_intents

    existing = [object()]
    with patch(
        "almanak.framework.teardown.residual_discovery.discover_teardown_residuals",
        side_effect=RuntimeError("gateway down"),
    ):
        intents, incomplete, warning = await _recover_pending_order_intents(None, object(), existing, None)

    assert intents == existing  # untouched — never blocks the next risk-reducing intent
    assert incomplete is True  # fail-closed: discovery couldn't complete → don't certify clean
    assert warning is not None and "manual on-chain verification" in warning


# --------------------------------------------------------------------------- #
# Layer 3: completeness gate credits a key-matched cancel (the audit blocker)
#
# The recovery lane appends a PERP_CANCEL_ORDER; the completeness gate is the
# FINAL word on teardown status. If it does not credit the cancel against the
# pending_order residual, a successfully-recovered order still marks the teardown
# FAILED. These assert the gate passes ONLY for a matching order_key.
# --------------------------------------------------------------------------- #


def test_completeness_pending_order_covered_by_matching_cancel():
    residual = _pending_residual()
    cancel = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="arbitrum")
    report = check_intent_coverage([residual], [cancel])
    assert report.complete is True
    assert report.uncovered == ()


def test_completeness_pending_order_not_covered_by_mismatched_cancel():
    """Fund-safety: a cancel for a DIFFERENT order must not cover this residual."""
    residual = _pending_residual()
    other_key = "0x" + "9999" * 16
    cancel = Intent.perp_cancel_order(order_key=other_key, protocol="gmx_v2", chain="arbitrum")
    report = check_intent_coverage([residual], [cancel])
    assert report.complete is False


def test_completeness_pending_order_not_covered_by_perp_close():
    """A pending order is reclaimed by a cancel, never by a PERP_CLOSE."""
    residual = _pending_residual()
    close = Intent.perp_close(market="ETH/USD", collateral_token="USDC", is_long=True, protocol="gmx_v2")
    report = check_intent_coverage([residual], [close])
    assert report.complete is False


def test_completeness_unverified_sentinel_stays_uncovered():
    """An UNMEASURED residual sentinel has no cancellable key — never coverable."""
    sentinel = _unmeasured_sentinel()
    cancel = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="arbitrum")
    report = check_intent_coverage([sentinel], [cancel])
    assert report.complete is False


# --------------------------------------------------------------------------- #
# Layer 4: top-level IntentCompiler dispatch (the real-fork blocker)
#
# The TeardownManager compiles via IntentCompiler.compile(), NOT the connector
# compiler directly. Without a PERP_CANCEL_ORDER branch there, the cancel fails
# "not supported by the compiler" and the whole recovery is unreachable from
# teardown — exactly the gap the connector-only unit tests missed.
# --------------------------------------------------------------------------- #


def test_intent_compiler_dispatches_perp_cancel_to_connector():
    from almanak.framework.intents.compiler import (
        CompilationStatus,
        IntentCompiler,
        IntentCompilerConfig,
    )

    compiler = IntentCompiler(chain="arbitrum", config=IntentCompilerConfig(allow_placeholder_prices=True))
    intent = Intent.perp_cancel_order(order_key=_KEY, protocol="gmx_v2", chain="arbitrum")

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.SUCCESS, result.error
    assert len(result.transactions) == 1
    tx = result.transactions[0]
    assert tx.value == 0
    assert tx.data[:10] == "0x7489ec23"  # cancelOrder(bytes32) — real 4-byte selector (VIB-5568)
    assert tx.data[10:].lower() == _KEY[2:].lower()
