"""VIB-6162 — the teardown lane attaches the bound, and refuses rather than widening.

These cover the WIRING, which is where the previous attempt actually failed. The clamp
logic itself was defensible; it was never reached for the identifier shape a real
strategy emits. So the properties asserted here are "did the bound reach the intent"
and "what happens when it cannot", not "is the arithmetic right".
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.teardown.lp_clamp import LpClampUnresolved
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.teardown.teardown_manager import TeardownManager

POOL = "0xcdac0d6c6c59727a65f871236188350531885c43"


class _Intent:
    """Stand-in exposing the fields the attach path reads.

    ``intent_type`` defaults to the REAL ``IntentType`` enum, not the string
    ``"LP_CLOSE"``. A stub that fabricates a plain string here is what let an inert fix
    ship: on a real intent ``str(intent_type)`` is ``"INTENTTYPE.LP_CLOSE"``, so a guard
    comparing it to ``"LP_CLOSE"`` rejects every production intent while this test
    passes. Fabricating an input production never produces is not a stand-in, it is a
    different test.
    """

    def __init__(self, intent_type=IntentType.LP_CLOSE, protocol="aerodrome", position_id=POOL, params=None):
        self.intent_type = intent_type
        self.protocol = protocol
        self.position_id = position_id
        self.pool = None
        self.protocol_params = params or {}

    def model_copy(self, *, update):
        clone = _Intent(self.intent_type, self.protocol, self.position_id)
        clone.protocol_params = update.get("protocol_params", self.protocol_params)
        return clone


def _bare(helpers: TeardownRunnerHelpers) -> TeardownManager:
    """Construct only what the attach path touches, avoiding full manager wiring."""
    mgr = TeardownManager.__new__(TeardownManager)
    mgr.runner_helpers = helpers
    return mgr


@pytest.mark.asyncio
async def test_the_bound_reaches_the_intent():
    """Catches the whole previous failure: a clamp that never reaches the compiler."""

    async def outstanding(_strategy, _protocol, _pid, _pool):
        return Decimal("1.5")

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    out, refusal = await mgr._attach_lp_outstanding(object(), _Intent())
    assert refusal is None
    assert out.protocol_params["deployment_outstanding_lp"] == "1.5"


@pytest.mark.asyncio
async def test_existing_protocol_params_are_preserved():
    """Catches the bound silently dropping a connector parameter the intent needed."""

    async def outstanding(_s, _p, _i, _pool):
        return Decimal("2")

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    out, _ = await mgr._attach_lp_outstanding(object(), _Intent(params={"keep": "me"}))
    assert out.protocol_params["keep"] == "me"
    assert out.protocol_params["deployment_outstanding_lp"] == "2"


@pytest.mark.asyncio
async def test_an_unresolvable_bound_refuses_the_close():
    """The inertness gate at the wiring layer: unresolved must NOT compile unbounded."""

    async def outstanding(_s, _p, _i, _pool):
        raise LpClampUnresolved("no canonical identity")

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    out, refusal = await mgr._attach_lp_outstanding(object(), _Intent())
    assert refusal is not None and "cannot bound" in refusal
    assert "deployment_outstanding_lp" not in out.protocol_params


@pytest.mark.asyncio
async def test_an_unclamped_venue_passes_through_untouched():
    """Curve today. The manifest's decision, not a silently skipped clamp."""

    async def outstanding(_s, _p, _i, _pool):
        return None

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    intent = _Intent(protocol="curve")
    out, refusal = await mgr._attach_lp_outstanding(object(), intent)
    assert refusal is None
    assert out is intent
    assert "deployment_outstanding_lp" not in out.protocol_params


@pytest.mark.asyncio
async def test_non_lp_close_intents_are_untouched():
    """A REPAY or WITHDRAW must not be delayed or altered by the LP clamp."""
    called = False

    async def outstanding(_s, _p, _i, _pool):
        nonlocal called
        called = True
        return Decimal("1")

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    intent = _Intent(intent_type=IntentType.REPAY)
    out, refusal = await mgr._attach_lp_outstanding(object(), intent)
    assert out is intent and refusal is None
    assert called is False


@pytest.mark.asyncio
async def test_a_real_intent_enum_is_recognised():
    """The regression this file missed. Uses a genuine Intent, not a stand-in."""
    from almanak.framework.intents.vocabulary import Intent

    real = Intent.lp_close(position_id="USDC/DAI/stable", pool="USDC/DAI/stable", protocol="aerodrome")

    async def outstanding(_s, _p, _i, _pool):
        return Decimal("0.6")

    mgr = _bare(TeardownRunnerHelpers(get_lp_outstanding=outstanding))
    out, refusal = await mgr._attach_lp_outstanding(object(), real)
    assert refusal is None
    assert out.protocol_params["deployment_outstanding_lp"] == "0.6", (
        "a REAL LPCloseIntent was not recognised as LP_CLOSE — the clamp is inert"
    )


@pytest.mark.asyncio
async def test_an_unwired_helper_leaves_behaviour_unchanged():
    """Tests and legacy callers construct TeardownRunnerHelpers() with nothing wired."""
    mgr = _bare(TeardownRunnerHelpers())
    intent = _Intent()
    out, refusal = await mgr._attach_lp_outstanding(object(), intent)
    assert out is intent and refusal is None
