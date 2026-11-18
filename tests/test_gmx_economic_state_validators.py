"""Simulated GMX observations and results refuse native requests, future state and drift.

The validators run on stand-in field values, so each refusal is isolated from the
unrelated validation of the full snapshot and result models.
"""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.economic_state import EconomicResult, EconomicSnapshot
from almanak.connectors.gmx_v2.exchange_domain import Revoke
from almanak.connectors.gmx_v2.exchange_observations import inventory_assets
from almanak.models import AssetAmount
from tests import gmx_synthetic_frames as frames

TARGET = frames.exchange_target()
OTHER = TARGET.model_copy(update={"allocation": "other"})
NATIVE = inventory_assets(TARGET)[-1]
WRAPPED = TARGET.market.wrapped_native


def snapshot(**updates):
    values = {
        "target": TARGET,
        "request": None,
        "pending_order": None,
        "pending_liquidity": None,
        "escrow": None,
        "block": SimpleNamespace(timestamp=100),
        "simulation_time": 100,
    }
    return SimpleNamespace(**{**values, **updates})


def test_hypothetical_snapshot_admits_current_state_and_its_own_escrow():
    value = snapshot(escrow=SimpleNamespace(intent=SimpleNamespace(target=TARGET)))
    assert EconomicSnapshot.hypothetical(value) is value
    assert EconomicSnapshot.hypothetical(snapshot(simulation_time=101)) is not None


@pytest.mark.parametrize("field", ["request", "pending_order", "pending_liquidity"])
def test_hypothetical_snapshot_refuses_native_requests(field):
    with pytest.raises(ValueError, match="cannot claim a native request"):
        EconomicSnapshot.hypothetical(snapshot(**{field: object()}))


def test_hypothetical_snapshot_refuses_another_escrow_target_and_future_state():
    other = SimpleNamespace(intent=SimpleNamespace(target=OTHER))
    with pytest.raises(ValueError, match="another escrow target"):
        EconomicSnapshot.hypothetical(snapshot(escrow=other))
    with pytest.raises(ValueError, match="future canonical state"):
        EconomicSnapshot.hypothetical(snapshot(simulation_time=99))


def native(amount=0, asset=NATIVE):
    return AssetAmount(asset=asset, amount=amount)


def result(**updates):
    values = {
        "intent": SimpleNamespace(target=TARGET),
        "before": SimpleNamespace(target=TARGET, native_balance=native()),
        "after": SimpleNamespace(target=TARGET, escrow=None, allowances=(native(),)),
        "complete": True,
        "phase": "settled",
        "success": True,
        "reason": None,
        "gas_paid": native(1),
        "keeper_paid": native(2),
        "fund_refund": native(3),
    }
    return SimpleNamespace(**{**values, **updates})


def test_result_scope_admits_settled_waiting_and_cancelled_revocations():
    settled = result()
    assert EconomicResult.scope(settled) is settled
    waiting = result(
        complete=False,
        phase="waiting",
        after=SimpleNamespace(target=TARGET, escrow=object(), allowances=()),
    )
    assert EconomicResult.scope(waiting) is waiting
    # A cancelled revocation still succeeds: authority was cleared, the request was not.
    revoke = Revoke(target=TARGET)
    cancelled = result(intent=revoke, phase="cancelled", reason="request cancelled")
    assert EconomicResult.scope(cancelled) is cancelled


def test_result_scope_refuses_another_target():
    with pytest.raises(ValueError, match="exact configured target"):
        EconomicResult.scope(result(before=SimpleNamespace(target=OTHER)))
    with pytest.raises(ValueError, match="exact configured target"):
        EconomicResult.scope(result(after=SimpleNamespace(target=OTHER)))


def test_result_scope_requires_completion_to_match_remaining_escrow_and_authority():
    pending = SimpleNamespace(target=TARGET, escrow=object(), allowances=())
    with pytest.raises(ValueError, match="remaining escrow"):
        EconomicResult.scope(result(after=pending))
    with pytest.raises(ValueError, match="remaining escrow"):
        EconomicResult.scope(result(complete=False))
    with pytest.raises(ValueError, match="remaining escrow"):
        EconomicResult.scope(result(phase="accepted"))
    granted = SimpleNamespace(target=TARGET, escrow=None, allowances=(native(5, WRAPPED),))
    with pytest.raises(ValueError, match="remaining escrow"):
        EconomicResult.scope(result(intent=Revoke(target=TARGET), after=granted))
    # Outstanding allowance does not keep a non-revocation intent open.
    assert EconomicResult.scope(result(after=granted)) is not None


def test_result_scope_requires_cancellation_to_keep_its_reason():
    with pytest.raises(ValueError, match="unsuccessful reason"):
        EconomicResult.scope(result(phase="cancelled", reason="cancelled"))
    with pytest.raises(ValueError, match="unsuccessful reason"):
        EconomicResult.scope(result(phase="cancelled", success=False))
    with pytest.raises(ValueError, match="unsuccessful reason"):
        EconomicResult.scope(result(reason="unexpected"))
    assert (
        EconomicResult.scope(result(phase="cancelled", success=False, reason="cancelled"))
        is not None
    )


@pytest.mark.parametrize("field", ["gas_paid", "keeper_paid", "fund_refund"])
def test_result_scope_requires_native_cost_units(field):
    with pytest.raises(ValueError, match="exact native units"):
        EconomicResult.scope(result(**{field: native(1, WRAPPED)}))
