"""Cancellation observes one locally scoped market request and refuses unsupported fields."""

import pytest

from almanak.connectors.gmx_v2.order_reader import validate_absent_order, validate_local_order
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS
from tests.gmx_evidence import accepted


def test_current_order_refuses_changed_recipients_cross_chain_and_callback_behavior():
    state = accepted()
    target = state.config.target("Main")
    addresses = (
        (target.fund.address,) * 3
        + (ZERO_ADDRESS,) * 2
        + (target.market.share_asset.reference, target.collateral_asset.reference, ())
    )
    numbers = (2,) + (0,) * 12
    flags = (True, False, False, False)
    validate_local_order(target, addresses, numbers, flags, ())
    with pytest.raises(DataError, match="recipients"):
        validate_local_order(target, (ZERO_ADDRESS, *addresses[1:]), numbers, flags, ())
    with pytest.raises(DataError, match="local market"):
        validate_local_order(target, addresses, (*numbers[:-1], 1), flags, ())
    # A local request names no UI fee receiver, so its snapshotted factor is zero.
    with pytest.raises(DataError, match="local market"):
        validate_local_order(target, addresses, (*numbers[:9], 1, *numbers[10:]), flags, ())
    with pytest.raises(DataError, match="extra behavior"):
        validate_local_order(target, addresses, numbers, (True, True, False, False), ())
    with pytest.raises(DataError, match="extra behavior"):
        validate_local_order(target, addresses, numbers, flags, (bytes(32),))
    validate_absent_order((ZERO_ADDRESS,) * 7 + ((),), (0,) * 13, (False,) * 4, ())
    with pytest.raises(DataError, match="inconsistent"):
        validate_absent_order((ZERO_ADDRESS,) * 7 + ((),), (1,) + (0,) * 12, (False,) * 4, ())


def test_unresolved_checkpoint_cannot_lose_its_original_admitted_decision():
    state = accepted()
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    with pytest.raises(ValueError, match="original admitted decision"):
        Checkpoint.model_validate({**state.model_dump(), "decision": None})
    changed = state.decision.model_copy(update={"identity": "0" * 32})
    with pytest.raises(ValueError, match="original admitted decision"):
        Checkpoint.model_validate({**state.model_dump(), "decision": changed})
