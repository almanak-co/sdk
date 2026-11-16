"""Canonical ABI and receipt effects must match admitted token and fee escrow exactly."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.gmx_v2 import abi, reconciliation
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import cancellation_state
from tests.gmx_native_runtime import current_position as checkpoint


def test_protocol_calls_and_successful_receipt_effects_match_frozen_runtime_evidence():
    state = checkpoint()
    cancelled = cancellation_state()[1]
    rows = [o for o in state.outcomes if o.kind == "gmx_transaction_outcome"] + [cancelled]
    for row in rows:
        assert abi.call(row.operation) == row.call
        pending = SimpleNamespace(operation=row.operation, before=row.before, decision=row.decision)
        request = reconciliation.completed_request(pending, row.receipt, row.after.block)
        assert request == row.request
        assets, owned = reconciliation.transaction_effects(
            pending, row.receipt, row.after, request, True
        )
        assert assets == row.assets and owned == row.after.owned
        assert reconciliation.transaction_effects(
            pending, row.receipt, row.after, request, False
        ) == ((), row.before.owned)


def test_approval_and_request_receipts_cannot_hide_unexpected_escrow_or_allowance():
    approve, create = checkpoint().outcomes[:2]
    with pytest.raises(DataError, match="admitted allowance"):
        reconciliation.validate_transaction_effects(
            approve.operation,
            approve.before,
            approve.after.model_copy(update={"allowance": 0}),
            approve.assets,
        )
    with pytest.raises(DataError, match="exact admitted escrow"):
        reconciliation.validate_transaction_effects(
            create.operation,
            create.before,
            create.after,
            (create.assets[0].model_copy(update={"amount": 0}), *create.assets[1:]),
        )


def test_simulation_requires_canonical_single_nonzero_request_identity():
    approval, creation = checkpoint().outcomes[:2]
    cancellation = cancellation_state()[1]
    abi.validate_simulation(
        approval.operation, "0x" + encode(("bool",), (True,)).hex(), approval.before
    )
    with pytest.raises(ValueError, match="refused"):
        abi.validate_simulation(
            approval.operation, "0x" + encode(("bool",), (False,)).hex(), approval.before
        )
    values = (b"", bytes.fromhex(creation.request.key[2:]))
    raw = encode(("bytes[]",), (values,))
    abi.validate_simulation(creation.operation, "0x" + raw.hex(), creation.before)
    for altered in [(b"", bytes(32)), (b"",), (b"unexpected", values[1])]:
        with pytest.raises(ValueError, match="exactly one request"):
            abi.validate_simulation(
                creation.operation, "0x" + encode(("bytes[]",), (altered,)).hex(), creation.before
            )
    with pytest.raises(ValueError, match="noncanonical"):
        abi.validate_simulation(creation.operation, "0x" + (raw + bytes(32)).hex(), creation.before)
    abi.validate_simulation(cancellation.operation, "0x", cancellation.before)
    with pytest.raises(ValueError, match="unexpected data"):
        abi.validate_simulation(cancellation.operation, "0x01", cancellation.before)
