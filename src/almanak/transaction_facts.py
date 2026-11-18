"""Each committed transaction, logged for a test run's progress and exported as history.

One `transaction_executed` line follows each outcome the checkpoint committed: its
allocation and operation, whether it succeeded, the gas it paid and the fund's token
balance changes. Changes are the connector's receipt-derived execution effects, the same
exact base-unit amounts the fund inventory is updated with; a connector whose effects
cannot be read logs none rather than a guess. Gas is paid by the submitter and is stated
apart from the changes.
"""

import json
import sys
from datetime import UTC

EVENT = "transaction_executed"
# A decoded revert longer than this is cut; the full text stays in the checkpoint.
REVERT_CHARACTERS = 300


def log(outcome, connector, clock) -> None:
    """Print the transaction's facts; neither a fact nor the write ever stops the runtime."""
    try:
        effects = getattr(connector, "execution_effects", None)
        line = json.dumps(facts(outcome, effects, clock()))
        print(line, file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001 - diagnostics only
        return


def facts(outcome, effects, time) -> dict:
    """The facts of one committed transaction outcome; `effects` is the connector's."""
    return {"event": EVENT, "time": timestamp(time), **summary(outcome, effects)}


def summary(outcome, effects) -> dict:
    """What one committed transaction did, shared by the test log and the deployment history."""
    operation = getattr(outcome, "operation", None)
    target = getattr(operation, "target", None)
    allocation = getattr(target, "allocation", None)
    reason = getattr(outcome, "revert_reason", None)
    gas = getattr(outcome, "gas_paid", None)
    return {
        "allocation": allocation if isinstance(allocation, str) else None,
        "operation": operation_kind(operation),
        "success": bool(outcome.success),
        "tx_hash": getattr(outcome, "tx_hash", None),
        "gas_paid": None if gas is None else {"asset": str(gas.asset), "amount": str(gas.amount)},
        "changes": changes(outcome, effects),
        "revert_reason": reason[:REVERT_CHARACTERS] if isinstance(reason, str) else None,
    }


APPROVALS = ("approve", "gmx_approval", "gmx_exchange_approval")


def operation_kind(operation):
    """The operation's kind, with an approval of zero stated as `revoke`: several connectors
    revoke (at teardown, or before re-approving) by approving zero, and it must not read as
    a new approval."""
    kind = getattr(operation, "kind", None)
    if kind in APPROVALS:
        amount = getattr(operation, "amount", None)
        if getattr(amount, "amount", amount) == 0:
            return "revoke"
    return kind


def timestamp(time) -> str:
    return time.astimezone(UTC).isoformat().replace("+00:00", "Z")


def changes(outcome, effects) -> list[dict]:
    """The fund's nonzero token changes, or none when the connector cannot state them."""
    if effects is None:
        return []
    try:
        deltas = effects(outcome)
    except Exception:  # noqa: BLE001 - an unreadable effect is not a guessed one
        return []
    return [
        {"asset": str(delta.key.asset), "amount": str(delta.amount)}
        for delta in deltas
        if delta.amount
    ]
