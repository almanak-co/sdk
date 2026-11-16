"""Retained GMX captures predate scope-only Safe authority (blueprint 27).

Their configurations and capabilities still carry the author fields that sized Roles
quotas, and their permission partitions still list the quotas. Those values are archived
history: the current models load a capture without them, and every other retained fact,
including the grants as they were installed, stays exactly as captured.
"""

import json
from pathlib import Path

import pytest

from scripts.legacy_configuration.gmx_authoring import RETIRED

# A byte copy of connectors/gmx_v2/artifacts/compiled-deployments.json before the 1 October
# re-pin (git blob 4e5b920a), the closure the retained prepared datasets were collected and
# verified against. It records no roles: role verification arrived with the re-pin.
RETIRED_CLOSURE = Path(__file__).parent / "fixtures/gmx-v2-retired-compiled-deployments.json"


@pytest.fixture
def retired_closure(monkeypatch):
    """Verify retained prepared GMX data against the retired closure it was collected from.

    The datasets replay `eth_getCode` for the retired role contracts and carry no RoleStore
    reads, so the current closure refuses them. Only tests whose claim is economic
    accounting, not deployment identity, use this; it is not evidence for the current
    closure, whose prepared recapture is still owed (2026-09-30 rotation report).
    """
    from almanak.connectors.gmx_v2 import deployments, networks, published_prices

    table = json.loads(RETIRED_CLOSURE.read_text())
    monkeypatch.setattr(deployments, "compiled", lambda: table)
    monkeypatch.setattr(networks, "compiled", lambda: table)
    # The retired Oracle's ValidatedPrice had no raw bounds.
    monkeypatch.setattr(published_prices, "PRICE_RESULT", RETIRED_PRICE_RESULT)
    monkeypatch.setattr(published_prices, "accepted_price", retired_accepted_price)


RETIRED_PRICE_RESULT = "(address,uint256,uint256,uint256,address)[]"


def retired_accepted_price(report, binding):
    """The retired Oracle's ValidatedPrice: token, min, max, timestamp, provider."""
    from almanak.connectors.gmx_v2.oracle_reader import normalized_bounds

    return (
        report.asset.reference,
        *normalized_bounds(binding, report),
        report.observed_at - binding.timestamp_adjustment,
        binding.provider,
    )


def current(value):
    if isinstance(value, list):
        return [current(item) for item in value]
    if not isinstance(value, dict):
        return value
    partition = "grants" in value and "quotas" in value
    return {
        name: current(item)
        for name, item in value.items()
        if name not in RETIRED and not (partition and name == "quotas")
    }


def current_json(raw: bytes | str) -> str:
    return json.dumps(current(json.loads(raw)))


def replayed_operation(plan, config, state, outcome):
    """Replay one captured exchange or GM transaction under request-sized approvals.

    Captured entry approvals were sized to the remaining quota, so a captured creation saw
    allowances the current planner would revoke. The replay starts from zero allowances,
    applies each approval the planner asks for, and returns those approvals with the
    operation it finally plans: the request for a captured creation, or the approval of the
    captured asset for a captured entry approval. A captured revocation replays from its
    own observation.
    """
    from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval

    before, intent, captured = outcome.before, outcome.decision.intent, outcome.operation
    approval = isinstance(captured, ExchangeApproval)
    if approval and not captured.amount.amount:
        return (), plan(config, state, intent, before)
    entry = captured.amount.asset if approval else None
    return replay_from_zero(lambda observed: plan(config, state, intent, observed), before, entry)


def replay_from_zero(planned, before, entry):
    """Plan from zero allowances, applying each approval until an operation other than an
    approval, or the approval of `entry`, is planned."""
    from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval

    allowances = tuple(value.model_copy(update={"amount": 0}) for value in before.allowances)
    approvals: list = []
    for _ in range(len(allowances) + 1):
        operation = planned(before.model_copy(update={"allowances": allowances}))
        if not isinstance(operation, ExchangeApproval) or operation.amount.asset == entry:
            return tuple(approvals), operation
        approvals.append(operation)
        allowances = approved(allowances, operation.amount)
    raise AssertionError("request-sized approvals did not converge on one request")


def approved(allowances, amount):
    """`allowances` with the allowance of `amount.asset` set to `amount.amount`."""
    return tuple(
        value.model_copy(update={"amount": amount.amount}) if value.asset == amount.asset else value
        for value in allowances
    )


def following_request(outcomes, index):
    """The captured request that the captured approval at `index` prepared, if any."""
    from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval

    for outcome in outcomes[index + 1 :]:
        if outcome.kind == "gmx_transaction_outcome" and not isinstance(
            outcome.operation, ExchangeApproval
        ):
            return outcome.operation
    return None


def assert_request_sized(approvals, operation, captured, request):
    """Approvals cover exactly the escrowed inputs; the request itself is unchanged.

    `request` is the captured request that follows a captured approval. A replayed entry
    approval keeps the captured target and asset, and its amount is exactly that asset's
    escrow in the request, where the capture sized it to the remaining quota.
    """
    from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval

    if not isinstance(captured, ExchangeApproval):
        assert_request_unchanged(approvals, operation, captured)
    elif captured.amount.amount:
        assert_approval_matches(operation, captured, request)
        assert_escrow_sized(approvals, operation, captured, request)
    else:
        assert operation == captured


def assert_approval_matches(operation, captured, request):
    """The replayed entry approval and the following request keep the captured target and
    asset."""
    from almanak.connectors.gmx_v2.exchange_operations import ExchangeApproval

    assert isinstance(operation, ExchangeApproval)
    assert (operation.target, operation.amount.asset) == (captured.target, captured.amount.asset)
    assert request is not None and request.target == captured.target


def assert_escrow_sized(approvals, operation, captured, request):
    """Every approval, the entry approval included, equals a positive escrow of the request."""
    escrow = {value.asset: value.amount for value in request.inputs}
    assert operation.amount.amount == escrow[captured.amount.asset] > 0
    assert all(escrow[value.amount.asset] == value.amount.amount for value in approvals)


def assert_request_unchanged(approvals, operation, captured):
    """A replayed request equals the capture, and its approvals cover exactly its inputs."""
    assert operation == captured
    assert all(value.target == captured.target for value in approvals)
    assert [value.amount for value in approvals] == [
        value for value in captured.inputs if value.amount
    ]
