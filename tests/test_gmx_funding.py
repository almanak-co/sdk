"""Actual short-position accrual creates owned claims; external changes are not adopted."""

import gzip

import pytest
from eth_abi import decode, encode

from almanak.connectors.gmx_v2 import abi, funding, lifecycle, reconciliation
from almanak.connectors.gmx_v2.operations import ClaimFunding
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json
from tests.retained_fixtures import retained


def diagnostic():
    path = (
        retained("2026-09-08-gmx-position-paths")
        / "diagnostic-short-unclaimed-funding/checkpoint.json.gz"
    )
    return Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))


def test_actual_owned_request_events_reproduce_claimable_funding_exactly():
    retained = ()
    for outcome in diagnostic().outcomes:
        if outcome.kind != "gmx_request_outcome":
            continue
        owned = outcome.before.owned.model_copy(update={"claimable_funding": retained})
        before = outcome.before.model_copy(update={"owned": owned})
        retained, issues = funding.accrue(before, outcome.after, outcome.receipt, outcome.request)
        assert not issues
        assert {value.asset: value.amount for value in retained} == {
            value.asset: value.amount for value in outcome.after.claimable_funding if value.amount
        }
    assert [value.amount for value in retained] == [49298439271, 787]


def test_unexplained_new_claimable_balance_is_visible_without_becoming_owned():
    outcome = next(o for o in diagnostic().outcomes if o.kind == "gmx_request_outcome")
    amounts = list(outcome.after.claimable_funding)
    amounts[0] = amounts[0].model_copy(update={"amount": 1})
    after = outcome.after.model_copy(update={"claimable_funding": tuple(amounts)})
    retained, issues = funding.accrue(outcome.before, after, outcome.receipt, outcome.request)
    assert retained == ()
    assert issues == ("GMX claimable funding includes unexplained external changes",)


def owned_closed():
    outcome = [o for o in diagnostic().outcomes if o.kind == "gmx_request_outcome"][-1]
    after = outcome.after
    owned = after.owned.model_copy(update={"claimable_funding": after.claimable_funding})
    return after.model_copy(update={"owned": owned})


def test_claim_refuses_open_position_and_unowned_entitlements():
    before = owned_closed()
    assert funding.claimable(before) == before.claimable_funding
    held = next(o.after.position for o in diagnostic().outcomes if o.after.position)
    with pytest.raises(ValueError, match="full position closure"):
        funding.claimable(before.model_copy(update={"position": held}))
    owned = before.owned.model_copy(update={"claimable_funding": ()})
    with pytest.raises(DataError, match="unowned"):
        funding.claimable(before.model_copy(update={"owned": owned}))


def test_claim_abi_binds_exact_assets_and_requires_canonical_simulated_entitlements():
    before = owned_closed()
    operation = ClaimFunding(target=before.target, amounts=funding.claimable(before))
    payload = abi.claim_data(before.target)
    markets, tokens, receiver = decode(("address[]", "address[]", "address"), payload[4:])
    assert markets == (before.target.market.share_asset.reference,) * 2
    assert tokens == tuple(value.asset.reference for value in operation.amounts)
    assert receiver == before.target.fund.address
    amounts = tuple(value.amount for value in operation.amounts)
    result = encode(("uint256[]",), (amounts,))
    abi.validate_simulation(operation, "0x" + result.hex(), before)
    with pytest.raises(ValueError, match="retained entitlements"):
        abi.validate_simulation(operation, "0x" + encode(("uint256[]",), ((0, 0),)).hex(), before)
    with pytest.raises(ValueError, match="noncanonical"):
        abi.validate_simulation(operation, "0x" + (result + bytes(32)).hex(), before)
    with pytest.raises(ValueError, match="both configured"):
        ClaimFunding(target=before.target, amounts=tuple(reversed(operation.amounts)))
    with pytest.raises(ValueError, match="nonzero"):
        ClaimFunding(
            target=before.target,
            amounts=tuple(a.model_copy(update={"amount": 0}) for a in operation.amounts),
        )


def successful_claim():
    path = (
        retained("2026-09-08-gmx-position-paths")
        / "arbitrum-short-safe-funding-runtime/checkpoint.json.gz"
    )
    state = Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))
    return state.outcomes[-1]


def test_actual_funding_claim_reconciles_both_assets_and_clears_owned_entitlements():
    outcome = successful_claim()
    funding.validate_claim(
        outcome.operation, outcome.before, outcome.after, outcome.receipt, outcome.assets
    )
    assert outcome.success and not outcome.accounting_issues
    assert outcome.decision.intent.kind == "revoke_position"
    assert outcome.strategy_result().status == "completed"
    assert [a.amount for a in outcome.operation.amounts] == [49298439271, 787]
    assert outcome.after.owned.claimable_funding == ()
    assert not any(a.amount for a in outcome.after.claimable_funding)
    assert outcome.gas_paid.amount > 0
    assert outcome.before.payer != outcome.operation.target.fund


def test_claim_requires_complete_events_exact_wallet_receipts_and_zero_remaining_balance():
    outcome = successful_claim()
    operation, before, after = outcome.operation, outcome.before, outcome.after
    with pytest.raises(DataError, match="one event per"):
        funding.validate_claim(
            operation, before, after, {**outcome.receipt, "logs": []}, outcome.assets
        )
    assets = tuple(a.model_copy(update={"amount": a.amount + 1}) for a in outcome.assets)
    with pytest.raises(DataError, match="measured fund token"):
        funding.validate_claim(operation, before, after, outcome.receipt, assets)
    changed = after.model_copy(update={"allowance": after.allowance + 1})
    with pytest.raises(DataError, match="unrelated"):
        funding.validate_claim(operation, before, changed, outcome.receipt, outcome.assets)
    changed = after.model_copy(update={"claimable_funding": before.claimable_funding})
    with pytest.raises(DataError, match="remaining entitlement"):
        funding.validate_claim(operation, before, changed, outcome.receipt, outcome.assets)


def test_owned_plus_foreign_same_market_token_funding_cannot_be_claimed():
    before = owned_closed()
    amounts = list(before.claimable_funding)
    amounts[0] = amounts[0].model_copy(update={"amount": amounts[0].amount + 1})
    foreign = before.model_copy(update={"claimable_funding": tuple(amounts)})
    assert foreign.owned == before.owned
    with pytest.raises(DataError, match="unowned funding entitlements"):
        funding.claimable(foreign)


def test_initialization_refuses_preexisting_same_fund_market_token_funding():
    from tests.gmx_evidence import checkpoint

    state = checkpoint()
    before = state.closure[0]
    amounts = list(before.claimable_funding)
    amounts[0] = amounts[0].model_copy(update={"amount": 1})
    foreign = before.model_copy(update={"claimable_funding": tuple(amounts)})
    with pytest.raises(ValueError, match="cannot adopt existing"):
        lifecycle.validate_initial(state, (foreign,))


def test_funding_claim_replay_restores_ownership_without_double_credit(monkeypatch):
    from types import SimpleNamespace

    row = successful_claim()
    pending = SimpleNamespace(
        operation=row.operation,
        before=row.before,
        decision=row.decision,
        nonce=row.nonce,
        tx_hash=row.tx_hash,
        call=row.call,
        execution=row.execution,
    )
    monkeypatch.setattr(
        reconciliation,
        "snapshot",
        lambda *args: row.after.model_copy(update={"owned": row.before.owned}),
    )
    monkeypatch.setattr(
        reconciliation, "charged_fees", lambda *args: (row.gas_paid, row.op_fee_evidence)
    )
    for _ in range(2):
        replay = reconciliation.reconcile(SimpleNamespace(), pending, row.receipt, row.after.block)
        assert replay == row
        assert replay.after.owned.claimable_funding == ()


def test_missing_historical_funding_measurement_is_not_replayed_as_zero():
    from almanak.connectors.gmx_v2.observations import Snapshot

    observed = successful_claim().after.model_dump()
    del observed["claimable_funding"]
    with pytest.raises(ValueError, match="claimable_funding"):
        Snapshot.model_validate(observed)
    observed["claimable_funding"] = ()
    with pytest.raises(ValueError, match="both market assets"):
        Snapshot.model_validate(observed)


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
def test_each_short_path_delivers_all_declared_outer_author_actions(chain, profile):
    path = (
        retained("2026-09-08-gmx-position-paths")
        / f"{chain}-short-{profile}-runtime/checkpoint.json.gz"
    )
    state = Checkpoint.model_validate_json(current_json(gzip.decompress(path.read_bytes())))
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    results = [o.strategy_result() for o in state.outcomes[: state.delivered]]
    completed = [r for r in results if r.status == "completed"]
    assert [r.intent.kind for r in completed] == [
        "increase_position",
        "increase_position",
        "decrease_position",
        "close_position",
        "revoke_position",
    ]
    assert all(r.success and not r.accounting_issues for r in completed)
    assert all(not o.needs_cleanup and not o.owned.claimable_funding for o in state.closure)


def receipt_with_another_execution(first, other):
    # Synthetic batching uses actual decoded event payloads with one receipt identity.
    receipt = dict(first.receipt)
    logs = list(receipt["logs"])
    offset = max(int(log["logIndex"], 16) for log in logs) + 1
    for index, log in enumerate(other.receipt["logs"]):
        logs.append(
            {
                **log,
                "logIndex": hex(offset + index),
                "transactionHash": receipt["transactionHash"],
                "blockHash": receipt["blockHash"],
                "blockNumber": receipt["blockNumber"],
            }
        )
    return {**receipt, "logs": logs}


def test_same_account_market_batched_increase_cannot_attribute_foreign_funding():
    rows = [o for o in diagnostic().outcomes if o.kind == "gmx_request_outcome"]
    own, foreign = rows[:2]
    assert (
        own.operation.target == foreign.operation.target and own.request.key != foreign.request.key
    )
    receipt = receipt_with_another_execution(own, foreign)
    with pytest.raises(DataError, match="one PositionIncrease"):
        reconciliation.position_effect(own.request, own.before, own.after, receipt)


def test_opposite_executor_funding_in_same_account_market_receipt_is_refused():
    rows = [o for o in diagnostic().outcomes if o.kind == "gmx_request_outcome"]
    own, foreign = rows[0], rows[2]
    assert own.operation.target == foreign.operation.target
    receipt = receipt_with_another_execution(own, foreign)
    with pytest.raises(DataError, match="verified contract or asset"):
        funding.accrue(own.before, own.after, receipt, own.request)
