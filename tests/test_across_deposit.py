"""A bridge is planned step by step, encoded exactly and reconciled against its receipt."""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.connectors.across import execution, planning
from almanak.connectors.across import quote as venue
from almanak.connectors.across.lifecycle import (
    AcrossLifecycle,
    ownership,
    validate_closure,
    validate_initial,
)
from almanak.connectors.across.observations import require_pinned
from almanak.connectors.across.operations import (
    ApproveOperation,
    DepositOperation,
    RevokeOperation,
    plan_call,
)
from almanak.connectors.across.receipts import (
    APPROVAL_TOPIC,
    FUNDS_DEPOSITED_TOPIC,
    TRANSFER_TOPIC,
    movement,
    opened_transfer,
)
from almanak.gateway.ethereum import DataError
from tests.across_support import (
    AMOUNT,
    BASE,
    BASE_USDC,
    CONFIG,
    INTENT,
    NOW,
    PIN,
    POOL,
    RECIPIENT,
    RELAYER,
    SENDER,
    TARGET,
    USDC,
    mined,
    quote,
    snapshot,
    state,
)

MINED_AT = NOW + 12
DEPOSIT_ID = 4_829_265


def word(value) -> str:
    raw = bytes.fromhex(value[2:]) if isinstance(value, str) else value.to_bytes(32)
    return raw.rjust(32, b"\0").hex()


def deposit(**changes):
    return planning.operation(
        CONFIG, state(), INTENT, snapshot(allowance=AMOUNT), quote=quote(**changes)
    )


def test_the_steps_are_an_exact_approval_then_the_quoted_deposit():
    approve = planning.operation(CONFIG, state(), INTENT, snapshot())
    assert approve == ApproveOperation(target=TARGET, amount=INTENT.amount)
    planned = deposit()
    assert isinstance(planned, DepositOperation)
    assert (planned.quote, planned.quote_time_buffer) == (quote(), 3600)
    assert planning.step(CONFIG, state(), INTENT, snapshot(allowance=AMOUNT)) == "deposit"


def test_a_deposit_is_planned_only_from_a_quote_and_an_exact_allowance():
    with pytest.raises(DataError) as error:
        planning.operation(CONFIG, state(), INTENT, snapshot(allowance=AMOUNT))
    assert error.value.kind == "quote_required"


def test_an_approval_another_decision_left_is_cleared_before_this_one():
    stale = snapshot(allowance=AMOUNT - 1)
    assert planning.step(CONFIG, state(), INTENT, stale) == "clear"
    cleared = planning.operation(CONFIG, state(), INTENT, stale)
    assert cleared == RevokeOperation(target=TARGET, final=False)
    assert planning.step(CONFIG, state(), INTENT, snapshot()) == "approve"


def test_the_revoke_intent_closes_a_standing_approval_and_nothing_else():
    revoke = CONFIG.revoke("move")
    closing = state(
        decision=SimpleNamespace(intent=revoke, identity="2" * 32),
        mode="teardown",
        outcomes=(mined("approve", AMOUNT),),
    )
    planned = planning.operation(CONFIG, closing, revoke, snapshot(allowance=AMOUNT))
    assert planned == RevokeOperation(target=TARGET, final=True)
    execution.validate_operation(backend(), closing, planned, snapshot(allowance=AMOUNT))
    with pytest.raises(ValueError, match="already closed"):
        planning.operation(CONFIG, closing, revoke, snapshot())
    lifecycle = AcrossLifecycle()
    assert lifecycle.increases_exposure(INTENT) and not lifecycle.increases_exposure(revoke)


def test_a_mined_deposit_is_never_planned_again():
    done = state(outcomes=(mined("deposit", 5),))
    assert planning.operation(CONFIG, done, INTENT, snapshot(allowance=5)) == RevokeOperation(
        target=TARGET, final=True
    )
    with pytest.raises(DataError) as error:
        planning.operation(CONFIG, done, INTENT, snapshot())
    assert error.value.kind == "incomplete_action"
    other = state(outcomes=(mined("deposit", 0, identity="1" * 32),))
    assert planning.step(CONFIG, other, INTENT, snapshot()) == "approve"


def test_planning_refuses_another_decision_target_or_capability():
    with pytest.raises(ValueError, match="exact retained economic decision"):
        planning.step(CONFIG, state(decision=None), INTENT, snapshot())
    with pytest.raises(ValueError, match="retained economic capabilities"):
        planning.step(CONFIG, state(capabilities=None), INTENT, snapshot())
    empty = state().capabilities.model_copy(update={"requirements": ()})
    with pytest.raises(ValueError, match="exactly one retained route capability"):
        planning.step(CONFIG, state(capabilities=empty), INTENT, snapshot())


def test_the_deposit_calldata_matches_a_hand_built_vector():
    call = plan_call(deposit())
    words = [
        word(SENDER),
        word(RECIPIENT),
        word(USDC.reference),
        word(BASE_USDC.reference),
        word(AMOUNT),
        word(999_897_038),
        word(8453),
        word(RELAYER),
        word(NOW - 69),
        word(NOW - 69 + 7200),
        word(3),
        # The empty message: the offset of the twelfth head word, then a zero length.
        word(12 * 32),
        word(0),
    ]
    assert call.data == "0xad5425c6" + "".join(words)
    assert (call.to, call.value, call.fund) == (PIN.address, 0, TARGET.fund)


def test_approval_and_revocation_name_only_the_spoke_pool():
    approve = plan_call(ApproveOperation(target=TARGET, amount=INTENT.amount))
    revoke = plan_call(RevokeOperation(target=TARGET, final=True))
    assert approve.to == revoke.to == USDC.reference
    assert approve.data == "0x095ea7b3" + word(PIN.address) + word(AMOUNT)
    assert revoke.data == "0x095ea7b3" + word(PIN.address) + word(0)


def test_only_a_deposit_expires_and_only_past_the_quote_buffer():
    planned = deposit()
    last = SimpleNamespace(timestamp=NOW - 69 + 3600)
    assert not execution.expired(planned, last)
    assert execution.expired(planned, SimpleNamespace(timestamp=last.timestamp + 1))
    assert execution.expired(planned, SimpleNamespace(timestamp=2**256))
    approve = ApproveOperation(target=TARGET, amount=INTENT.amount)
    assert not execution.expired(approve, SimpleNamespace(timestamp=2**256))


def log(address, topics, data):
    return {"address": address, "topics": topics, "data": data, "logIndex": "0x7"}


def deposit_log(operation, **changes):
    values = {
        "recipient": bytes.fromhex(word(RECIPIENT)),
        "output": operation.quote.output_amount,
        "exclusivity": MINED_AT + 3,
        "message": b"",
        **changes,
    }
    data = encode(
        ("bytes32",) * 2 + ("uint256",) * 2 + ("uint32",) * 3 + ("bytes32",) * 2 + ("bytes",),
        (
            bytes.fromhex(word(USDC.reference)),
            bytes.fromhex(word(BASE_USDC.reference)),
            AMOUNT,
            values["output"],
            operation.quote.timestamp,
            operation.quote.fill_deadline,
            values["exclusivity"],
            values["recipient"],
            bytes.fromhex(word(RELAYER)),
            values["message"],
        ),
    )
    topics = [
        FUNDS_DEPOSITED_TOPIC,
        "0x" + word(8453),
        "0x" + word(DEPOSIT_ID),
        "0x" + word(SENDER),
    ]
    return log(PIN.address, topics, "0x" + data.hex())


def token_log(topic, counterparty, amount):
    topics = [topic, "0x" + word(SENDER), "0x" + word(counterparty)]
    return log(USDC.reference, topics, "0x" + word(amount))


def receipt(operation, moved=AMOUNT, **changes):
    logs = [token_log(TRANSFER_TOPIC, PIN.address, moved), deposit_log(operation, **changes)]
    return {"status": "0x1", "logs": logs}


def test_a_mined_deposit_reconciles_and_opens_a_transfer():
    operation = deposit()
    moved, identity = movement(receipt(operation), operation, MINED_AT)
    assert moved == AMOUNT
    assert identity == {
        "deposit_id": DEPOSIT_ID,
        "exclusivity_deadline": MINED_AT + 3,
        "log_index": 7,
    }
    transfer = opened_transfer(operation, identity)
    assert transfer.open and transfer.reference == str(DEPOSIT_ID)
    assert (transfer.sent.amount, transfer.offered.amount) == (AMOUNT, 999_897_038)
    assert (transfer.source.account, transfer.destination.account) == ("arb", "base")
    assert (transfer.destination.chain, transfer.destination.asset) == (BASE, BASE_USDC)
    assert int(transfer.fill_deadline.timestamp()) == NOW - 69 + 7200


@pytest.mark.parametrize(
    "changes",
    [
        {"recipient": bytes.fromhex(word("0x" + "99" * 20))},
        {"output": 1},
        {"exclusivity": MINED_AT + 4},
        {"message": b"\x01"},
        {"moved": AMOUNT - 1},
    ],
)
def test_a_receipt_that_differs_from_the_signed_deposit_is_refused(changes):
    operation = deposit()
    with pytest.raises(DataError) as error:
        movement(receipt(operation, **changes), operation, MINED_AT)
    assert error.value.kind == "receipt_mismatch"


def test_a_receipt_needs_exactly_one_deposit_and_one_token_movement():
    operation = deposit()
    complete = receipt(operation)
    for logs in (complete["logs"][:1], complete["logs"][1:], complete["logs"] * 2):
        with pytest.raises(DataError, match="exactly one attributed"):
            movement({"status": "0x1", "logs": logs}, operation, MINED_AT)
    elsewhere = token_log(TRANSFER_TOPIC, "0x" + "99" * 20, AMOUNT)
    with pytest.raises(DataError, match="other accounts"):
        movement({"logs": [elsewhere, complete["logs"][1]]}, operation, MINED_AT)
    malformed = {**complete["logs"][1], "data": "0x00"}
    with pytest.raises(DataError, match="malformed"):
        movement({"logs": [complete["logs"][0], malformed]}, operation, MINED_AT)


def test_an_approval_receipt_names_the_exact_spender_and_bound():
    approve = ApproveOperation(target=TARGET, amount=INTENT.amount)
    granted = {"logs": [token_log(APPROVAL_TOPIC, PIN.address, AMOUNT)]}
    assert movement(granted, approve, MINED_AT) == (0, None)
    assert movement(
        {"logs": [token_log(APPROVAL_TOPIC, PIN.address, 0)]},
        RevokeOperation(target=TARGET, final=True),
        MINED_AT,
    ) == (0, None)
    with pytest.raises(DataError, match="another owner, spender or bound"):
        movement(granted, RevokeOperation(target=TARGET, final=True), MINED_AT)


def backend():
    return SimpleNamespace(config=CONFIG, guard=lambda: None)


def test_the_gateway_asks_the_venue_only_for_the_deposit(monkeypatch):
    asked = []

    def fetch(target, amount, *, api_key=None):
        asked.append((target, amount, api_key))
        return quote()

    monkeypatch.setattr(venue, "fetch", fetch)
    monkeypatch.setenv(execution.API_KEY_ENV, "secret")
    approve = execution.quoted_operation(backend(), state(), INTENT, snapshot())
    # The approval signs no quote, but the venue is asked first so a refusal leaves none.
    assert isinstance(approve, ApproveOperation) and asked == [(TARGET, AMOUNT, "secret")]
    planned = execution.quoted_operation(backend(), state(), INTENT, snapshot(allowance=AMOUNT))
    assert planned == deposit() and asked == [(TARGET, AMOUNT, "secret")] * 2
    stale = execution.quoted_operation(backend(), state(), INTENT, snapshot(allowance=1))
    assert stale == RevokeOperation(target=TARGET, final=False) and len(asked) == 2
    assert "secret" not in planned.model_dump_json()


def test_a_quote_above_the_ceiling_is_refused_before_anything_is_planned(monkeypatch):
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: quote(outputAmount=str(AMOUNT // 2)))
    with pytest.raises(DataError) as error:
        execution.quoted_operation(backend(), state(), INTENT, snapshot(allowance=AMOUNT))
    assert error.value.kind == "fee_above_ceiling"
    with pytest.raises(DataError) as error:
        execution.quoted_operation(backend(), state(), INTENT, snapshot())
    assert error.value.kind == "fee_above_ceiling"


def approved_state(**changes):
    return state(outcomes=(mined("approve", AMOUNT),), **changes)


def test_validation_rechecks_the_quote_against_the_pinned_snapshot():
    before = snapshot(allowance=AMOUNT)
    execution.validate_operation(backend(), approved_state(), deposit(), before)
    execution.validate_operation(
        backend(), state(), ApproveOperation(target=TARGET, amount=INTENT.amount), snapshot()
    )
    dear = deposit().model_copy(update={"quote": quote(outputAmount=str(AMOUNT // 2))})
    with pytest.raises(DataError) as error:
        execution.validate_operation(backend(), approved_state(), dear, before)
    assert error.value.kind == "fee_above_ceiling"
    with pytest.raises(ValueError, match="differs from its retained economic decision"):
        execution.validate_operation(
            backend(), approved_state(), RevokeOperation(target=TARGET, final=True), before
        )


@pytest.mark.parametrize(
    ("before", "changes", "kind"),
    [
        ({"pool": POOL.model_copy(update={"deposits_paused": True})}, {}, "deposits_paused"),
        (
            {"pool": POOL.model_copy(update={"implementation": "0x" + "33" * 20})},
            {},
            "deployment_changed",
        ),
        ({"wallet_balance": AMOUNT - 1}, {}, "funding_unavailable"),
        ({}, {"mode": "teardown"}, "entry_unavailable"),
        ({"allowance": 0}, {}, "allowance_mismatch"),
    ],
)
def test_a_deposit_is_refused_when_its_preconditions_fail(before, changes, kind):
    observed = snapshot(**{"allowance": AMOUNT, **before})
    operation = (
        deposit() if observed.allowance else ApproveOperation(target=TARGET, amount=INTENT.amount)
    )
    with pytest.raises(DataError) as error:
        execution.validate_operation(backend(), approved_state(**changes), operation, observed)
    assert error.value.kind == kind


def test_a_low_payer_balance_refuses_a_new_bridge():
    low = snapshot(native_balance=snapshot().native_balance.model_copy(update={"amount": 1}))
    with pytest.raises(DataError) as error:
        execution.validate_operation(
            backend(), state(), ApproveOperation(target=TARGET, amount=INTENT.amount), low
        )
    assert error.value.kind == "payer_unavailable"


def test_a_signed_deposit_is_rechecked_at_the_current_block():
    before = snapshot(allowance=AMOUNT)
    plan = SimpleNamespace(before=before, operation=deposit(fillDeadline=str(NOW + 700)))
    execution.validate_current(backend(), plan, before)
    for seconds, kind in ((200, "fill_deadline"), (3600, "quote_time")):
        later = before.block.model_copy(update={"timestamp": NOW + seconds})
        with pytest.raises(DataError) as error:
            execution.validate_current(backend(), plan, snapshot(allowance=AMOUNT, block=later))
        assert error.value.kind == kind
    for changed, kind in (
        ({"allowance": 1}, "precondition_changed"),
        ({"allowance": AMOUNT, "wallet_balance": 1}, "precondition_changed"),
        (
            {"allowance": AMOUNT, "pool": POOL.model_copy(update={"deposits_paused": True})},
            "precondition_changed",
        ),
    ):
        with pytest.raises(DataError) as error:
            execution.validate_current(backend(), plan, snapshot(**changed))
        assert error.value.kind == kind
    paused = snapshot(pool=POOL.model_copy(update={"deposits_paused": True}))
    revoke = SimpleNamespace(
        before=snapshot(), operation=RevokeOperation(target=TARGET, final=True)
    )
    execution.validate_current(backend(), revoke, paused)
    # Tokens that left the fund after signing do not hold a revocation back.
    execution.validate_current(backend(), revoke, snapshot(wallet_balance=1))
    with pytest.raises(DataError):
        execution.validate_current(backend(), revoke, snapshot(allowance=1))


def test_simulation_results_are_checked_per_step():
    approve = ApproveOperation(target=TARGET, amount=INTENT.amount)
    execution.validate_simulation(approve, "0x" + word(1), snapshot())
    execution.validate_simulation(deposit(), "0x", snapshot())
    with pytest.raises(DataError):
        execution.validate_simulation(approve, "0x" + word(0), snapshot())
    with pytest.raises(DataError):
        execution.validate_simulation(deposit(), "0x" + word(1), snapshot())


def test_allowance_and_balances_reconcile_after_each_step():
    approve = ApproveOperation(target=TARGET, amount=INTENT.amount)
    assert execution.allowance_after(approve, 0, 0) == AMOUNT
    assert execution.allowance_after(deposit(), AMOUNT, AMOUNT) == 0
    assert execution.allowance_after(RevokeOperation(target=TARGET, final=True), 5, 0) == 0
    before = snapshot()
    spent = before.native_balance.model_copy(update={"amount": before.native_balance.amount - 7})
    after = snapshot(wallet_balance=0, native_balance=spent)
    assert execution.accounting_issues(before, after, AMOUNT, 7) == ()
    assert len(execution.accounting_issues(before, after, AMOUNT - 1, 8)) == 2


def test_funding_and_gas_follow_the_source_account():
    adapter = execution.AcrossExecution
    (row,) = adapter.requirements(CONFIG, "arb")
    assert (row.asset, row.amount) == (USDC, AMOUNT)
    assert adapter.requirements(CONFIG, "base") == ()
    assert (
        adapter.entry_transactions(CONFIG, "arb"),
        adapter.entry_transactions(CONFIG, "base"),
    ) == (2, 0)
    tokens, pristine = adapter.preflight(CONFIG, "arb", (snapshot(),))
    assert tokens[0]["funded"] and pristine
    assert adapter.preflight(CONFIG, "arb", (snapshot(allowance=1),))[1] is False
    assert adapter.increases_exposure(CONFIG, deposit())
    assert not adapter.increases_exposure(CONFIG, RevokeOperation(target=TARGET, final=True))
    adapter.validate_config(CONFIG)
    assert adapter.call(CONFIG, deposit()) == plan_call(deposit())
    with pytest.raises(DataError):
        adapter.validate_replacement(CONFIG, deposit())


def test_a_changed_implementation_refuses_new_deposits():
    require_pinned(POOL, TARGET.route.chain.chain_id)
    with pytest.raises(DataError, match="another implementation"):
        require_pinned(
            POOL.model_copy(update={"implementation_code_hash": "0x" + "00" * 32}),
            TARGET.route.chain.chain_id,
        )


def test_a_mined_deposit_adds_its_transfer_to_the_owned_state():
    operation = deposit()
    transfer = opened_transfer(operation, {"deposit_id": DEPOSIT_ID})
    held = state()
    assert ownership(held) == {"owned": (), "transfers": ()}
    assert ownership(held, SimpleNamespace(transfer=None)) == {"owned": (), "transfers": ()}
    assert ownership(held, SimpleNamespace(transfer=transfer))["transfers"] == (transfer,)
    with pytest.raises(ValueError, match="retained only once"):
        ownership(state(transfers=(transfer,)), SimpleNamespace(transfer=transfer))
    assert AcrossLifecycle.increases_exposure(INTENT)


def test_teardown_waits_for_a_transfer_in_flight_and_a_cleared_allowance():
    owned = (SimpleNamespace(target=TARGET),)
    transfer = opened_transfer(deposit(), {"deposit_id": DEPOSIT_ID})
    validate_closure(state(owned=owned), (snapshot(),))
    with pytest.raises(DataError) as error:
        validate_closure(state(owned=owned, transfers=(transfer,)), (snapshot(),))
    assert error.value.kind == "transfer_in_flight"
    with pytest.raises(DataError):
        validate_closure(state(owned=owned), (snapshot(allowance=1),))
    with pytest.raises(DataError) as error:
        validate_initial(state(owned=owned), (snapshot(allowance=1),))
    assert error.value.kind == "existing_exposure"
    with pytest.raises(DataError) as error:
        validate_initial(state(owned=()), (snapshot(),))
    assert error.value.kind == "ownership_mismatch"


def reconciled(monkeypatch, operation, mined_receipt, after):
    """Reconcile with the chain reads and the fee reading replaced by fixed values."""
    from almanak.connectors.across import state as records

    fees = after.native_balance.model_copy(update={"amount": 7})
    monkeypatch.setattr(execution, "snapshot", lambda backend, name, block: after)
    monkeypatch.setattr(execution, "charged_fees", lambda *a: (fees, None))
    monkeypatch.setattr(records, "Outcome", lambda **fields: SimpleNamespace(**fields))
    pending = SimpleNamespace(
        operation=operation,
        before=snapshot(allowance=AMOUNT),
        decision="decision",
        tx_hash="0x" + "cd" * 32,
        nonce=3,
        call="call",
        execution="execution",
    )
    block = SimpleNamespace(timestamp=MINED_AT)
    return execution.reconcile(backend(), pending, mined_receipt, block)


def test_reconciliation_records_the_deposit_and_the_transfer_it_opened(monkeypatch):
    operation = deposit()
    before = snapshot()
    spent = before.native_balance.model_copy(update={"amount": before.native_balance.amount - 7})
    after = snapshot(wallet_balance=0, native_balance=spent)
    outcome = reconciled(monkeypatch, operation, receipt(operation), after)
    assert (outcome.success, outcome.assets, outcome.accounting_issues) == (True, AMOUNT, ())
    assert outcome.transfer == opened_transfer(operation, {"deposit_id": DEPOSIT_ID})
    with pytest.raises(DataError) as error:
        reconciled(monkeypatch, operation, receipt(operation), snapshot(allowance=1))
    assert error.value.kind == "ownership_mismatch"


def test_a_reverted_deposit_moves_nothing_and_opens_no_transfer(monkeypatch):
    reverted = {"status": "0x0", "logs": []}
    outcome = reconciled(monkeypatch, deposit(), reverted, snapshot(allowance=AMOUNT))
    assert (outcome.success, outcome.assets, outcome.transfer) == (False, 0, None)


def test_a_safe_account_has_no_bridge_grants_yet():
    from almanak.connectors.across.connector import connector
    from almanak.connectors.across.permissions import AcrossPermissions

    with pytest.raises(ValueError, match="no Roles grants yet"):
        AcrossPermissions.derive(None, None)
    with pytest.raises(ValueError, match="no Roles grants yet"):
        AcrossPermissions.identities(None, "arb", None)
    binding = connector()
    transfer = opened_transfer(deposit(), {"deposit_id": DEPOSIT_ID})
    report = binding.projection.report(state(transfers=(transfer,)))
    assert report["transfers"][0]["reference"] == str(DEPOSIT_ID)
    with pytest.raises(ValueError, match="no dashboard"):
        binding.projection.dashboard(state())
    assert (
        binding.execution_effects(
            SimpleNamespace(success=True, assets=AMOUNT, operation=deposit())
        )[0].amount
        == -AMOUNT
    )
    assert binding.execution_effects(SimpleNamespace(success=False, assets=0)) == ()


def test_the_gateway_plans_through_the_quote_hook_only_where_one_exists(monkeypatch):
    from almanak.gateway.execution import EvmGateway, plan_operation

    monkeypatch.setattr(venue, "fetch", lambda *a, **k: quote())
    quoting = SimpleNamespace(
        connector=SimpleNamespace(execution=execution.AcrossExecution),
        config=CONFIG,
        guard=lambda: None,
    )
    planned = plan_operation(quoting, state(), INTENT, snapshot(allowance=AMOUNT))
    assert planned == deposit()
    plain = SimpleNamespace(
        connector=SimpleNamespace(
            execution=SimpleNamespace(operation=lambda config, *rest: (config, *rest))
        ),
        config="config",
    )
    assert plan_operation(plain, "state", "intent", "before") == (
        "config",
        "state",
        "intent",
        "before",
    )
    assert EvmGateway.enforces_deadline(quoting, deposit())
    assert not EvmGateway.enforces_deadline(quoting, RevokeOperation(target=TARGET, final=True))


def with_deposit_log(operation, change):
    mined_receipt = receipt(operation)
    mined_receipt["logs"][1] = change(mined_receipt["logs"][1])
    return mined_receipt


@pytest.mark.parametrize(
    "change",
    [
        # Another contract emits the same event: nothing is attributed to the SpokePool.
        lambda entry: {**entry, "address": "0x" + "77" * 20},
        # Indexed fields: the destination chain, then the depositor.
        lambda entry: {
            **entry,
            "topics": [entry["topics"][0], "0x" + word(10), *entry["topics"][2:]],
        },
        lambda entry: {**entry, "topics": [*entry["topics"][:3], "0x" + word("0x" + "99" * 20)]},
        lambda entry: {**entry, "topics": entry["topics"][:3]},
        lambda entry: {**entry, "data": entry["data"][:-64]},
    ],
)
def test_a_deposit_event_from_elsewhere_or_with_other_indexed_fields_is_refused(change):
    operation = deposit()
    with pytest.raises(DataError) as error:
        movement(with_deposit_log(operation, change), operation, MINED_AT)
    assert error.value.kind == "receipt_mismatch"


def test_the_deposit_deadline_is_the_last_timestamp_the_spoke_pool_mines_it_at():
    planned = deposit()
    assert planned.deadline == planned.quote.timestamp + 3600
    assert not execution.expired(planned, SimpleNamespace(timestamp=planned.deadline))
    assert execution.expired(planned, SimpleNamespace(timestamp=planned.deadline + 1))


def test_an_approval_whose_quote_is_refused_leaves_teardown_free_to_close(monkeypatch):
    """An approval is mined, the deposit's quote is then refused, and teardown revokes."""
    approved = approved_state()
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: quote(isAmountTooLow=True))
    with pytest.raises(DataError) as error:
        execution.quoted_operation(backend(), approved, INTENT, snapshot(allowance=AMOUNT))
    assert error.value.kind == "amount_too_low"
    revoke = CONFIG.revoke("move")
    closing = approved_state(
        decision=SimpleNamespace(intent=revoke, identity="2" * 32), mode="teardown"
    )
    standing = snapshot(allowance=AMOUNT)
    planned = execution.quoted_operation(backend(), closing, revoke, standing)
    assert planned == RevokeOperation(target=TARGET, final=True)
    execution.validate_operation(backend(), closing, planned, standing)
    owned = (SimpleNamespace(target=TARGET),)
    with pytest.raises(DataError):
        validate_closure(state(owned=owned, mode="teardown"), (standing,))
    validate_closure(state(owned=owned, mode="teardown"), (snapshot(),))


def test_a_stale_revocation_does_not_complete_the_bridge_it_precedes():
    from almanak.connectors.across.state import Outcome

    settled = Outcome.strategy_result
    fields = {
        "decision": SimpleNamespace(intent=INTENT),
        "success": True,
        "assets": 0,
        "transfer": None,
        "after": snapshot(),
        "tx_hash": "0x" + "cd" * 32,
    }
    stale = SimpleNamespace(operation=RevokeOperation(target=TARGET, final=False), **fields)
    final = SimpleNamespace(operation=RevokeOperation(target=TARGET, final=True), **fields)
    assert settled(stale).status == "intermediate" and settled(final).status == "completed"
