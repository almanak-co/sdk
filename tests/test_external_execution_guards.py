"""External history joins ordinary canonicality and durable submission guards."""

from functools import partial
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from tests.test_external_effects import page, scope

external_page = page
external_scope = scope


@pytest.mark.parametrize("canonical", [True, False])
def test_external_audit_receives_full_history_without_treating_effect_as_submission(
    external_page, canonical
):
    chain = external_page.scope.chain
    outcome = SimpleNamespace(
        operation=SimpleNamespace(target=SimpleNamespace(fund=SimpleNamespace(chain=chain))),
        tx_hash="ordinary-transaction",
        after=SimpleNamespace(block=SimpleNamespace(number=10, hash="canonical-hash")),
    )
    state = SimpleNamespace(outcomes=(outcome, external_page.outcome))
    audit = Mock()
    if not canonical:
        audit.side_effect = DataError("reorganization", "external page no longer canonical")
    backend = SimpleNamespace(
        guard=Mock(),
        reader=SimpleNamespace(
            chain=chain,
            _rpc=Mock(return_value={"blockHash": "canonical-hash"}),
            pin_block=Mock(return_value=outcome.after.block),
        ),
        connector=SimpleNamespace(
            requests=SimpleNamespace(audit=Mock()),
            external_effects=SimpleNamespace(audit=audit),
        ),
    )
    if canonical:
        EvmGateway.audit_outcomes(backend, state)
    else:
        with pytest.raises(DataError, match="external page no longer canonical"):
            EvmGateway.audit_outcomes(backend, state)
    backend.reader._rpc.assert_called_once_with(
        "eth_getTransactionReceipt", [outcome.tx_hash], allow_none=True
    )
    backend.connector.requests.audit.assert_called_once_with(backend, state)
    audit.assert_called_once_with(backend, state)


def test_observed_submission_does_not_reserve_spending_again():
    pending = SimpleNamespace(family=None, tx_hash="retained-identity")
    backend = SimpleNamespace(
        config=SimpleNamespace(production=object()),
        reader=SimpleNamespace(_rpc=Mock(return_value={"hash": pending.tx_hash})),
        observe=Mock(side_effect=AssertionError("observed submission must reconcile")),
    )
    EvmGateway.authorize_submission(backend, SimpleNamespace(pending=pending))
    backend.observe.assert_not_called()
    backend.reader._rpc.assert_called_once_with(
        "eth_getTransactionByHash", [pending.tx_hash], allow_none=True
    )


def observing(select, block):
    """A gateway whose connector states `select` as its current observer."""
    backend = SimpleNamespace(
        guard=Mock(),
        connector=SimpleNamespace(execution=SimpleNamespace(current_observer=select)),
        reader=SimpleNamespace(_rpc=Mock(return_value="0x7"), pin_block=Mock(return_value=block)),
        snapshot=Mock(return_value="shared"),
        observe=Mock(side_effect=AssertionError("the connector states its own observation")),
    )
    backend.own_observer = partial(EvmGateway.own_observer, backend)
    backend.current_snapshot = partial(EvmGateway.current_snapshot, backend)
    return backend


PLAN = SimpleNamespace(
    operation=SimpleNamespace(allocation="position", target="target"), before="before", family=None
)


def test_a_plan_is_observed_at_the_head_as_its_connector_states_it():
    block = SimpleNamespace(number=7, hash="canonical")
    shared = SimpleNamespace(
        connector=SimpleNamespace(execution=SimpleNamespace()),
        snapshot=Mock(return_value="shared"),
    )
    shared.own_observer = partial(EvmGateway.own_observer, shared)
    assert EvmGateway.current_snapshot(shared, PLAN, block) == "shared"
    shared.snapshot.assert_called_once_with("position", block, "before")

    own = Mock(return_value="own")
    stated = observing(Mock(return_value=own), block)
    assert EvmGateway.current_snapshot(stated, PLAN, block) == "own"
    own.assert_called_once_with(stated, PLAN, block)
    stated.snapshot.assert_not_called()
    # The connector's own observation is refused at a block that left the chain, as
    # the shared one is.
    stated.reader.pin_block.return_value = SimpleNamespace(number=7, hash="other")
    with pytest.raises(DataError, match="snapshot block is no longer canonical") as refusal:
        EvmGateway.current_snapshot(stated, PLAN, block)
    assert (refusal.value.kind, own.call_count) == ("reorganization", 1)
    # A connector that states none for this plan leaves it the shared observation.
    declined = observing(Mock(return_value=None), block)
    assert EvmGateway.current_snapshot(declined, PLAN, block) == "shared"
    declined.snapshot.assert_called_once_with("position", block, "before")


def test_a_transaction_family_never_takes_the_connectors_own_observation():
    block = SimpleNamespace(number=7, hash="canonical")
    select = Mock(side_effect=AssertionError("a fee candidate is still to be signed"))
    backend = observing(select, block)
    # Fee replacement validates a candidate it is about to sign through the same path
    # as recorded bytes, so a family is observed as unsigned work is.
    candidate = SimpleNamespace(**{**vars(PLAN), "family": object(), "raw_transaction": "0x02"})
    assert EvmGateway.own_observer(backend, candidate) is None
    assert EvmGateway.current_snapshot(backend, candidate, block) == "shared"
    backend.snapshot.assert_called_once_with("position", block, "before")
    select.assert_not_called()


def test_the_fee_budget_of_a_signed_attempt_reads_the_payer_as_its_connector_observes():
    state = SimpleNamespace(pending=PLAN)
    seen = (
        SimpleNamespace(target="other", native_balance=3),
        SimpleNamespace(target="target", native_balance=5),
    )
    for select in (None, Mock(return_value=None)):
        execution = (
            SimpleNamespace() if select is None else SimpleNamespace(current_observer=select)
        )
        shared = SimpleNamespace(
            connector=SimpleNamespace(execution=execution), observe=Mock(return_value=seen)
        )
        shared.own_observer = partial(EvmGateway.own_observer, shared)
        assert EvmGateway.payer_balance(shared, state) == 5
        shared.observe.assert_called_once_with(state)

    block = SimpleNamespace(number=7, hash="canonical")
    own = Mock(return_value=SimpleNamespace(native_balance=6))
    stated = observing(Mock(return_value=own), block)
    assert EvmGateway.payer_balance(stated, state) == 6
    stated.guard.assert_called_once_with()
    own.assert_called_once_with(stated, PLAN, block)


def test_a_composition_observes_a_plan_as_its_component_does(monkeypatch):
    from almanak.connectors.composed import execution as composed

    own = Mock(return_value="component")
    stating = SimpleNamespace(execution=SimpleNamespace(current_observer=Mock(return_value=own)))
    declining = SimpleNamespace(execution=SimpleNamespace(current_observer=Mock(return_value=None)))
    silent = SimpleNamespace(execution=SimpleNamespace())
    found = {"stating": stating, "declining": declining, "silent": silent}
    monkeypatch.setattr(composed, "connector_for_target", lambda target: found[target])
    monkeypatch.setattr(composed, "backend_view", lambda backend, target: (backend, target))

    def plan(target):
        return SimpleNamespace(operation=SimpleNamespace(target=target))

    # A component with no own observation of the plan leaves it the shared one.
    for target in ("declining", "silent"):
        assert composed.ComposedExecution.current_observer(plan(target)) is None
    observed = plan("stating")
    observe = composed.ComposedExecution.current_observer(observed)
    assert observe("backend", observed, "block") == "component"
    # The component reads through its own view of the gateway, as its checks do.
    own.assert_called_once_with(("backend", "stating"), observed, "block")
    stating.execution.current_observer.assert_called_once_with(observed)


def test_signing_rejects_changed_protocol_call_before_signer_access():
    plan = SimpleNamespace(
        attempt_id="durable-attempt",
        before=SimpleNamespace(block=object()),
        operation=SimpleNamespace(account="fund"),
        call="tampered-call",
    )
    state = SimpleNamespace(plan=plan, pending=None, permissions="installed-authority")
    backend = SimpleNamespace(
        config=object(),
        audit_history=Mock(),
        require_fresh=Mock(),
        validate_plan_budget=Mock(),
        validate_operation=Mock(),
        verify_authority=Mock(),
        connector=SimpleNamespace(
            execution=SimpleNamespace(call=Mock(return_value="bounded-call"))
        ),
        _sign=Mock(side_effect=AssertionError("invalid plan must not reach signer")),
        signed_pending=Mock(side_effect=AssertionError("invalid plan must not be retained")),
    )
    backend.authorize_signing = partial(EvmGateway.authorize_signing, backend)
    with pytest.raises(ValueError, match="authorized protocol call"):
        EvmGateway.sign(backend, state)
    backend.audit_history.assert_called_once_with(state)
    backend.verify_authority.assert_called_once_with("installed-authority", "fund")
    backend._sign.assert_not_called()


def test_unknown_nonce_occupant_blocks_retained_transaction_broadcast():
    pending = SimpleNamespace(
        family=None, tx_hash="retained-identity", operation=SimpleNamespace(account="fund"), nonce=5
    )
    profile = SimpleNamespace(submitter=SimpleNamespace(address="submitter"))
    backend = SimpleNamespace(
        guard=Mock(),
        validate_pending=Mock(),
        verify_authority=Mock(),
        config=SimpleNamespace(
            accounts=SimpleNamespace(binding=Mock(return_value=SimpleNamespace(profile=profile)))
        ),
        reader=SimpleNamespace(chain="chain", _rpc=Mock(side_effect=[None, "0x5", "0x6"])),
        validate_current=Mock(side_effect=AssertionError("nonce conflict must refuse submission")),
    )
    backend.submitted = partial(EvmGateway.submitted, backend)
    backend.check_submission = partial(EvmGateway.check_submission, backend)
    backend.require_nonce_unused = partial(EvmGateway.require_nonce_unused, backend)
    with pytest.raises(DataError, match="unknown execution occupies"):
        EvmGateway.broadcast(backend, pending, "installed-authority")
    backend.validate_current.assert_not_called()
    assert [c.args[0] for c in backend.reader._rpc.call_args_list] == [
        "eth_getTransactionByHash",
        "eth_getTransactionCount",
        "eth_getTransactionCount",
    ]
