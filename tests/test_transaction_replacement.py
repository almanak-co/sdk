"""Offline admission, identity and accounting proofs for the shared family path."""

from types import SimpleNamespace

import pytest
from eth_account import Account
from eth_utils import keccak
from pydantic import ValidationError

from almanak.gateway import replacement
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.production_config import ForkGasExecution, ReplacementPolicy
from almanak.gateway.production_fees import validate_budget
from almanak.gateway.service import GatewayService
from almanak.gateway.signed_transaction import verify_signed
from almanak.transaction_family import (
    Anchor,
    candidate_view,
    digest,
    eligibility,
    family_report,
    minimum_price,
    start_family,
    with_family,
)
from tests.test_production import TEST_KEY, backend_for
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded

BOUNDS = dict(interval_seconds=5, minimum_bump_bps=1000, max_candidates=3, max_elapsed_seconds=60)


class FamilyReader:
    def __init__(self, pending):
        self.chain = pending.call.fund.chain
        self.transport_chain = self.chain.chain_id
        self.blocks = {pending.before.block.number: pending.before.block}
        self.head = pending.before.block
        self.nonce = pending.nonce
        self.queued = pending.nonce
        self.transactions = {}
        self.receipts = {}
        self.error = None
        self.calls = []

    def pin_block(self, selector):
        return self.head if selector == "latest" else self.blocks[selector]

    def advance(self):
        self.head = self.head.model_copy(
            update={
                "number": self.head.number + 1,
                "hash": "0x" + f"{self.head.number + 1:064x}",
                "timestamp": self.head.timestamp + 10,
            }
        )
        self.blocks[self.head.number] = self.head

    def _rpc(self, method, params, **kwargs):
        self.calls.append((method, params))
        if self.error:
            raise self.error
        if method == "eth_getTransactionCount":
            return hex(self.queued if params[1] == "pending" else self.nonce)
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        if method == "eth_getTransactionByHash":
            return self.transactions.get(params[0])
        if method == "eth_feeHistory":
            return {"oldestBlock": params[1], "baseFeePerGas": ["0x1", "0x1"], "reward": [["0x1"]]}
        if method == "eth_sendRawTransaction":
            tx_hash = "0x" + keccak(bytes.fromhex(params[0][2:])).hex()
            self.transactions[tx_hash] = {"hash": tx_hash}
            self.queued = self.nonce + 1
            return tx_hash
        raise AssertionError(method)


@pytest.fixture
def case(production_attempt, monkeypatch):
    state, pending, outcome = production_attempt
    policy = state.config.production.gas[0].model_copy(
        update={"replacement": ReplacementPolicy(**BOUNDS)}
    )
    config = state.config.model_copy(
        update={"production": state.config.production.model_copy(update={"gas": (policy,)})}
    )
    backend = backend_for(config, pending.before.block)
    backend.signers = {pending.operation.account: Account.from_key(TEST_KEY)}
    backend.reader.chain = pending.call.fund.chain
    pending = pending.model_copy(update={"gas_price": 100})
    signed = backend._sign(pending.operation, pending.execution, pending.nonce, pending.gas, 100)
    pending = start_family(
        config,
        pending.model_copy(
            update={
                "raw_transaction": "0x" + signed.raw_transaction.hex(),
                "tx_hash": "0x" + signed.hash.hex(),
            }
        ),
    )
    state = state.model_copy(update={"config": config, "pending": pending})
    backend.reader = FamilyReader(pending)
    backend.require_fresh = lambda block: None
    backend.validate_operation = lambda *args: None
    backend.verify_authority = lambda *args: None
    backend.validate_current = lambda view: pending.before
    clock = [100.0]
    monkeypatch.setattr(replacement.time, "time", lambda: clock[0])
    return SimpleNamespace(state=state, backend=backend, clock=clock, outcome=outcome)


def step(case, action, index):
    case.state = replacement.command(
        case.backend, case.state, action, case.state.pending.family.identity, index
    )
    return case.state.pending.family


def ready(case):
    step(case, "attempt", 0)
    case.backend.broadcast(case.state.pending, state=case.state)
    case.clock[0] += 5
    case.backend.reader.advance()


def second(case):
    ready(case)
    step(case, "reserve", 1)
    return step(case, "sign", 1)


@pytest.mark.parametrize(
    "field,value",
    [
        ("interval_seconds", 0),
        ("minimum_bump_bps", 0),
        ("max_candidates", 1),
        ("max_elapsed_seconds", 5),
        ("interval_seconds", True),
    ],
)
def test_explicit_bounds_reject_invalid_inputs(field, value):
    with pytest.raises(ValidationError):
        ReplacementPolicy(**{**BOUNDS, field: value})
    with pytest.raises(ValidationError):
        ReplacementPolicy(**{k: v for k, v in BOUNDS.items() if k != field})


@pytest.mark.parametrize("price,bump,result", [(100, 1000, 110), (101, 1000, 112), (1, 1, 2)])
def test_bump_uses_ceiling_and_strict_increase(price, bump, result):
    assert minimum_price(price, bump) == result


def test_reserved_and_signed_retries_keep_identity_and_never_reestimate(case):
    ready(case)
    proposal = step(case, "reserve", 1).proposal
    case.clock[0] += 1
    assert step(case, "reserve", 1).proposal == proposal
    family = step(case, "sign", 1)
    assert family.candidates[1].gas_price == 110
    assert step(case, "sign", 1) == family
    assert step(case, "reserve", 1) == family
    assert all(c.configuration_revision == family.configuration_revision for c in family.candidates)
    assert not any(m == "eth_estimateGas" for m, _ in case.backend.reader.calls)
    for c in family.candidates:
        verify_signed(
            candidate_view(case.state.pending, c),
            case.state.pending.execution,
            case.backend.reader.transport_chain,
        )


def test_identified_fork_policy_uses_the_shared_replacement_path(case):
    production = case.state.config.production
    assert production is not None
    config = case.state.config.model_copy(
        update={"production": None, "fork_gas": ForkGasExecution(gas=production.gas)}
    )
    pending = start_family(config, case.state.pending.model_copy(update={"family": None}))
    case.state = case.state.model_copy(update={"config": config, "pending": pending})
    case.backend.config = config
    case.backend.estimate_gas = lambda *_: {"gas_estimate": 1}

    fees = case.backend.prepare_fees(case.state, pending.operation, pending.before, {"data": "0x"})
    assert fees["fee_quote"].block == pending.before.block
    assert not any(method == "eth_gasPrice" for method, _ in case.backend.reader.calls)

    ready(case)
    family = second_after_ready(case)
    assert len(family.candidates) == 2
    assert family.candidates[1].gas_price > family.candidates[0].gas_price


def second_after_ready(case):
    step(case, "reserve", 1)
    return step(case, "sign", 1)


def test_missing_policy_does_not_create_replacement_authority(production_attempt):
    state, pending, _ = production_attempt
    assert start_family(state.config, pending) == pending
    assert (
        eligibility(state.config, pending, Anchor(at=100.0, block=pending.before.block))
        == "replacement_disabled"
    )


def test_interval_requires_elapsed_time_and_a_new_head(case):
    step(case, "attempt", 0)
    assert step(case, "inspect", 0).blocked == "replacement_interval"
    case.clock[0] += 5
    assert step(case, "inspect", 0).blocked == "replacement_interval"
    case.backend.reader.advance()
    assert step(case, "inspect", 0).blocked is None


@pytest.mark.parametrize("condition", ["elapsed", "clock", "count"])
def test_terminal_bounds_survive_later_observations(case, condition):
    ready(case)
    if condition == "elapsed":
        case.clock[0] = 160.0
    elif condition == "clock":
        case.clock[0] = 99.0
    else:
        step(case, "reserve", 1)
        step(case, "sign", 1)
        step(case, "attempt", 1)
        case.clock[0] += 5
        case.backend.reader.advance()
        step(case, "reserve", 2)
        step(case, "sign", 2)
        step(case, "attempt", 2)
    index = len(case.state.pending.family.candidates) - 1
    blocked = step(case, "inspect", index)
    assert blocked.stopped
    case.clock[0] = 200.0
    assert step(case, "inspect", index).blocked == blocked.blocked
    assert step(case, "reserve", index + 1).candidates == blocked.candidates


@pytest.mark.parametrize("mutation", ["nonce", "gas", "gas_price", "raw_transaction", "tx_hash"])
def test_original_payload_and_signature_cannot_be_forged(case, mutation):
    pending = case.state.pending
    value = {
        "nonce": pending.nonce + 1,
        "gas": pending.gas + 1,
        "gas_price": 101,
        "raw_transaction": "0x00",
        "tx_hash": "0x" + "00" * 32,
    }[mutation]
    with pytest.raises(DataError, match="family|original"):
        case.backend.validate_pending(pending.model_copy(update={mutation: value}))


def test_unreserved_signing_and_forged_checkpoints_are_rejected(case, tmp_path):
    ready(case)
    with pytest.raises(ValueError, match="durable fee reservation"):
        step(case, "sign", 1)
    before = case.state
    step(case, "reserve", 1)
    service = GatewayService(
        before.config, tmp_path / "state.sqlite", case.backend, lease_seconds=30
    )
    with pytest.raises(ValueError, match="resolved"):
        service.validate_update(before, case.state)
    with pytest.raises(ValueError, match="skipped"):
        step(case, "reserve", 3)


def test_teardown_blocks_both_replacement_signing_and_unattempted_submission(case):
    second(case)
    case.state = case.state.model_copy(update={"mode": "teardown"})
    assert step(case, "attempt", 1).blocked == "teardown"
    assert case.state.pending.family.candidates[1].submission is None
    with pytest.raises(DataError, match="record submission"):
        case.backend.broadcast(case.state.pending, state=case.state)


def test_unknown_nonce_and_rpc_outage_preserve_family(case):
    ready(case)
    previous = case.state.pending.family
    case.backend.reader.nonce += 1
    assert step(case, "reserve", 1).blocked == "nonce_conflict"
    assert case.state.pending.family.candidates == previous.candidates
    case.backend.reader.nonce -= 1
    case.backend.reader.error = DataError("transport", "offline")
    assert step(case, "reserve", 1).blocked == "transport"
    assert case.state.pending.family.maximum_price == previous.maximum_price


def test_budget_refusals_name_the_values_the_author_can_change(case):
    family = second(case)
    pending = case.state.pending
    policy = case.state.config.production.gas[0]
    fee = pending.gas * family.maximum_price
    reserve = policy.exit_reserve.amount
    value = pending.execution.value if pending.execution else 0
    short = pending.before.native_balance.model_copy(update={"amount": fee + reserve - 1})
    with pytest.raises(DataError) as refused:
        validate_budget(case.state, pending.operation, short, pending.gas, 100)
    assert refused.value.kind == "gas_budget"
    assert str(refused.value) == (
        f"payer balance cannot cover execution and exit reserve: {fee} maximum fee + "
        f"{reserve} exit_reserve + {value} value exceeds the payer's {fee + reserve - 1} "
        f"({policy.asset} base units)"
    )
    budget = policy.total_budget.model_copy(update={"amount": fee + reserve - 1})
    config = case.state.config
    limited = policy.model_copy(update={"total_budget": budget})
    production = config.production.model_copy(update={"gas": (limited,)})
    state = case.state.model_copy(
        update={"config": config.model_copy(update={"production": production})}
    )
    balance = short.model_copy(update={"amount": fee + reserve})
    with pytest.raises(DataError) as refused:
        validate_budget(state, pending.operation, balance, pending.gas, 100)
    assert refused.value.kind == "gas_budget"
    assert str(refused.value).startswith("remaining budget cannot cover execution")
    assert (
        f"+ {fee} maximum fee + {reserve} exit_reserve exceeds total_budget {fee + reserve - 1}"
        in str(refused.value)
    )


def test_maximum_exposure_is_reserved_instead_of_sum(case):
    family = second(case)
    pending = case.state.pending
    policy = case.state.config.production.gas[0]
    bound = pending.gas * family.maximum_price + policy.exit_reserve.amount
    balance = pending.before.native_balance.model_copy(update={"amount": bound})
    validate_budget(case.state, pending.operation, balance, pending.gas, 100)
    with pytest.raises(DataError, match="exit reserve"):
        validate_budget(
            case.state,
            pending.operation,
            balance.model_copy(update={"amount": bound - 1}),
            pending.gas,
            100,
        )
    assert family_report(case.state)["pending"]["maximum_reserved_fee"]["amount"] == str(
        pending.gas * 110
    )


@pytest.mark.parametrize("winner", [0, 1])
@pytest.mark.parametrize("success", [True, False])
def test_reconcile_selects_winner_and_retains_family_without_signing(case, winner, success):
    family = second(case)
    candidate = family.candidates[winner]
    block = case.backend.reader.head
    receipt = {
        **case.outcome.receipt,
        "transactionHash": candidate.tx_hash,
        "blockHash": block.hash,
        "blockNumber": hex(block.number),
        "effectiveGasPrice": hex(candidate.gas_price),
        "status": "0x1" if success else "0x0",
    }
    case.backend.reader.receipts[candidate.tx_hash] = receipt
    case.backend.signers = {}
    assert case.backend.receipt(case.state.pending) == receipt
    view = candidate_view(case.state.pending, candidate)
    # Receipt interpretation remains connector-owned; assert the selected inputs at that boundary.
    called = []
    original = EvmGateway.connector
    adapter = SimpleNamespace(
        reconcile=lambda b, p, r, blk: (
            called.append(p)
            or case.outcome.model_copy(update={"tx_hash": p.tx_hash, "success": success})
        )
    )
    try:
        EvmGateway.connector = property(lambda _: SimpleNamespace(execution=adapter))
        result = case.backend.reconcile(case.state.pending, receipt)
    finally:
        EvmGateway.connector = original
    assert called == [view]
    assert result.family == family and result.tx_hash == candidate.tx_hash
    assert result.transaction_plan.gas == case.state.pending.gas


def test_explicit_unsigned_abandonment_retains_evidence(case):
    ready(case)
    proposal = step(case, "reserve", 1).proposal
    family = step(case, "abandon", 1)
    assert family.proposal is None and family.abandoned == (proposal,)
    assert family.candidates[0].submission.at == 100.0


def configure(case, changes):
    config = case.state.config
    policy = config.production.gas[0].model_copy(update=changes)
    config = config.model_copy(
        update={"production": config.production.model_copy(update={"gas": (policy,)})}
    )
    case.backend.config = config
    pending = start_family(config, case.state.pending.model_copy(update={"family": None}))
    case.state = case.state.model_copy(update={"config": config, "pending": pending})


@pytest.mark.parametrize("limit", ["price", "transaction", "lifetime"])
def test_replacement_never_clamps_a_quote_to_caps(case, limit):
    policy = case.state.config.production.gas[0]
    fee = case.state.pending.gas * 110
    spent = sum(o.gas_paid.amount for o in case.state.outcomes)
    changes = {
        "price": {"max_gas_price_wei": 109},
        "transaction": {
            "max_transaction_fee": policy.max_transaction_fee.model_copy(update={"amount": fee - 1})
        },
        "lifetime": {
            "total_budget": policy.total_budget.model_copy(
                update={"amount": spent + fee + policy.exit_reserve.amount - 1}
            )
        },
    }[limit]
    configure(case, changes)
    ready(case)
    family = step(case, "reserve", 1)
    assert family.blocked == {"price": "gas_price", "transaction": "gas_fee_quote"}.get(
        limit, "gas_budget"
    )
    assert family.proposal is None and len(family.candidates) == 1


@pytest.mark.parametrize("stage", ["reserve", "sign", "attempt", "broadcast"])
@pytest.mark.parametrize("ceiling", ["transaction", "lifetime"])
def test_op_replacement_reserves_complete_candidate_exposure(case, stage, ceiling):
    policy = case.state.config.production.gas[0]
    exposure = 500
    replacement_fee = case.state.pending.gas * 110
    spent = sum(o.gas_paid.amount for o in case.state.outcomes)
    changes = {
        "transaction": {
            "max_transaction_fee": policy.max_transaction_fee.model_copy(
                update={"amount": replacement_fee + exposure - 1}
            )
        },
        "lifetime": {
            "total_budget": policy.total_budget.model_copy(
                update={
                    "amount": spent + replacement_fee + policy.exit_reserve.amount + exposure - 1
                }
            )
        },
    }[ceiling]
    configure(case, changes)
    ready(case)
    case.backend.op_exposure = lambda *args: exposure

    # Later stages independently enforce the retained reservation. Bypass only
    # the earlier stage so each admission boundary is exercised in isolation.
    validate = case.backend.validate_fee_budget
    if stage != "reserve":
        case.backend.validate_fee_budget = lambda *args: None
        step(case, "reserve", 1)
    if stage in ("attempt", "broadcast"):
        step(case, "sign", 1)
    if stage == "broadcast":
        step(case, "attempt", 1)
    case.backend.validate_fee_budget = validate

    if stage == "broadcast":
        with pytest.raises(DataError, match="fee ceiling|remaining budget"):
            replacement.broadcast(case.backend, case.state)
    else:
        family = step(case, stage, 1)
        kind = {"transaction": "gas_fee_quote", "lifetime": "gas_budget"}[ceiling]
        assert family.blocked == kind
        if stage == "reserve":
            assert family.proposal is None
        elif stage == "sign":
            assert len(family.candidates) == 1
        else:
            assert family.candidates[1].submission is None


def test_family_report_includes_retained_additional_fee_exposure(case):
    ready(case)
    exposure = 500
    exposure_blocks = []

    def op_exposure(operation, block, data, gas):
        exposure_blocks.append(block)
        return exposure

    case.backend.op_exposure = op_exposure
    step(case, "reserve", 1)
    pending = case.state.pending
    report = family_report(case.state)["pending"]
    assert exposure_blocks and all(block == case.backend.reader.head for block in exposure_blocks)
    assert case.backend.reader.head != pending.before.block
    assert report["proposal"]["fee_quote"]["additional_fee_exposure"] == exposure
    assert report["maximum_reserved_fee"]["amount"] == str(pending.gas * 110 + exposure)


def test_family_reserves_highest_complete_fee_when_later_op_exposure_falls(case):
    policy = case.state.config.production.gas[0]
    gas = case.state.pending.gas
    earlier_exposure = gas * 20
    earlier_fee = gas * 110 + earlier_exposure
    spent = sum(o.gas_paid.amount for o in case.state.outcomes)
    configure(
        case,
        {
            "total_budget": policy.total_budget.model_copy(
                update={"amount": spent + earlier_fee + policy.exit_reserve.amount}
            )
        },
    )
    ready(case)
    case.backend.op_exposure = lambda *args: earlier_exposure
    step(case, "reserve", 1)
    step(case, "sign", 1)
    step(case, "attempt", 1)
    case.clock[0] += 5
    case.backend.reader.advance()
    case.backend.op_exposure = lambda *args: 0
    proposal = step(case, "reserve", 2).proposal
    assert proposal is not None and proposal.gas_price == 121
    assert gas * proposal.gas_price < earlier_fee
    balance = case.state.pending.before.native_balance.model_copy(
        update={"amount": earlier_fee - 1}
    )
    with pytest.raises(DataError, match="payer balance"):
        validate_budget(case.state, case.state.pending.operation, balance, gas, 121)
    assert family_report(case.state)["pending"]["maximum_reserved_fee"]["amount"] == str(
        earlier_fee
    )


def test_fee_and_budget_exact_boundaries_preserve_selected_source(case):
    fee = case.state.pending.gas * 110
    policy = case.state.config.production.gas[0]
    configure(
        case,
        {
            "max_gas_price_wei": 110,
            "max_transaction_fee": policy.max_transaction_fee.model_copy(update={"amount": fee}),
        },
    )
    ready(case)
    proposal = step(case, "reserve", 1).proposal
    assert proposal.gas_price == 110
    assert case.state.config.production.gas[0].source == "network_rpc_fee_history"
    assert proposal.fee_quote.block == case.backend.reader.head
    assert proposal.fee_quote.next_base_fee == proposal.fee_quote.priority_fee == 1


def test_quote_reproduction_rejects_tampering_and_staleness(case):
    ready(case)
    family = step(case, "reserve", 1)
    forged = family.proposal.model_copy(update={"gas_price": 111})
    case.state = with_family(case.state, family.model_copy(update={"proposal": forged}))
    with pytest.raises(DataError, match="retained observation"):
        step(case, "sign", 1)
    case.state = with_family(case.state, family)
    case.backend.require_fresh = refuse_stale
    assert step(case, "sign", 1).blocked == "stale_plan"
    assert len(case.state.pending.family.candidates) == 1


def refuse_stale(*args):
    raise DataError("stale_plan", "fixture observation expired")


@pytest.mark.parametrize(
    "field,value",
    [
        ("nonce", 99),
        ("gas", 1),
        ("to", "0x" + "12" * 20),
        ("value", 1),
        ("data", "0x12"),
        ("chainId", 31337),
        ("gasPrice", 111),
    ],
)
def test_signed_candidate_cannot_change_envelope(case, field, value):
    family = second(case)
    pending = case.state.pending
    execution = pending.execution
    tx = dict(
        chainId=1,
        nonce=pending.nonce,
        gas=pending.gas,
        gasPrice=110,
        to=execution.to,
        value=execution.value,
        data=execution.data,
    )
    from eth_utils import to_checksum_address

    tx["to"] = to_checksum_address(tx["to"])
    tx[field] = value
    signed = Account.from_key(TEST_KEY).sign_transaction(tx)
    forged = family.candidates[1].model_copy(
        update={
            "raw_transaction": "0x" + signed.raw_transaction.hex(),
            "tx_hash": "0x" + signed.hash.hex(),
        }
    )
    with pytest.raises(DataError, match="identity"):
        verify_signed(
            candidate_view(pending, forged), execution, case.backend.reader.transport_chain
        )


def test_forged_revision_and_nonlegacy_bytes_are_rejected(case):
    pending = case.state.pending
    family = pending.family.model_copy(update={"configuration_revision": "00" * 32})
    with pytest.raises(DataError, match="revision"):
        case.backend.validate_pending(pending.model_copy(update={"family": family}))
    with pytest.raises(DataError, match="legacy envelope"):
        verify_signed(
            pending.model_copy(update={"raw_transaction": "0x00"}),
            pending.execution,
            case.backend.reader.transport_chain,
        )
    assert pending.family.configuration_revision == digest(case.state.config)


def deny_authority(*args):
    raise DataError("authority", "fixture role revoked")


def test_revocation_blocks_fee_actions_but_receipt_reads_need_no_signer(case):
    second(case)
    case.backend.verify_authority = deny_authority
    assert step(case, "attempt", 1).blocked == "authority"
    case.backend.signers = {}
    assert case.backend.receipt(case.state.pending) is None
    assert len(case.state.pending.family.candidates) == 2


def test_terminal_abandonment_cannot_reset_escalation(case):
    ready(case)
    step(case, "reserve", 1)
    case.clock[0] = 161.0
    assert step(case, "sign", 1).blocked == "elapsed_limit"
    family = step(case, "abandon", 1)
    assert family.stopped and family.blocked == "elapsed_limit"
    assert step(case, "reserve", 1).proposal is None


def test_submission_ack_loss_rebroadcasts_same_identity(case):
    family = second(case)
    step(case, "attempt", 1)
    pending = case.state.pending
    after, sent = replacement.record_broadcast(case.backend, case.state)
    assert sent
    restored, sent = replacement.record_broadcast(case.backend, case.state)
    assert not sent and restored == after
    assert restored.pending.family.candidates[1].tx_hash == family.candidates[1].tx_hash
    assert case.backend.receipt(pending) is None


def test_submission_error_is_durable_without_erasing_attempt(case):
    ready(case)
    original = case.state.pending.family.candidates[0]
    case.backend.reader.error = DataError("transport", "lost acknowledgement")
    state, sent = replacement.record_broadcast(case.backend, case.state)
    assert not sent
    candidate = state.pending.family.candidates[0]
    assert candidate.submission_error == "transport"
    assert candidate.submission == original.submission
    assert candidate.raw_transaction == original.raw_transaction


def runtime_port(case):
    faults = []

    def family(pending, action, index):
        step(case, action, index)
        return case.state

    def broadcast(pending):
        case.state, sent = replacement.record_broadcast(case.backend, case.state)
        return sent

    return SimpleNamespace(
        gateway=SimpleNamespace(family=family, broadcast=broadcast),
        fault=lambda stage, pending: faults.append((stage, pending.family)),
        faults=faults,
    )


def test_runtime_schedules_stages_and_abandons_expired_unsigned_proposal(case):
    from almanak.replacement_runtime import progress

    runtime = runtime_port(case)
    progress(runtime, case.state.pending)
    progress(runtime, case.state.pending)
    case.clock[0] += 5
    case.backend.reader.advance()
    progress(runtime, case.state.pending)
    assert case.state.pending.family.proposal is not None
    progress(runtime, case.state.pending)
    progress(runtime, case.state.pending)
    progress(runtime, case.state.pending)
    assert len(case.state.pending.family.candidates) == 2
    case.clock[0] += 5
    case.backend.reader.advance()
    progress(runtime, case.state.pending)
    case.clock[0] = 161.0
    progress(runtime, case.state.pending)
    progress(runtime, case.state.pending)
    assert case.state.pending.family.proposal is None
    assert case.state.pending.family.stopped
    assert [stage for stage, _ in runtime.faults[:5]] == [
        "replacement_attempt",
        "broadcast",
        "replacement_reserve",
        "replacement_sign",
        "replacement_attempt",
    ]


@pytest.mark.parametrize("kind", ["authority", "gateway_unavailable", "inconsistent"])
def test_runtime_does_not_turn_send_refusal_into_an_extra_bid(case, kind):
    from almanak.replacement_runtime import progress

    runtime = runtime_port(case)
    progress(runtime, case.state.pending)

    def fail(pending):
        raise DataError(kind, "fixture refusal")

    runtime.gateway.broadcast = fail
    if kind == "authority":
        progress(runtime, case.state.pending)
    else:
        with pytest.raises(DataError):
            progress(runtime, case.state.pending)
    assert len(case.state.pending.family.candidates) == 1
    assert case.state.pending.family.proposal is None


def test_gateway_fences_family_admission_and_replays_durable_ack(
    case, tmp_path, reference_strategy
):
    ready(case)
    case.backend.audit_history = lambda _: None
    service = GatewayService(
        case.state.config, tmp_path / "state.sqlite", case.backend, lease_seconds=30
    )
    service.journal.create(case.state.config, reference_strategy)
    service.journal.update(lambda _: case.state)
    coordinator = "ab" * 16
    service.journal.acquire(coordinator, 30)
    request = service.request_type(
        coordinator=coordinator,
        deployment_id=case.state.deployment_id,
        family_id=case.state.pending.family.identity,
        candidate_index=1,
        action="reserve",
    )
    retained = service.dispatch("family", request)
    restarted = GatewayService(
        case.state.config, service.journal.path, case.backend, lease_seconds=30
    )
    assert restarted.dispatch("family", request) == retained
    assert retained.pending.family.proposal is not None
    service.journal.release(coordinator)
    service.journal.acquire("cd" * 16, 30)
    with pytest.raises(RuntimeError, match="superseded"):
        restarted.dispatch("family", request.model_copy(update={"action": "sign"}))
    assert service.journal.load() == retained


@pytest.mark.parametrize("action", ["reserve", "sign"])
@pytest.mark.parametrize("clock_at,reason", [(161.0, "elapsed_limit"), (104.0, "clock_reversed")])
def test_admission_rechecks_elapsed_limit_and_clock(case, action, clock_at, reason):
    ready(case)
    if action == "sign":
        step(case, "reserve", 1)

    def slow(view):
        case.clock[0] = clock_at
        return case.state.pending.before

    case.backend.validate_current = slow
    family = step(case, action, 1)
    assert family.stopped and family.blocked == reason
    assert len(family.candidates) == 1


def test_replacement_checks_token_revisions_without_selecting_addresses(monkeypatch):
    from almanak.caip import AssetType
    from almanak.gateway import replacement_assets as assets
    from almanak.gateway.permission_models import ContractIdentity

    asset = AssetType("eip155:1/erc20:0x" + "12" * 20)
    for revision in assets.REVISIONS:
        token = ContractIdentity(
            address=asset.reference,
            implementation="0x" + "34" * 20 if revision.implementation_hash else None,
            **revision.model_dump(),
        )
        monkeypatch.setattr(
            assets,
            "AuthorityReader",
            lambda *args, expected=token: SimpleNamespace(identity=lambda _: expected),
        )
        assets.verify_asset(None, asset, None)
    unsupported = AssetType("eip155:43114/erc20:0x" + "12" * 20)
    with pytest.raises(DataError, match="no verified"):
        assets.verify_asset(None, unsupported, None)
    changed = token.model_copy(update={"implementation_hash": "0x" + "56" * 32})
    monkeypatch.setattr(
        assets, "AuthorityReader", lambda *args: SimpleNamespace(identity=lambda _: changed)
    )
    with pytest.raises(DataError, match="code differs"):
        assets.verify_asset(None, asset, None)


def test_future_unsupported_operations_refuse_replacement_explicitly(case):
    from almanak.connectors.aave_v4.execution import AaveExecution
    from almanak.connectors.uniswap_v4.execution import UniswapExecution

    unknown = SimpleNamespace(account=case.state.pending.operation.account)
    for connector in (AaveExecution, UniswapExecution):
        with pytest.raises(DataError) as refused:
            connector.validate_replacement(case.state.config, unknown)
        assert refused.value.kind == "replacement_unsupported"
    assert case.backend.receipt(case.state.pending) is None


def test_elapsed_limit_blocks_first_attempt_of_a_signed_increase(case):
    second(case)
    case.clock[0] = 161.0
    family = step(case, "attempt", 1)
    assert family.stopped and family.blocked == "elapsed_limit"
    assert family.candidates[1].submission is None
    with pytest.raises(DataError, match="record submission"):
        case.backend.broadcast(case.state.pending, state=case.state)


def test_attempted_candidate_can_be_rebroadcast_after_escalation_expiry(case):
    second(case)
    step(case, "attempt", 1)
    case.clock[0] = 161.0
    family = step(case, "inspect", 1)
    assert family.stopped
    assert case.backend.broadcast(case.state.pending, state=case.state)
    assert case.backend.reader.transactions[family.candidates[1].tx_hash]


def test_clock_rollback_during_original_attempt_stops_future_escalation(case):
    def backwards(view):
        case.clock[0] = 99.0
        return case.state.pending.before

    case.backend.validate_current = backwards
    family = step(case, "attempt", 0)
    assert family.stopped and family.blocked == "clock_reversed"
    assert family.candidates[0].submission is None
    case.backend.validate_current = lambda view: case.state.pending.before
    case.clock[0] = 101.0
    assert step(case, "attempt", 0).blocked == "clock_reversed"
