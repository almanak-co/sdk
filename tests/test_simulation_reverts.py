"""A reverting pre-signing simulation is named and re-planned, never a generic RPC refusal."""

import json
import secrets
import threading
from types import SimpleNamespace

import httpx
import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak import runtime as runtime_module
from almanak.connectors.aave_v4.state import TransactionPlan
from almanak.connectors.composition import connector_for
from almanak.gateway.accounts import inner_revert, revert_diagnosis
from almanak.gateway.ethereum import DataError, Reverted, member_result
from almanak.gateway.execution import mined_revert, simulated
from almanak.gateway.reverts import (
    MAX_REASON_CHARS,
    MAX_REVERT_BYTES,
    MODULE_TRANSACTION_FAILED,
    describe,
    is_revert,
    revert_data,
)
from almanak.gateway.service import http_server
from almanak.models import Model
from almanak.runtime import REPLAN_ATTEMPTS, REPLAN_BACKOFF_SECONDS, ReplansExhausted
from tests.test_aave_debt import borrowing_proof as borrowing_proof
from tests.test_execution_profiles import captured_pending
from tests.test_execution_profiles import production_debt as production_debt
from tests.test_expired_attempt_release import MemoryJournal
from tests.test_production import Reader
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded
from tests.test_runtime_recovery import coordinator


def error_string(text: str) -> bytes:
    return keccak(text="Error(string)")[:4] + encode(("string",), (text,))


SLIPPAGE = error_string("Price slippage check")


@pytest.mark.parametrize(
    "data,expected",
    [
        (None, "no revert data"),
        (b"", "no revert data"),
        (b"\x01\x02", "malformed revert data"),
        (SLIPPAGE, 'Error("Price slippage check")'),
        (error_string("bad\nline\x00"), 'Error("bad?line?")'),
        (keccak(text="Error(string)")[:4] + b"\x00" * 3, "Error(string) with malformed data"),
        (keccak(text="Panic(uint256)")[:4] + encode(("uint256",), (0x11,)), "Panic(0x11: "),
        (keccak(text="Panic(uint256)")[:4] + encode(("uint256",), (2**255,)), "Panic(unknown"),
        (keccak(text="Panic(uint256)")[:4], "Panic(uint256) with malformed data"),
        (MODULE_TRANSACTION_FAILED, "ModuleTransactionFailed()"),
        (bytes.fromhex("deadbeef") + b"\x00" * 32, "custom error 0xdeadbeef"),
    ],
)
def test_revert_data_is_described_in_standard_encodings(data, expected):
    assert describe(data).startswith(expected)


def test_long_reasons_are_truncated():
    described = describe(error_string("x" * 1000))
    assert len(described) <= MAX_REASON_CHARS + len('Error("...")')


@pytest.mark.parametrize(
    "error,reverted",
    [
        ({"code": 3, "message": "execution reverted: whatever", "data": "0x"}, True),
        ({"code": -32000, "message": "execution reverted"}, True),
        ({"code": -32000, "message": "header not found"}, False),
        ({"code": 3.0, "message": "execution reverted"}, False),
        ({"code": -32603, "message": "execution reverted"}, False),
        ("execution reverted", False),
    ],
)
def test_only_execution_reverts_are_classified(error, reverted):
    assert is_revert(error) is reverted


@pytest.mark.parametrize(
    "data,expected",
    [
        ("0x" + SLIPPAGE.hex(), SLIPPAGE),
        ({"data": "0x" + SLIPPAGE.hex()}, SLIPPAGE),
        ("0xABCD", bytes.fromhex("abcd")),
        ("0xabc", None),
        ("execution reverted", None),
        ("0x" + "00" * (MAX_REVERT_BYTES + 1), None),
        (7, None),
    ],
)
def test_revert_data_is_bounded_hex(data, expected):
    assert revert_data({"code": 3, "data": data}) == expected


def test_a_revert_keeps_its_data_but_never_the_provider_message():
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": "execution reverted via https://provider.invalid/v2/secret-key",
            "data": "0x" + SLIPPAGE.hex(),
        },
    }
    with pytest.raises(Reverted) as error:
        member_result("eth_call", payload)
    # Reads that tolerate a reverting view call still see the generic kind.
    assert error.value.kind == "rpc" and error.value.data == SLIPPAGE and error.value.code == 3
    assert str(error.value) == "eth_call: RPC rejected the requested read (JSON-RPC 3)"
    refused = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "secret-key"}}
    with pytest.raises(DataError) as error:
        member_result("eth_call", refused)
    assert type(error.value) is DataError and "secret" not in str(error.value)


class SequencedReader(Reader):
    """Answers each eth_call from a queue; an exception in the queue is raised."""

    def __init__(self, block, answers):
        super().__init__(block)
        self.answers = list(answers)

    def _rpc(self, method, params, **kwargs):
        if method != "eth_call":
            return super()._rpc(method, params, **kwargs)
        self.calls.append((method, params))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def roles_revert():
    return Reverted(
        "eth_call: RPC rejected the requested read (JSON-RPC 3)", MODULE_TRANSACTION_FAILED
    )


def failed_inner(data):
    return "0x" + encode(("bool", "bytes"), (False, data)).hex()


def test_roles_simulation_revert_reports_the_inner_protocol_reason(production_debt):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    pending = captured_pending(production_debt, outcome)
    backend = SimpleNamespace(
        config=production_debt.config,
        reader=SequencedReader(outcome.before.position.reserve.block, []),
    )
    backend.reader.answers = [roles_revert(), failed_inner(SLIPPAGE)]
    from almanak.gateway.execution import EvmGateway

    with pytest.raises(DataError) as error:
        EvmGateway.simulate_current(backend, pending, pending.before)
    assert error.value.kind == "simulation_reverted"
    assert str(error.value) == 'eth_call: simulation reverted: Error("Price slippage check")'
    (simulation, selector), (diagnosis, again) = [p for m, p in backend.reader.calls]
    assert selector == again == {"blockHash": pending.before.block.hash, "requireCanonical": True}
    head = 10 + 5 * 64
    # Only shouldRevert differs: the diagnosis reads the same authorized call.
    assert simulation["data"][head : head + 64] == "0" * 63 + "1"
    assert diagnosis["data"][head : head + 64] == "0" * 64
    assert diagnosis["data"][:head] == simulation["data"][:head]
    assert diagnosis["data"][head + 64 :] == simulation["data"][head + 64 :]
    assert {k: v for k, v in diagnosis.items() if k != "data"} == {
        k: v for k, v in simulation.items() if k != "data"
    }


@pytest.mark.parametrize(
    "diagnosis",
    [
        DataError("rpc", "eth_call: RPC rejected the requested read (JSON-RPC -32000)"),
        "0x" + encode(("bool", "bytes"), (True, b"")).hex(),
        "0x",
    ],
)
def test_an_unreadable_diagnosis_keeps_the_outer_reason(production_debt, diagnosis):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    pending = captured_pending(production_debt, outcome)
    reader = SequencedReader(outcome.before.position.reserve.block, [roles_revert(), diagnosis])
    with pytest.raises(DataError, match=r"simulation reverted: ModuleTransactionFailed\(\)$"):
        simulated(reader, pending.execution, "eth_call", [{}, "latest"])


def test_direct_reverts_are_described_without_a_diagnosis():
    reader = SequencedReader(
        SimpleNamespace(chain=SimpleNamespace(chain_id="eip155:1")),
        [
            Reverted(
                "eth_call: RPC rejected",
                keccak(text="Panic(uint256)")[:4] + encode(("uint256",), (0x12,)),
            )
        ],
    )
    eoa = SimpleNamespace(binding=SimpleNamespace(profile=object()))
    assert revert_diagnosis(eoa) is None
    with pytest.raises(DataError, match=r"simulation reverted: Panic\(0x12: division"):
        simulated(reader, eoa, "eth_call", [{}, "latest"])
    assert len(reader.calls) == 1


def test_inner_revert_reads_only_failed_bounded_results():
    assert inner_revert(failed_inner(SLIPPAGE)) == SLIPPAGE
    assert inner_revert("0x" + encode(("bool", "bytes"), (True, SLIPPAGE)).hex()) is None
    assert inner_revert(failed_inner(b"\x00" * (MAX_REVERT_BYTES + 1))) is None
    assert inner_revert("0xzz") is None and inner_revert(None) is None


@pytest.mark.parametrize(
    "answer,expected",
    [
        (failed_inner(SLIPPAGE), 'Error("Price slippage check")'),
        (
            "0x" + encode(("bool", "bytes"), (True, b"")).hex(),
            "inner revert not reproduced at the parent block",
        ),
        (DataError("rpc", "eth_call: RPC rejected the requested read (JSON-RPC -32000)"), None),
    ],
)
def test_a_mined_roles_revert_is_named_at_its_parent_block(production_debt, answer, expected):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    pending = captured_pending(production_debt, outcome)
    reader = SequencedReader(outcome.before.position.reserve.block, [answer])
    assert mined_revert(reader, pending.execution, {"blockNumber": "0x10"}) == expected
    ((_, (diagnosis, selector)),) = reader.calls
    assert diagnosis == revert_diagnosis(pending.execution)
    assert selector == {"blockNumber": "0xf"}


def test_direct_or_unbound_mined_reverts_are_not_replayed():
    reader = SequencedReader(SimpleNamespace(chain=SimpleNamespace(chain_id="eip155:1")), [])
    eoa = SimpleNamespace(binding=SimpleNamespace(profile=object()))
    assert mined_revert(reader, eoa, {"blockNumber": "0x10"}) is None
    assert mined_revert(reader, None, {"blockNumber": "0x10"}) is None
    assert reader.calls == []


def test_a_recorded_revert_reason_names_the_stop_and_keeps_old_records_unchanged(
    production_debt,
):
    outcome = next(o for o in production_debt.outcomes if o.operation.kind == "repay")
    assert "revert_reason" not in outcome.model_dump(mode="json")
    failed = outcome.model_copy(
        update={"success": False, "revert_reason": 'Error("Price slippage check")'}
    )
    assert type(outcome).model_validate_json(failed.model_dump_json()) == failed
    state = production_debt.model_copy(
        update={"mode": "running", "outcomes": (*production_debt.outcomes, failed)}
    )
    with pytest.raises(DataError) as error:
        runtime_module.Runtime.decide(None, state)
    assert error.value.kind == "execution_failed"
    assert str(error.value) == (
        'recorded execution failure (Error("Price slippage check")) blocks new exposure; '
        "request teardown"
    )


def test_gateway_log_names_the_revert_reason(tmp_path, capsys):
    token = tmp_path / "gateway.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    refusals = iter(
        [
            DataError("simulation_reverted", 'eth_call: simulation reverted: Error("Price")'),
            DataError("rpc", "eth_call: RPC rejected the requested read (JSON-RPC -32000)"),
        ]
    )

    def dispatch(method, request):
        raise next(refusals)

    server = http_server(SimpleNamespace(request_type=Model, dispatch=dispatch), token, 0)
    serving = threading.Thread(target=server.serve_forever)
    serving.start()
    try:
        bodies = [
            httpx.post(
                f"http://127.0.0.1:{server.server_port}/state",
                headers={"Authorization": "Bearer " + token.read_text()},
                json={},
                timeout=5,
            ).json()
            for _ in range(2)
        ]
    finally:
        server.shutdown()
        serving.join(timeout=5)
        server.server_close()
    assert bodies[0] == {
        "error": "simulation_reverted",
        "message": 'eth_call: simulation reverted: Error("Price")',
    }
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert lines[0]["status"] == 409
    assert (
        lines[0]["reason"] == 'simulation_reverted: eth_call: simulation reverted: Error("Price")'
    )
    # Other refusals keep their messages out of the gateway log, as before.
    assert "reason" not in lines[1]


REVERTED = 'eth_call: simulation reverted: Error("Price slippage check")'


def planned(state, pending):
    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    )
    return state.model_copy(update={"pending": None, "plan": plan})


def events(capsys):
    return [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if line.startswith('{"event": "unsigned_plan_refused"')
    ]


def test_a_reverting_signature_simulation_is_replanned_then_signed(production_attempt, capsys):
    state, pending, _ = production_attempt
    journal = MemoryJournal(planned(state, pending))
    outcomes = iter([DataError("simulation_reverted", REVERTED), pending])

    def sign():
        answer = next(outcomes)
        if isinstance(answer, Exception):
            raise answer
        return answer

    gateway = SimpleNamespace(sign=sign, prepare=lambda state, intent: planned(state, pending).plan)
    runtime = coordinator(journal, gateway)
    assert runtime.tick() is False
    # Nothing was signed, so the plan and its decision are discarded as for an expired plan.
    assert journal.load().plan is None and journal.load().pending is None
    assert journal.load().decision is None
    assert runtime_module.next_delay(runtime, 1, 0) == REPLAN_BACKOFF_SECONDS
    assert events(capsys) == [
        {
            "event": "unsigned_plan_refused",
            "kind": "simulation_reverted",
            "stage": "sign",
            "attempt": 1,
            "limit": REPLAN_ATTEMPTS,
            "reason": REVERTED,
        }
    ]
    # The next decision is prepared from fresh state and signs.
    journal.state = journal.state.model_copy(update={"decision": state.decision})
    assert runtime.tick() is False and journal.load().plan is not None
    assert runtime_module.next_delay(runtime, 1, 0) == 1
    assert runtime.tick() is False
    assert journal.load().pending == pending and journal.load().plan is None
    # The budget runs until the attempt is sent: a refusal of its first send still counts.
    assert runtime.replanning().attempts == {None: 1}


MOVED = "pool price moved beyond the retained liquidity tolerance"


def test_persistent_market_refusals_stop_with_the_reason_after_the_cap(production_attempt, capsys):
    state, pending, _ = production_attempt

    def sign():
        raise DataError("simulation_reverted", REVERTED)

    def prepare(state, intent):
        raise DataError("price_movement", MOVED)

    journal = MemoryJournal(planned(state, pending))
    runtime = coordinator(journal, SimpleNamespace(sign=sign, prepare=prepare))
    for attempt in range(1, REPLAN_ATTEMPTS):
        # Alternate stages: a moved price at preparation and a revert at signing count
        # into one consecutive budget.
        if attempt % 2:
            journal.state = planned(state, pending)
        else:
            journal.state = state.model_copy(update={"pending": None, "plan": None})
        assert runtime.tick() is False
        assert journal.load().plan is None and journal.load().decision is None
        delay = runtime_module.next_delay(runtime, 1, 0)
        assert delay == attempt * REPLAN_BACKOFF_SECONDS
    stopped = planned(state, pending)
    journal.state = stopped
    with pytest.raises(DataError, match="Price slippage check") as error:
        runtime.tick()
    assert error.value.kind == "simulation_reverted"
    # The unsigned plan is retained for the operator, exactly as other refusals leave it.
    assert journal.load() == stopped
    logged = events(capsys)
    assert [e["attempt"] for e in logged] == list(range(1, REPLAN_ATTEMPTS + 1))
    assert {(e["stage"], e["kind"], e["reason"]) for e in logged} == {
        ("sign", "simulation_reverted", REVERTED),
        ("prepare", "price_movement", MOVED),
    }


def test_chain_scoped_copies_count_into_one_budget(production_attempt):
    from copy import copy

    state, pending, _ = production_attempt
    runtime = coordinator(MemoryJournal(planned(state, pending)), SimpleNamespace())
    runtime.replanning()
    for _ in range(2):
        scoped = copy(runtime)
        scoped.gateway = SimpleNamespace(chain="eip155:56")
        scoped.count_replan(DataError("simulation_reverted", REVERTED), "sign")
    assert runtime.replanning().attempts == {"eip155:56": 2}


def test_other_signing_refusals_still_stop_without_counting(production_attempt):
    state, pending, _ = production_attempt
    journal = MemoryJournal(planned(state, pending))

    def sign():
        raise DataError("rpc", "eth_call: RPC rejected the requested read (JSON-RPC -32000)")

    runtime = coordinator(journal, SimpleNamespace(sign=sign))
    with pytest.raises(DataError, match="JSON-RPC -32000"):
        runtime.tick()
    assert journal.load().plan is not None and runtime.replanning().attempts == {}


class ChainParent:
    """The unscoped gateway behind a ScopedGateway: answers each routed method."""

    def __init__(self, state, answers):
        self.state, self.answers = state, answers
        self.connector = connector_for(state.config)

    def load(self):
        return self.state

    def update(self, change, *, chain=None):
        self.state = change(self.state)
        return self.state

    def scoped(self, method, *, chain, **fields):
        answer = self.answers.get(method)
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def single_route(monkeypatch):
    # One route whose projection is the whole checkpoint, so advance_chain's real error
    # handling runs over the ordinary single-chain admission path.
    from almanak import work

    monkeypatch.setattr(work, "project", lambda state, chain: state)
    monkeypatch.setattr(work, "merge", lambda state, chain, changed: changed)
    monkeypatch.setattr(work, "availability", lambda state: ())


def test_capped_market_refusal_stops_a_multichain_coordinator(production_attempt, single_route):
    state, pending, _ = production_attempt
    planned_state = planned(state, pending)
    refusal = DataError("simulation_reverted", REVERTED)
    parent = ChainParent(planned_state, {"audit": None, "sign": refusal})
    runtime = coordinator(MemoryJournal(planned_state), parent)
    for attempt in range(1, REPLAN_ATTEMPTS):
        parent.state = planned_state
        runtime.advance_chain(planned_state, "eip155:56")
        assert parent.state.plan is None
        assert runtime.replanning().attempts == {"eip155:56": attempt}
    parent.state = planned_state
    with pytest.raises(ReplansExhausted, match="Price slippage check") as error:
        runtime.advance_chain(planned_state, "eip155:56")
    assert error.value.kind == "simulation_reverted"
    assert parent.state == planned_state


def test_unmeasured_hook_terms_at_preparation_are_replanned_then_stop(production_attempt, capsys):
    # A transient failure of a hook's term view refuses one entry; persistent failure stops.
    state, pending, _ = production_attempt
    missing = "Uniswap V4 output-haircut hook terms are unmeasured"

    def prepare(state, intent):
        raise DataError("hook_terms", missing)

    deciding = state.model_copy(update={"pending": None, "plan": None})
    journal = MemoryJournal(deciding)
    runtime = coordinator(journal, SimpleNamespace(sign=None, prepare=prepare))
    for attempt in range(1, REPLAN_ATTEMPTS):
        journal.state = deciding
        assert runtime.tick() is False
        assert journal.load().decision is None
        assert runtime_module.next_delay(runtime, 1, 0) == attempt * REPLAN_BACKOFF_SECONDS
    journal.state = deciding
    with pytest.raises(DataError, match="unmeasured") as error:
        runtime.tick()
    assert error.value.kind == "hook_terms"
    assert [e["kind"] for e in events(capsys)] == ["hook_terms"] * REPLAN_ATTEMPTS


@pytest.mark.parametrize("kind", ["simulation_reverted", "price_movement", "rpc"])
def test_other_chain_local_refusals_still_advance_the_scheduler(
    production_attempt, single_route, kind
):
    state, pending, _ = production_attempt
    # A signed attempt refused at broadcast stays a chain-local fault, whatever its kind.
    signed = state.model_copy(update={"plan": None, "pending": pending})
    parent = ChainParent(
        signed, {"audit": None, "receipt": None, "broadcast": DataError(kind, "refused")}
    )
    runtime = coordinator(MemoryJournal(signed), parent)
    assert runtime.advance_chain(signed, "eip155:56") is None
    assert parent.state == signed and runtime.replanning().attempts == {}
