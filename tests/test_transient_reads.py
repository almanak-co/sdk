"""Transient provider reads: receipt waits, the gateway boundary and coordinator holds.

A load-balanced provider can serve a just-mined receipt and then answer its block with
null from a lagging backend, or throttle an execution read. Both
must hold the coordinator's tick instead of stopping the deployment.
"""

import json
import secrets
from datetime import timedelta

import httpx
import pytest

from almanak import runtime as runtime_module
from almanak.gateway import ethereum
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.production import ETHEREUM_GENESIS, ProductionGateway
from almanak.gateway.service import data_error_reply
from almanak.runtime import TRANSIENT_HOLD_HORIZON_SECONDS
from almanak.state import Journal
from tests.test_runtime_idle import Stop, events, loop  # noqa: F401
from tests.test_runtime_recovery import CapturedGateway, coordinator, pending_for
from tests.test_triggers import START, scheduled  # noqa: F401


@pytest.fixture
def no_backoff(monkeypatch):
    waits = []
    monkeypatch.setattr(ethereum, "sleep", waits.append)
    monkeypatch.setattr(ethereum, "monotonic", lambda: sum(waits))
    return waits


def lagging_gateway(outcome, lag):
    """A production gateway whose provider serves the receipt before its block."""
    chain = outcome.operation.target.fund.chain
    sent = []

    def respond(request):
        body = json.loads(request.content)
        method, params = body["method"], body["params"]
        sent.append(method)
        result = None
        if method == "eth_chainId":
            result = hex(ethereum.chain_number(reader.transport_chain))
        elif method == "eth_getTransactionReceipt":
            result = outcome.receipt
        elif method == "eth_getBlockByNumber" and sent.count(method) > lag:
            result = {
                "number": params[0],
                "hash": outcome.receipt["blockHash"],
                "timestamp": "0x1",
            }
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": result})

    reader = EthereumReader(
        chain,
        "https://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(respond)),
        retry_budget=ethereum.EXECUTION_RETRY_BUDGET,
    )
    gateway = object.__new__(ProductionGateway)
    gateway.reader = reader
    gateway.guard = lambda: None
    gateway.validate_pending = lambda pending: None
    return gateway, sent


def test_receipt_waits_for_a_block_the_provider_serves_late(recorded, no_backoff):
    outcome = next(o for o in recorded.outcomes if o.operation.kind == "supply")
    gateway, sent = lagging_gateway(outcome, lag=2)
    assert gateway.receipt(pending_for(recorded.config, outcome)) == outcome.receipt
    assert sent.count("eth_getBlockByNumber") == 3 and no_backoff == [0.5, 1.0]


def test_receipt_block_still_lagging_after_the_budget_is_transient(recorded, no_backoff):
    outcome = next(o for o in recorded.outcomes if o.operation.kind == "supply")
    gateway, sent = lagging_gateway(outcome, lag=100)
    with pytest.raises(DataError) as lagging:
        gateway.receipt(pending_for(recorded.config, outcome))
    assert lagging.value.kind == "unavailable" and lagging.value.transient
    assert sum(no_backoff) <= ethereum.EXECUTION_RETRY_BUDGET
    assert "eth_sendRawTransaction" not in sent


def test_production_readers_retry_and_fork_readers_do_not(monkeypatch):
    budgets = []
    monkeypatch.setattr(
        "almanak.gateway.execution.EthereumReader",
        lambda *args, **kwargs: budgets.append(kwargs["retry_budget"]),
    )
    monkeypatch.setattr(ProductionGateway, "guard", lambda self: None)
    from almanak.gateway.execution import EvmGateway

    config = type("Config", (), {"accounts": None})()
    monkeypatch.setattr(EvmGateway, "connector", property(lambda self: _Connector()))
    EvmGateway.__init__(object.__new__(ProductionGateway), config, "x", {}, chain="c")
    EvmGateway.__init__(
        object.__new__(ProductionGateway), config, "x", {}, chain="c", local_transport=True
    )
    assert budgets == [ethereum.EXECUTION_RETRY_BUDGET, 0.0]


class _Connector:
    class execution:  # noqa: N801
        @staticmethod
        def validate_config(config):
            return None


@pytest.mark.parametrize("transient", [True, False])
def test_the_transient_mark_crosses_the_gateway_boundary(tmp_path, transient):
    error = DataError("unavailable", "eth_getBlockByNumber: unavailable", transient=transient)
    reply = data_error_reply(error)
    assert reply.get("transient", False) is transient
    token = tmp_path / "auth.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    client = GatewayClient("http://127.0.0.1:1", token)
    client.http = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(409, json=reply))
    )
    with pytest.raises(DataError) as raised:
        client.request("receipt")
    assert (raised.value.kind, raised.value.transient) == ("unavailable", transient)


def failing_observe(s, failures, error):
    original = s.gateway.observe
    attempts = []

    def observe():
        attempts.append(None)
        if len(attempts) <= failures:
            raise error
        return original()

    s.gateway.observe = observe
    return attempts


def stop_at(s, seconds):
    def interrupt():
        if s.clock() - START >= timedelta(seconds=seconds):
            raise Stop

    return interrupt


def throttle():
    return DataError("http", "eth_getCode: HTTP 429", code=429, transient=True)


def test_held_ticks_log_back_off_and_recover(loop, capsys):  # noqa: F811
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    failing_observe(s, 2, throttle())
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 21))
    assert s.journal.load().memory.values == {"evaluations": 2}
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    held = [e for e in lines if e["event"] == "transient_read_hold"]
    assert held == [
        {
            "event": "transient_read_hold",
            "status": "held",
            "kind": "http",
            "reason": "eth_getCode: HTTP 429",
            "mode": "running",
            "attempt": n,
            "elapsed_seconds": elapsed,
            "retry_in_seconds": retry,
            "stops_after_seconds": TRANSIENT_HOLD_HORIZON_SECONDS,
        }
        for n, elapsed, retry in ((1, 0, 5), (2, 5, 10))
    ]
    recovered = [e for e in lines if e["event"] == "transient_read_recovered"]
    assert recovered == [
        {"event": "transient_read_recovered", "attempts": 2, "elapsed_seconds": 15}
    ]


def test_a_non_transient_read_failure_still_stops_at_once(loop):  # noqa: F811
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    failing_observe(s, 1, DataError("unavailable", "native balance is below the minimum"))
    with pytest.raises(DataError, match="below the minimum"):
        s.runner.run()
    assert s.sleeps == [] and s.runner.closed


def test_a_running_deployment_stops_only_after_the_hold_horizon(loop, capsys):  # noqa: F811
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    before = s.journal.load()
    failing_observe(s, 10_000, throttle())
    with pytest.raises(DataError, match="HTTP 429") as stopped:
        s.runner.run()
    assert stopped.value.transient and s.journal.load() == before
    logged = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if "transient_read_hold" in line
    ]
    *held, final = logged
    assert all(e["status"] == "held" for e in held)
    assert held[-1]["elapsed_seconds"] < TRANSIENT_HOLD_HORIZON_SECONDS
    assert final["status"] == "stopped" and final["retry_in_seconds"] is None
    assert final["elapsed_seconds"] >= TRANSIENT_HOLD_HORIZON_SECONDS


def test_holds_never_stop_a_deployment_in_teardown(loop, capsys):  # noqa: F811
    s = loop
    s.runner.tick()
    s.journal.teardown()
    failures = 2 * TRANSIENT_HOLD_HORIZON_SECONDS // runtime_module.TIMEOUT_BACKOFF_CAP_SECONDS
    failing_observe(s, failures, throttle())
    assert s.runner.run().mode == "closed"
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    logged = [e for e in lines if e["event"] == "transient_read_hold"]
    assert len(logged) == failures
    assert {(e["mode"], e["status"], e["stops_after_seconds"]) for e in logged} == {
        ("teardown", "held", None)
    }
    # Teardown keeps holding past the horizon, but tells the operator once that it is.
    [escalated] = [e for e in lines if e["event"] == "transient_read_hold_escalated"]
    assert escalated["mode"] == "teardown" and escalated["kind"] == "http"
    assert escalated["elapsed_seconds"] >= TRANSIENT_HOLD_HORIZON_SECONDS
    first = next(e for e in logged if e["elapsed_seconds"] >= TRANSIENT_HOLD_HORIZON_SECONDS)
    assert escalated["attempts"] == first["attempt"]
    assert [e["event"] for e in lines if e["event"].startswith("transient_read_rec")] == [
        "transient_read_recovered"
    ]


def test_a_hold_whose_mode_read_also_times_out_keeps_holding(loop, capsys):  # noqa: F811
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    load = s.journal.load
    calls = []

    def flaky_load():
        calls.append(None)
        # The tick's own load passes; the hold's mode read times out once.
        if len(calls) == 2:
            raise DataError("gateway_unavailable", "gateway request failed", transient=True)
        return load()

    failing_observe(s, 1, throttle())
    s.journal.load = flaky_load
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 16))
    [held] = events(capsys, "transient_read_hold")
    assert held["mode"] is None and held["stops_after_seconds"] is None


@pytest.mark.parametrize(
    ("method", "kind", "transient"),
    [
        ("receipt", "gateway_unavailable", True),
        ("observe", "gateway_unavailable", True),
        ("state", "gateway_unavailable", True),
        ("reconcile", "gateway_unavailable", True),
        ("prices", "timeout", False),
        ("broadcast", "gateway_unavailable", False),
        ("sign", "gateway_unavailable", False),
    ],
)
def test_a_read_only_gateway_request_that_times_out_is_transient(tmp_path, method, kind, transient):
    token = tmp_path / "auth.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    client = GatewayClient("http://127.0.0.1:1", token)

    def timeout(request):
        raise httpx.ReadTimeout("slow", request=request)

    client.http = httpx.Client(transport=httpx.MockTransport(timeout))
    with pytest.raises(DataError) as raised:
        client.request(method)
    assert (raised.value.kind, raised.value.transient) == (kind, transient)


def test_a_held_receipt_reconciliation_never_resubmits(
    tmp_path, recorded, reference_strategy, monkeypatch
):
    """The supply is mined; its receipt block lags at the provider for two ticks."""
    monkeypatch.setattr(runtime_module.time, "sleep", lambda seconds: None)
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    gateway = CapturedGateway(journal, recorded)
    gateway.close = lambda: None
    reconcile, lags = gateway.reconcile, []

    def lagging(pending, receipt):
        if pending.operation.kind == "supply" and len(lags) < 2:
            lags.append(pending.tx_hash)
            raise DataError(
                "unavailable",
                "eth_getBlockByNumber: requested data is unavailable",
                transient=True,
            )
        return reconcile(pending, receipt)

    gateway.reconcile = lagging
    runtime = coordinator(journal, gateway)

    def interrupt():
        state = journal.load()
        if any(o.operation.kind == "supply" for o in state.outcomes) and state.pending is None:
            raise Stop

    with pytest.raises(Stop):
        runtime.run(interrupt=interrupt)
    state = journal.load()
    supply = gateway.outcome("supply")
    assert lags == [supply.tx_hash, supply.tx_hash]
    # Each signed transaction was sent once; the holds only read its receipt again.
    assert gateway.broadcasts == [gateway.outcome("approve").tx_hash, supply.tx_hash]
    assert [o.tx_hash for o in state.outcomes] == gateway.broadcasts


def test_reconcile_without_a_receipt_still_stops_the_coordinator():
    """A receipt that vanished between the receipt and reconcile requests is not assumed to
    be provider lag: it may be a dropped or replaced transaction, so it is not held."""
    from types import SimpleNamespace

    from almanak.gateway.service import GatewayService

    service = object.__new__(GatewayService)
    service.backend = SimpleNamespace(receipt=lambda pending: None)
    with pytest.raises(DataError) as missing:
        service.dispatch_pending("reconcile", SimpleNamespace(pending=object()))
    assert missing.value.kind == "unavailable" and not missing.value.transient


def test_an_overrun_observe_holds_the_tick_not_the_decision(loop, capsys):  # noqa: F811
    """Unlike an overrun price read, which holds that decision and advances the schedule,
    an overrun observation commits nothing: the same decision is evaluated on retry."""
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    due = s.journal.load().schedule.next_due_at
    overrun = DataError("gateway_unavailable", "gateway request failed", transient=True)
    failing_observe(s, 1, overrun)
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 11))
    evaluation = s.journal.load().schedule.last_evaluation
    # The retry five seconds later keeps the original deadline.
    assert evaluation.due_at == due and evaluation.started_at == due + timedelta(seconds=5)
    assert s.journal.load().memory.values == {"evaluations": 2}
    [held] = events(capsys, "transient_read_hold")
    assert held["kind"] == "gateway_unavailable" and held["status"] == "held"


def test_an_overrun_observe_on_one_chain_is_not_kept_as_a_chain_fault():
    """A gateway failure, transient or not, leaves a chain-scoped tick, so the run loop
    holds it instead of recording a chain fault the gateway never wrote."""
    from almanak.runtime import GATEWAY_FAILURES

    overrun = DataError("gateway_unavailable", "gateway request failed", transient=True)
    assert overrun.kind in GATEWAY_FAILURES


def test_refusal_details_cross_the_gateway_boundary(tmp_path):
    details = {"remaining": [{"allocation": "loan", "asset": "a", "debt_scaled": "5"}]}
    reply = data_error_reply(DataError("remaining_obligations", "retains", details=details))
    token = tmp_path / "auth.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    client = GatewayClient("http://127.0.0.1:1", token)
    replies = iter([reply, {**reply, "details": ["not an object"]}])
    client.http = httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(409, json=next(replies)))
    )
    with pytest.raises(DataError) as raised:
        client.request("update")
    assert raised.value.details == details
    with pytest.raises(DataError) as raised:
        client.request("update")
    assert raised.value.details is None


def test_a_page_that_is_not_json_is_read_again_and_a_malformed_answer_is_not(no_backoff):
    from almanak.caip import ChainId
    from almanak.models import Chain

    answers = [
        httpx.Response(200, text="<html>overloaded</html>"),
        httpx.Response(200, json={"jsonrpc": "2.0", "id": 0, "result": {"number": "0x10"}}),
        httpx.Response(200, json={"jsonrpc": "2.0", "id": "wrong"}),
    ]
    sent = []

    def respond(request):
        sent.append(json.loads(request.content)["id"])
        answer = answers.pop(0)
        if answer.headers.get("content-type") == "application/json":
            body = json.loads(answer.content)
            body["id"] = sent[-1] if body["id"] == 0 else body["id"]
            answer = httpx.Response(200, json=body)
        return answer

    reader = EthereumReader(
        Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
        "https://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(respond)),
        retry_budget=ethereum.EXECUTION_RETRY_BUDGET,
    )
    assert reader._rpc("eth_getBlockByNumber", ["0x10", False]) == {"number": "0x10"}
    assert len(sent) == 2 and len(no_backoff) == 1
    with pytest.raises(DataError, match="invalid response envelope") as malformed:
        reader._rpc("eth_getBlockByNumber", ["0x11", False])
    assert malformed.value.kind == "malformed" and not malformed.value.transient
    assert len(sent) == 3
    assert ethereum.transient("eth_call", ethereum.UnreadableAnswer("malformed", "page"))
    assert not ethereum.transient("eth_sendRawTransaction", ethereum.UnreadableAnswer("x", "y"))


def answering(failures, client="Geth/v1.16.0"):
    """A mainnet execution reader whose provider fails `failures` in order, then answers."""
    from almanak.caip import ChainId
    from almanak.models import Chain

    failures = list(failures)
    sent = []
    results = {
        "eth_chainId": "0x1",
        "web3_clientVersion": client,
        "eth_getBlockByNumber": {
            "number": "0x0",
            "hash": ETHEREUM_GENESIS,
            "timestamp": "0x0",
            "parentHash": "0x" + "00" * 32,
        },
    }

    def respond(request):
        body = json.loads(request.content)
        sent.append(body["method"])
        if body["method"] == "web3_clientVersion" and failures:
            failure = failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure if isinstance(failure, httpx.Response) else httpx.Response(failure)
        answer = {"jsonrpc": "2.0", "id": body["id"], "result": results[body["method"]]}
        return httpx.Response(200, json=answer)

    reader = EthereumReader(
        Chain(chain_id=ChainId("eip155:1"), environment="mainnet"),
        "https://rpc.invalid",
        client=httpx.Client(transport=httpx.MockTransport(respond)),
        retry_budget=ethereum.EXECUTION_RETRY_BUDGET,
    )
    return reader, sent


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), 503, 429, 408],
    ids=["connection", "timeout", "http_503", "http_429", "http_408"],
)
def test_the_client_version_read_is_sent_again_after_a_transient_failure(no_backoff, failure):
    reader, sent = answering([failure])
    assert reader._rpc("web3_clientVersion", []) == "Geth/v1.16.0"
    assert sent == ["web3_clientVersion"] * 2 and len(no_backoff) == 1


@pytest.mark.parametrize("failure", [httpx.ConnectError("refused"), 503, 429, 408])
def test_a_client_version_read_that_keeps_failing_is_raised_transient(no_backoff, failure):
    reader, sent = answering([failure] * 20)
    with pytest.raises(DataError) as raised:
        reader._rpc("web3_clientVersion", [])
    assert raised.value.transient and len(sent) > 1
    # A refusal is still final: only failures a later attempt may pass are sent again.
    refused, sent = answering([403])
    with pytest.raises(DataError) as final:
        refused._rpc("web3_clientVersion", [])
    assert final.value.kind == "http" and not final.value.transient and len(sent) == 1


def test_a_request_timeout_answer_is_a_provider_fault_like_a_5xx(no_backoff):
    """HTTP 408: the server gave up waiting for the request, so a repeat may pass."""
    kept = {}
    for status in (408, 503):
        reader, sent = answering([status] * 20)
        with pytest.raises(ethereum.ProviderFault) as raised:
            reader._rpc("web3_clientVersion", [])
        kept[status] = (raised.value.kind, raised.value.code, raised.value.transient, len(sent))
        assert str(raised.value) == f"web3_clientVersion: HTTP {status}"
    assert kept[408] == kept[503] and kept[408][:3] == ("http", None, True) and kept[408][3] > 1


JSON_RPC_REFUSAL = {"jsonrpc": "2.0", "id": 1, "error": {"code": -32602, "message": "too wide"}}


@pytest.mark.parametrize(
    ("answer", "code"),
    [
        (httpx.Response(400), None),
        # Providers refuse a read with HTTP 400 and a JSON-RPC error; callers read the code.
        (httpx.Response(400, json=JSON_RPC_REFUSAL), -32602),
        (httpx.Response(401), None),
        (httpx.Response(403), None),
        (httpx.Response(404), None),
        (httpx.Response(410), None),
    ],
    ids=["http_400", "http_400_json_rpc", "http_401", "http_403", "http_404", "http_410"],
)
def test_every_other_4xx_answer_is_final_and_sent_once(no_backoff, answer, code):
    reader, sent = answering([answer] * 20)
    with pytest.raises(DataError) as final:
        reader._rpc("web3_clientVersion", [])
    assert not isinstance(final.value, ethereum.ProviderFault)
    assert (final.value.kind, final.value.code, final.value.transient) == ("http", code, False)
    assert sent == ["web3_clientVersion"] and no_backoff == []


def test_the_production_guard_passes_when_its_client_version_read_fails_once(no_backoff):
    from types import SimpleNamespace

    reader, sent = answering([503])
    assert ProductionGateway.verify_provider(SimpleNamespace(reader=reader))
    assert sent.count("web3_clientVersion") == 2
    # The answer of a read that was sent again is checked like a first one.
    reader, sent = answering([503], client="anvil/v1.0.0")
    with pytest.raises(DataError) as refused:
        ProductionGateway.verify_provider(SimpleNamespace(reader=reader))
    assert refused.value.kind == "environment_mismatch" and not refused.value.transient
