import json
import secrets
import socket
import sqlite3
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from almanak.connectors.aave_v4.models import LendingConfig
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import GatewayService, http_server, refusal_reason
from almanak.state import Journal


@pytest.fixture
def gateway(tmp_path, recorded, reference_strategy, request):
    raw = recorded.config.model_dump()
    instance = "0x" + uuid4().hex * 2
    raw["fork_instance"] = instance
    for binding in raw["accounts"]["bindings"]:
        binding["profile"]["fund"]["chain"]["environment"] = "anvil:" + instance
    for allocation in raw["allocations"]:
        allocation["reserve"]["chain"]["environment"] = "anvil:" + instance
    config = LendingConfig.model_validate(raw)
    token_file = tmp_path / "auth.token"
    token_file.write_text(secrets.token_hex(32))
    token_file.chmod(0o600)
    calls = []

    def forbidden(*args):
        calls.append(args)
        raise AssertionError("unadmitted request reached the signer or submitter")

    backend = SimpleNamespace(
        sign=forbidden, broadcast=forbidden, audit_history=lambda _: None, close=lambda: None
    )
    service = GatewayService(config, tmp_path / "state.sqlite", backend, lease_seconds=2)
    server = http_server(service, token_file, 0)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    client = GatewayClient(endpoint, token_file)
    try:
        if getattr(request, "param", True):
            client.create(config, reference_strategy)
        yield SimpleNamespace(
            client=client,
            service=service,
            endpoint=endpoint,
            token_file=token_file,
            config=config,
            strategy=reference_strategy,
            calls=calls,
        )
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize("gateway", [False], indirect=True)
def test_state_before_creation_reports_missing_deployment_without_creating_it(gateway):
    response = httpx.post(
        gateway.endpoint + "/state",
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        json={},
    )
    assert response.status_code == 409
    assert response.json()["error"] == "deployment_not_found"
    assert "first start" in response.json()["message"]
    with pytest.raises(DataError) as error:
        gateway.client.load()
    assert error.value.kind == "deployment_not_found"
    assert not gateway.service.journal.path.exists()
    assert not gateway.calls
    gateway.client.create(gateway.config, gateway.strategy)
    assert gateway.client.load().config == gateway.config


def test_missing_existing_database_does_not_create_new_deployment(gateway):
    before = gateway.client.load()
    database = gateway.service.journal.path
    retained = database.with_suffix(".retained")
    database.rename(retained)
    try:
        with pytest.raises(DataError) as error:
            gateway.client.load()
        assert error.value.kind == "deployment_not_found"
        assert "original database" in str(error.value)
        assert not database.exists()
        assert not gateway.calls
    finally:
        retained.rename(database)
    assert gateway.client.load() == before


@pytest.mark.parametrize("failure", [PermissionError, sqlite3.OperationalError])
def test_storage_failure_remains_unavailable(gateway, monkeypatch, failure):
    def unavailable():
        raise failure("synthetic storage failure")

    monkeypatch.setattr(gateway.service.journal, "body", unavailable)
    response = httpx.post(
        gateway.endpoint + "/state",
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        json={},
    )
    assert response.status_code == 503
    assert response.json()["error"] == "unavailable"
    assert not gateway.calls


@pytest.mark.parametrize("credential", [None, "Bearer wrong"])
@pytest.mark.parametrize("method", ["state", "create", "sign", "broadcast", "update", "teardown"])
def test_http_authentication_precedes_privileged_work(gateway, credential, method):
    before = gateway.service.journal.load()
    headers = {} if credential is None else {"Authorization": credential}
    response = httpx.post(gateway.endpoint + "/" + method, headers=headers, json={})
    assert response.status_code == 401
    assert gateway.service.journal.load() == before
    assert not gateway.calls


def test_token_rotation_takes_effect_without_gateway_restart(gateway):
    gateway.token_file.write_text(secrets.token_hex(32))
    with pytest.raises(DataError, match="authentication failed"):
        gateway.client.load()
    replacement = GatewayClient(gateway.endpoint, gateway.token_file)
    try:
        assert replacement.load().config == gateway.config
    finally:
        replacement.close()


def test_wrong_deployment_and_forged_state_cannot_reach_signer(gateway):
    gateway.client.acquire()
    state = gateway.client.load()
    fields = {"coordinator": gateway.client.coordinator, "deployment_id": "another-deployment"}
    with pytest.raises(DataError):
        gateway.client.request("sign", **fields)
    fields["deployment_id"] = state.deployment_id
    with pytest.raises(DataError):
        gateway.client.request("sign", **fields, state=state.model_dump(mode="json"))
    with pytest.raises(DataError):
        gateway.client.request("broadcast", **fields, raw_transaction="0x00")
    with pytest.raises(DataError, match="gateway request refused"):
        gateway.client.scoped("broadcast")
    assert not gateway.calls


@pytest.mark.parametrize("failure", ["transport", "storage"])
def test_transient_heartbeat_failure_retries_the_same_lease(gateway, monkeypatch, failure):
    original = gateway.client.request
    dispatch = gateway.service.dispatch
    recovered = threading.Event()
    attempts = []

    def transient_dispatch(method, request):
        if method == "renew" and len(attempts) == 1 and failure == "storage":
            raise sqlite3.OperationalError("synthetic database lock contention")
        return dispatch(method, request)

    monkeypatch.setattr(gateway.service, "dispatch", transient_dispatch)

    def request(method, **fields):
        if method == "renew":
            attempts.append(fields["coordinator"])
            if len(attempts) == 1 and failure == "transport":
                raise DataError("gateway_unavailable", "synthetic transport interruption")
            result = original(method, **fields)
            recovered.set()
            return result
        return original(method, **fields)

    monkeypatch.setattr(gateway.client, "request", request)
    gateway.client.acquire()
    assert recovered.wait(3)
    assert attempts == [gateway.client.coordinator, gateway.client.coordinator]
    assert gateway.client.lease_error is None


@pytest.mark.parametrize("failure", ["unauthorized", "gateway_unavailable"])
def test_heartbeat_stops_on_revocation_or_expired_retry_window(gateway, monkeypatch, failure):
    original = gateway.client.request
    attempts = []

    def request(method, **fields):
        if method == "renew":
            attempts.append(method)
            raise DataError(failure, "synthetic renewal failure")
        return original(method, **fields)

    monkeypatch.setattr(gateway.client, "request", request)
    gateway.client.acquire()
    deadline = time.monotonic() + 4
    while gateway.client.lease_error is None and time.monotonic() < deadline:
        time.sleep(0.02)
    assert gateway.client.lease_error is not None
    if failure == "unauthorized":
        assert attempts == ["renew"]
    with pytest.raises(RuntimeError, match="heartbeat failed"):
        gateway.client.scoped("broadcast")
    assert not gateway.calls


def test_coordinator_is_durable_and_stale_owner_is_fenced(gateway):
    state = gateway.client.load()
    first, second = uuid4().hex, uuid4().hex
    gateway.client.request("acquire", coordinator=first, deployment_id=state.deployment_id)
    with pytest.raises(DataError, match="another coordinator"):
        gateway.client.request("acquire", coordinator=second, deployment_id=state.deployment_id)
    # Expiration models a dead coordinator without spending the test on a timer.
    with gateway.service.journal.connect() as db:
        db.execute("UPDATE coordinator SET expires=?", (time.time() - 1,))
    gateway.client.request("acquire", coordinator=second, deployment_id=state.deployment_id)
    replaced = GatewayService(
        gateway.config, gateway.service.journal.path, gateway.service.backend, lease_seconds=2
    )
    with pytest.raises(RuntimeError, match="superseded"):
        with replaced.journal.authorized(first, 2):
            pytest.fail("stale coordinator regained authority")
    with pytest.raises(DataError, match="superseded"):
        gateway.client.request("broadcast", coordinator=first, deployment_id=state.deployment_id)
    assert not gateway.calls


def test_checkpoint_cas_preserves_concurrent_teardown_and_immutable_config(gateway):
    gateway.client.acquire()
    before = gateway.client.load()
    gateway.client.teardown()
    with pytest.raises(DataError, match="checkpoint changed"):
        gateway.client.scoped("update", before=before, after=before)
    current = gateway.client.load()
    altered = current.model_copy(update={"deployment_id": "forged"})
    with pytest.raises(DataError):
        gateway.client.scoped("update", before=current, after=altered)
    assert gateway.client.load() == current


def test_unchanged_durable_authority_is_checked_once_and_changes_are_refused(gateway, monkeypatch):
    checked = []
    check_authority = gateway.service.check_authority

    def counted(state):
        checked.append(state)
        return check_authority(state)

    monkeypatch.setattr(gateway.service, "check_authority", counted)
    gateway.service.verified = None
    state = gateway.client.load()
    assert gateway.client.load() == state
    assert len(checked) == 1
    raw = gateway.config.model_dump()
    raw["allocations"][0]["capital"] *= 2
    forged = state.model_copy(update={"config": LendingConfig.model_validate(raw)})
    with gateway.service.journal.connect() as db:
        db.execute("UPDATE checkpoint SET body=?", (forged.model_dump_json(),))
    with pytest.raises(DataError):
        gateway.client.load()
    assert len(checked) == 2
    with gateway.service.journal.connect() as db:
        db.execute("UPDATE checkpoint SET body=?", (state.model_dump_json(),))
    # A refused checkpoint never replaces the authority already accepted.
    assert gateway.client.load() == state
    assert len(checked) == 2


def test_configuration_mismatch_is_refused_before_creation(gateway, tmp_path):
    raw = gateway.config.model_dump()
    raw["allocations"][0]["capital"] *= 2
    before = gateway.client.load()
    with pytest.raises(ValueError, match="configuration changed"):
        gateway.client.create(LendingConfig.model_validate(raw), gateway.strategy)
    assert gateway.client.load() == before


@pytest.mark.parametrize("field", ["chain", "asset"])
def test_http_checkpoint_rejects_non_caip_identity_before_mutation(gateway, field):
    gateway.client.acquire()
    before = gateway.client.load()
    after = before.model_dump(mode="json")
    source = after["config"]["allocations"][0]["reserve"]
    if field == "chain":
        source["chain"]["chain_id"] = 31337
    else:
        source["asset"] = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    with pytest.raises(DataError):
        gateway.client.scoped("update", before=before, after=after)
    assert gateway.client.load() == before
    assert not gateway.calls


def test_legacy_checkpoint_remains_auditable_but_cannot_be_served(gateway):
    from pathlib import Path

    raw = (Path(__file__).parent / "fixtures/aave-v4-fork-lifecycle.json").read_text()
    with gateway.service.journal.connect() as db:
        db.execute("UPDATE checkpoint SET body=?", (raw,))
    assert Journal(gateway.service.journal.path).audit()["legacy_checkpoint"] == json.loads(raw)
    with pytest.raises(ValueError, match="audit-only"):
        GatewayService(
            gateway.config, gateway.service.journal.path, gateway.service.backend, lease_seconds=2
        )


def test_malformed_http_requests_do_not_echo_input_or_change_state(gateway):
    before = gateway.client.load()
    private_input = "synthetic-secret-must-not-be-echoed"
    response = httpx.post(
        gateway.endpoint + "/sign",
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        content=json.dumps({"private_key": private_input}),
    )
    assert response.status_code == 400
    assert private_input not in response.text
    assert gateway.client.load() == before


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.com:9000",
        "https://127.0.0.1:9000",
        "http://127.0.0.1:9000/other",
        "http://:password@127.0.0.1:9000",
    ],
)
def test_client_refuses_non_loopback_gateway_before_reading_auth(tmp_path, endpoint):
    with pytest.raises(ValueError, match="loopback"):
        GatewayClient(endpoint, tmp_path / "missing")


@pytest.mark.parametrize("gateway", [False], indirect=True)
def test_provider_limit_errors_keep_their_http_category(gateway, monkeypatch):
    def refused(*_):
        raise DataError("rate_limit", "gateway provider access refused")

    monkeypatch.setattr(gateway.service, "dispatch", refused)
    response = httpx.post(
        gateway.endpoint + "/state",
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        json={},
    )
    assert response.status_code == 429
    assert response.json()["error"] == "rate_limit"
    with pytest.raises(DataError) as error:
        gateway.client.load()
    assert error.value.kind == "rate_limit"
    assert not gateway.calls


def test_refused_request_logs_its_reason_but_replies_generically(gateway, capsys):
    capsys.readouterr()
    response = httpx.post(
        gateway.endpoint + "/sign",
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        json={"coordinator": gateway.client.coordinator, "unexpected": "value-not-logged"},
    )
    assert response.status_code == 400
    assert response.json() == {"error": "invalid_request", "message": "gateway request refused"}
    (line,) = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert line["operation"] == "sign" and line["status"] == 400
    # The operator log names the failing fields, never the submitted values.
    assert line["reason"].startswith("ValidationError: ")
    assert "value-not-logged" not in line["reason"]


def test_malformed_content_length_is_not_echoed_into_the_log(gateway, capsys):
    capsys.readouterr()
    with socket.create_connection(("127.0.0.1", int(gateway.endpoint.rsplit(":", 1)[1]))) as conn:
        conn.sendall(
            b"POST /sign HTTP/1.1\r\nHost: gateway\r\n"
            + b"Authorization: Bearer "
            + gateway.token_file.read_text().encode()
            + b"\r\n"
            + b"Content-Length: header-secret\r\nConnection: close\r\n\r\n"
        )
        assert conn.recv(4096).startswith(b"HTTP/1.0 400")
    (line,) = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert line["reason"] == "ValueError: gateway request has an invalid Content-Length"


def test_refusal_reason_names_the_check_that_refused():
    error = ValueError("signer envelope is not a signed legacy transaction")
    assert refusal_reason(error) == (
        "ValueError: signer envelope is not a signed legacy transaction"
    )
    assert refusal_reason(KeyError("eip155:1/erc20:0xsubmitted")) == "KeyError: missing lookup key"


def test_fenced_success_keeps_the_lease_while_renewals_wait_on_the_journal(gateway, monkeypatch):
    """A long plan or sign holds the journal lock, so the heartbeat's renewals time out
    (503) meanwhile. The gateway extended the lease when that request committed, so the
    coordinator must not declare its lease lost (staging rc34, deployment 3e9dda44)."""
    original = gateway.client.request
    renewals = []

    def request(method, **fields):
        if method == "renew":
            renewals.append(method)
            raise DataError("gateway_unavailable", "synthetic journal lock timeout")
        return original(method, **fields)

    monkeypatch.setattr(gateway.client, "request", request)
    gateway.client.acquire()
    # Fenced requests keep succeeding for longer than the 2s lease while every renewal fails.
    end = time.monotonic() + 3.5
    while time.monotonic() < end:
        gateway.client.scoped("audit")
        time.sleep(0.3)
    assert renewals
    assert gateway.client.lease_error is None
    gateway.client.scoped("audit")


def test_renewal_failure_rereads_a_lease_confirmed_while_it_waited(gateway, monkeypatch):
    """A fenced request can succeed while a renewal is blocked on the journal; the failed
    renewal must retry against that newer confirmation, not its stale deadline."""
    original = gateway.client.request

    def request(method, **fields):
        if method == "renew":
            # The blocked renewal outlives the 2s lease; a fenced request confirms it meanwhile.
            time.sleep(2.2)
            gateway.client.lease_confirmed = time.monotonic()
            raise DataError("gateway_unavailable", "synthetic journal lock timeout")
        return original(method, **fields)

    monkeypatch.setattr(gateway.client, "request", request)
    gateway.client.acquire()
    time.sleep(3.2)
    assert gateway.client.lease_error is None


@pytest.mark.parametrize("committed", [True, False], ids=["committed", "not_committed"])
def test_a_refusal_says_whether_its_lease_was_committed(gateway, monkeypatch, committed):
    """Over loopback the refusal carries `lease_extended` only when the commit that ended
    the request extended the lease, and the client counts its lease from that answer."""
    dispatch = gateway.service.dispatch

    def refused(method, request):
        if method != "audit":
            return dispatch(method, request)
        if committed:
            with gateway.service.journal.authorized(request.coordinator, 2):
                raise RuntimeError("coordinator refused after the commit")
        raise RuntimeError("coordinator refused before any commit")

    monkeypatch.setattr(gateway.service, "dispatch", refused)
    # No heartbeat, so only the request below moves the client's lease count.
    gateway.client.stopped.set()
    gateway.client.acquire()
    admitted = gateway.client.lease_confirmed
    response = post(
        gateway,
        "audit",
        coordinator=gateway.client.coordinator,
        deployment_id=gateway.client.deployment_id,
    )
    assert response.status_code == 409
    assert response.json().get("lease_extended") is (True if committed else None)
    asked = time.monotonic()
    with pytest.raises(DataError) as error:
        gateway.client.scoped("audit")
    assert error.value.kind == "authority"
    if committed:
        assert gateway.client.lease_confirmed >= asked
    else:
        assert gateway.client.lease_confirmed == admitted


def post(gateway, method, **fields):
    return httpx.post(
        gateway.endpoint + "/" + method,
        headers={"Authorization": "Bearer " + gateway.token_file.read_text()},
        json=fields,
    )


def counted_decodes(monkeypatch):
    from almanak.gateway import service

    decoded = []

    def decode(body):
        decoded.append(None)
        return original(body)

    original = service.decode_checkpoint
    monkeypatch.setattr(service, "decode_checkpoint", decode)
    return decoded


def renewing(gateway):
    gateway.client.acquire()
    gateway.client.stopped.set()
    return {
        "coordinator": gateway.client.coordinator,
        "deployment_id": gateway.client.deployment_id,
    }


def rewrite_checkpoint(gateway, change):
    with sqlite3.connect(gateway.service.journal.path) as db:
        wire = json.loads(db.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()[0])
        change(wire)
        db.execute("UPDATE checkpoint SET body=? WHERE id=1", (json.dumps(wire),))


def lease_expiry(gateway):
    with sqlite3.connect(gateway.service.journal.path) as db:
        return db.execute("SELECT expires FROM coordinator WHERE id=1").fetchone()[0]


def test_state_is_served_byte_identical_and_decoded_only_until_checked(gateway, monkeypatch):
    stored = gateway.service.journal.load()
    expected = json.dumps({"result": stored.model_dump(mode="json")}).encode()
    rewrite_checkpoint(gateway, lambda wire: None)
    decoded = counted_decodes(monkeypatch)
    first = post(gateway, "state")
    assert first.status_code == 200 and first.content == expected
    assert len(decoded) == 1
    for _ in range(3):
        assert post(gateway, "state").content == expected
    assert len(decoded) == 1
    assert gateway.client.load() == stored


def test_renew_checks_the_lease_and_deployment_without_decoding(gateway, monkeypatch):
    fields = renewing(gateway)
    decoded = counted_decodes(monkeypatch)
    gateway.client.request("renew", **fields)
    with pytest.raises(DataError, match="superseded"):
        gateway.client.request("renew", **{**fields, "coordinator": secrets.token_hex(16)})
    with pytest.raises(DataError, match="gateway request refused"):
        gateway.client.request("renew", **{**fields, "deployment_id": "another-deployment"})
    with pytest.raises(DataError, match="gateway request refused"):
        gateway.client.request("renew", deployment_id=fields["deployment_id"])
    assert decoded == []


def test_an_unchanged_checkpoint_is_decoded_once_across_state_and_renew(gateway, monkeypatch):
    fields = renewing(gateway)
    rewrite_checkpoint(gateway, lambda wire: None)
    decoded = counted_decodes(monkeypatch)
    for _ in range(3):
        gateway.client.request("renew", **fields)
        gateway.client.request("state")
    assert len(decoded) == 1


@pytest.mark.parametrize(
    "change",
    [
        lambda wire: wire.update(delivered=len(wire["outcomes"]) + 1),
        lambda wire: wire["config"]["decision_trigger"].update(
            seconds=float(wire["config"]["decision_trigger"]["seconds"])
        ),
        lambda wire: wire["config"].update(fork_instance="0x" + uuid4().hex * 2),
    ],
    ids=["outside-authority", "authority-int-to-float", "authority-value"],
)
def test_a_checkpoint_changed_after_its_check_is_refused_by_state_and_renew(gateway, change):
    fields = renewing(gateway)
    gateway.client.request("renew", **fields)
    gateway.client.request("state")
    rewrite_checkpoint(gateway, change)
    expiry = lease_expiry(gateway)
    for method, body in (("state", {}), ("renew", fields)):
        with pytest.raises(DataError, match="gateway request refused"):
            gateway.client.request(method, **body)
    assert lease_expiry(gateway) == expiry


def test_a_fenced_request_decodes_a_new_checkpoint_once(gateway, monkeypatch):
    fields = renewing(gateway)
    rewrite_checkpoint(gateway, lambda wire: None)
    decoded = counted_decodes(monkeypatch)
    gateway.client.request("audit", **fields)
    gateway.client.request("audit", **fields)
    assert len(decoded) == 1
