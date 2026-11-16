"""Authenticated gateway and runtime recovery against retained archival RPC reads."""

import json
import secrets
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.configuration import bootstrap as runtime
from almanak.configuration.resolution import AnchorSelection, ResolutionAnchors
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import FIELDS, BootstrapService, ResolutionResult
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.operator_config import OperatorNetwork
from almanak.gateway.providers import UrlProvider
from almanak.gateway.resolution_evm import checkpoint
from almanak.gateway.service import http_server
from tests.test_aave_resolution import archive as archive
from tests.test_aave_resolution import selected, selector
from tests.test_resolution_context import context as context
from tests.test_resolution_context import operator as operator
from tests.test_resolution_context import revalidate

REVERTED = {"code": 3, "message": "execution reverted"}


def symbol_read(body):
    return body["method"] == "eth_call" and body["params"][0].get("data") == "0x95d89b41"


# The contracts the fork compatibility check reads for the reference strategy: its
# market and funded tokens. The archives predate the check and hold none of these reads;
# each answers ordinary EVM bytecode, and any other uncaptured code read still fails.
CHECKED_CODE = frozenset(
    (
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    )
)
EVM_CODE = "0x6080604052"


def uncaptured_reply(body):
    """The captured archives predate display symbols and the fork compatibility check:
    an uncaptured symbol read reverts and a checked token's code read is EVM bytecode."""
    if symbol_read(body):
        return {"error": REVERTED}
    if body["method"] == "eth_getCode" and body["params"][0].lower() in CHECKED_CODE:
        return {"result": EVM_CODE}
    return None


@pytest.fixture
def rpc(archive):
    _, block, rows = archive
    responses = {
        json.dumps([r["request"]["method"], r["request"]["params"]], sort_keys=True): r["result"]
        for r in rows
    }
    header = responses[json.dumps(["eth_getBlockByNumber", [hex(block.number), False]])]
    state = SimpleNamespace(calls=[], mutation=None, uncaptured=[], throttle=[])

    def answer(body):
        state.calls.append(body)
        key = json.dumps([body["method"], body["params"]], sort_keys=True)
        if body["method"] == "eth_getBlockByNumber" and body["params"][0] == "latest":
            value = header
        elif body["method"] == "eth_getBlockByHash" and body["params"][0] == block.hash:
            value = header
        else:
            value = responses.get(key)
        reply = {"jsonrpc": "2.0", "id": body["id"], "result": value}
        if value is None and key not in responses:
            substitute = uncaptured_reply(body)
            if substitute is None:
                state.uncaptured.append(key)
            else:
                reply = {"jsonrpc": "2.0", "id": body["id"], **substitute}
        if state.mutation:
            reply = state.mutation(body, reply)
        return reply

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            methods = [m["method"] for m in body] if isinstance(body, list) else [body["method"]]
            throttled = next((m for m in methods if m in state.throttle), None)
            if throttled is not None:
                # The provider's rate limit, answered once for each listed method.
                state.throttle.remove(throttled)
                self.send_response(429)
                self.send_header("Retry-After", "0")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            # A batch answers each member as a single request would be answered.
            reply = [answer(member) for member in body] if isinstance(body, list) else answer(body)
            encoded = json.dumps(reply).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            try:
                self.wfile.write(encoded)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    state.endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        yield state
    finally:
        server.shutdown()
        thread.join(5)
        server.server_close()
        assert not state.uncaptured


@pytest.fixture
def setup(tmp_path, archive, rpc, operator, context):
    source, block, _ = archive
    operator = revalidate(
        operator,
        networks=(
            OperatorNetwork(
                chain=source.chain.chain_id,
                provider=UrlProvider(kind="rpc_url", url_env="ARCHIVE_RPC_URL"),
            ),
        ),
    )
    context = revalidate(
        context,
        operator_digest=operator.digest,
        markets=(revalidate(context.markets[0], selection=selected(source)),),
        anchors=(AnchorSelection(chain=source.chain.chain_id, fresh=True),),
    )
    env_file = tmp_path / "gateway.env"
    env_file.write_text(f"ARCHIVE_RPC_URL={rpc.endpoint}\nUNRELATED=must-not-appear\n")
    token_file = tmp_path / "gateway.token"
    token_file.write_text(secrets.token_hex(32))
    token_file.chmod(0o600)
    return SimpleNamespace(
        context=context,
        operator=operator,
        env_file=env_file,
        token_file=token_file,
        path=tmp_path / "resolution.json",
        source=source,
        block=block,
        rpc=rpc,
    )


@contextmanager
def connected(setup, *, mode=RunMode.FORK):
    service = BootstrapService(setup.context, setup.operator, mode, setup.env_file)
    server = http_server(service, setup.token_file, 0, fields=FIELDS)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    client = GatewayClient(f"http://127.0.0.1:{server.server_port}", setup.token_file)
    try:
        yield client
    finally:
        client.close()
        server.shutdown()
        thread.join(5)
        server.server_close()


def test_real_http_resolution_retains_verified_facts_and_reuses_published_result(setup):
    with connected(setup) as client:
        result = runtime.resolve_configuration(setup.path, setup.context, client)
        assert result.markets[0].resolved.reserve == setup.source
        assert result.markets[0].resolved.block == setup.block
        assert result.binding.authority == "read_only"
        assert result.status == "facts_resolved"
        assert {e.method for e in result.markets[0].evidence} >= {"eth_call", "eth_getCode"}
        assert ResolutionResult.model_validate_json(result.model_dump_json()) == result
        raw = setup.path.read_text()
        assert setup.path.stat().st_mode & 0o777 == 0o600
        for secret in (setup.rpc.endpoint, setup.token_file.read_text(), "must-not-appear"):
            assert secret not in raw
        setup.rpc.calls.clear()
        again = runtime.resolve_configuration(setup.path, setup.context, client)
        assert again == result
        assert setup.path.read_text() == raw
        assert {c["method"] for c in setup.rpc.calls} == {"eth_chainId", "eth_getBlockByNumber"}


def test_a_throttled_resolution_read_is_waited_out(setup):
    setup.rpc.throttle = ["eth_getCode", "eth_call"]
    with connected(setup) as client:
        result = runtime.resolve_configuration(setup.path, setup.context, client)
    assert result.status == "facts_resolved" and setup.rpc.throttle == []
    assert {e.method for e in result.markets[0].evidence} >= {"eth_call", "eth_getCode"}


@pytest.mark.parametrize("assertion", [False, True])
def test_named_aliases_share_one_verified_target_read(setup, assertion):
    from almanak.connectors.aave_v4.resolution import ReserveAssertions

    first = setup.context.markets[0]
    second = revalidate(first, name="bluechip")
    if assertion:
        second = revalidate(
            second,
            selection=revalidate(
                second.selection,
                assertions=ReserveAssertions(decimals=18),
            ),
        )
    setup.context = revalidate(setup.context, markets=(first, second))
    with connected(setup) as client:
        result = runtime.resolve_configuration(setup.path, setup.context, client)
    assert result.markets[0].resolved.reserve == result.markets[1].resolved.reserve
    assert result.markets[1].resolved.selection == second.selection
    calls = [c for c in setup.rpc.calls if c["method"] == "eth_call"]
    assert (
        sum(c["params"][0]["data"].startswith(selector("getReserve(uint256)")) for c in calls) == 1
    )


@pytest.mark.parametrize(
    "operation", ["sign", "broadcast", "create", "read", "permissions-generate"]
)
def test_execution_and_raw_rpc_operations_are_absent(setup, operation):
    with connected(setup) as client:
        with pytest.raises(DataError) as error:
            client.request(operation, context_digest=setup.context.digest)
    assert error.value.kind == "invalid_request"
    assert not setup.rpc.calls


@pytest.mark.parametrize("fault", ["token", "context", "extra_target", "null_field", "chain"])
def test_unadmitted_http_requests_never_reach_provider(setup, fault):
    with connected(setup) as client:
        method = "resolution-context"
        fields = {"context_digest": setup.context.digest}
        if fault == "token":
            client.token = "0" * 64
        elif fault == "context":
            fields["context_digest"] = "b" * 64
        elif fault == "extra_target":
            fields["target"] = "0x" + "12" * 20
        elif fault == "null_field":
            fields["chain"] = None
        else:
            method = "resolution-anchor"
            fields["chain"] = "eip155:43114"
        with pytest.raises(DataError):
            client.request(method, **fields)
    assert not setup.rpc.calls


@pytest.mark.parametrize("field", ["mode", "operator_digest", "accounts"])
def test_gateway_start_refuses_different_operator_authority(setup, field):
    changes = {
        "mode": RunMode.PRODUCTION,
        "operator_digest": "b" * 64,
        "accounts": (revalidate(setup.context.accounts[0], name="Missing"),),
    }
    context = revalidate(setup.context, **{field: changes[field]})
    with pytest.raises(ValueError):
        BootstrapService(context, setup.operator, RunMode.FORK, setup.env_file)
    assert not setup.rpc.calls


def reject_archive(body, reply):
    if body["method"] == "eth_getCode":
        return {
            "jsonrpc": "2.0",
            "id": body["id"],
            "error": {
                "code": -1,
                "message": "https://provider.invalid/private-secret unavailable",
            },
        }
    return reply


def test_failed_archive_read_then_gateway_replacement_reuses_retained_fresh_anchor(setup):
    setup.rpc.mutation = reject_archive
    with connected(setup) as client:
        with pytest.raises(DataError) as error:
            runtime.resolve_configuration(setup.path, setup.context, client)
    assert error.value.kind == "rpc"
    assert "private-secret" not in str(error.value)
    retained = runtime.read_progress(setup.path)
    assert retained.checkpoints == (checkpoint(setup.block),)
    assert retained.result is retained.result_digest is None
    setup.rpc.mutation = None
    setup.rpc.calls.clear()
    with connected(setup) as client:
        result = runtime.resolve_configuration(setup.path, setup.context, client)
    assert result.markets[0].resolved.block == setup.block
    assert not any(c["params"] == ["latest", False] for c in setup.rpc.calls)


def test_lost_result_publication_retries_reads_without_publishing_partial_facts(setup, monkeypatch):
    publish = runtime.publish

    def fail_result(*args, **changes):
        if "result" in changes:
            raise OSError("simulated publication interruption")
        return publish(*args, **changes)

    monkeypatch.setattr(runtime, "publish", fail_result)
    with connected(setup) as client:
        with pytest.raises(OSError, match="publication interruption"):
            runtime.resolve_configuration(setup.path, setup.context, client)
        assert runtime.read_progress(setup.path).result is None
        monkeypatch.setattr(runtime, "publish", publish)
        result = runtime.resolve_configuration(setup.path, setup.context, client)
    assert runtime.read_progress(setup.path).result_digest == result.digest


@pytest.mark.parametrize("completed", [False, True])
def test_invalidated_anchor_halts_with_retained_progress(setup, completed):
    if not completed:
        setup.rpc.mutation = reject_archive
    with connected(setup) as client:
        try:
            runtime.resolve_configuration(setup.path, setup.context, client)
        except DataError:
            assert not completed
        before = setup.path.read_bytes()

        def reorganized(body, reply):
            if body["method"] == "eth_getBlockByNumber":
                return {**reply, "result": {**reply["result"], "hash": "0x" + "12" * 32}}
            return reply

        setup.rpc.mutation = reorganized
        with pytest.raises(DataError) as error:
            runtime.resolve_configuration(setup.path, setup.context, client)
        assert error.value.kind == "block_mismatch"
        assert setup.path.read_bytes() == before


def test_mismatched_retained_context_refuses_before_http(setup):
    with connected(setup) as client:
        runtime.resolve_configuration(setup.path, setup.context, client)
        setup.rpc.calls.clear()
        with pytest.raises(ValueError, match="another provisional"):
            runtime.resolve_configuration(
                setup.path,
                revalidate(setup.context, authored_digest="b" * 64),
                client,
            )
        assert not setup.rpc.calls


def test_tampered_publication_digest_refuses_offline(setup):
    with connected(setup) as client:
        runtime.resolve_configuration(setup.path, setup.context, client)
    raw = json.loads(setup.path.read_text())
    raw["result_digest"] = "b" * 64
    setup.path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="different digest"):
        runtime.read_progress(setup.path)


def test_wrong_anchor_scope_refuses_before_market_reads(setup):
    anchors = ResolutionAnchors(context_digest="b" * 64, checkpoints=(checkpoint(setup.block),))
    with connected(setup) as client:
        with pytest.raises(DataError):
            client.request(
                "resolution-markets", context_digest=setup.context.digest, anchors=anchors
            )
    assert not setup.rpc.calls


@pytest.mark.parametrize("kind", ["height", "hash", "both"])
def test_explicit_anchor_selection_resolves_full_canonical_identity(setup, kind):
    from almanak.chains import ChainReference

    fields = {}
    if kind in ("height", "both"):
        fields["height"] = setup.block.number
    if kind in ("hash", "both"):
        fields["reference"] = ChainReference(setup.block.hash)
    setup.context = revalidate(
        setup.context,
        anchors=(
            AnchorSelection(
                chain=setup.source.chain.chain_id,
                **fields,
            ),
        ),
    )
    with connected(setup) as client:
        result = runtime.resolve_configuration(setup.path, setup.context, client)
    assert result.markets[0].resolved.block == setup.block
    assert not any(c["params"] == ["latest", False] for c in setup.rpc.calls)
    assert any(c["method"] == "eth_getBlockByHash" for c in setup.rpc.calls) == (kind == "hash")


@pytest.mark.parametrize("offset", [0, 5, 11])
def test_time_anchor_resolves_to_the_last_block_at_the_selected_time(offset):
    from almanak.gateway.resolution_evm import ResolutionReader, resolve_anchor
    from almanak.models import Chain
    from tests.backtest_support import Archive, archive_server

    history = Archive()
    server, _ = archive_server(history)
    try:
        chain = ChainId("eip155:1")
        reader = ResolutionReader(
            Chain(chain_id=chain, environment="mainnet"),
            f"http://127.0.0.1:{server.server_port}",
        )
        with reader:
            selection = AnchorSelection(chain=chain, at=history.start + 7 * 12 + offset)
            result = resolve_anchor(reader, selection)
            assert result.height == 7 and result.timestamp == history.start + 7 * 12
            selection.require(result)
            methods = {method for method, _ in history.calls}
            assert methods == {"eth_chainId", "eth_getBlockByNumber"}
            assert not any(p and p[0] == "latest" for _, p in history.calls)
            with pytest.raises(DataError, match="exceeds pinned history"):
                resolve_anchor(reader, AnchorSelection(chain=chain, at=history.start + 10**6))
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("fault", ["wrong_chain", "missing_header", "bad_implementation"])
def test_provider_identity_and_archival_authority_fail_closed(setup, fault, monkeypatch):
    from almanak.gateway import ethereum

    # A header the provider does not serve is re-read within the retry budget first.
    monkeypatch.setattr(ethereum, "sleep", lambda seconds: None)

    def corrupted(body, reply):
        if fault == "wrong_chain" and body["method"] == "eth_chainId":
            return {**reply, "result": "0xa86a"}
        if fault == "missing_header" and body["method"] == "eth_getBlockByNumber":
            return {**reply, "result": None}
        if fault == "bad_implementation" and body["method"] == "eth_getCode":
            return {**reply, "result": "0x00"}
        return reply

    setup.rpc.mutation = corrupted
    with connected(setup) as client:
        with pytest.raises(DataError):
            runtime.resolve_configuration(setup.path, setup.context, client)
    assert runtime.read_progress(setup.path).result is None
    assert not any(c["method"] == "eth_call" for c in setup.rpc.calls)


def test_concurrent_runtime_owner_refuses_before_gateway_io(setup):
    import fcntl

    with setup.path.with_suffix(".lock").open("a") as lock, connected(setup) as client:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="active owner"):
            runtime.resolve_configuration(setup.path, setup.context, client)
    assert not setup.rpc.calls
    assert not setup.path.exists()


def test_second_market_failure_cannot_publish_first_market_as_complete(setup):
    from almanak.connectors.aave_v4.resolution import ReserveAssertions

    first = setup.context.markets[0]
    wrong = revalidate(first.selection, assertions=ReserveAssertions(decimals=6))
    setup.context = revalidate(
        setup.context,
        markets=(
            first,
            revalidate(first, name="wrong", selection=wrong),
        ),
    )
    with connected(setup) as client:
        with pytest.raises(DataError):
            runtime.resolve_configuration(setup.path, setup.context, client)
    progress = runtime.read_progress(setup.path)
    assert progress.result is progress.result_digest is None
    assert progress.checkpoints == (checkpoint(setup.block),)
