"""Node-native contracts refuse a fork lifecycle before funding or decisions.

Base's B20 stock tokens answer eth_getCode with 0xef: the Base node implements them,
so mainnet reads pass while every Anvil call fails with "EVM error OpcodeNotFound".
"""

from types import SimpleNamespace

import pytest

from almanak import supervisor_forks, supervisor_preparation, testflow, workspace
from almanak.caip import AssetType, ChainId
from almanak.checks import CheckFailure, stage
from almanak.gateway import read_endpoints
from almanak.gateway.ethereum import DataError, Reverted, known_reason, member_result
from almanak.gateway.fork_compatibility import (
    UNSUPPORTED_ON_FORK,
    Offender,
    UnsupportedOnFork,
    check_node_code,
    check_provider_code,
    executable_on_fork,
    fork_contracts,
    require_fork_executable,
)
from almanak.models import Block, Chain
from almanak.supervisor import Supervisor

BASE = ChainId("eip155:8453")
BASE_CHAIN = Chain(chain_id=BASE, environment="mainnet")
USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
# Base B20 stock tokens.
SPCXC = AssetType("eip155:8453/erc20:0xb2000000000000000000007b9fcbd005511acbd5")
AMZNC = AssetType("eip155:8453/erc20:0xb200000000000000000000d9192b6b456483c2e8")
NATIVE = AssetType("eip155:8453/slip44:60")
POOL = "0x" + "a1" * 20
ROUTER = "0x" + "a2" * 20
FUNDED = AssetType("eip155:8453/erc20:0x" + "a3" * 20)
BYTECODE = "0x6080604052348015600f57600080fd5b50"
DELEGATION = "0xef0100" + "a4" * 20
BLOCK = Block(chain=BASE_CHAIN, number=36_000_000, hash="0x" + "b" * 64, timestamp=1_760_000_000)


def market(name, *assets, identities=()):
    resolved = SimpleNamespace(
        assets=tuple(SimpleNamespace(identifier=a) for a in assets),
        identities=tuple(SimpleNamespace(address=i) for i in identities),
        block=BLOCK,
    )
    return SimpleNamespace(name=name, resolved=resolved)


def fund(*assets):
    return SimpleNamespace(
        assets=tuple(SimpleNamespace(balance=SimpleNamespace(asset=a)) for a in assets)
    )


def refusal():
    return UnsupportedOnFork([Offender(BASE, SPCXC.reference, "token", "0xef")])


def b20_pool():
    return market("pool", USDC, SPCXC, identities=(POOL, ROUTER))


def plan(*funds, block=BLOCK):
    target = SimpleNamespace(chain=BASE_CHAIN, provider="provider", block=block)
    return SimpleNamespace(forks=(target,), funds=funds)


def codes(table, default=BYTECODE):
    reads = []

    def read_codes(chain, contracts):
        reads.append((chain, tuple(contracts)))
        return [table.get(contract, default) for contract in contracts]

    return read_codes, reads


@pytest.mark.parametrize(
    ("code", "executable"),
    [
        ("0xef", False),
        ("0xEF", False),
        ("0xef0001010004", False),  # EOF container: never activated, not executable
        ("0xef0100" + "a4" * 19, False),  # truncated delegation designator
        (DELEGATION, True),  # EIP-7702 delegation: Anvil runs the delegate
        (BYTECODE, True),
        ("0x", True),  # an EOA or empty account keeps its earlier handling
    ],
)
def test_only_reserved_prefix_code_is_not_executable(code, executable):
    assert executable_on_fork(code) is executable


@pytest.mark.parametrize("code", [None, "", "ef", 1, "0xzz", "0xabc", "0x60 80"])
def test_a_malformed_code_answer_fails_closed(code):
    with pytest.raises(DataError) as error:
        executable_on_fork(code)
    assert error.value.kind == "malformed"


def test_contracts_cover_market_tokens_identities_and_funded_tokens_without_native():
    found = fork_contracts(
        [b20_pool(), market("native", NATIVE, AMZNC)], [fund(USDC, FUNDED, NATIVE)]
    )
    assert found == {
        BASE: {
            USDC.reference: "token",
            SPCXC.reference: "token",
            POOL: "pool contract",
            ROUTER: "pool contract",
            AMZNC.reference: "token",
            FUNDED.reference: "token",
        }
    }


def test_a_market_without_identities_contributes_its_tokens():
    bare = SimpleNamespace(
        name="bare",
        resolved=SimpleNamespace(assets=(SimpleNamespace(identifier=SPCXC),), block=BLOCK),
    )
    assert fork_contracts([bare]) == {BASE: {SPCXC.reference: "token"}}


def test_a_b20_token_refuses_with_an_infrastructure_classification():
    read_codes, _ = codes({SPCXC.reference: "0xef"})
    with pytest.raises(UnsupportedOnFork) as refusal:
        require_fork_executable(read_codes, fork_contracts([b20_pool()]))
    error = refusal.value
    assert error.kind == UNSUPPORTED_ON_FORK
    assert [(o.chain, o.contract, o.role) for o in error.offenders] == [
        (BASE, SPCXC.reference, "token")
    ]
    message = str(error)
    assert message.startswith(
        f"local fork simulation cannot execute token {SPCXC.reference} on eip155:8453 "
        "(code 0xef): no EVM bytecode"
    )
    assert "infrastructure limitation of local fork simulation" in message
    assert "not a strategy fault" in message
    assert "can still be exported and deployed" in message
    assert error.check_code == UNSUPPORTED_ON_FORK and "export and deploy" in error.remedy


def test_every_offending_contract_is_named_in_one_refusal():
    long_code = "0xef00" + "00" * 100
    read_codes, reads = codes({SPCXC.reference: "0xef", AMZNC.reference: "0xEF", POOL: long_code})
    pools = [b20_pool(), market("other", USDC, AMZNC)]
    with pytest.raises(UnsupportedOnFork) as refusal:
        require_fork_executable(read_codes, fork_contracts(pools))
    assert [o.contract for o in refusal.value.offenders] == [
        SPCXC.reference,
        POOL,
        AMZNC.reference,
    ]
    message = str(refusal.value)
    assert f"pool contract {POOL} on eip155:8453 (code 0xef000000...)" in message
    assert f"token {AMZNC.reference} on eip155:8453 (code 0xEF)" in message
    # One batch per chain, every contract read before refusing.
    assert len(reads) == 1 and len(reads[0][1]) == 5


def test_a_short_code_batch_fails_closed():
    with pytest.raises(DataError) as error:
        require_fork_executable(lambda chain, contracts: ["0x"], fork_contracts([b20_pool()]))
    assert error.value.kind == "malformed"


def test_bytecode_empty_and_delegated_accounts_pass_reading_every_contract():
    read_codes, reads = codes({SPCXC.reference: "0x", ROUTER: DELEGATION})
    require_fork_executable(read_codes, fork_contracts([b20_pool()], [fund(FUNDED)]))
    assert reads == [(BASE, (USDC.reference, SPCXC.reference, POOL, ROUTER, FUNDED.reference))]


class Node:
    def __init__(self, table):
        self.table, self.calls = table, []

    def rpc(self, method, params):
        self.calls.append((method, params))
        return self.table.get(params[0], BYTECODE)


def test_fork_admission_reads_code_from_the_started_node():
    node = Node({AMZNC.reference: "0xef"})
    nodes = SimpleNamespace(nodes={BASE_CHAIN: node})
    with pytest.raises(UnsupportedOnFork, match=AMZNC.reference):
        check_node_code(plan(), [market("pool", USDC, AMZNC)], nodes)
    assert node.calls[-1] == ("eth_getCode", [AMZNC.reference, "latest"])


class Reader:
    def __init__(self, table):
        self.table, self.calls, self.closed, self.verified = table, [], False, False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def verify_chain(self):
        self.verified = True

    def batch(self, calls):
        self.calls.append(list(calls))
        return [self.table.get(params[0], BYTECODE) for _, params in calls]


@pytest.fixture
def provider(monkeypatch):
    opened = []

    def reader_for(chain, endpoint):
        assert (chain, endpoint) == (BASE, "https://provider.invalid")
        reader = Reader({SPCXC.reference: "0xef"})
        opened.append(reader)
        return reader

    monkeypatch.setattr(read_endpoints, "mainnet_reader", reader_for)
    monkeypatch.setattr(
        "almanak.gateway.fork_config.provider_endpoint",
        lambda provider, env_file: "https://provider.invalid",
    )
    return opened


def test_the_online_check_reads_code_at_the_fork_block_through_one_reader(provider, tmp_path):
    with pytest.raises(UnsupportedOnFork, match=SPCXC.reference):
        check_provider_code(plan(fund(USDC)), [b20_pool()], tmp_path / ".env")
    [reader] = provider
    selector = {"blockHash": BLOCK.hash, "requireCanonical": True}
    contracts = (USDC.reference, SPCXC.reference, POOL, ROUTER)
    assert reader.calls == [[("eth_getCode", [c, selector]) for c in contracts]]
    assert reader.verified and reader.closed


def test_the_online_check_follows_a_latest_fork_target(provider, tmp_path):
    check_provider_code(plan(block="latest"), [market("pool", USDC)], tmp_path / ".env")
    assert provider[0].calls == [[("eth_getCode", [USDC.reference, "latest"])]]


def test_a_check_stage_failure_carries_the_refusal_code_and_remedy():
    error = refusal()
    detail = CheckFailure(error, [stage("fork_compatibility", "failed", str(error))], "op").detail
    assert detail["code"] == UNSUPPORTED_ON_FORK
    assert detail["field"] == "fork_compatibility" and detail["remedy"] == error.remedy
    plain = CheckFailure(ValueError("x"), [stage("fixture", "failed", "x")], "op").detail
    assert plain["code"] == "check_stage_failed"


def test_the_verdict_copies_the_structured_failure_kind(tmp_path, monkeypatch):
    message = str(refusal())
    states = iter([{"stage": "failed", "failure": UNSUPPORTED_ON_FORK, "diagnostic": message}])
    monkeypatch.setattr(testflow, "supervisor_status", lambda root: next(states))
    verdict = testflow.empty_verdict()
    assert not testflow.admitted(tmp_path, SimpleNamespace(poll=lambda: None), verdict)
    assert verdict["admission"]["refusal"] == {
        "stage": "failed",
        "message": message,
        "error": UNSUPPORTED_ON_FORK,
        "kind": UNSUPPORTED_ON_FORK,
        "class": "infra",
        "transient": False,
    }


def test_a_refusal_without_a_failure_kind_has_no_error():
    states = iter([{"stage": "action_required", "failure": "start", "diagnostic": "funding"}])
    verdict = testflow.empty_verdict()
    testflow.refused(verdict, "action_required", "funding", testflow.failure_kind(next(states)))
    assert verdict["admission"]["refusal"] == {
        "stage": "action_required",
        "message": "funding",
        "kind": "action_required",
        "class": "strategy",
        "transient": False,
    }


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (
            refusal(),
            {
                "stage": "failed",
                "prefix": "local fork simulation cannot execute token ",
                "failure": UNSUPPORTED_ON_FORK,
            },
        ),
        (RuntimeError("boom"), {"stage": "failed", "prefix": "boom", "failure": "start"}),
    ],
)
def test_setup_refusal_waits_for_action_instead_of_a_retryable_failure(raised, expected):
    persisted = []
    host = SimpleNamespace(
        password="x",
        setup_active=True,
        state={"stage": "provisioning"},
        persist=lambda **changes: persisted.append(changes),
    )

    def work():
        raise raised

    Supervisor.setup_worker(host, work)
    [changes] = persisted
    assert changes["stage"] == expected["stage"]
    assert changes["diagnostic"].startswith(expected["prefix"])
    assert changes["failure"] == expected["failure"]
    assert not host.setup_active and host.password is None


def rejected(error):
    return {"jsonrpc": "2.0", "id": 1, "error": error}


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (
            {"code": -32603, "message": "EVM error OpcodeNotFound"},
            "eth_call: RPC rejected the requested read (JSON-RPC -32603: EVM error OpcodeNotFound)",
        ),
        (
            {"code": -32000, "message": "header not found"},
            "eth_call: RPC rejected the requested read (JSON-RPC -32000: header not found)",
        ),
        (
            {"message": "header not found"},
            "eth_call: RPC rejected the requested read (header not found)",
        ),
        ({"code": -32000}, "eth_call: RPC rejected the requested read (JSON-RPC -32000)"),
        (
            {"code": -32000, "message": "nonce too low: secret-key"},
            "eth_call: RPC rejected the requested read (JSON-RPC -32000)",
        ),
        ({"code": "x", "message": None}, "eth_call: RPC rejected the requested read"),
    ],
)
def test_a_rejection_adds_only_a_known_fixed_phrase(error, message):
    with pytest.raises(DataError) as refused:
        member_result("eth_call", rejected(error))
    assert str(refused.value) == message and refused.value.kind == "rpc"


def test_a_reverted_call_keeps_its_data_and_no_phrase():
    error = {"code": 3, "message": "execution reverted: invalid opcode", "data": "0x08c379a0"}
    with pytest.raises(Reverted) as refused:
        member_result("eth_call", rejected(error))
    assert str(refused.value) == "eth_call: RPC rejected the requested read (JSON-RPC 3)"


@pytest.mark.parametrize(
    ("message", "phrase"),
    [
        ("EVM error OpcodeNotFound", "EVM error OpcodeNotFound"),
        ("evm error: opcodenotfound at https://rpc.invalid/v2/KEY", "EVM error OpcodeNotFound"),
        ("Opcode not found (pc 12)", "EVM error OpcodeNotFound"),
        ("INVALID OPCODE 0xfe", "EVM error OpcodeNotFound"),
        ("header not found", "header not found"),
        ("Header Not Found for 0xabc via secret-key", "header not found"),
        ("execution reverted", None),
        ("invalid project id: 9aa3d95b3bc440fa", None),
        ("https://only.example/secret", None),
        ("", None),
        (None, None),
    ],
)
def test_known_reasons_map_to_fixed_phrases_only(message, phrase):
    assert known_reason({"message": message}) == phrase
    assert known_reason("not an object") is None


@pytest.mark.parametrize(
    ("state", "command"),
    [
        ({"stage": "failed", "failure": UNSUPPORTED_ON_FORK}, "shutdown"),
        ({"stage": "failed", "failure": "start"}, "resume"),
        ({"stage": "failed", "failure": "execution"}, "resume"),
        ({"stage": "failed", "failure": "teardown_required"}, "teardown"),
        ({"stage": "paused", "failure": "teardown_required"}, "resume"),
        ({"stage": "paused", "failure": UNSUPPORTED_ON_FORK}, "resume"),
    ],
)
def test_status_offers_no_resume_a_fork_refusal_would_repeat(tmp_path, state, command):
    assert workspace.next_commands(tmp_path, state) == [f"almanak {command} --run {tmp_path}"]
    closed = workspace.next_commands(tmp_path, {"stage": "closed"}, {"mode": "closed"})
    assert closed == [f"almanak shutdown --run {tmp_path}"]
    # Production services stopped with obligations recorded can still be resumed.
    stopped = {"stage": "shutdown", "mode": "production"}
    assert workspace.next_commands(tmp_path, stopped, {"mode": "running"}) == [
        f"almanak resume --run {tmp_path}"
    ]
    assert workspace.next_commands(tmp_path, stopped, {"mode": "closed"}) == [
        f"almanak report --run {tmp_path}"
    ]


def test_a_followed_fork_refusal_exits_one(tmp_path, monkeypatch):
    stopped = {"stage": "failed", "failure": UNSUPPORTED_ON_FORK, "coordinator": "stopped"}
    monkeypatch.setattr(workspace, "wait_ready", lambda root: stopped)
    monkeypatch.setattr(workspace, "request", lambda root, action: stopped)
    monkeypatch.setattr(workspace, "emit", lambda result, args: None)
    assert workspace.follow(tmp_path, SimpleNamespace()) == 1


@pytest.fixture
def bundle_steps(tmp_path, monkeypatch):
    """Record prepare_fixture_bundle's steps in order with every collaborator replaced."""
    steps = type("Steps", (list,), {"refuse": False})()
    progress = SimpleNamespace(result=SimpleNamespace(anchors="anchors", markets="markets"))
    plan_ = SimpleNamespace(name="plan")
    nodes = SimpleNamespace(name="nodes")

    def step(name, value=None):
        def record(*args, **kwargs):
            steps.append(name)
            return value

        return record

    def check(plan, markets, attached):
        assert (plan, markets, attached) == (plan_, "markets", nodes)
        steps.append("check_node_code")
        if steps.refuse:
            raise refusal()

    monkeypatch.setattr(supervisor_forks, "require_fork_policy", step("policy"))
    monkeypatch.setattr(supervisor_forks, "read_progress", step("read_progress", progress))
    monkeypatch.setattr(supervisor_preparation, "resolve_progress", step("resolve", progress))
    monkeypatch.setattr(supervisor_forks, "prepare_fixture", step("prepare_fixture", plan_))
    monkeypatch.setattr(supervisor_forks, "attach_nodes", step("attach_nodes", nodes))
    monkeypatch.setattr(supervisor_forks, "check_node_code", check)
    monkeypatch.setattr(supervisor_forks, "reconcile_fixture", step("reconcile_fixture"))
    monkeypatch.setattr(supervisor_forks, "provision_accounts", step("provision_accounts"))
    monkeypatch.setattr(supervisor_forks, "prepare_fork", step("prepare_fork"))
    monkeypatch.setattr(supervisor_forks, "PreparedDeployment", step("bundle"))
    host = SimpleNamespace(
        root=tmp_path,
        deployment=tmp_path / "deployment",
        persist=lambda **changes: steps.append("persist " + changes["stage"]),
    )
    host.deployment.mkdir()
    return SimpleNamespace(steps=steps, host=host)


@pytest.mark.parametrize("reuse", [False, True])
def test_fixture_setup_checks_code_before_reconciling_or_funding(bundle_steps, reuse):
    if reuse:
        (bundle_steps.host.deployment / "preparation.json").write_text("{}")
    supervisor_forks.prepare_fixture_bundle(bundle_steps.host, "doc", "def", "ctx", "op")
    assert bundle_steps.steps == [
        "policy",
        "read_progress" if reuse else "resolve",
        "prepare_fixture",
        "attach_nodes",
        "check_node_code",
        "reconcile_fixture",
        "persist provisioning",
        "provision_accounts",
        "persist preparing",
        "prepare_fork",
        "bundle",
    ]


def test_a_refused_fixture_is_neither_reconciled_nor_funded(bundle_steps):
    bundle_steps.steps.refuse = True
    with pytest.raises(UnsupportedOnFork):
        supervisor_forks.prepare_fixture_bundle(bundle_steps.host, "doc", "def", "ctx", "op")
    assert bundle_steps.steps[-2:] == ["attach_nodes", "check_node_code"]
