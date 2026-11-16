"""Fixture setup retains exact accounts and refuses uncertain funding retries."""

import fcntl
import json
from types import SimpleNamespace

import pytest

from almanak.gateway.fixture_accounts import FixtureAccounts, provision_accounts
from almanak.gateway.fixture_preparation import compile_fixture
from almanak.gateway.fork_fixture import Fork, fixture_signer
from almanak.models import Account, SafeRoles
from tests.test_operator_fixture import anchors as anchors
from tests.test_operator_fixture import context as context
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import operator as operator
from tests.test_operator_fixture import original as original
from tests.test_resolution_context import revalidate


@pytest.fixture
def case(tmp_path, operator, context, anchors, monkeypatch):
    plan = compile_fixture(context, operator, anchors)
    writes, balances, assets = [], {}, []

    def rpc(method, params=(), **_):
        if method == "anvil_setBalance":
            balances[params[0]] = params[1]
            writes.append((method, params))
            return True
        if method == "eth_getBalance":
            return balances[params[0]]
        if method == "eth_getCode":
            return "0x"
        raise AssertionError(method)

    rpc.endpoint = "http://127.0.0.1:12345"
    node = SimpleNamespace(
        origin=plan.forks[0].block,
        metadata={"instanceId": "0x" + "a" * 64},
        rpc=rpc,
        _validate_origin=lambda: None,
    )
    node.funded_signer = Fork.funded_signer.__get__(node)
    node.prepare_eoa_accounts = Fork.prepare_eoa_accounts.__get__(node)
    node.prepare_explicit_eoa = Fork.prepare_explicit_eoa.__get__(node)

    def deploy(rpc, chain, owner, operator, name):
        # Synthetic deployment responses isolate setup ordering and signer binding.
        # Actual contract deployment and generated permission proofs are separate.
        index = len([w for w in writes if w[0] == "deploy"]) + 1
        writes.append(("deploy", name))
        return SafeRoles(
            kind="safe_roles",
            fund=Account(chain=chain, address="0x" + f"{index:040x}"),
            operator=Account(chain=chain, address=operator.address),
            roles_module="0x" + f"{index + 100:040x}",
            role_key="0x" + f"{index:064x}",
            safe_deployment_block=node.origin.number + 1,
            roles_deployment_block=node.origin.number + 2,
        ), ({"fixture_test_receipt": name},)

    def fund(node, owner, account, funding):
        assets.append((account, funding))
        return {"fixture_test_fund": account, "funding": funding.model_dump(mode="json")}

    monkeypatch.setattr("almanak.gateway.safe_funding.deploy_profile", deploy)
    monkeypatch.setattr("almanak.gateway.safe_funding.fund_asset", fund)
    return SimpleNamespace(
        directory=tmp_path / "fixture",
        operator=operator,
        context=context,
        plan=plan,
        nodes=SimpleNamespace(config=plan, nodes={node.origin.chain: node}),
        node=node,
        writes=writes,
        assets=assets,
    )


def run(case):
    return provision_accounts(case.directory, case.plan, case.context, case.operator, case.nodes)


def files(case):
    return {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in case.directory.iterdir()}


def test_safe_accounts_preserve_funding_and_shared_operator_then_resume_without_writes(
    case, monkeypatch
):
    record = run(case)
    assert record.authority == "unadmitted"
    assert len(record.accounts.bindings) == 2
    assert len({a.profile.submitter for a in record.accounts.bindings}) == 1
    assert [item for _, item in case.assets] == [a for f in case.plan.funds for a in f.assets]
    assert record.accounts != case.operator.accounts
    assert case.directory.stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in case.directory.iterdir())
    before = files(case)
    writes = list(case.writes)

    def forbidden(*_, **__):
        raise AssertionError("account setup must not repeat")

    monkeypatch.setattr("almanak.gateway.fixture_accounts.prepare_accounts", forbidden)
    case.node.fixture_owners = {}
    assert run(case) == record
    assert files(case) == before and case.writes == writes
    assert set(case.node.fixture_owners) == {f.account for f in case.plan.funds}
    assert FixtureAccounts.model_validate_json(record.model_dump_json()) == record


def use_eoa(case):
    from almanak.gateway.operator_fixture import EoaFixtureFund

    fixture = case.operator.fork
    fixture = revalidate(
        fixture,
        operators=(),
        funds=tuple(
            EoaFixtureFund(
                kind="eoa",
                account=f.account,
                native_balance=fixture.native_balance(f),
                assets=f.assets,
            )
            for f in fixture.funds
        ),
    )
    case.operator = revalidate(case.operator, fork=fixture)
    case.context = revalidate(case.context, operator_digest=case.operator.digest)
    anchors = revalidate(case.plan.anchors, context_digest=case.context.digest)
    case.plan = compile_fixture(case.context, case.operator, anchors)
    case.nodes.config = case.plan


def test_eoa_accounts_use_exact_compiled_indices_and_separate_native_wrapping_balance(case):
    use_eoa(case)
    record = run(case)
    for index, binding in enumerate(record.accounts.bindings):
        assert binding.profile.kind == "eoa"
        assert binding.profile.fund.address == fixture_signer(941167 + index).address.lower()
        fund = case.plan.funds[index]
        funded = int(case.writes[index][1][1], 16)
        assert funded == fund.native_balance.amount + sum(
            a.balance.amount for a in fund.assets if a.method == "wrap_native"
        )


@pytest.mark.parametrize("code", ["0x01", "0xef0100" + "a" * 40])
def test_eoa_code_refusal_cannot_fund_or_try_another_index(case, code):
    from almanak.gateway.ethereum import DataError

    use_eoa(case)
    original = case.node.rpc
    checked = []

    def rpc(method, params=(), **kwargs):
        if method == "eth_getCode":
            checked.append(params[0])
            return code
        return original(method, params, **kwargs)

    rpc.endpoint = original.endpoint
    case.node.rpc = rpc
    with pytest.raises(DataError, match="empty code"):
        run(case)
    assert checked == [fixture_signer(941167).address]
    assert not case.writes and (case.directory / "fixture-started.json").is_file()
    with pytest.raises(RuntimeError, match="automatic reprovisioning is refused"):
        run(case)


def test_partial_funding_retains_intent_and_cannot_repeat(case, monkeypatch):
    def fail(node, *_):
        assert (case.directory / "fixture-started.json").is_file()
        node.rpc("anvil_setBalance", ["funding_started", "0x1"])
        raise RuntimeError("lost setup reply")

    monkeypatch.setattr("almanak.gateway.fixture_accounts.prepare_accounts", fail)
    with pytest.raises(RuntimeError, match="lost setup reply"):
        run(case)
    before = files(case)
    with pytest.raises(RuntimeError, match="automatic reprovisioning is refused"):
        run(case)
    assert len(case.writes) == 1 and files(case) == before


@pytest.mark.parametrize(
    "name",
    ["fork-keys.json", "fork-routes.json", "fixture-installation.json", "fixture-started.json"],
)
def test_changed_private_artifacts_refuse_reuse_without_funding(case, name):
    run(case)
    before = len(case.writes)
    (case.directory / name).write_text("{}")
    with pytest.raises(ValueError):
        run(case)
    assert len(case.writes) == before


def test_reuse_accepts_the_venues_a_connector_added_to_the_routes(case):
    record = run(case)
    path = case.directory / "fork-routes.json"
    venue = {"hypercore": {"model": "http://127.0.0.1:9", "agent_keys": "/keys.json"}}
    routes = [{**entry, "venues": venue} for entry in json.loads(path.read_text())]
    path.write_text(json.dumps(routes))
    writes = list(case.writes)
    assert run(case) == record and case.writes == writes


def test_replaced_node_cannot_rebind_frozen_accounts(case):
    run(case)
    before = files(case)
    case.node.metadata["instanceId"] = "0x" + "b" * 64
    with pytest.raises(ValueError, match="original setup"):
        run(case)
    assert files(case) == before


def test_context_change_refuses_before_fixture_side_effects(case):
    case.context = revalidate(case.context, identity="b" * 32)
    with pytest.raises(ValueError, match="another read context"):
        run(case)
    assert not case.writes and not case.directory.exists()


def test_concurrent_provisioning_is_refused(case):
    with case.directory.with_name("fixture.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="active owner"):
            run(case)
    assert not case.writes


def test_completed_record_cannot_change_profile_or_submitter(case):
    record = run(case)
    binding = record.accounts.bindings[0]
    profile = revalidate(
        binding.profile, operator=Account(chain=binding.profile.fund.chain, address="0x" + "9" * 40)
    )
    accounts = revalidate(
        record.accounts,
        bindings=(revalidate(binding, profile=profile), record.accounts.bindings[1]),
    )
    with pytest.raises(ValueError, match="deterministic operator"):
        revalidate(record, accounts=accounts)


@pytest.mark.parametrize("index", [-1, True, "0", 2**31])
def test_signer_derivation_requires_an_explicit_supported_integer(index):
    with pytest.raises(ValueError, match="Foundry account index"):
        fixture_signer(index)
