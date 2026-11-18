"""Compile the actual Aave fixture without changing funding, topology or origin."""

import fcntl
import json
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.chains import ChainReference, CheckpointReference
from almanak.configuration.fixture import prepare_fixture
from almanak.configuration.resolution import (
    AccountRequirement,
    AnchorSelection,
    NamedMarket,
    ReadContext,
    ResolutionAnchors,
)
from almanak.connectors.aave_v4.resolution import MarketSelection
from almanak.connectors.support import RunMode
from almanak.gateway.fixture_preparation import FixturePlan, compile_fixture
from almanak.gateway.fork_config import ForkEoaFund
from almanak.gateway.operator_config import (
    AlchemyAccess,
    OperatorConfig,
    OperatorNetwork,
    read_operator_config,
)
from almanak.gateway.operator_fixture import ForkFixtureInputs
from almanak.gateway.safe_funding import USDC_STORAGE_ASSETS, fund_usdc_storage, reviewed_funder
from almanak.models import StrategyAccounts
from tests.test_author_resolution import frozen_fork
from tests.test_resolution_context import revalidate

ROOT = Path(__file__).parents[1]
RETAINED = ROOT / "tests" / "fixtures" / "retained"


def test_gasless_fixture_preserves_archived_operator_and_plan_identity():
    operator = read_operator_config(RETAINED / "curve_ethereum_stable_lp.operator.yaml")
    plan = FixturePlan.model_validate_json(
        (RETAINED / "curve_ethereum_stable_lp.fixture-plan.json").read_text()
    )

    assert operator.digest == "86a4094bd926580a6172f2929fb72f742201472bb9ea15096dda48b5f50ae957"
    assert "gas" not in operator.model_dump(mode="json")["fork"]
    assert plan.operator_digest == operator.digest
    assert plan.digest == "69475e4b9a0c3a16a23808769e126cf8eed9f13ea0b9dde967317ea2a79125bc"


@pytest.fixture
def original():
    return frozen_fork()


@pytest.fixture
def fixture(original):
    block = original.origin
    return ForkFixtureInputs.model_validate_json(
        json.dumps(
            {
                "anchors": [
                    {
                        "chain": str(block.chain.chain_id),
                        "height": block.number,
                        "reference": block.hash,
                    }
                ],
                "funds": [
                    {
                        "kind": "safe_roles",
                        "account": f.account,
                        "operator": f.operator,
                        "assets": [a.balance.model_dump(mode="json") for a in f.assets],
                    }
                    for f in original.funds
                ],
                "operators": [
                    {"name": o.name, "native_balance": o.native_balance.model_dump(mode="json")}
                    for o in original.operators
                ],
            }
        )
    )


@pytest.fixture
def operator(fixture):
    # Read-scope identities only. Generated fixture accounts are a later provisioning result.
    chain = fixture.anchors[0].chain
    accounts = StrategyAccounts.model_validate_json(
        json.dumps(
            {
                "bindings": [
                    {
                        "name": f.account,
                        "profile": {
                            "kind": "eoa",
                            "fund": {
                                "chain": {"chain_id": str(chain), "environment": "mainnet"},
                                "address": "0x" + f"{index + 1:040x}",
                            },
                        },
                    }
                    for index, f in enumerate(fixture.funds)
                ]
            }
        )
    )
    return OperatorConfig(
        modes=(RunMode.FORK,),
        accounts=accounts,
        fork=fixture,
        networks=(
            OperatorNetwork(
                chain=chain, provider=AlchemyAccess(kind="alchemy", api_key_env="ALCHEMY_API_KEY")
            ),
        ),
    )


@pytest.fixture
def context(original, operator):
    return ReadContext(
        identity=uuid4().hex,
        authored_digest="a" * 64,
        operator_digest=operator.digest,
        mode=RunMode.FORK,
        anchors=operator.fork.anchors,
        accounts=tuple(
            AccountRequirement(name=f.account, chain=operator.fork.chain(f))
            for f in operator.fork.funds
        ),
        markets=tuple(
            NamedMarket(
                name=a.name,
                selection=MarketSelection(
                    connector="aave_v4",
                    chain=a.reserve.chain.chain_id,
                    spoke=a.reserve.spoke,
                    reserve_id=a.reserve.reserve_id,
                ),
            )
            for a in original.allocations
        ),
    )


@pytest.fixture
def anchors(original, context):
    return ResolutionAnchors(
        context_digest=context.digest,
        checkpoints=(
            CheckpointReference(
                chain=original.origin.chain,
                height=original.origin.number,
                reference=ChainReference(original.origin.hash),
                timestamp=original.origin.timestamp,
            ),
        ),
    )


def test_actual_aave_fixture_preserves_existing_harness_configuration(
    original, operator, context, anchors
):
    plan = compile_fixture(context, operator, anchors)
    assert plan.authority == "unprovisioned"
    assert plan.forks == original.forks
    assert plan.funds == original.funds
    assert plan.operators == original.operators
    assert (
        type(original).model_validate(
            {
                **original.model_dump(),
                "forks": plan.forks,
                "funds": plan.funds,
                "operators": plan.operators,
            }
        )
        == original
    )
    assert [(k.role, k.name, k.foundry_account_index) for k in plan.keys] == [
        ("operator", "operator_shared", 0),
        ("owner", "lending_0", 1_000_000_000),
        ("owner", "lending_1", 1_000_000_001),
    ]
    assert "ALCHEMY_API_KEY" in plan.model_dump_json()
    assert "key_file" not in plan.model_dump_json()


def test_ordering_does_not_change_derived_keys_funding_or_shared_operator(
    operator, context, anchors
):
    first = compile_fixture(context, operator, anchors)
    changed = revalidate(operator, fork=revalidate(operator.fork, funds=operator.fork.funds[::-1]))
    ctx = revalidate(context, operator_digest=changed.digest)
    pin = revalidate(anchors, context_digest=ctx.digest)
    second = compile_fixture(ctx, changed, pin)
    assert (second.keys, second.funds, second.operators, second.forks) == (
        first.keys,
        first.funds,
        first.operators,
        first.forks,
    )
    assert len(second.operators) == 1


def test_fresh_anchor_is_frozen_and_reuse_never_rewrites_a_plan(
    tmp_path, operator, context, anchors, monkeypatch
):
    fixture = revalidate(
        operator.fork, anchors=(AnchorSelection(chain=context.anchors[0].chain, fresh=True),)
    )
    operator = revalidate(operator, fork=fixture)
    context = revalidate(context, operator_digest=operator.digest, anchors=fixture.anchors)
    anchors = revalidate(anchors, context_digest=context.digest)
    path = tmp_path / "fixture.json"
    result = prepare_fixture(path, context, operator, anchors)
    before = path.read_bytes(), path.stat().st_mtime_ns

    def forbidden(*_, **__):
        raise AssertionError("retained fixture publication must not be repeated")

    monkeypatch.setattr("almanak.configuration.fixture.save", forbidden)
    assert prepare_fixture(path, context, operator, anchors) == result
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before
    assert path.stat().st_mode & 0o777 == 0o600
    later = revalidate(
        anchors,
        checkpoints=(revalidate(anchors.checkpoints[0], height=anchors.checkpoints[0].height + 1),),
    )
    with pytest.raises(ValueError, match="automatic reprovisioning is refused"):
        prepare_fixture(path, context, operator, later)
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


def test_partial_publication_and_concurrent_owner_cannot_replace_plan(
    tmp_path, operator, context, anchors, monkeypatch
):
    path = tmp_path / "fixture.json"
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="active owner"):
            prepare_fixture(path, context, operator, anchors)

    def fail(*_, **__):
        raise OSError("disk failed")

    with monkeypatch.context() as scoped:
        scoped.setattr("almanak.configuration.fixture.save", fail)
        with pytest.raises(OSError, match="disk failed"):
            prepare_fixture(path, context, operator, anchors)
    assert not path.exists()
    assert prepare_fixture(path, context, operator, anchors).anchors == anchors


@pytest.mark.parametrize("fault", ["index", "funding", "origin", "provider"])
def test_tampered_compiled_plan_refuses(operator, context, anchors, fault):
    plan = compile_fixture(context, operator, anchors)
    raw = plan.model_dump(mode="json")
    if fault == "index":
        raw["operators"][0]["foundry_account_index"] += 1
    elif fault == "funding":
        raw["funds"][0]["assets"][0]["balance"]["amount"] += 1
    elif fault == "origin":
        raw["forks"][0]["block"]["timestamp"] += 1
    else:
        raw["forks"][0]["provider"]["api_key_env"] = "OTHER_PROVIDER"
    changed = FixturePlan.model_validate_json(json.dumps(raw))
    with pytest.raises(ValueError, match="retained fixture differs"):
        changed.require(context, operator, anchors)


@pytest.mark.parametrize(
    "fault",
    [
        "index",
        "method",
        "duplicate_asset",
        "missing_balance",
        "missing_operator",
        "extra_operator",
        "chain",
        "native",
    ],
)
def test_invalid_or_implementation_specific_fixture_inputs_refuse(fixture, fault):
    raw = fixture.model_dump(mode="json")
    if fault == "index":
        raw["operators"][0]["foundry_account_index"] = 2
    elif fault == "method":
        raw["funds"][0]["assets"][0]["method"] = "local_storage"
    elif fault == "duplicate_asset":
        raw["funds"][0]["assets"] *= 2
    elif fault == "missing_balance":
        del raw["operators"][0]["native_balance"]
    elif fault == "missing_operator":
        raw["operators"] = []
    elif fault == "extra_operator":
        raw["operators"].append({**raw["operators"][0], "name": "unused"})
    elif fault == "chain":
        raw["funds"][0]["assets"][0]["asset"] = raw["funds"][0]["assets"][0]["asset"].replace(
            "eip155:1", "eip155:43114"
        )
    else:
        raw["operators"][0]["native_balance"]["asset"] = "eip155:1/slip44:1"
    with pytest.raises(ValidationError):
        ForkFixtureInputs.model_validate_json(json.dumps(raw))


def test_unreviewed_erc20_compiles_for_the_fork_funding_ladder(operator, context, anchors):
    raw = operator.model_dump(mode="json")
    asset = raw["fork"]["funds"][0]["assets"][0]
    asset.update(asset="eip155:1/erc20:0x" + "12" * 20, amount=0)
    operator = OperatorConfig.model_validate_json(json.dumps(raw))
    context = revalidate(context, operator_digest=operator.digest)
    anchors = revalidate(anchors, context_digest=context.digest)
    plan = compile_fixture(context, operator, anchors)
    funding = plan.funds[0].assets[0]
    assert str(funding.balance.asset) == "eip155:1/erc20:0x" + "12" * 20
    assert funding.method == "local_storage"


@pytest.mark.parametrize(
    "fault", ["mode", "selection", "assertion", "accounts", "operator", "no_fixture"]
)
def test_fixture_requires_the_exact_context_and_operator_grant(operator, context, anchors, fault):
    if fault == "mode":
        context = revalidate(context, mode=RunMode.PRODUCTION)
    elif fault == "selection":
        context = revalidate(
            context, anchors=(AnchorSelection(chain=context.anchors[0].chain, fresh=True),)
        )
    elif fault == "assertion":
        anchors = revalidate(
            anchors,
            checkpoints=(
                revalidate(anchors.checkpoints[0], reference=ChainReference("0x" + "34" * 32)),
            ),
        )
    elif fault == "accounts":
        context = revalidate(context, accounts=context.accounts[:1])
    elif fault == "operator":
        operator = revalidate(operator, modes=(RunMode.BACKTEST, RunMode.FORK))
    else:
        operator = revalidate(operator, fork=None)
        context = revalidate(context, operator_digest=operator.digest)
    anchors = revalidate(anchors, context_digest=context.digest)
    with pytest.raises(ValueError):
        compile_fixture(context, operator, anchors)


def test_explicit_eoa_balances_and_independent_keys(operator, context, anchors):
    raw = operator.model_dump(mode="json")
    native = raw["fork"]["operators"][0]["native_balance"]
    raw["fork"]["operators"] = []
    for fund in raw["fork"]["funds"]:
        fund.pop("operator")
        fund.update(kind="eoa", native_balance=native)
    operator = OperatorConfig.model_validate_json(json.dumps(raw))
    context = revalidate(context, operator_digest=operator.digest)
    anchors = revalidate(anchors, context_digest=context.digest)
    plan = compile_fixture(context, operator, anchors)
    assert not plan.operators
    assert all(isinstance(fund, ForkEoaFund) for fund in plan.funds)
    assert [k.foundry_account_index for k in plan.keys] == [941167, 941168]
    assert [fund.native_balance.amount for fund in plan.funds] == [5 * 10**18] * 2
    assert all(len(fund.assets) == 2 for fund in plan.funds)


def test_reviewed_selector_uses_exact_existing_usdc_and_leaves_unknown_assets_to_the_ladder():
    for asset in USDC_STORAGE_ASSETS.values():
        assert reviewed_funder(asset) is fund_usdc_storage
    assert reviewed_funder(AssetType("eip155:1/erc20:0x" + "12" * 20)) is None


def test_two_chain_fixtures_keep_independent_native_balances_and_key_scopes(
    operator, context, anchors
):
    raw = operator.model_dump(mode="json")
    chain = ChainId("eip155:43114")
    raw["networks"].append({**raw["networks"][0], "chain": str(chain)})
    account = json.loads(json.dumps(raw["accounts"]["bindings"][0]))
    account["name"] = "avalanche"
    account["profile"]["fund"]["chain"]["chain_id"] = str(chain)
    raw["accounts"]["bindings"].append(account)
    raw["fork"]["anchors"].append({"chain": str(chain), "height": 500})
    raw["fork"]["funds"].append(
        {
            "kind": "eoa",
            "account": "avalanche",
            "assets": [],
            "native_balance": {"asset": "eip155:43114/slip44:9005", "amount": 1234},
        }
    )
    operator = OperatorConfig.model_validate_json(json.dumps(raw))
    context = revalidate(
        context,
        operator_digest=operator.digest,
        anchors=operator.fork.anchors,
        accounts=context.accounts + (AccountRequirement(name="avalanche", chain=chain),),
    )
    checkpoint = revalidate(
        anchors.checkpoints[0],
        chain=revalidate(anchors.checkpoints[0].chain, chain_id=chain),
        height=500,
    )
    anchors = revalidate(
        anchors, context_digest=context.digest, checkpoints=anchors.checkpoints + (checkpoint,)
    )
    plan = compile_fixture(context, operator, anchors)
    assert len(plan.forks) == 2
    assert plan.operators[0].native_balance.amount == 5 * 10**18
    assert plan.funds[-1].native_balance.amount == 1234
    assert len({(key.chain, key.foundry_account_index) for key in plan.keys}) == len(plan.keys)
    assert next(key for key in plan.keys if key.name == "avalanche").foundry_account_index == 941167


@pytest.mark.parametrize(
    "fault", ["disabled_mode", "legacy", "missing_account", "wrong_chain", "mixed_family"]
)
def test_fixture_operator_binding_refuses_invalid_scope(operator, fault):
    raw = operator.model_dump(mode="json")
    if fault == "disabled_mode":
        raw["modes"] = ["backtest"]
    elif fault == "legacy":
        raw["schema_version"] = "operator-read-config-v1"
    elif fault == "missing_account":
        raw["fork"]["funds"][0]["account"] = "unbound"
    elif fault == "wrong_chain":
        raw["accounts"]["bindings"][0]["profile"]["fund"]["chain"]["chain_id"] = "eip155:43114"
    else:
        fund = raw["fork"]["funds"][0]
        fund.pop("operator")
        fund.update(kind="eoa", native_balance=raw["fork"]["operators"][0]["native_balance"])
    with pytest.raises(ValidationError):
        OperatorConfig.model_validate_json(json.dumps(raw))
