"""Fork operator generation, unenforced OP author gas and the `almanak test` lifecycle helpers."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import TypeAdapter

from almanak.caip import ChainId
from almanak.configuration.admission import PreparedDeployment
from almanak.configuration.fork_preparation import require_fork_policy
from almanak.configuration.gas_parameters import PreparedConfig
from almanak.configuration.prepared import PreparationProgress
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.support import RunMode
from almanak.fork_operator import FixturePlan, funded_assets, payer_need
from almanak.gateway.operator_config import OperatorConfig
from almanak.gateway.operator_execution import PolicyRefusal
from almanak.gateway.policy_resolution import ForkGasUnenforced
from almanak.gateway.production_config import UnenforcedGas
from almanak.testflow import classify, settled, transactions
from almanak.triggers import offset_clock, utc_now
from tests.test_check_warnings import inputs as op_inputs

ROOT = Path(__file__).parents[1]
ANCHOR = {
    "height": 100,
    "reference": "0x" + "ab" * 32,
}
FUNDING = {
    "accounts": [
        {
            "account": "fund",
            "assets": [
                {
                    "asset": "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                    "amount_base_units": "8100000",
                },
            ],
        }
    ],
    "payers": [
        {
            "accounts": ["fund"],
            "entry_minimum_base_units": "1000000000000000",
            "gas": {"total_budget_base_units": "3000000000000000"},
        }
    ],
}


def plan_for(tmp_path):
    source = (ROOT / "strategies" / "uniswap_v4_avalanche_trading" / "config.yaml").read_text()
    path = tmp_path / "config.yaml"
    path.write_text(source)
    parsed = read_inputs(path)
    chains = sorted({r.chain for r in parsed.inputs.requirements}, key=str)
    return parsed, FixturePlan(parsed.inputs, chains)


def test_generated_operator_mirrors_authored_gas_and_validates(tmp_path):
    parsed, plan = plan_for(tmp_path)
    anchors = [{"chain": "eip155:43114", **ANCHOR}]
    funding = {
        "accounts": [{"account": name, "assets": []} for name in parsed.inputs.accounts],
        "payers": [],
    }
    operator = plan.document(anchors, funding)
    config = OperatorConfig.model_validate_json(json.dumps(operator))
    require_fork_policy(parsed, config)
    [policy] = parsed.inputs.strategy.gas.values()
    [ceiling] = operator["fork"]["gas"]
    assert ceiling["max_gas_limit"] == policy.max_gas_limit
    assert ceiling["max_gas_price_wei"] == policy.max_gas_price_wei
    assert "author_gas" not in operator["fork"]
    assert plan.notes == []
    assert {f["kind"] for f in operator["fork"]["funds"]} == {"safe_roles"}


def test_op_family_policy_generates_a_gasless_fork_with_a_note(tmp_path):
    plan = FixturePlan(op_inputs(tmp_path), [ChainId("eip155:8453")])
    operator = plan.document([{"chain": "eip155:8453", **ANCHOR}], None)
    assert operator["fork"]["author_gas"] == "unenforced"
    assert "gas" not in operator["fork"]
    [note] = plan.notes
    assert (note["code"], note["field"]) == ("fork_gas", "strategy.gas")
    assert note["message"].startswith("eip155:8453 is an OP-family chain")


def test_gasless_and_policy_accounts_on_one_chain_get_distinct_operators(tmp_path):
    _, plan = plan_for(tmp_path)
    plan.groups = {("eip155:1", None): ["a"], ("eip155:1", "gas"): ["b"]}
    assert {plan.operator_name(key) for key in plan.groups} == {"operator_0", "operator_1"}


def test_funding_adds_a_small_buffer_and_operator_native_covers_the_budget():
    [asset] = funded_assets(FUNDING, "fund")
    assert asset["amount"] == 8100000 + 81000
    assert payer_need(FUNDING, ["fund"]) == 4000000000000000


def operator_with(fork_overrides):
    source = yaml.safe_load(
        (ROOT / "examples/operators/aave_v3_base_lending.fork.yaml").read_text()
    )
    source["fork"].update(fork_overrides)
    return OperatorConfig.model_validate_json(json.dumps(source))


def op_policy(chain="eip155:8453"):
    from almanak.gateway.production_config import GasPolicy

    native = f"{chain}/slip44:60"
    return GasPolicy.model_validate_json(
        json.dumps(
            {
                "name": "base",
                "accounts": ["fund"],
                "asset": native,
                "source": "network_rpc_fee_history",
                "base_fee_basis": "observed_block",
                "reward_percentile": 50,
                "base_fee_multiplier_bps": 20000,
                "max_gas_price_wei": 10**9,
                "max_gas_limit": 1_500_000,
                "gas_limit_buffer_bps": 2500,
                "max_transaction_fee": {"asset": native, "amount": 10**15},
                "total_budget": {"asset": native, "amount": 6 * 10**15},
                "exit_reserve": {"asset": native, "amount": 2 * 10**15},
            }
        )
    )


def test_unenforced_author_gas_is_accepted_only_for_op_family_forks():
    operator = operator_with({"author_gas": "unenforced"})
    assert operator.fork.check_gas((op_policy(),)) == UnenforcedGas(gas=(op_policy(),))
    with pytest.raises(PolicyRefusal, match="limited to Base and Optimism"):
        operator.fork.check_gas((op_policy("eip155:42161"),))


def test_unenforced_author_gas_still_covers_exactly_the_fixture_accounts():
    operator = operator_with({"author_gas": "unenforced"})
    stranger = op_policy().model_copy(update={"accounts": ("other",)})
    with pytest.raises(PolicyRefusal, match="cover exactly the fixture fund accounts"):
        operator.fork.check_gas((stranger,))
    with pytest.raises(PolicyRefusal, match="exactly one gas policy"):
        operator.fork.check_gas((op_policy(), op_policy()))


def test_unenforced_author_gas_takes_no_ceilings():
    ceiling = {
        "accounts": ["fund"],
        "max_gas_price_wei": 1,
        "max_gas_limit": 1,
        "max_transaction_fee": {"asset": "eip155:8453/slip44:60", "amount": 1},
        "total_budget": {"asset": "eip155:8453/slip44:60", "amount": 1},
        "allow_replacement": False,
    }
    with pytest.raises(ValueError, match="takes no fork gas ceilings"):
        operator_with({"author_gas": "unenforced", "gas": [ceiling]})


def test_enforced_op_fork_policy_keeps_its_refusal(tmp_path, monkeypatch):
    parsed, _ = plan_for(tmp_path)
    monkeypatch.setattr(
        "almanak.configuration.fork_preparation.gas_policies", lambda *_: (op_policy(),)
    )
    with pytest.raises(ConfigurationError, match="OP fork gas admission is unavailable"):
        require_fork_policy(parsed, operator_with({}))
    require_fork_policy(parsed, operator_with({"author_gas": "unenforced"}))


def prepared_fork(gas, unenforced):
    return SimpleNamespace(
        gas=gas,
        fork_gas=None,
        unenforced_gas=unenforced,
        production=None,
        preparation_digest="a" * 64,
        fork_bindings=("binding",),
    )


def test_prepared_op_fork_requires_the_retained_unenforced_choice():
    policies = (op_policy(),)
    PreparedConfig.prepared_scope(prepared_fork(policies, UnenforcedGas(gas=policies)))
    with pytest.raises(ValueError, match="requires matching admitted execution policy"):
        PreparedConfig.prepared_scope(prepared_fork(policies, None))
    arbitrum = (op_policy("eip155:42161"),)
    with pytest.raises(ValueError, match="requires matching admitted execution policy"):
        PreparedConfig.prepared_scope(prepared_fork(arbitrum, None))
    with pytest.raises(ValueError, match="limited to Base and Optimism"):
        UnenforcedGas(gas=arbitrum)


def unenforced_record(operator):
    return ForkGasUnenforced(
        context_digest="c" * 64,
        operator_digest="d" * 64,
        accounts=operator.accounts,
        execution=UnenforcedGas(gas=(op_policy(),)),
    )


def deployment(operator, gas, policy):
    fixture = SimpleNamespace(
        accounts=operator.accounts,
        digest="f",
        bindings=(),
        require_resolution=lambda resolution: None,
    )
    parameters = SimpleNamespace(gas=gas, accounts=operator.accounts, positions=())
    context = SimpleNamespace(digest="c" * 64, operator_digest="d" * 64, mode=RunMode.FORK)
    return SimpleNamespace(
        fixture=fixture,
        resolution=SimpleNamespace(context=context),
        preparation=SimpleNamespace(
            prepared=SimpleNamespace(parameters=parameters, fixture_digest="f"), policy=policy
        ),
    )


def test_admission_requires_the_retained_unenforced_record_for_op_gas():
    operator = operator_with({"author_gas": "unenforced"})
    record = unenforced_record(operator)
    PreparedDeployment.require_fixture(deployment(operator, (op_policy(),), record))
    with pytest.raises(ValueError, match="requires retained operator compatibility"):
        PreparedDeployment.require_fixture(deployment(operator, (op_policy(),), None))
    arbitrum = (op_policy("eip155:42161"),)
    with pytest.raises(ValueError, match="requires retained operator compatibility"):
        PreparedDeployment.require_fixture(deployment(operator, arbitrum, None))
    with pytest.raises(ValueError, match="changed the requested economics"):
        PreparedDeployment.require_fixture(deployment(operator, arbitrum, record))
    decoded = PreparationProgress.model_fields["policy"].annotation
    assert TypeAdapter(decoded).validate_json(record.model_dump_json()) == record


def test_offset_clock_adds_the_test_offset(tmp_path):
    path = tmp_path / "clock-offset"
    path.write_text("3600")
    before = utc_now()
    assert (offset_clock(path)() - before).total_seconds() >= 3600


def outcome(started_at, kind, *, success=True, roles="0xroles", to="0xroles"):
    return {
        "decision": {"trigger": {"started_at": started_at}, "intent": {"kind": kind}},
        "operation": {"kind": "swap", "target": {"allocation": "position"}},
        "execution": {"binding": {"profile": {"roles_module": roles}}},
        "receipt": {"gasUsed": hex(400289), "to": to},
        "success": success,
        "tx_hash": "0x" + "22" * 32,
    }


def test_decisions_classify_as_hold_or_their_intents():
    wire = {"outcomes": [outcome("t1", "swap_exact_input")]}
    assert classify(wire, "t0") == {"time": "t0", "result": "hold"}
    assert classify(wire, "t1") == {
        "time": "t1",
        "result": "intent",
        "intents": ["swap_exact_input"],
    }


def test_transactions_report_gas_and_roles_routing():
    wire = {"outcomes": [outcome("t1", "x"), outcome("t2", "x", to="0xother", success=False)]}
    first, second = transactions(wire)
    assert (first["gas_used"], first["via_roles"], first["status"]) == (400289, True, "success")
    assert (second["via_roles"], second["status"]) == (False, "failed")


def test_settled_requires_no_pending_plan_or_decision():
    assert settled({"pending": None, "plan": None, "decision": None})
    assert not settled({"pending": None, "plan": None, "decision": {"identity": "x"}})
    assert not settled({"work": [{"pending": {"x": 1}}]})
    # A multi-chain checkpoint keeps a chain's decision and plan in that chain's work.
    assert not settled({"work": [{"chain": "a"}, {"decision": {"identity": "x"}}]})
    assert not settled({"work": [{"plan": {"x": 1}}]})
    assert settled({"work": [{"chain": "a", "decision": None}]})


def bridge_fixture(positions, bound, bindings):
    record = deployment(operator_with({"author_gas": "unenforced"}), (), None)
    record.preparation.policy = None
    parameters = record.preparation.prepared.parameters
    parameters.positions = tuple(SimpleNamespace(chain=chain) for chain in positions)
    accounts = SimpleNamespace(
        bindings=tuple(
            SimpleNamespace(profile=SimpleNamespace(fund=SimpleNamespace(chain=c))) for c in bound
        )
    )
    parameters.accounts = record.fixture.accounts = accounts
    record.fixture.bindings = tuple(SimpleNamespace(chain=chain) for chain in bindings)
    return record


def test_a_fixture_chain_is_acted_on_or_holds_a_receiving_account():
    # A bridge acts on its source chain only; its receiving account binds the other.
    PreparedDeployment.require_fixture(bridge_fixture(("a",), ("a", "b"), ("a", "b")))
    for positions, bound, bindings in (
        (("a", "c"), ("a", "b"), ("a", "b")),
        (("a",), ("a",), ("a", "b")),
    ):
        with pytest.raises(ValueError, match="fork positions differ"):
            PreparedDeployment.require_fixture(bridge_fixture(positions, bound, bindings))


def test_coordinator_takes_the_test_clock_offset(tmp_path, monkeypatch):
    from argparse import Namespace

    from almanak import cli

    captured = {}
    runners = []

    class Runtime:
        def __init__(self, url, auth, **options):
            captured.update(options)
            runners.append(self)
            self.journal = Namespace(load=lambda: Namespace(deployment_id="d"))

        def run(self, interrupt):
            return "state"

    monkeypatch.setattr("almanak.runtime.Runtime", Runtime)
    offset = tmp_path / "clock-offset"
    offset.write_text("120")
    args = Namespace(
        config=None,
        product="resume",
        gateway_url="http://gateway",
        gateway_auth_file=tmp_path / "token",
        clock_offset_file=offset,
    )
    assert cli.coordinated_lifecycle(args, lambda: None) == "state"
    assert (captured["clock"]() - utc_now()).total_seconds() >= 119
    assert runners[-1].decision_facts is False
    args.decision_facts = True
    cli.coordinated_lifecycle(args, lambda: None)
    assert runners[-1].decision_facts is True
    args.clock_offset_file = None
    cli.coordinated_lifecycle(args, lambda: None)
    assert captured["clock"] is utc_now
