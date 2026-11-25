"""Converted references get production rehearsal inputs the gateway would admit."""

import json
from pathlib import Path

import pytest
import yaml

from almanak.configuration.anchors import AnchorSelection
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.support import RunMode
from almanak.gateway.credentials import load_production_signers
from almanak.gateway.operator_config import read_operator_config
from almanak.gateway.operator_execution import PolicyRefusal
from almanak.gateway.policy_resolution import check_policy
from scripts.ci.check_strategies import CONVERTED
from tests.strategy_fork import rehearsal
from tests.strategy_fork.rehearsal import (
    POLICY,
    QUALIFY_ENV,
    UNREHEARSED,
    account_chains,
    production_references,
    qualifying_references,
    rehearsal_author,
    rehearsal_fork_operator,
    rehearsal_operator,
)
from tests.test_author_resolution import REFERENCE

ROOT = Path(__file__).parents[1]


def explicit_production_references() -> list[str]:
    return sorted(
        path.name.removesuffix(".production.yaml")
        for path in (ROOT / "examples" / "operators").glob("*.production.yaml")
    )


def gas_policies(inputs):
    return tuple(policy.resolve(name) for name, policy in inputs.strategy.gas.items())


def test_converted_references_without_declared_production_stay_out_of_the_rehearsal():
    excluded = set(CONVERTED) - set(production_references())
    # A production Aave V3 market does not admit a composed reference whose other
    # connector lacks production support on the same chain (GMX V2 on Avalanche).
    assert "avalanche_aave_borrow_and_trade_gmx" in excluded
    assert "base_morpho_borrow_and_trade" not in excluded
    assert "uniswap_v3_base_trading" not in excluded
    assert "base_aave_aerodrome_stable_borrow_and_trade" not in excluded
    assert "aave_v4_multichain_borrowing" not in excluded


def test_unrehearsed_references_join_the_rehearsal_only_when_named_for_qualification(monkeypatch):
    assert UNREHEARSED <= CONVERTED and not UNREHEARSED & set(production_references())
    default = set(production_references())
    withheld = frozenset({"aave_v4_weth_supply"})
    monkeypatch.setattr(rehearsal, "UNREHEARSED", UNREHEARSED | withheld)
    assert set(production_references()) == default - withheld
    assert set(production_references(withheld)) == default
    with pytest.raises(ValueError, match="not unrehearsed"):
        production_references(frozenset({"aave_v4_avalanche_borrowing"}))
    monkeypatch.setattr(rehearsal, "declares_production", lambda name: name not in withheld)
    with pytest.raises(ValueError, match="no production EOA support"):
        production_references(withheld)


def test_qualifying_references_come_from_the_environment(monkeypatch):
    monkeypatch.delenv(QUALIFY_ENV, raising=False)
    assert qualifying_references() == frozenset()
    withheld = frozenset({"uniswap_v4_lp", "uniswap_v4_trading"})
    monkeypatch.setattr(rehearsal, "UNREHEARSED", withheld)
    monkeypatch.setenv(QUALIFY_ENV, "uniswap_v4_lp, ,uniswap_v4_trading ")
    assert qualifying_references() == {"uniswap_v4_lp", "uniswap_v4_trading"}
    monkeypatch.setenv(QUALIFY_ENV, "aave_v4_weth_supply")
    with pytest.raises(ValueError, match="outside UNREHEARSED"):
        qualifying_references()


# Production references whose connector declares no EOA profile: each rehearses through its
# own Safe rehearsal instead of the production EOA one.
SAFE_REHEARSED = {
    "hyperliquid_hyperevm_perp": "tests/fork/test_hyperliquid_production_rehearsal.py",
}


def test_explicit_production_operator_examples_map_one_to_one_to_rehearsable_references():
    references = explicit_production_references()
    assert references == [
        "aave_v4_avalanche_borrowing",
        "aave_v4_multichain_borrowing",
        "aave_v4_weth_supply",
        "across_usdc_arbitrum_base_bridge",
        "hyperliquid_hyperevm_perp",
        "uniswap_v3_arbitrum_lp",
        "uniswap_v3_base_lp",
        "uniswap_v3_optimism_lp",
        "uniswap_v4_avalanche_lp",
        "uniswap_v4_avalanche_trading",
    ]
    assert set(references) - set(SAFE_REHEARSED) <= set(production_references())
    for name, own in SAFE_REHEARSED.items():
        assert name not in production_references() and (ROOT / own).is_file()


# Production author files no fork admits: policies on Arbitrum and on Base. A fork refuses
# authored ceilings on Base, and leaves them unenforced only when every policy is on an
# OP chain. The rehearsal runs the production gateway, which takes them; fork runs use
# the reference's config.yaml, which states no gas.
NO_FORK_GAS = {"across_usdc_arbitrum_base_bridge"}


@pytest.mark.parametrize(
    "name", [name for name in explicit_production_references() if name not in SAFE_REHEARSED]
)
def test_explicit_production_rehearsal_preserves_exact_authored_gas(name, tmp_path):
    reference = ROOT / "strategies" / name
    source = reference / "config.production.yaml"
    if not source.is_file():
        source = reference / "config.yaml"
    expected = gas_policies(read_inputs(source).inputs)
    author = rehearsal_author(reference, tmp_path)
    actual = gas_policies(read_inputs(author).inputs)
    assert expected and actual == expected
    operator = read_operator_config(rehearsal_fork_operator(name))
    assert operator.fork is not None
    if name in NO_FORK_GAS:
        with pytest.raises(PolicyRefusal, match="no gas ceilings"):
            operator.fork.check_gas(actual)
        return
    admitted = operator.fork.check_gas(actual)
    assert admitted is not None and admitted.gas == actual


@pytest.mark.parametrize("name", production_references())
def test_production_rehearsal_fork_fixture_matches_author_accounts(name, tmp_path):
    author = rehearsal_author(ROOT / "strategies" / name, tmp_path)
    inputs = read_inputs(author).inputs
    operator_path = rehearsal_fork_operator(name)
    operator = read_operator_config(operator_path)
    assert operator.fork is not None
    names = set(inputs.accounts)
    assert {binding.name for binding in operator.accounts.bindings} == names
    assert {fund.account for fund in operator.fork.funds} == names
    assert operator_path.is_file()


@pytest.mark.parametrize("name", production_references())
def test_converted_reference_rehearsal_inputs_pass_the_offline_policy_check(name, tmp_path):
    author = rehearsal_author(ROOT / "strategies" / name, tmp_path)
    operator_path = rehearsal_operator(author, tmp_path)
    inputs = read_inputs(author).inputs
    operator = read_operator_config(operator_path)
    assert operator.modes == (RunMode.PRODUCTION,)
    anchors = tuple(AnchorSelection(chain=chain, height=1) for chain in account_chains(inputs))
    context = inputs.context(operator, RunMode.PRODUCTION, anchors)
    policies = gas_policies(inputs)
    assert policies and check_policy(context, operator, policies).execution.gas == policies
    signing = operator.production.signing
    signers = load_production_signers(operator, signing.key_file, signing.password_file)
    assert {name: signer.address.lower() for name, signer in signers.items()} == {
        b.name: b.profile.fund.address for b in operator.accounts.bindings
    }
    assert "ALCHEMY" not in operator_path.read_text()


def test_references_without_a_production_file_get_the_fixture_policy_as_author_gas(tmp_path):
    reference = tmp_path / "reference"
    reference.mkdir()
    raw = yaml.safe_load((REFERENCE / "config.yaml").read_text())
    raw["strategy"].pop("gas")
    (reference / "config.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))
    run = tmp_path / "run"
    run.mkdir()
    inputs = read_inputs(rehearsal_author(reference, run)).inputs
    (policy,) = gas_policies(inputs)
    expected = json.loads(POLICY.read_text())["eip155:1"]
    assert policy.name == "payer_1" and set(policy.accounts) == set(inputs.accounts)
    assert str(policy.asset) == expected["asset"]
    assert policy.total_budget.amount == expected["total_budget"]["amount"]
    assert policy.max_gas_limit == expected["max_gas_limit"]
    rehearsal_operator(run / "config.yaml", run)
    with pytest.raises(FileExistsError):
        rehearsal_operator(run / "config.yaml", run)
