"""Assembly checks preserve fund, payer and configured fork coin scope."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from almanak.connectors.curve import connector as binding
from almanak.connectors.curve import observations
from almanak.connectors.curve.deployment import identities
from almanak.connectors.curve.fork import ForkConfig
from almanak.connectors.curve.support import candidate_support
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Account, AccountBinding, SafeRoles, StrategyAccounts
from scripts.legacy_configuration.author import read_author
from tests.legacy_references import harness_directory
from tests.test_curve_economic_model import component
from tests.test_curve_reader import captured as captured
from tests.test_curve_settlement import target as target


@pytest.mark.parametrize("kind", ("trading", "lp"))
def test_reference_fork_configuration_roundtrip_preserves_ordered_coins(monkeypatch, kind):
    original = binding.connector
    monkeypatch.setattr(
        binding, "connector", lambda: replace(original(), chain_support=candidate_support())
    )
    path = harness_directory(f"curve_ethereum_stable_{kind}") / "config.yaml"
    config = read_author(path).fork_config()
    assert ForkConfig.model_validate_json(config.model_dump_json()) == config
    assert len(config.allocations) == (2 if kind == "lp" else 1)
    assert all(tuple(v.asset for v in a.capital.root) == a.pool.coins for a in config.allocations)


def test_permission_identities_filter_accounts_and_deduplicate_shared_pool(captured):
    _, pool, reader, block = captured
    tokens = {
        asset.reference: ContractIdentity(address=asset.reference, code_hash="0x" + "a" * 64)
        for asset in pool.coins
    }
    config = SimpleNamespace(
        allocations=(
            SimpleNamespace(account="selected", pool=pool),
            SimpleNamespace(account="selected", pool=pool),
            SimpleNamespace(account="other", pool=None),
        )
    )
    contracts = identities(
        config, "selected", SimpleNamespace(reader=reader, block=block, identity=tokens.__getitem__)
    )
    assert len(contracts) == 6
    assert set(tokens) <= {contract.address for contract in contracts}
    assert identities(config, "missing", None) == ()


def test_observation_assembly_reads_fund_assets_and_operator_gas(captured, target, monkeypatch):
    _, pool, reader, block = captured
    model, _ = component(target)
    profile = SafeRoles(
        kind="safe_roles",
        fund=target.fund,
        operator=Account(chain=target.fund.chain, address="0x" + "34" * 20),
        roles_module="0x" + "56" * 20,
        role_key="0x" + "ab" * 32,
    )
    config = model.config.model_copy(
        update={
            "accounts": StrategyAccounts(
                bindings=(AccountBinding(name=target.account, profile=profile),)
            )
        }
    )
    original_call, original_rpc = reader.call, reader._rpc

    def call(address, signature, at, **kwargs):
        if signature == "balanceOf(address)":
            assert kwargs["args"] == (target.fund.address,)
            return (20 if address == pool.pool else 10,)
        if signature == "allowance(address,address)":
            assert kwargs["args"] == (target.fund.address, pool.pool)
            return (0,)
        return original_call(address, signature, at, **kwargs)

    def rpc(method, params):
        if method == "eth_getBalance":
            assert params == [
                profile.operator.address,
                {"blockHash": block.hash, "requireCanonical": True},
            ]
            return "0x123"
        return original_rpc(method, params)

    monkeypatch.setattr(reader, "call", call)
    monkeypatch.setattr(reader, "_rpc", rpc)
    backend = SimpleNamespace(config=config, reader=reader)
    observed = observations.snapshot(backend, target.allocation, block, quotes=False)
    assert observed.payer == profile.operator and observed.native_balance.amount == 0x123
    assert observed.wallets.root[0].asset == target.assets[0]
    assert observed.wallets.root[0].amount == 10
    assert observed.position.shares.amount == 20
    assert observed.owned.position is None
    assert observations.snapshot(backend, target.allocation, block, observed) == observed
    foreign = observed.owned.model_copy(
        update={"target": target.model_copy(update={"allocation": "foreign"})}
    )
    with pytest.raises(ValueError, match="reference differs"):
        observations.snapshot(backend, target.allocation, block, foreign)
