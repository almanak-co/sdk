"""Support declarations, identifier families and independent scoped admission."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.backtest.types import ConnectorId
from almanak.caip import AssetType, ChainId
from almanak.chains import ChainReference, CheckpointReference, TransactionReference, evm_network
from almanak.connectors.composition import connector_for
from almanak.connectors.support import EconomicAction, RunMode, declarations
from almanak.gateway.execution import EvmGateway
from almanak.gateway.production_fees import validate_budget
from almanak.models import AssetAmount, Chain
from almanak.work import ObservationScope, validate_dependencies
from tests.network_support import production_config


def test_connector_chain_selection_is_explicit_and_does_not_follow_network_inventory():
    base = ChainId("eip155:8453")
    support = declarations((EconomicAction.SUPPLY,), (), {}, chains=(base,))
    assert tuple(entry.chain for entry in support) == (base,)
    assert RunMode.BACKTEST not in support[0].modes
    with pytest.raises(ValueError, match="unique"):
        declarations((), (), {}, chains=(base, base))
    with pytest.raises(ValueError, match="validated"):
        declarations((), (), {}, chains=("eip155:8453",))
    with pytest.raises(ValueError, match="declared execution chain"):
        declarations((), (), {ChainId("eip155:1"): ()}, chains=(base,))


@pytest.mark.parametrize(
    "name,chain",
    [
        ("aave_v4_multichain_borrowing", ChainId("eip155:8453")),
        ("aave_v4_multichain_borrowing", ChainId("eip155:42161")),
        ("aave_v4_multichain_borrowing", ChainId("eip155:4663")),
    ],
)
def test_network_identity_does_not_grant_connector_execution_authority(name, chain):
    network = evm_network(chain)
    assert network.native_asset == AssetType(f"{chain}/slip44:60")
    assert network.wrapped_native.chain == chain
    with pytest.raises(ValueError, match="does not support"):
        connector_for(production_config(name)).support_for(chain)


@pytest.mark.parametrize("name", ["aave_v4_multichain_borrowing", "uniswap_v4_avalanche_lp"])
def test_support_enumerates_modes_actions_and_profiles_without_authority(name):
    connector = connector_for(production_config(name))
    if name.startswith("uniswap"):
        expected = (
            ChainId("eip155:1"),
            ChainId("eip155:43114"),
            ChainId("eip155:8453"),
            ChainId("eip155:42161"),
            ChainId("eip155:10"),
            ChainId("eip155:56"),
            ChainId("eip155:137"),
            ChainId("eip155:4663"),
        )
        production = expected
    else:
        production = (ChainId("eip155:1"), ChainId("eip155:43114"))
        expected = production
    assert connector.supported_chains(RunMode.FORK) == expected
    assert connector.supported_chains(RunMode.PRODUCTION) == production
    assert connector.supported_chains(RunMode.BACKTEST) == expected
    for chain in connector.supported_chains(RunMode.FORK):
        support = connector.support_for(chain)
        for mode in (RunMode.FORK, RunMode.PRODUCTION):
            if chain not in production and mode == RunMode.PRODUCTION:
                with pytest.raises(ValueError, match="unsupported"):
                    support.require(mode)
                continue
            for action in support.actions:
                for profile in support.account_profiles:
                    support.require(mode, action, profile)
        with pytest.raises(ValidationError, match="frozen"):
            support.chain = ChainId("eip155:1")
        with pytest.raises(ValueError, match="account profile"):
            support.require(RunMode.FORK, account_profile="owner_key")
        with pytest.raises(ValueError, match="unsupported"):
            support.require(RunMode.FORK, action="arbitrary_call")
    for chain in expected:
        support = connector.support_for(chain)
        with pytest.raises(ValueError, match="historical model"):
            support.require(RunMode.BACKTEST)
        for historical in support.historical:
            for action in historical.actions:
                support.require(RunMode.BACKTEST, action, model=historical.model)
    with pytest.raises(ValueError, match="does not support"):
        connector.support_for(ChainId("eip155:31337"))
    with pytest.raises(ValueError, match="validated"):
        connector.support_for("eip155:1")
    with pytest.raises(ValueError, match="typed"):
        connector.supported_chains("fork")


def test_historical_metadata_does_not_infer_coverage_from_an_omitted_start():
    connector = connector_for(production_config())
    support = connector.support_for(ChainId("eip155:1"))
    detailed, economic = support.historical
    with pytest.raises(ValueError, match="unsupported"):
        support.require(RunMode.BACKTEST, EconomicAction.BORROW, model=detailed.model)
    support.require(RunMode.BACKTEST, EconomicAction.BORROW, model=economic.model)
    historical = detailed.model_copy(update={"backtest_start_at": None})
    declared = support.model_copy(update={"historical": (historical,)})
    from almanak.connectors.support import historical_capability

    capability = historical_capability(
        declared,
        Chain(chain_id=declared.chain, environment="mainnet"),
        ConnectorId("aave_v4"),
        historical.model,
    )
    assert capability.backtest_start_at is None and "borrow" not in capability.actions
    assert capability.model == historical.model


def test_gateway_independently_refuses_unsupported_actions_and_wrong_routes(monkeypatch):
    config = production_config()
    target = config.target(config.allocations[0].name)
    operation = SimpleNamespace(target=target, account=target.account)
    state = SimpleNamespace(decision=SimpleNamespace(intent=SimpleNamespace(kind="borrow")))
    connector = connector_for(config)
    support = tuple(
        s.model_copy(update={"actions": (EconomicAction.SUPPLY,)}) for s in connector.chain_support
    )

    def configured_target(selected):
        assert selected == target
        return replace(connector, chain_support=support)

    monkeypatch.setattr("almanak.connectors.target_routing.connector_for_target", configured_target)
    backend = SimpleNamespace(
        config=config,
        guard=lambda: None,
        reader=SimpleNamespace(chain=target.fund.chain),
        connector=connector,
    )
    with pytest.raises(ValueError, match="borrow is unsupported"):
        EvmGateway.validate_operation(backend, state, operation, None)
    backend.reader.chain = config.target(config.allocations[1].name).fund.chain
    with pytest.raises(ValueError, match="different execution route"):
        EvmGateway.validate_operation(backend, state, operation, None)


def test_same_address_has_independent_native_identity_and_payer_budget():
    config = production_config()
    first, second = config.accounts.bindings
    assert first.profile.fund.address == second.profile.fund.address
    assert first.profile.submitter != second.profile.submitter
    operation = SimpleNamespace(account=second.name, kind="withdraw")
    policy = config.production.policy(second.name)
    state = SimpleNamespace(
        config=config,
        pending=None,
        outcomes=(
            SimpleNamespace(
                operation=SimpleNamespace(account=first.name),
                gas_paid=AssetAmount(
                    asset=config.production.policy(first.name).asset, amount=10**24
                ),
            ),
        ),
    )
    # The other chain's expenditure cannot consume this payer's native budget.
    validate_budget(state, operation, policy.total_budget, 21000, 1)
    assert evm_network(ChainId("eip155:43114")).native_asset == AssetType(
        "eip155:43114/slip44:9005"
    )


def test_family_references_preserve_case_and_require_evm_hashes():
    chain = Chain(chain_id=ChainId("testing:CaseSensitive"), environment="fixture")
    reference = ChainReference("Ledger-Checkpoint-A")
    assert CheckpointReference(chain=chain, reference=reference, timestamp=1).height is None
    assert TransactionReference(chain=chain, reference=reference).reference == reference
    assert (
        TransactionReference(chain=chain, reference=ChainReference("Ledger-Checkpoint-a")).reference
        != reference
    )
    evm = Chain(chain_id=ChainId("eip155:43114"), environment="mainnet")
    with pytest.raises(ValidationError, match="canonical 32-byte hash"):
        TransactionReference(chain=evm, reference=reference)
    with pytest.raises(ValidationError, match="block height"):
        CheckpointReference(chain=evm, reference=ChainReference("0x" + "12" * 32), timestamp=1)


def test_declared_dependencies_cannot_substitute_other_chain_observations():
    config = production_config()
    first, second = (config.target(a.name) for a in config.allocations)
    required = ObservationScope(
        chain=second.fund.chain, account=second.account, allocation=second.allocation
    )
    decision = SimpleNamespace(
        observations=(SimpleNamespace(target=first),), required_observations=(required,)
    )
    with pytest.raises(ValueError, match="lack required observations"):
        validate_dependencies(decision)
    decision.observations += (SimpleNamespace(target=second),)
    validate_dependencies(decision)
