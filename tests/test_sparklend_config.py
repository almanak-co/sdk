"""Distinct Spark targets and capability codecs using shared scope-only Pool grants."""

import json

import pytest
from pydantic import TypeAdapter

from almanak.connectors.aave_v3.authority import Capabilities as AaveCapabilities
from almanak.connectors.aave_v3.models import Intent as AaveIntent
from almanak.connectors.sparklend.authority import admit, capability_codec, declared
from almanak.connectors.sparklend.models import Allocation, Intent, Parameters
from almanak.connectors.sparklend.permissions import derive
from almanak.gateway.evm import native_asset
from almanak.models import AccountBinding, AssetMetadata, Eoa, StrategyAccounts
from almanak.triggers import IntervalTrigger
from tests.lending_health_scope import check_health_floor_scope
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support
from tests.sparklend_helpers import recorded_spark as recorded_spark
from tests.test_aave_v3_permissions import by_asset, grant, profile, value
from tests.test_sparklend_risk import position


@pytest.fixture
def parameters():
    observations = (position(0), position(1))
    allocations = []
    for index, p in enumerate(observations):
        quote = p.reserve.oracle
        assert quote is not None
        allocations.append(
            Allocation(
                name="collateral" if index == 0 else "loan",
                account="fund",
                reserve=p.reserve.source,
                capital=10**18 if index == 0 else 0,
                borrow=index == 1,
                collateral=index == 0,
                minimum_health_factor=2 * 10**18,
                oracle=quote.oracle,
                price_source=quote.feed,
                native_asset=native_asset(p.fund.chain.chain_id),
                minimum_native_balance=0,
            )
        )
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        # WETH and USDC, which the recorded reserves hold.
        asset_metadata={
            observations[0].reserve.source.asset: AssetMetadata(decimals=18),
            observations[1].reserve.source.asset: AssetMetadata(decimals=6),
        },
        accounts=StrategyAccounts(
            bindings=(
                AccountBinding(
                    name="fund",
                    profile=Eoa(kind="eoa", fund=observations[0].fund),
                ),
            )
        ),
        allocations=tuple(allocations),
    )


def test_spark_helpers_and_capabilities_retain_their_concrete_protocol(parameters):
    capabilities = capability_codec().declared(parameters)
    intents = (
        parameters.supply("collateral", 10**18),
        parameters.borrow("loan", 1000 * 10**6),
        parameters.withdraw_amount("collateral", 10**17),
        parameters.withdraw("collateral"),
        parameters.repay_amount("loan", 500 * 10**6),
        parameters.repay("loan"),
        parameters.set_collateral("collateral", False),
    )
    for intent in intents:
        assert TypeAdapter(Intent).validate_json(intent.model_dump_json()) == intent
        assert intent.target.reserve.kind == "sparklend"
        admit(capabilities, intent)
        with pytest.raises(ValueError):
            TypeAdapter(AaveIntent).validate_json(intent.model_dump_json())
    with pytest.raises(ValueError):
        AaveCapabilities.model_validate_json(capabilities.model_dump_json())
    # Capital is not a limit: any amount stays within the allocation's own scope.
    admit(capabilities, parameters.borrow("loan", 10**30))
    admit(capabilities, parameters.supply("loan", 10**30))
    with pytest.raises(ValueError, match="require a borrowing allocation"):
        admit(capabilities, parameters.borrow("collateral", 1))


@pytest.mark.parametrize("fault", ["alias", "account", "oracle", "health"])
def test_spark_parameters_refuse_ambiguous_fund_pool_ownership(parameters, fault):
    first, second = parameters.allocations
    updates = {
        "alias": {"reserve": first.reserve},
        "account": {"account": "unbound"},
        "oracle": {"oracle": "0x" + "12" * 20},
        "health": {"minimum_health_factor": 3 * 10**18},
    }
    changed = parameters.model_copy(
        update={"allocations": (first, second.model_copy(update=updates[fault]))}
    )
    with pytest.raises(ValueError):
        Parameters.model_validate_json(changed.model_dump_json())


def test_spark_roles_grants_scope_pool_asset_and_fund_without_amounts(parameters):
    safe = profile(parameters)
    grants = derive(safe, declared(parameters))
    for operation, allocation, fund_index in (("supply", "collateral", 2), ("borrow", "loan", 4)):
        target = parameters.target(allocation)
        args = by_asset(grant(grants, operation))[target.asset.reference]
        assert grant(grants, operation).target == target.reserve.pool
        assert args[1].operator == 17 and value(args[1]) == 0
        assert value(args[fund_index], "address") == safe.fund.address
    (repay,) = by_asset(grant(grants, "repay")).values()
    assert value(repay[2]) == 2
    assert value(repay[3], "address") == safe.fund.address
    assert not {c.operator for g in grants for c in g.conditions} & {18, 28}


def test_candidate_reference_parses_under_explicit_fork_scope(candidate_spark_support):

    from almanak.connectors.composition import connector_for
    from scripts.legacy_configuration.author import read_author
    from tests.legacy_references import harness_directory

    path = harness_directory("sparklend_ethereum_lending") / "config.yaml"
    author = read_author(path)
    assert author.connector == author.fork_config().connector == "sparklend"
    binding = connector_for(author)
    assert [str(s.chain) for s in binding.chain_support] == ["eip155:1"]
    assert binding.economic is not None


def test_permission_identity_set_includes_stable_token_and_configured_oracle(
    recorded_spark, monkeypatch
):
    from types import SimpleNamespace

    from almanak.connectors.sparklend import permissions

    state = recorded_spark
    reserves = {o.target.reserve: o.position.reserve for o in state.baseline}
    block = state.baseline[0].block

    def read(reader, source, at):
        assert at == block
        return reserves[source]

    monkeypatch.setattr(permissions, "read_reserve", read)
    reader = SimpleNamespace(reader=object(), block=block, identity=lambda address: address)
    result = permissions.identities(state.config, "fund", reader)
    for reserve in reserves.values():
        assert reserve.stable_debt_token.reference in result
        assert reserve.variable_debt_token.reference in result
        assert reserve.a_token.reference in result
        assert reserve.oracle.oracle in result
        assert reserve.oracle.feed in result
    assert permissions.identities(state.config, "unbound", reader) == ()
    first = state.config.allocations[0]
    reserves[first.reserve] = reserves[first.reserve].model_copy(update={"oracle": None})
    with pytest.raises(ValueError, match="oracle source differs"):
        permissions.identities(state.config, "fund", reader)


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_retired_budget_fields_are_refused(parameters, field):
    wire = parameters.allocations[0].model_dump(mode="json")
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        Allocation.model_validate_json(json.dumps({**wire, field: 10**18}))


def test_minimum_health_factor_is_required_only_where_debt_is_possible(parameters):
    check_health_floor_scope(parameters)
