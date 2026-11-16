import hashlib
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from almanak.caip import ChainId
from almanak.chains import ChainReference, CheckpointReference
from almanak.configuration.resolution import (
    AccountRequirement,
    AnchorSelection,
    NamedMarket,
    ReadContext,
    ResolutionAnchors,
)
from almanak.connectors.aave_v4.resolution import MarketSelection
from almanak.connectors.support import RunMode
from almanak.gateway.operator_config import (
    ALCHEMY_NETWORKS,
    AlchemyAccess,
    OperatorConfig,
    OperatorNetwork,
    read_operator_config,
)
from almanak.gateway.providers import provider_endpoint
from almanak.models import Chain, StrategyAccounts

ETH = ChainId("eip155:1")


@pytest.fixture
def operator():
    accounts = StrategyAccounts.model_validate_json(
        json.dumps(
            {
                "bindings": [
                    {
                        "name": "Main",
                        "profile": {
                            "kind": "eoa",
                            "fund": {
                                "chain": {"chain_id": "eip155:1", "environment": "mainnet"},
                                "address": "0x" + "12" * 20,
                            },
                        },
                    }
                ]
            }
        )
    )
    return OperatorConfig(
        modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
        networks=(
            OperatorNetwork(
                chain=ETH, provider=AlchemyAccess(kind="alchemy", api_key_env="RPC_KEY")
            ),
        ),
        accounts=accounts,
    )


@pytest.fixture
def context(operator):
    return ReadContext(
        identity=uuid4().hex,
        authored_digest="a" * 64,
        operator_digest=operator.digest,
        mode=RunMode.FORK,
        accounts=(AccountRequirement(name="Main", chain=ETH),),
        markets=(
            NamedMarket(
                name="weth",
                selection=MarketSelection(
                    connector="aave_v4", chain=ETH, spoke="0x" + "34" * 20, reserve_id=0
                ),
            ),
        ),
        anchors=(AnchorSelection(chain=ETH, height=123),),
    )


def revalidate(model, **changes):
    return type(model).model_validate({**model.model_dump(), **changes})


def test_operator_yaml_and_generated_context_round_trip(tmp_path, operator, context):
    path = tmp_path / "gateway.yaml"
    path.write_text(operator.model_dump_json())
    assert read_operator_config(path) == operator
    assert ReadContext.model_validate_json(context.model_dump_json()) == context
    assert context.match_operator(operator, RunMode.FORK) == operator.accounts
    assert len(operator.digest) == len(context.digest) == 64


def test_empty_vendor_access_preserves_the_historical_operator_digest(operator):
    wire = operator.model_dump(mode="json")
    assert "vendors" not in wire
    encoded = json.dumps(wire, sort_keys=True, separators=(",", ":"))
    assert operator.digest == hashlib.sha256(encoded.encode()).hexdigest()


@pytest.mark.parametrize("chain,slug", ALCHEMY_NETWORKS.items())
def test_provider_network_is_derived_without_key_in_identity(tmp_path, chain, slug):
    network = OperatorNetwork(
        chain=chain, provider=AlchemyAccess(kind="alchemy", api_key_env="RPC_KEY")
    )
    env = tmp_path / "operator.env"
    env.write_text("RPC_KEY=test-key/$literal\n")
    assert provider_endpoint(network.binding(), env) == (
        f"https://{slug}.g.alchemy.com/v2/test-key%2F%24literal"
    )
    assert "test-key" not in network.model_dump_json()
    with pytest.raises(ValidationError):
        revalidate(network, provider={**network.provider.model_dump(), "network": "other"})


def test_unknown_alchemy_chain_and_missing_key_refuse(tmp_path, operator):
    with pytest.raises(ValidationError, match="no Alchemy network"):
        revalidate(operator.networks[0], chain=ChainId("eip155:999999"))
    env = tmp_path / "operator.env"
    env.write_text("UNRELATED=secret\n")
    with pytest.raises(ValueError, match="RPC_KEY is required"):
        provider_endpoint(operator.networks[0].binding(), env)
    assert "secret" not in operator.model_dump_json()


@pytest.mark.parametrize("field", ["modes", "networks"])
def test_operator_duplicate_scope_refuses(operator, field):
    with pytest.raises(ValidationError, match="must be unique"):
        revalidate(operator, **{field: getattr(operator, field) * 2})


@pytest.mark.parametrize("fault", ["mode", "digest", "disabled", "missing_account", "chain"])
def test_read_context_must_match_selected_operator(operator, context, fault):
    if fault == "mode":
        mode = RunMode.PRODUCTION
    else:
        mode = RunMode.FORK
    if fault == "digest":
        context = revalidate(context, operator_digest="b" * 64)
    if fault == "disabled":
        operator = revalidate(operator, modes=(RunMode.BACKTEST,))
        context = revalidate(context, operator_digest=operator.digest)
    if fault == "missing_account":
        context = revalidate(context, accounts=(AccountRequirement(name="Other", chain=ETH),))
    if fault == "chain":
        chain = ChainId("eip155:43114")
        context = revalidate(
            context,
            accounts=(AccountRequirement(name="Main", chain=chain),),
            anchors=(AnchorSelection(chain=chain, fresh=True),),
            markets=(
                revalidate(
                    context.markets[0],
                    selection=revalidate(context.markets[0].selection, chain=chain),
                ),
            ),
        )
    with pytest.raises(ValueError):
        context.match_operator(operator, mode)


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"fresh": True, "height": 0},
        {"height": -1},
        {"height": True},
        {"reference": ChainReference("invalid")},
        {"at": 5, "fresh": True},
        {"at": 5, "height": 1},
        {"at": -1},
    ],
)
def test_invalid_anchor_selections_refuse(fields):
    with pytest.raises(ValidationError):
        AnchorSelection(chain=ETH, **fields)


def test_time_anchor_selects_the_last_block_at_or_before_the_time():
    selection = AnchorSelection(chain=ETH, at=500)
    assert "at" not in AnchorSelection(chain=ETH, height=1).model_dump(mode="json")
    assert AnchorSelection.model_validate_json(selection.model_dump_json()) == selection
    checkpoint = CheckpointReference(
        chain=Chain(chain_id=ETH, environment="mainnet"),
        height=7,
        reference=ChainReference("0x" + "12" * 32),
        timestamp=500,
    )
    selection.require(checkpoint)
    with pytest.raises(ValueError, match="later than the selected time"):
        selection.require(revalidate(checkpoint, timestamp=501))


def test_backtest_context_binds_simulation_accounts_without_operator_bindings(operator, context):
    from almanak.gateway.bootstrap import ReadBinding
    from scripts.legacy_configuration.backtest import adapter_account

    operator = revalidate(
        operator, modes=(RunMode.BACKTEST,), accounts=StrategyAccounts(bindings=())
    )
    context = revalidate(
        context,
        mode=RunMode.BACKTEST,
        operator_digest=operator.digest,
        accounts=(
            AccountRequirement(name="Main", chain=ETH),
            AccountRequirement(name="Other", chain=ETH),
        ),
    )
    accounts = context.match_operator(operator, RunMode.BACKTEST)
    assert [b.name for b in accounts.bindings] == ["Main", "Other"]
    for binding in accounts.bindings:
        expected = adapter_account(binding.name, {"chain_id": "eip155:1", "environment": "mainnet"})
        assert binding.profile.kind == "eoa"
        assert binding.profile.fund.address.lower() == expected["address"]
        assert binding.profile.fund.chain.environment == "mainnet"
    ReadBinding(
        context_digest=context.digest,
        operator_digest=operator.digest,
        mode=RunMode.BACKTEST,
        accounts=accounts,
    ).require(context)
    disabled = revalidate(operator, modes=(RunMode.FORK,))
    with pytest.raises(ValueError, match="does not enable"):
        revalidate(context, operator_digest=disabled.digest).match_operator(
            disabled, RunMode.BACKTEST
        )
    # Only backtests synthesize identities; every other mode needs operator bindings.
    with pytest.raises(ValueError, match="binding Main is missing"):
        revalidate(context, mode=RunMode.FORK, operator_digest=disabled.digest).match_operator(
            disabled, RunMode.FORK
        )


def test_retained_anchors_require_exact_scope_and_assertions(context):
    reference = ChainReference("0x" + "12" * 32)
    context = revalidate(
        context, anchors=(AnchorSelection(chain=ETH, height=123, reference=reference),)
    )
    checkpoint = CheckpointReference(
        chain=Chain(chain_id=ETH, environment="mainnet"),
        height=123,
        reference=reference,
        timestamp=456,
    )
    anchors = ResolutionAnchors(context_digest=context.digest, checkpoints=(checkpoint,))
    anchors.require(context)
    assert anchors.checkpoint(ETH) == checkpoint
    with pytest.raises(ValueError, match="another read context"):
        anchors.require(revalidate(context, identity=uuid4().hex))
    for changes in (
        {"height": 124},
        {"reference": ChainReference("0x" + "34" * 32)},
        {"chain": Chain(chain_id=ETH, environment="anvil:test")},
    ):
        wrong = revalidate(anchors, checkpoints=(revalidate(checkpoint, **changes),))
        with pytest.raises(ValueError):
            wrong.require(context)


@pytest.mark.parametrize("field", ["accounts", "markets", "anchors"])
def test_context_duplicate_names_or_chains_refuse(context, field):
    with pytest.raises(ValidationError):
        revalidate(context, **{field: getattr(context, field) * 2})


@pytest.mark.parametrize("fault", ["simulation", "fork", "missing_network"])
def test_operator_origin_bindings_are_explicit(operator, fault):
    raw = operator.model_dump(mode="json")
    profile = raw["accounts"]["bindings"][0]["profile"]
    if fault == "simulation":
        profile["kind"] = "simulation"
    elif fault == "fork":
        profile["fund"]["chain"]["environment"] = "anvil:test"
    else:
        profile["fund"]["chain"]["chain_id"] = "eip155:43114"
    with pytest.raises(ValidationError):
        OperatorConfig.model_validate_json(json.dumps(raw))


def test_market_support_is_declared_by_the_market_connector(operator, context):
    optimism = ChainId("eip155:10")
    binding = operator.accounts.bindings[0]
    fund = revalidate(binding.profile.fund, chain=Chain(chain_id=optimism, environment="mainnet"))
    operator = revalidate(
        operator,
        networks=(
            *operator.networks,
            OperatorNetwork(
                chain=optimism, provider=AlchemyAccess(kind="alchemy", api_key_env="RPC_KEY")
            ),
        ),
        accounts=StrategyAccounts(
            bindings=(revalidate(binding, profile=revalidate(binding.profile, fund=fund)),)
        ),
    )
    context = revalidate(
        context,
        operator_digest=operator.digest,
        accounts=(AccountRequirement(name="Main", chain=optimism),),
        anchors=(AnchorSelection(chain=optimism, fresh=True),),
        markets=(
            revalidate(
                context.markets[0],
                selection=revalidate(context.markets[0].selection, chain=optimism),
            ),
        ),
    )
    with pytest.raises(ValueError, match="connector does not support eip155:10"):
        context.match_operator(operator, RunMode.FORK)
