"""The actual Aave reference resolves and checks with unchanged economic choices."""

import gzip
import json
import lzma
import secrets
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from eth_abi import encode

from almanak.caip import AssetType
from almanak.checks import CheckFailure
from almanak.configuration.bootstrap import read_progress, resolve_configuration
from almanak.configuration.parameter_codec import origin_scope
from almanak.configuration.parameters import construct_parameters, gas_policies
from almanak.configuration.preparation import check_preparation
from almanak.configuration.resolution import AnchorSelection
from almanak.configuration.unresolved import parse_inputs, public_root, read_inputs
from almanak.configuration.yaml import ConfigurationError, load, parse
from almanak.connectors.aave_v4.models import LendingParameters
from almanak.connectors.composition import author_configuration_from_json
from almanak.connectors.support import RunMode
from almanak.gateway.ethereum import DataError
from almanak.gateway.operator_config import OperatorConfig, OperatorNetwork
from almanak.gateway.production_config import GasPolicy
from almanak.gateway.providers import UrlProvider
from almanak.models import Block, StrategyAccounts
from almanak.strategy import StrategyDefinition
from scripts.legacy_configuration.author import read_author
from tests.test_aave_resolution import selector
from tests.test_bootstrap_gateway import connected, symbol_read
from tests.test_bootstrap_gateway import rpc as rpc

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies/aave_v4_weth_supply"
FIXTURES = ROOT / "tests/fixtures"
FROZEN = FIXTURES / "author/aave_v4_weth_supply"
# The frozen effective fixture predates author asset metadata; these are the
# reference's declared precisions.
ASSET_METADATA = {
    "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"decimals": 6},
    "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"decimals": 18},
    "eip155:1/slip44:60": {"decimals": 18},
}


def frozen_fork():
    """The reference's frozen effective fork configuration, independent of its YAML."""
    raw = json.loads((FROZEN / "config.json").read_text())
    return author_configuration_from_json(json.dumps({**raw, "asset_metadata": ASSET_METADATA}))


def frozen_production_gas():
    raw = json.loads((FROZEN / "production.example.json").read_text())
    return tuple(
        {**policy, "name": name}
        for name, policy in zip(("treasury_payer",), raw["production"]["gas"], strict=True)
    )


def parsed(raw):
    raw = json.loads(json.dumps(raw))
    return parse_inputs(parse(yaml.safe_dump(raw, sort_keys=False), file="config.yaml"))


LEGACY = FROZEN / "legacy"
# The frozen fixtures and archives decide every 5 seconds; the shipped reference decides
# at a live interval. Tests comparing the two run the reference at the frozen cadence.
FROZEN_INTERVAL = "5s"


def frozen_cadence(raw):
    """The reference as the frozen snapshot saw it: at its cadence, without the example
    gas policy the shipped file added later. Cases that need gas author their own."""
    raw["strategy"]["decision_interval"] = FROZEN_INTERVAL
    raw["strategy"].pop("gas", None)
    return raw


def frozen_cadence_reference(directory) -> Path:
    """The shipped reference config at the frozen cadence, written under `directory`."""
    path = Path(directory) / "frozen-cadence" / "config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = frozen_cadence(load(REFERENCE / "config.yaml").value)
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


def gasless_reference(directory) -> Path:
    """The shipped reference config without its example gas policy, written under
    `directory`, for the captured fork fixtures, which state no gas ceilings."""
    path = Path(directory) / "gasless" / "config.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = load(REFERENCE / "config.yaml").value
    raw["strategy"].pop("gas")
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


@pytest.fixture
def raw():
    return frozen_cadence(load(REFERENCE / "config.yaml").value)


@pytest.fixture(scope="module")
def archive(tmp_path_factory):
    path = tmp_path_factory.mktemp("resolution-archive") / "archive.sqlite"
    path.write_bytes(
        lzma.decompress(
            (FIXTURES / "aave-v4-ethereum-reference-economic-archive.sqlite.xz").read_bytes()
        )
    )
    with sqlite3.connect(path) as db:
        rows = [json.loads(row[0]) for row in db.execute("SELECT body FROM observations")]
    fork = frozen_fork()
    sources = tuple(dict.fromkeys(a.reserve for a in (*fork.allocations, *fork.debts)))
    hashes = []
    for source in sources:
        data = selector("getReserve(uint256)") + f"{source.reserve_id:064x}"
        hashes.append(
            {
                row["request"]["params"][1]["blockHash"]
                for row in rows
                if row["request"]["method"] == "eth_call"
                and row["request"]["params"][0] == {"to": source.spoke, "data": data}
            }
        )
    common = set.intersection(*hashes)
    assert len(common) == 1
    identity = next(iter(common))
    header = next(
        r["result"]
        for r in rows
        if r["request"]["method"] == "eth_getBlockByNumber" and r["result"]["hash"] == identity
    )
    block = Block(
        chain=sources[0].chain,
        number=int(header["number"], 16),
        hash=identity,
        timestamp=int(header["timestamp"], 16),
    )
    return sources[0], block, rows


@pytest.fixture
def setup(tmp_path, archive, rpc):
    profiles = []
    for index, filename in enumerate(
        (
            "aave_v4_multichain_borrowing-lifecycle.json.gz",
            "aave-v4-composed-ethereum-initialized.json.gz",
        )
    ):
        capture = json.loads(gzip.decompress((FIXTURES / filename).read_bytes()))
        binding = next(
            b
            for b in capture["config"]["accounts"]["bindings"]
            if b["profile"]["fund"]["chain"]["chain_id"] == "eip155:1"
        )
        profiles.append(origin_scope({**binding, "name": f"lending_{index}"}))
    accounts = StrategyAccounts.model_validate_json(json.dumps({"bindings": profiles}))
    operator = OperatorConfig(
        modes=(RunMode.FORK, RunMode.PRODUCTION),
        accounts=accounts,
        networks=(
            OperatorNetwork(
                chain=archive[0].chain.chain_id,
                provider=UrlProvider(kind="rpc_url", url_env="ARCHIVE_RPC_URL"),
            ),
        ),
    )
    token = tmp_path / "token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    env = tmp_path / "operator.env"
    env.write_text(f"ARCHIVE_RPC_URL={rpc.endpoint}\nUNRELATED=hidden-input-secret\n")
    return SimpleNamespace(
        operator=operator, env_file=env, token_file=token, root=tmp_path, block=archive[1], rpc=rpc
    )


def resolve(document, setup, mode=RunMode.FORK):
    setup.context = document.inputs.context(
        setup.operator,
        mode,
        (AnchorSelection(chain=setup.block.chain.chain_id, height=setup.block.number),),
    )
    path = setup.root / f"resolution-{uuid4().hex}.json"
    with connected(setup, mode=mode) as client:
        resolve_configuration(path, setup.context, client)
    return read_progress(path)


def frozen_parameters(accounts: StrategyAccounts) -> LendingParameters:
    wire = frozen_fork().model_dump(mode="json")
    return LendingParameters.model_validate_json(
        json.dumps(
            {
                **{k: wire[k] for k in ("decision_trigger", "allocations", "debts")},
                "parameters": wire.get("parameters", {}),
                "asset_metadata": ASSET_METADATA,
                "accounts": accounts.model_dump(mode="json"),
            }
        )
    )


def expected_parameters(setup):
    return frozen_parameters(setup.operator.accounts)


def test_actual_reference_preserves_complete_parameters_and_sandboxed_capabilities(raw, setup):
    from almanak.runtime import strategy_capabilities

    document = parsed(raw)
    progress = resolve(document, setup)
    before_calls = len(setup.rpc.calls)
    parameters = construct_parameters(document, progress)
    assert parameters == expected_parameters(setup)
    assert [a.capital for a in parameters.allocations] == [10**16] * 4
    definition = StrategyDefinition.read(REFERENCE / "strategy.py")
    report = check_preparation(document, definition, progress, normalized=True)
    expected = strategy_capabilities(definition, expected_parameters(setup))
    assert report["capabilities"] == expected.model_dump(mode="json")
    assert report["dependencies"] == {"assets": [], "markets": [], "prices": []}
    assert report["normalized"]["parameters"] == parameters.model_dump(mode="json")
    assert report["valid"] and not report["ready"]
    assert len(setup.rpc.calls) == before_calls
    assert "hidden-input-secret" not in json.dumps(report)
    assert len(report["capabilities"]["requirements"]) == 6


def test_offline_input_check_never_runs_hooks_or_guesses_parameters(raw):
    document = parsed(raw)
    definition = StrategyDefinition(filename="strategy.py", source="raise AssertionError('loaded')")
    report = check_preparation(document, definition, normalized=True)
    states = {s["stage"]: s["status"] for s in report["stages"]}
    assert states["configuration"] == states["sandbox"] == "passed"
    for name in ("resolution", "parameters", "strategy", "capabilities", "data_dependencies"):
        assert states[name] == "not_checked"
    assert report["normalized"]["parameters"] is None
    assert report["authored_digest"] == document.inputs.digest
    assert report["strategy_digest"] == definition.sha256
    assert "context_digest" not in report
    assert not report["ready"]


def test_public_root_is_detected_by_shape_and_refused_by_the_harness_loader(raw, tmp_path):
    assert public_root(raw)
    assert public_root(load(REFERENCE / "config.production.yaml").value)
    assert not public_root(load(LEGACY / "config.yaml").value)
    assert not public_root(load(LEGACY / "config.production.yaml").value)
    assert not public_root({"strategy": {}, "markets": {}})
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    with pytest.raises(ConfigurationError, match="public author root") as error:
        read_author(path)
    assert error.value.file == str(path) and error.value.line
    assert read_author(LEGACY / "config.yaml").connector == "aave_v4"


def test_facts_digest_ignores_retrieval_times_and_context_identity(raw, setup):
    document = parsed(raw)
    result = resolve(document, setup).result
    later = result.model_copy(
        update={
            "markets": tuple(
                m.model_copy(
                    update={
                        "evidence": tuple(
                            e.model_copy(update={"retrieved_at": e.retrieved_at + 1})
                            for e in m.evidence
                        )
                    }
                )
                for m in result.markets
            )
        }
    )
    assert later.digest != result.digest
    assert later.facts_digest == result.facts_digest
    again = resolve(document, setup).result
    assert again.anchors.context_digest != result.anchors.context_digest
    assert again.facts_digest == result.facts_digest
    assert len(result.facts_digest) == 64


@pytest.mark.parametrize("value", [1, 0.01, True, "NaN", "1e3", {"base_units": -1}])
def test_quantity_syntax_is_checked_without_precision_or_provider(raw, value):
    raw["strategy"]["allocations"]["main_weth"]["capital"] = value
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert error.value.field.startswith("strategy.allocations.main_weth.capital")
    assert error.value.file == "config.yaml" and error.value.line is not None


@pytest.mark.parametrize("fault", ["market", "account", "chain", "collateral", "old_execution"])
def test_unknown_names_and_operational_inputs_refuse_offline(raw, fault):
    allocation = raw["strategy"]["allocations"]["main_weth"]
    if fault in ("market", "account"):
        allocation[fault] = "unknown"
    elif fault == "chain":
        raw["accounts"]["lending_0"]["chain"] = "eip155:43114"
    elif fault == "collateral":
        raw["strategy"]["debts"]["main_usdc"]["collateral"] = ["missing"]
    else:
        raw["execution"] = {}
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert error.value.file == "config.yaml"
    assert error.value.line is not None


def test_resolved_precision_error_names_the_author_allocation_and_line(raw, setup):
    raw["strategy"]["allocations"]["main_weth"]["capital"] = "0.0100000000000000001"
    document = parsed(raw)
    progress = resolve(document, setup)
    with pytest.raises(ConfigurationError) as error:
        construct_parameters(document, progress)
    assert error.value.field == "strategy.allocations.main_weth.capital"
    assert error.value.line == document.source.locations[error.value.field]
    assert "18 decimal places" in str(error.value)


@pytest.mark.parametrize(("group", "name"), [("allocations", "main_weth"), ("debts", "main_usdc")])
def test_retired_aave_v4_amount_is_refused_as_unknown(raw, group, name):
    raw["strategy"][group][name]["amount"] = "1"
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert error.value.field.startswith(f"strategy.{group}.{name}")


def test_parameter_changes_cannot_reuse_another_authored_context(raw, setup):
    document = parsed(raw)
    progress = resolve(document, setup)
    raw["strategy"]["allocations"]["main_weth"]["capital"] = "0.02"
    with pytest.raises(ConfigurationError, match="authored choices"):
        construct_parameters(parsed(raw), progress)


def test_optional_metadata_is_a_checked_assertion(raw, setup):
    asset = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    raw["assets"] = {asset: {"decimals": 18}}
    document = parsed(raw)
    progress = resolve(document, setup)
    with pytest.raises(ConfigurationError) as error:
        construct_parameters(document, progress)
    assert error.value.field == f"assets.{asset}.decimals"


def test_actual_production_author_gas_values_survive_resolution(setup):
    from almanak.configuration.parameter_codec import validation_parameters
    from almanak.connectors.aave_v4.models import LendingConfig

    raw = load(REFERENCE / "config.production.yaml").value
    binding = setup.operator.accounts.bindings[0].model_copy(update={"name": "treasury"})
    accounts = StrategyAccounts(bindings=(binding,))
    setup.operator = setup.operator.model_copy(update={"accounts": accounts})
    document = parsed(raw)
    progress = resolve(document, setup, RunMode.PRODUCTION)
    parameters = construct_parameters(document, progress)
    expected = tuple(
        GasPolicy.model_validate_json(json.dumps(policy)) for policy in frozen_production_gas()
    )
    assert parameters.gas == expected
    assert validation_parameters(parameters).gas == expected
    # A read result cannot become an executable config with an ignored gas request.
    with pytest.raises(ValueError):
        LendingConfig.model_validate(parameters.model_dump())
    report = check_preparation(
        document, StrategyDefinition.read(REFERENCE / "strategy.py"), progress
    )
    assert report["valid"] and not report["ready"]


def test_production_cannot_omit_author_gas_policy(raw, setup):
    document = parsed(raw)
    progress = resolve(document, setup, RunMode.PRODUCTION)
    with pytest.raises(ConfigurationError) as error:
        construct_parameters(document, progress)
    assert error.value.field == "strategy.gas"


def test_shared_payer_requires_one_scoped_policy_and_keeps_replacement_economics(raw, setup):
    original = load(REFERENCE / "config.production.yaml").value
    policy = json.loads(json.dumps(next(iter(original["strategy"]["gas"].values()))))
    policy["accounts"] = ["lending_0", "lending_1"]
    policy["replacement"] = {
        "interval_seconds": "30s",
        "minimum_bump_bps": 1200,
        "max_candidates": 3,
        "max_elapsed_seconds": "5m",
    }
    # A synthetic second Safe binding tests shared payer economics. This is not
    # evidence that the captured EOA has deployed Safe code or installed permissions.
    first, second = setup.operator.accounts.bindings
    profile = type(first.profile).model_validate(
        {
            **first.profile.model_dump(),
            "fund": second.profile.fund,
            "roles_module": "0x" + "34" * 20,
        }
    )
    accounts = StrategyAccounts(bindings=(first, second.model_copy(update={"profile": profile})))
    setup.operator = setup.operator.model_copy(update={"accounts": accounts})
    raw["strategy"]["gas"] = {"shared": policy}
    document = parsed(raw)
    fork_parameters = construct_parameters(document, resolve(document, setup))
    progress = resolve(document, setup, RunMode.PRODUCTION)
    parameters = construct_parameters(document, progress)
    assert fork_parameters.gas == parameters.gas
    assert parameters.gas[0].replacement.interval_seconds == 30
    assert parameters.gas[0].replacement.max_elapsed_seconds == 300
    assert parameters.gas[0].replacement.minimum_bump_bps == 1200
    separate = {
        name: {**policy, "accounts": [account]}
        for name, account in (("first", "lending_0"), ("second", "lending_1"))
    }
    raw["strategy"]["gas"] = separate
    document = parsed(raw)
    progress = resolve(document, setup)
    with pytest.raises(ConfigurationError, match="sharing a gas payer"):
        construct_parameters(document, progress)


def test_all_hooks_receive_the_same_authoritative_parameter_instance(raw, setup, monkeypatch):
    from almanak import checks

    document = parsed(raw)
    progress = resolve(document, setup)
    instances, validations = [], []
    worker = checks.strategy_worker
    capability = checks.strategy_capabilities

    def observed_worker(request):
        from almanak.strategy import ValidationRequest

        instances.append(request.config)
        if isinstance(request, ValidationRequest):
            validations.append(request)
        return worker(request)

    def observed_capability(definition, config):
        instances.append(config)
        return capability(definition, config)

    monkeypatch.setattr(checks, "strategy_worker", observed_worker)
    monkeypatch.setattr(checks, "strategy_capabilities", observed_capability)
    check_preparation(document, StrategyDefinition.read(REFERENCE / "strategy.py"), progress)
    assert len(validations) == 1
    assert len(instances) == 3
    assert all(item is instances[0] for item in instances)
    assert isinstance(instances[0], LendingParameters)


def test_tampered_resolution_facts_cannot_supply_metadata_to_hooks(raw, setup, monkeypatch):
    from almanak.gateway.ethereum import DataError

    document = parsed(raw)
    progress = resolve(document, setup)
    raw_progress = progress.model_dump(mode="json")
    raw_progress["result"]["markets"][0]["resolved"]["asset"]["decimals"] = 6
    from almanak.configuration.bootstrap import ResolutionProgress
    from almanak.gateway.bootstrap import ResolutionResult

    result = ResolutionResult.model_validate_json(json.dumps(raw_progress["result"]))
    raw_progress["result_digest"] = result.digest
    progress = ResolutionProgress.model_validate_json(json.dumps(raw_progress))
    monkeypatch.setattr("almanak.checks.check_validation", lambda *_: pytest.fail("hook ran"))
    with pytest.raises(CheckFailure, match="differ from retained reads") as failure:
        check_preparation(document, StrategyDefinition.read(REFERENCE / "strategy.py"), progress)
    assert isinstance(failure.value.__cause__, DataError)
    states = {s["stage"]: s["status"] for s in failure.value.diagnostic()["stages"]}
    assert states["parameters"] == "failed" and states["strategy"] == "not_checked"


def test_base_units_still_require_verified_resolution(raw):
    raw["strategy"]["allocations"]["main_weth"]["capital"] = {"base_units": 10**16}
    document = parsed(raw)
    result = check_preparation(document, StrategyDefinition.read(REFERENCE / "strategy.py"))
    assert not result["ready"]
    assert (
        next(s for s in result["stages"] if s["stage"] == "parameters")["status"] == "not_checked"
    )


@pytest.mark.parametrize("value", [[], "wrong", None])
def test_malformed_market_container_has_a_source_diagnostic(raw, value):
    raw["markets"] = value
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert error.value.field == "markets"
    assert error.value.line is not None


def test_positions_take_their_connector_from_the_named_market(raw, tmp_path):
    document = parsed(raw)
    allocation = document.inputs.strategy.allocations["main_weth"]
    assert allocation.connector == "aave_v4"
    dump = document.inputs.model_dump(mode="json")
    assert "connector" not in dump["strategy"]["allocations"]["main_weth"]
    assert "connector" not in dump["strategy"]["debts"]["main_usdc"]
    assert parsed(dump).inputs == document.inputs
    authored = json.loads(json.dumps(raw))
    authored["strategy"]["allocations"]["main_weth"]["connector"] = "aave_v4"
    with pytest.raises(ConfigurationError, match="comes from its named market") as error:
        parsed(authored)
    assert error.value.field == "strategy.allocations.main_weth.connector"
    assert error.value.line


@pytest.mark.parametrize("value", [1, [], {}, None])
def test_malformed_position_market_reference_has_a_source_diagnostic(raw, value):
    raw["strategy"]["allocations"]["main_weth"]["market"] = value
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    detail = error.value.diagnostic()
    assert detail["code"] == "invalid_named_reference"
    assert detail["field"] == "strategy.allocations.main_weth.market"
    assert detail["line"] is not None
    assert "must be a string" in detail["message"]


def test_markets_of_an_unknown_connector_are_refused_with_a_suggestion(raw):
    """Every canonical connector is bound, so only an unknown name refuses here."""
    raw["markets"]["main_weth_market"]["connector"] = "aave_v5"
    with pytest.raises(ConfigurationError, match="Unknown connector") as unknown:
        parsed(raw)
    assert unknown.value.field == "markets.main_weth_market.connector"
    assert unknown.value.code == "unknown_connector" and unknown.value.line


@pytest.mark.parametrize(
    ("field", "wrong", "suggested"),
    [
        ("market", "main_weth_markt", "main_weth_market"),
        ("account", "lendng_0", "lending_0"),
    ],
)
def test_unknown_local_targets_name_configured_choices_without_substitution(
    raw, field, wrong, suggested
):
    allocation = raw["strategy"]["allocations"]["main_weth"]
    allocation[field] = wrong
    with pytest.raises(ConfigurationError) as failure:
        parsed(raw)
    detail = failure.value.diagnostic()
    assert detail["code"] == "unknown_named_reference"
    assert detail["field"] == f"strategy.allocations.main_weth.{field}"
    assert f"Did you mean '{suggested}'?" in detail["message"]
    assert "suggestions never change the selection" in detail["remedy"]


@pytest.mark.parametrize("fault", ["unknown_account", "foreign_asset", "duplicate", "valid"])
def test_inventory_names_owned_cash_per_account_on_its_chain(raw, fault):
    weth = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    entry = {"account": "lending_0", "balance": {"asset": weth, "quantity": "0.5"}}
    raw["inventory"] = [entry]
    if fault == "valid":
        inputs = parsed(raw).inputs
        assert len(inputs.inventory) == 1 and "inventory" in inputs.model_dump(mode="json")
        shipped = read_inputs(REFERENCE / "config.yaml").inputs
        assert "inventory" not in shipped.model_dump(mode="json")
        assert shipped.digest == "a6427a66d1fe8cf8237121c9cdf144298cae6eb660b801df43d7298d947c00e4"
        return
    if fault == "unknown_account":
        entry["account"] = "treasury"
        field = "inventory.0.account"
    elif fault == "foreign_asset":
        entry["balance"]["asset"] = "eip155:8453/erc20:0x4200000000000000000000000000000000000006"
        field = "inventory.0.balance.asset"
    else:
        raw["inventory"].append(dict(entry))
        field = "inventory.1"
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert error.value.field == field


def test_undeclared_market_chains_are_refused_offline_with_the_support_message(raw):
    for market in raw["markets"].values():
        market["chain"] = "eip155:56"
    for account in raw["accounts"].values():
        account["chain"] = "eip155:56"
    with pytest.raises(ConfigurationError) as error:
        parsed(raw)
    assert str(error.value) == "connector does not support eip155:56"
    assert error.value.field == "markets.main_weth_market.chain"


def test_gas_policies_resolve_in_every_mode_and_production_requires_them():
    policy = SimpleNamespace(resolve=lambda name: ("resolved", name))
    authored = SimpleNamespace(
        inputs=SimpleNamespace(strategy=SimpleNamespace(gas={"payer": policy})),
        error=lambda error, field=None: error,
    )
    for mode in RunMode:
        assert gas_policies(authored, mode) == (("resolved", "payer"),)
    unpaid = SimpleNamespace(
        inputs=SimpleNamespace(strategy=SimpleNamespace(gas={})),
        error=lambda error, field=None: error,
    )
    assert gas_policies(unpaid, RunMode.FORK) == ()
    with pytest.raises(ConfigurationError, match="explicit author gas policy"):
        gas_policies(unpaid, RunMode.PRODUCTION)


USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")


def named(symbols):
    """Answer the archive's unanswered `symbol()` reads with each token's own name."""

    def mutation(body, reply):
        token = body["params"][0].get("to") if symbol_read(body) else None
        if token not in symbols:
            return reply
        value = "0x" + encode(["string"], [symbols[token]]).hex()
        return {"jsonrpc": "2.0", "id": body["id"], "result": value}

    return mutation


def test_resolved_assets_carry_token_symbols_outside_the_verified_facts(raw, setup):
    document = parsed(raw)
    unnamed = resolve(document, setup)
    assert all(not market.symbols for market in unnamed.result.markets)
    assert "symbols" not in unnamed.result.markets[0].model_dump()
    # Markup and overlong text is contract-controlled; such a token stays unnamed.
    setup.rpc.mutation = named({WETH.reference: " WETH ", USDC.reference: "<b>USDC</b>"})
    progress = resolve(document, setup)
    symbols = {a: s for market in progress.result.markets for a, s in market.symbols.items()}
    assert symbols == {WETH: "WETH"}
    assert progress.result.facts_digest == unnamed.result.facts_digest
    assert progress.result.digest != unnamed.result.digest
    metadata = construct_parameters(document, progress).asset_metadata
    assert (metadata[WETH].symbol, metadata[WETH].decimals) == ("WETH", 18)
    assert metadata[USDC].symbol is None
    raw["assets"] = {str(WETH): {"symbol": "Wrapped Ether"}, str(USDC): {"decimals": 6}}
    authored = parsed(raw)
    metadata = construct_parameters(authored, resolve(authored, setup)).asset_metadata
    assert metadata[WETH].symbol == "Wrapped Ether" and metadata[USDC].symbol is None


def test_symbols_that_differ_from_their_retained_reads_are_refused(raw, setup):
    setup.rpc.mutation = named({WETH.reference: "WETH"})
    document = parsed(raw)
    progress = resolve(document, setup)
    markets = tuple(
        market.model_copy(update={"symbols": {asset: "USDC" for asset in market.symbols}})
        for market in progress.result.markets
    )
    result = progress.result.model_copy(update={"markets": markets})
    forged = progress.model_copy(update={"result": result, "result_digest": result.digest})
    with pytest.raises(DataError, match="asset symbols differ from retained reads"):
        construct_parameters(document, forged)
