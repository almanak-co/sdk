"""The public check resolves current facts through a read-only gateway and nothing else."""

import json
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest
import yaml

from almanak import checks
from almanak.checks import CheckFailure, check
from almanak.configuration.resolution import AnchorSelection
from almanak.configuration.yaml import load
from almanak.connectors.support import RunMode
from almanak.gateway.operator_execution import ProductionAuthority
from almanak.models import StrategyAccounts
from tests.test_author_resolution import LEGACY, REFERENCE, frozen_production_gas
from tests.test_author_resolution import archive as archive
from tests.test_author_resolution import rpc as rpc
from tests.test_author_resolution import setup as setup
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import original as original
from tests.test_permissions import authority_trace as authority_trace
from tests.test_resolution_context import revalidate


def write_reference(directory, raw):
    directory.mkdir(exist_ok=True)
    (directory / "config.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))
    (directory / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    return directory


def arguments(directory, operator_path=None, env_file=None, **flags):
    return SimpleNamespace(
        directory=directory,
        config=None,
        strategy=None,
        fork=flags.pop("fork", False),
        production=flags.pop("production", False),
        gateway_config=operator_path,
        env_file=env_file,
        online=flags.pop("online", False),
        normalized=flags.pop("normalized", False),
        backtest_config=flags.pop("backtest_config", None),
        **flags,
    )


@pytest.fixture
def fork_case(setup, fixture, tmp_path):
    inputs = revalidate(
        fixture,
        anchors=(AnchorSelection(chain=setup.block.chain.chain_id, height=setup.block.number),),
    )
    setup.operator = revalidate(setup.operator, modes=(RunMode.FORK,), fork=inputs)
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(setup.operator.model_dump_json())
    raw = load(REFERENCE / "config.yaml").value
    # The captured fork fixture states no gas ceilings, so it runs the example gasless.
    raw["strategy"].pop("gas")
    directory = write_reference(tmp_path / "reference", raw)
    return SimpleNamespace(setup=setup, directory=directory, operator_path=operator_path)


@pytest.fixture
def production_case(setup, tmp_path, monkeypatch):
    # The captured archive holds no fee history; the policy is quoted at a fixed price.
    monkeypatch.setattr(checks.MarketReads, "gas_price", lambda self, policy: QUOTED_PRICE)
    monkeypatch.setattr(checks.MarketReads, "window_bid", lambda self, policy: QUOTED_PRICE)
    raw = load(REFERENCE / "config.production.yaml").value
    policy = frozen_production_gas()[0]
    ceiling = {
        key: policy[key]
        for key in ("accounts", "max_gas_price_wei", "max_gas_limit", "max_transaction_fee")
    }
    ceiling["total_budget"] = policy["total_budget"]
    authority = ProductionAuthority.model_validate_json(
        json.dumps(
            {
                "networks": [{"chain": "eip155:1", "max_observation_age_seconds": 120}],
                "payers": [{**ceiling, "allow_replacement": False}],
                "signing": {"key_file": "/private/never-opened-keystore"},
            }
        )
    )
    binding = revalidate(setup.operator.accounts.bindings[0], name="treasury")
    setup.operator = revalidate(
        setup.operator,
        modes=(RunMode.PRODUCTION,),
        accounts=StrategyAccounts(bindings=(binding,)),
        production=authority,
    )
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(setup.operator.model_dump_json())
    directory = write_reference(tmp_path / "reference", raw)
    return SimpleNamespace(
        setup=setup, directory=directory, operator_path=operator_path, authority=authority
    )


QUOTED_PRICE = 1_000_000_000


def states(report):
    return {s["stage"]: s["status"] for s in report["stages"]}


FUNDING_SCHEMA = json.loads(
    (Path(__file__).parents[1] / "docs/contracts/funding-v1.schema.json").read_text()
)


PERMISSIONS_SCHEMA = json.loads(
    (Path(__file__).parents[1] / "docs/contracts/permissions-v1.schema.json").read_text()
)


def funding(report):
    """The retained funding block, checked against its published schema."""
    block = report["funding"]
    jsonschema.validate(block, FUNDING_SCHEMA)
    assert block["parameters_digest"] == report.get("parameters_digest", block["parameters_digest"])
    return block


def test_offline_check_with_operator_binds_the_context_without_io(fork_case):
    case = fork_case
    report = check(arguments(case.directory, case.operator_path, case.setup.env_file, fork=True))
    assert states(report)["operator_configuration"] == "passed"
    assert states(report)["resolution"] == "not_checked"
    assert report["valid"] and not report["ready"]
    assert "funding" not in report
    assert not case.setup.rpc.calls


def test_online_fork_check_resolves_once_runs_hooks_once_and_previews_facts(fork_case, monkeypatch):
    case = fork_case
    validations = []
    worker = checks.strategy_worker

    def observed(request):
        validations.append(type(request).__name__)
        return worker(request)

    monkeypatch.setattr(checks, "strategy_worker", observed)
    report = check(
        arguments(case.directory, case.operator_path, case.setup.env_file, fork=True, online=True)
    )
    assert states(report) == {
        "configuration": "passed",
        "operator_configuration": "passed",
        "resolution": "passed",
        "parameters": "passed",
        "sandbox": "passed",
        "strategy": "passed",
        "capabilities": "passed",
        "data_dependencies": "passed",
        "pool_liquidity": "passed",
        "fixture": "passed",
        "fork_compatibility": "passed",
        "online_admission": "not_checked",
    }
    assert report["valid"] and not report["ready"]
    assert validations.count("ValidationRequest") == 1
    assert len(report["capabilities"]["requirements"]) == 6
    assert len(report["fixture_digest"]) == 64
    preview = report["preview"]
    assert [m["name"] for m in preview["markets"]] == [
        "main_weth_market",
        "bluechip_weth_market",
        "main_usdc_market",
    ]
    assert preview["markets"][0]["decimals"] == 18 and preview["markets"][2]["decimals"] == 6
    assert preview["anchors"][0]["height"] == case.setup.block.number
    assert [a["name"] for a in preview["accounts"]] == ["lending_0", "lending_1"]
    assert all(a["binding"] in ("eoa", "safe_roles") for a in preview["accounts"])
    assert len(report["resolution_facts"]) == 64
    assert "normalized" not in report
    block = funding(report)
    assert block["mode"] == "fork"
    assert [a["account"] for a in block["accounts"]] == ["lending_0", "lending_1"]
    # Each account supplies WETH; borrowed USDC carries no prefunded interest buffer.
    assert all(len(a["assets"]) == 1 for a in block["accounts"])
    for payer in block["payers"]:
        assert payer["gas"] is None
        assert payer["entry_minimum_base_units"] == payer["minimum_native_balance_base_units"]
    assert {p["payer"] for p in block["payers"]} == {a["payer"] for a in block["accounts"]}
    text = json.dumps(report)
    assert "hidden-input-secret" not in text and case.setup.rpc.endpoint not in text
    assert "retrieved_at" not in text


def test_online_check_reports_normalized_parameters(fork_case):
    case = fork_case
    report = check(
        arguments(
            case.directory,
            case.operator_path,
            case.setup.env_file,
            fork=True,
            online=True,
            normalized=True,
        )
    )
    parameters = report["normalized"]["parameters"]
    assert [a["capital"] for a in parameters["allocations"]] == [10**16] * 4
    assert report["normalized"]["authored"]["strategy"]["allocations"]["main_weth"]["capital"] == (
        "0.01"
    )


def test_online_production_check_admits_compatible_ceilings_and_refuses_lowered(
    production_case, safe_generation, tmp_path, monkeypatch
):
    case = production_case
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    assert states(report)["policy"] == "passed"
    assert states(report)["online_admission"] == "not_checked"
    assert report["ceilings"][0]["accounts"] == ["treasury"]
    assert states(report)["permissions"] == "passed"
    assert "never-opened-keystore" not in json.dumps(report)
    block = funding(report)
    (account,) = block["accounts"]
    assert account["account"] == "treasury" and account["assets"][0]["decimals"] == 18
    (payer,) = block["payers"]
    gas = payer["gas"]
    # Each of the two supply allocations approves and supplies: four fee ceilings,
    # then the reserve.
    assert payer["entry_transactions"] == 4
    reserve = 4 * int(gas["max_transaction_fee_base_units"]) + int(gas["exit_reserve_base_units"])
    assert payer["accounts"] == ["treasury"] and payer["payer"] == account["payer"]
    assert int(payer["entry_minimum_base_units"]) == max(
        reserve, int(payer["minimum_native_balance_base_units"])
    )
    lowered = json.loads(case.authority.model_dump_json())
    lowered["payers"][0]["max_gas_limit"] = 1
    authority = ProductionAuthority.model_validate_json(json.dumps(lowered))
    operator = revalidate(case.setup.operator, production=authority)
    lowered_path = tmp_path / "lowered.json"
    lowered_path.write_text(operator.model_dump_json())
    lowered_args = arguments(
        case.directory, lowered_path, case.setup.env_file, production=True, online=True
    )
    price = {"code": "gas_price", "field": "strategy.gas.x", "message": "x"}
    monkeypatch.setattr("almanak.checks.market_findings", lambda *_: [price])
    with pytest.raises(CheckFailure) as failure:
        check(lowered_args)
    assert states(failure.value.diagnostic())["policy"] == "failed"
    assert states(failure.value.diagnostic())["capabilities"] == "passed"
    assert failure.value.diagnostic()["funding"] == block
    assert price in failure.value.diagnostic()["warnings"]


@pytest.fixture
def safe_generation(authority_trace, monkeypatch):
    """Treat the production binding as a Safe and record what generation receives."""
    from almanak.connectors.aave_v4.permission_adapter import PermissionManifest
    from almanak.gateway import permissions, production_cli

    manifest = PermissionManifest.model_validate_json(json.dumps(authority_trace["manifest"]))
    calls: dict = {"closed": 0}

    def routes(config, env_file):
        calls["routes"] = (config, env_file)
        return SimpleNamespace(close=lambda: calls.update(closed=calls["closed"] + 1))

    def generate(config, definition, backend, *, connector, prepared):
        if "error" in calls:
            raise calls["error"]
        calls.update(config=config, definition=definition, prepared=prepared)
        return manifest

    monkeypatch.setattr("almanak.production_setup.requires_permissions", lambda _: True)
    monkeypatch.setattr(production_cli, "production_routes", routes)
    monkeypatch.setattr(permissions, "generate", generate)
    return SimpleNamespace(manifest=manifest, calls=calls, statement=permissions.statement)


def test_online_production_check_states_the_safe_permissions_admission_requires(
    production_case, safe_generation
):
    case, calls = production_case, safe_generation.calls
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    assert states(report)["policy"] == "passed" and states(report)["permissions"] == "passed"
    block = report["permissions"]
    jsonschema.validate(block, PERMISSIONS_SCHEMA)
    assert block == safe_generation.statement(safe_generation.manifest)
    # Generation reuses the hook's capabilities and the configuration provisioning freezes.
    assert calls["prepared"].capabilities.model_dump(mode="json") == report["capabilities"]
    assert calls["prepared"].parameters_digest == report["parameters_digest"]
    assert calls["config"].production is not None
    assert calls["routes"] == (calls["config"], case.setup.env_file) and calls["closed"] == 1
    assert calls["definition"].sha256 == report["strategy_digest"]
    funding(report)


def test_production_check_has_no_exit_quota_stage(production_case, monkeypatch):
    # Scope-only authority (blueprint 27) leaves no counter quota for an exit to exhaust.
    case = production_case
    monkeypatch.setattr("almanak.production_setup.requires_permissions", lambda _: False)
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    assert "exit_quota" not in states(report) and states(report)["policy"] == "passed"
    assert "warnings" not in report


def test_production_check_warns_when_the_price_ceiling_is_below_the_market_quote(
    production_case, monkeypatch
):
    # 6f654439: every transaction is refused as gas_price until the market falls.
    case = production_case
    monkeypatch.setattr(checks.MarketReads, "gas_price", lambda self, policy: 6 * 10**9)
    monkeypatch.setattr("almanak.production_setup.requires_permissions", lambda _: False)
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    [finding] = report["warnings"]
    assert finding["code"] == "gas_price"
    assert finding["field"] == "strategy.gas.treasury_payer.max_gas_price_wei"
    assert "is 6000000000 wei" in finding["message"]


def test_fork_check_reports_market_findings_as_warnings(fork_case, monkeypatch):
    case = fork_case
    price = {"code": "gas_price", "field": "strategy.gas.x", "message": "x"}
    monkeypatch.setattr("almanak.checks.market_findings", lambda *_: [price])
    report = check(
        arguments(case.directory, case.operator_path, case.setup.env_file, fork=True, online=True)
    )
    assert report["warnings"] == [price]
    assert "exit_quota" not in states(report) and states(report)["fixture"] == "passed"


def test_fork_check_refuses_a_token_the_fork_cannot_execute(fork_case):
    # A node-native token (code 0xef) resolves on mainnet but fails every
    # call on Anvil; the check refuses it before admission with its own error kind.
    case = fork_case
    usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def native(body, reply):
        if body["method"] == "eth_getCode" and body["params"][0].lower() == usdc:
            return {**reply, "result": "0xef"}
        return reply

    case.setup.rpc.mutation = native
    with pytest.raises(CheckFailure) as failure:
        check(
            arguments(
                case.directory, case.operator_path, case.setup.env_file, fork=True, online=True
            )
        )
    report = failure.value.diagnostic()
    assert states(report)["fixture"] == "passed"
    assert states(report)["fork_compatibility"] == "failed"
    assert states(report)["online_admission"] == "not_checked"
    assert report["code"] == "unsupported_on_fork" and report["field"] == "fork_compatibility"
    assert f"token {usdc} on eip155:1" in report["message"]
    assert "not a strategy fault" in report["message"]
    assert "export and deploy" in report["remedy"]
    assert case.setup.rpc.endpoint not in json.dumps(report)


def test_online_production_check_without_safe_accounts_reads_no_roles_authority(
    production_case, monkeypatch
):
    case = production_case
    monkeypatch.setattr("almanak.production_setup.requires_permissions", lambda _: False)
    monkeypatch.setattr(
        "almanak.gateway.production_cli.production_routes", lambda *_: pytest.fail("Roles read")
    )
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    assert states(report)["policy"] == "passed"
    assert "permissions" not in report and "permissions" not in states(report)


def test_refused_safe_authority_fails_the_permissions_stage_and_keeps_funding(
    production_case, safe_generation
):
    case, calls = production_case, safe_generation.calls
    calls["error"] = ValueError("Roles modifier is not enabled on the Safe")
    with pytest.raises(CheckFailure) as failure:
        check(
            arguments(
                case.directory,
                case.operator_path,
                case.setup.env_file,
                production=True,
                online=True,
            )
        )
    detail = failure.value.diagnostic()
    assert states(detail)["policy"] == "passed" and states(detail)["permissions"] == "failed"
    assert "Roles modifier is not enabled" in detail["message"]
    assert calls["closed"] == 1
    funding(detail)


@pytest.mark.parametrize("fault", ["no_operator", "no_env", "no_mode", "operator_without_mode"])
def test_online_and_operator_inputs_are_explicit(fork_case, fault):
    case = fork_case
    args = arguments(
        case.directory, case.operator_path, case.setup.env_file, fork=True, online=True
    )
    if fault == "no_operator":
        args.gateway_config = None
    elif fault == "no_env":
        args.env_file = None
    elif fault == "no_mode":
        args.fork = False
    else:
        args.fork, args.online = False, False
        with pytest.raises(CheckFailure) as failure:
            check(args)
        assert states(failure.value.diagnostic())["operator_configuration"] == "failed"
        assert not case.setup.rpc.calls
        return
    with pytest.raises(ValueError, match="online checking requires"):
        check(args)
    assert not case.setup.rpc.calls


def test_fork_policy_refusal_stops_check_before_fixture_or_provider(fork_case, monkeypatch):
    case = fork_case

    def refuse(*_):
        raise ValueError("operator fixture ceilings do not cover authored gas policy")

    monkeypatch.setattr("almanak.configuration.fork_preparation.require_fork_policy", refuse)
    with pytest.raises(CheckFailure) as failure:
        check(arguments(case.directory, case.operator_path, fork=True))
    report = failure.value.diagnostic()
    assert states(report)["operator_configuration"] == "failed"
    assert not report["valid"] and not report["ready"]
    assert not case.setup.rpc.calls


def test_offline_production_check_refuses_a_root_without_gas_policy(production_case, tmp_path):
    case = production_case
    report = check(arguments(case.directory, case.operator_path, production=True))
    assert states(report)["configuration"] == "passed"
    assert states(report)["operator_configuration"] == "passed"
    assert "warnings" not in report
    raw = load(REFERENCE / "config.production.yaml").value
    del raw["strategy"]["gas"]
    unpaid = write_reference(tmp_path / "unpaid", raw)
    for operator_path in (case.operator_path, None):
        with pytest.raises(CheckFailure) as failure:
            check(arguments(unpaid, operator_path, production=True))
        detail = failure.value.diagnostic()
        assert detail["field"] == "strategy.gas" and detail["line"]
        assert detail["message"] == "production requires explicit author gas policy"
        assert states(detail)["configuration"] == "failed"
        assert not detail["valid"] and not detail["ready"]
    assert not case.setup.rpc.calls


def test_modeless_check_warns_that_production_needs_a_gas_policy(tmp_path):
    raw = load(REFERENCE / "config.production.yaml").value
    raw["strategy"]["decision_interval"] = "30s"
    paid = check(arguments(write_reference(tmp_path / "paid", raw)))
    assert "warnings" not in paid
    del raw["strategy"]["gas"]
    report = check(arguments(write_reference(tmp_path / "unpaid", raw)))
    assert report["valid"] and not report["ready"]
    assert [w["field"] for w in report["warnings"]] == ["strategy.gas"]
    fork = check(arguments(tmp_path / "unpaid", fork=True))
    assert "warnings" not in fork


def test_production_checks_warn_about_decisions_closer_than_thirty_seconds(tmp_path):
    """Every live decision starts a sandboxed worker; 5 s starved the container."""
    raw = load(REFERENCE / "config.production.yaml").value
    raw["strategy"]["decision_interval"] = "29s"
    directory = write_reference(tmp_path / "short", raw)
    for report in (check(arguments(directory)), check(arguments(directory, production=True))):
        [finding] = [w for w in report["warnings"] if w["code"] == "decision_interval"]
        assert finding["field"] == "strategy.decision_interval"
        assert "29 seconds" in finding["message"] and "at least 30 seconds" in finding["message"]
        assert report["valid"]
    assert "warnings" not in check(arguments(directory, fork=True))


def test_legacy_root_fails_the_configuration_stage_with_its_line(tmp_path):
    directory = write_reference(tmp_path / "legacy", load(LEGACY / "config.yaml").value)
    with pytest.raises(CheckFailure) as failure:
        check(arguments(directory, fork=True))
    detail = failure.value.diagnostic()
    assert detail["stages"][0]["status"] == "failed" and detail["line"]
    assert detail["field"] == "execution"


def test_fixture_anchors_must_cover_the_authored_chains(fork_case, tmp_path):
    case = fork_case
    inputs = case.setup.operator.fork
    operator = revalidate(
        case.setup.operator,
        fork=revalidate(
            inputs, anchors=(AnchorSelection(chain=inputs.anchors[0].chain, fresh=True),)
        ),
    )
    path = tmp_path / "fresh.json"
    path.write_text(operator.model_dump_json())
    report = check(arguments(case.directory, path, case.setup.env_file, fork=True))
    assert states(report)["operator_configuration"] == "passed"
    raw = json.loads(operator.model_dump_json())
    raw["fork"]["anchors"][0]["chain"] = "eip155:43114"
    raw["networks"].append({"chain": "eip155:43114", "provider": raw["networks"][0]["provider"]})
    path.write_text(json.dumps(raw))
    with pytest.raises(CheckFailure) as failure:
        check(arguments(case.directory, path, case.setup.env_file, fork=True))
    assert states(failure.value.diagnostic())["operator_configuration"] == "failed"


def test_cli_online_check_emits_json_without_secrets(fork_case, monkeypatch, capsys):
    from almanak.cli import main

    case = fork_case
    monkeypatch.setattr(
        "sys.argv",
        [
            "almanak",
            "check",
            str(case.directory),
            "--fork",
            "--online",
            "--gateway-config",
            str(case.operator_path),
            "--env-file",
            str(case.setup.env_file),
            "--normalized",
            "--json",
        ],
    )
    assert main() == 0
    out = capsys.readouterr().out
    result = json.loads(out)
    assert result["normalized"]["parameters"]["allocations"][0]["capital"] == 10**16
    assert "hidden-input-secret" not in out


def test_backtest_inputs_are_checked_offline_against_the_author_accounts(tmp_path):
    args = arguments(REFERENCE, backtest_config=REFERENCE / "backtest-economic.yaml")
    report = check(args)
    assert states(report)["backtest_configuration"] == "passed"
    assert states(report)["resolution"] == "not_checked"
    broken = tmp_path / "backtest.yaml"
    broken.write_text(
        (REFERENCE / "backtest.yaml").read_text().replace("lending_1:", "unknown_fund:")
    )
    with pytest.raises(CheckFailure, match="no author account") as failure:
        check(arguments(REFERENCE, backtest_config=broken))
    assert states(failure.value.diagnostic())["backtest_configuration"] == "failed"
    with pytest.raises(ValueError, match="cannot be combined"):
        check(arguments(REFERENCE, fork=True, backtest_config=broken))


def test_online_backtest_check_resolves_at_the_window_start_and_reports_the_model(tmp_path):
    from tests.backtest_support import Archive, archive_server, converted_config

    history = Archive()
    server, thread = archive_server(history)
    try:
        config, operator, env_file = converted_config(tmp_path, history, server)
        (tmp_path / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
        report = check(
            arguments(
                tmp_path,
                operator,
                env_file,
                online=True,
                backtest_config=tmp_path / "backtest.yaml",
            )
        )
        assert states(report)["backtest_configuration"] == "passed"
        assert states(report)["backtest_model"] == "passed"
        assert states(report)["online_admission"] == "not_checked"
        assert report["backtest"]["market_model"] == "historical_path"
        assert report["backtest"]["funds"][0]["name"] == "lending_0"
        assert "funding" not in report
        anchor = report["preview"]["anchors"][0]
        assert anchor["timestamp"] <= history.start + 1
        assert [m["name"] for m in report["preview"]["markets"]] == [
            "main_weth_market",
            "bluechip_weth_market",
        ]
        assert "hidden" not in json.dumps(report)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_market_reads_quote_the_policy_gas_price_once_per_chain(tmp_path, monkeypatch):
    """The online reads pin one latest block per chain and quote with the author policy."""
    from contextlib import ExitStack

    from almanak.caip import ChainId
    from almanak.models import Block, Chain
    from tests.test_check_warnings import inputs

    chain = ChainId("eip155:8453")
    block = Block(
        chain=Chain(chain_id=chain, environment="mainnet"),
        number=100,
        hash="0x" + "11" * 32,
        timestamp=1_790_000_000,
    )
    opened = []

    class Reader:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            opened.append("closed")

        def pin_block(self, selector):
            return block

        def _rpc(self, method, params):
            assert method == "eth_feeHistory" and params[1] == hex(block.number)
            return {"oldestBlock": hex(100), "reward": [["0x7"]], "baseFeePerGas": ["0x64", "0xc8"]}

    def reader_for(selected, endpoint):
        assert (selected, endpoint) == (chain, "https://rpc.invalid")
        opened.append("opened")
        return Reader()

    monkeypatch.setattr("almanak.gateway.read_endpoints.mainnet_reader", reader_for)
    monkeypatch.setattr(
        "almanak.gateway.providers.provider_endpoint", lambda *_: "https://rpc.invalid"
    )
    operator = SimpleNamespace(network=lambda _: SimpleNamespace(binding=lambda: None))
    policy = inputs(tmp_path).strategy.gas["base"].resolve("base")
    with ExitStack() as stack:
        reads = checks.MarketReads(stack, operator, tmp_path / ".env")
        # Base reports tips in fee history: 2 x the next base fee 200, plus the 7 wei tip.
        assert reads.gas_price(policy) == 407
        assert opened == ["opened"]
    assert opened == ["opened", "closed"]


class WindowReader:
    """A pinned reader answering one 100-block fee history window and eth_gasPrice."""

    def __init__(self, block, history, node_gas_price=0):
        self.block, self.history, self.node_gas_price = block, history, node_gas_price
        self.calls = []

    def pin_block(self, selector):
        return self.block

    def _rpc(self, method, params):
        self.calls.append((method, params))
        if method == "eth_gasPrice":
            return hex(self.node_gas_price)
        return self.history


def window_history(block, bases, rewards):
    return {
        "oldestBlock": hex(block.number - len(rewards) + 1),
        "baseFeePerGas": [hex(base) for base in bases],
        "reward": [[hex(reward)] for reward in rewards],
    }


def window_case(tmp_path, chain="eip155:8453", **update):
    from almanak.caip import ChainId
    from almanak.models import Block, Chain
    from tests.test_check_warnings import on_chain, resolved_policy

    block = Block(
        chain=Chain(chain_id=ChainId(chain), environment="mainnet"),
        number=1_000,
        hash="0x" + "11" * 32,
        timestamp=1_790_000_000,
    )
    policy = on_chain(resolved_policy(tmp_path), chain).model_copy(update=update)
    # Base fee 100 throughout except a 1000 next-block fee after the checked block,
    # tip 7 except a 500 tip ten blocks into the window.
    bases = [100] * 100 + [1_000]
    rewards = [7] * 100
    rewards[10] = 500
    return block, policy, window_history(block, bases, rewards)


def test_the_window_bid_is_the_highest_bid_over_the_last_100_blocks(tmp_path):
    from almanak.check_warnings import WINDOW_BLOCKS
    from almanak.gateway.production_fees import window_bid

    block, policy, history = window_case(tmp_path)
    reader = WindowReader(block, history)
    # next_block: the last block bids 2 x the 1000 next base fee + 7.
    assert window_bid(reader, policy, block, WINDOW_BLOCKS) == 2_007
    assert reader.calls == [("eth_feeHistory", ["0x64", hex(block.number), [50]])]
    # observed_block reads each block's own base fee: the 500 tip is the highest bid.
    observed = policy.model_copy(update={"base_fee_basis": "observed_block"})
    assert window_bid(WindowReader(block, history), observed, block, WINDOW_BLOCKS) == 700
    history["baseFeePerGas"] = history["baseFeePerGas"][:100]
    assert window_bid(WindowReader(block, history), observed, block, WINDOW_BLOCKS) == 700


def test_a_window_without_tips_is_floored_by_the_node_gas_price(tmp_path):
    from almanak.gateway.production_fees import window_bid

    block, policy, history = window_case(tmp_path, "eip155:56")
    assert window_bid(WindowReader(block, history, 3_000), policy, block, 100) == 3_000
    assert window_bid(WindowReader(block, history, 1_000), policy, block, 100) == 2_007
    # Chains whose fee history reports tips never read the node suggestion.
    block, policy, history = window_case(tmp_path)
    reader = WindowReader(block, history, 3_000)
    assert window_bid(reader, policy, block, 100) == 2_007
    assert [method for method, _ in reader.calls] == ["eth_feeHistory"]


def test_a_short_or_reorganized_window_is_a_data_error(tmp_path):
    from almanak.gateway.ethereum import DataError
    from almanak.gateway.production_fees import window_bid

    block, policy, history = window_case(tmp_path)
    short = window_history(block, [100] * 11, [7] * 10)
    with pytest.raises(DataError, match="window is incomplete"):
        window_bid(WindowReader(block, short), policy, block, 100)
    moved = WindowReader(block, history)
    moved.pin_block = lambda selector: block.model_copy(update={"hash": "0x" + "22" * 32})
    with pytest.raises(DataError, match="changed during the check"):
        window_bid(moved, policy, block, 100)


def test_market_reads_bound_the_l1_fee_as_admission_does_for_max_gas_limit(tmp_path, monkeypatch):
    from contextlib import ExitStack

    from almanak.check_warnings import L1_DATA_BYTES

    block, policy, history = window_case(tmp_path)
    reader = WindowReader(block, history)
    seen = []

    def bound(reader_, block_, gas, length, multiplier):
        seen.append((reader_, block_, gas, length, multiplier))
        return 42

    monkeypatch.setattr("almanak.gateway.op_fees.exposure_bound", bound)
    with ExitStack() as stack:
        reads = checks.MarketReads(stack, None, None)
        reads.readers[policy.asset.chain] = reader
        reads.latest[policy.asset.chain] = block
        assert reads.l1_fee(policy) == 42
        assert reads.window_bid(policy) == 2_007
    assert seen == [(reader, block, policy.max_gas_limit, L1_DATA_BYTES, 20_000)]


def test_an_unexpected_market_read_failure_is_one_warning_and_keeps_the_report(
    production_case, monkeypatch
):
    """Market reads run outside any stage: a stray error must not drop funding or stages."""
    case = production_case

    def broken(self, policy):
        raise KeyError("baseFeePerGas")

    monkeypatch.setattr(checks.MarketReads, "gas_price", broken)
    monkeypatch.setattr("almanak.production_setup.requires_permissions", lambda _: False)
    report = check(
        arguments(
            case.directory, case.operator_path, case.setup.env_file, production=True, online=True
        )
    )
    [finding] = report["warnings"]
    assert (finding["code"], finding["field"]) == ("market_read", "strategy")
    assert "KeyError" in finding["message"] and "baseFeePerGas" in finding["message"]
    assert "exit_quota" not in states(report) and states(report)["policy"] == "passed"
    funding(report)


def test_a_failure_closing_the_market_readers_keeps_the_findings_already_made(monkeypatch):
    price = {"code": "gas_price", "field": "strategy.gas.x", "message": "y"}

    def opened(self, stack, operator, env_file):
        stack.callback(lambda: (_ for _ in ()).throw(RuntimeError("client close failed")))

    monkeypatch.setattr(checks.MarketReads, "__init__", opened)
    monkeypatch.setattr("almanak.check_warnings.market_gas_warnings", lambda *_: [price])
    args = SimpleNamespace(env_file=None)
    warnings = checks.market_findings(SimpleNamespace(), None, args, RunMode.PRODUCTION)
    assert warnings[0] == price and warnings[1]["code"] == "market_read"
    assert "RuntimeError: client close failed" in warnings[1]["message"]
