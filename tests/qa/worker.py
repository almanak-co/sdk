"""One isolated QA case using the existing lifecycle and CLI boundaries."""

import argparse
import json
import signal
import sys
from dataclasses import asdict, replace
from pathlib import Path

from almanak.backtest.engine import Journal
from almanak.caip import ChainId
from almanak.configuration.unresolved import parse_inputs, public_root
from almanak.configuration.yaml import load
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import allocation_connector, target_connector
from scripts.legacy_configuration.author import read_author
from tests.exchange_scope import without_exchange_budgets
from tests.legacy_references import without_retired_fields
from tests.qa.catalog import ROOT, catalog, converted
from tests.qa.inputs import preparation_timeout, resolved_author, resolved_history, write_json
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.process import cli, pytest_workers, run
from tests.qa.variants import matches as matches_variant


def interrupt(signum, frame):
    raise KeyboardInterrupt("QA worker cancelled")


def fork_case(case, directory, config, env_file):
    from tests.strategy_fork.harness import discover

    source = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == case.reference
    )
    source = replace(source, config=config)
    temporary = directory / "workspace"
    temporary.mkdir()
    execute_fork_case(case, source, config, env_file, temporary, directory / "evidence")
    checkpoint = next((directory / "evidence").rglob("checkpoint.json"))
    state = json.loads(checkpoint.read_text())
    assert state["mode"] == "closed" and state["outcomes"], "lifecycle produced no closure evidence"
    assert_fork_actions(case, state, config)
    connector = connector_from_wire(state["config"])
    parsed = connector.codec.checkpoint.model_validate_json(
        json.dumps(without_exchange_budgets(json.loads(checkpoint.read_text())))
    )
    connector.lifecycle.validate_closure(parsed, parsed.closure)
    for check in case.checks():
        assert_native_direct(check, parsed)


def execute_fork_case(case, source, config, env_file, temporary, artifact):
    """A scenario that needs an internal driver outranks the generic supervised lane.

    The supervised lane covers both account profiles of a converted reference:
    the case operator file already carries the Safe or EOA fixture.
    """
    if case.scenario == "gmx_cleanup":
        from tests.qa.gmx import cleanup_case

        cleanup_case(case, source, config, env_file, temporary, artifact)
    elif uses_fresh_gmx_harness(case):
        from tests.qa.gmx_fresh import execute

        execute(case, source, config, env_file, temporary, artifact)
    elif uses_curve_harness(case):
        curve_case(case, source, config, env_file, temporary, artifact)
    elif uses_uniswap_cleanup_harness(case):
        uniswap_cleanup_case(case, source, config, env_file, temporary, artifact)
    elif converted(case):
        from tests.strategy_fork.supervised import run_supervised_case

        run_supervised_case(
            source,
            config.parent / "operator.yaml",
            env_file,
            temporary,
            artifact,
            case.operator_mode,
        )
    elif uses_lending_eoa_harness(case):
        lending_eoa_case(case, source, config, env_file, temporary, artifact)
    else:
        execute_account_case(case, source, config, env_file, temporary, artifact)


def author_fork_configuration(case, config, env_file):
    """The connector fork configuration an internal driver accepts, from either root format.

    A converted reference has no author execution block: its providers, bindings
    and fixture come from the case operator file, resolved once through the read
    gateway beside the case configuration.
    """
    if not public_root(load(config).value):
        return read_author(config).fork_config()
    from tests.strategy_fork.adapter import fork_config_for, resolve_reference

    operator = config.parent / "operator.yaml"
    progress = resolve_reference(config, operator, env_file, config.parent)
    return fork_config_for(config, operator, progress)


def uses_fresh_gmx_harness(case):
    return case.input_policy == "fresh" and any(
        check.connector == ConnectorId("gmx_v2") for check in case.checks()
    )


def execute_account_case(case, source, config, env_file, temporary, artifact):
    from tests.strategy_fork.harness import run_case

    if case.profile == "safe_roles":
        run_case(source, env_file, {}, temporary, artifact, case.operator_mode)
    else:
        eoa_case(case, source, config, env_file, temporary, artifact)


def uses_lending_eoa_harness(case):
    return (
        case.scopes
        or case.connector
        in (
            ConnectorId("aave_v3"),
            ConnectorId("uniswap_v4"),
            ConnectorId("uniswap_v3"),
            ConnectorId("sparklend"),
            ConnectorId("compound_v3"),
            ConnectorId("morpho_blue"),
            ConnectorId("pancakeswap_v3"),
            ConnectorId("aerodrome_slipstream"),
            ConnectorId("aerodrome"),
            ConnectorId("gmx_v2"),
        )
    ) and case.profile == "eoa"


def uses_curve_harness(case):
    return case.scenario == "curve_cleanup" or (
        case.connector == ConnectorId("curve") and case.profile == "eoa"
    )


def uses_uniswap_cleanup_harness(case):
    return case.scenario == "uniswap_cleanup" and case.connector in (
        ConnectorId("uniswap_v3"),
        ConnectorId("uniswap_v4"),
        ConnectorId("pancakeswap_v3"),
    )


def eoa_fork_configuration(case, config, env_file):
    source = author_fork_configuration(case, config, env_file)
    if converted(case):
        return source
    if case.connector == ConnectorId("gmx_v2"):
        from tests.qa.gmx import eoa_configuration as gmx_configuration

        return gmx_configuration(source)
    return indexed_eoa_configuration(case, source)


def indexed_eoa_configuration(case, source):
    from tests.fork.test_aave_v3_base_eoa import eoa_configuration
    from tests.fork.test_aerodrome_eoa import qualified_fund_indices as aerodrome_funds
    from tests.fork.test_pancakeswap_v3_eoa import qualified_fund_indices as pancake_funds
    from tests.fork.test_slipstream_eoa import qualified_fund_indices as slipstream_funds
    from tests.fork.test_uniswap_v3_eoa import lp_fund_indices
    from tests.fork.test_uniswap_v4_eoa import qualified_fund_indices as uniswap_v4_funds

    author = eoa_configuration(source)
    lp = case.reference.endswith("_lp")
    indexers = {
        ConnectorId("pancakeswap_v3"): lambda value: pancake_funds(value, lp=lp),
        ConnectorId("aerodrome_slipstream"): lambda value: slipstream_funds(value, lp=lp),
        ConnectorId("aerodrome"): lambda value: aerodrome_funds(value, lp=lp),
    }
    author = primary_eoa_index(case, author, lp, indexers, lp_fund_indices)
    return uniswap_v4_eoa_index(case, author, lp, uniswap_v4_funds)


def primary_eoa_index(case, author, lp, indexers, uniswap_v3_lp):
    if case.connector == ConnectorId("uniswap_v3") and lp:
        return uniswap_v3_lp(author)
    indexer = indexers.get(case.connector)
    return indexer(author) if indexer is not None else author


def uniswap_v4_eoa_index(case, author, lp, indexer):
    if any(check.connector == ConnectorId("uniswap_v4") for check in case.checks()):
        return indexer(author, lp=lp)
    return author


def lending_eoa_case(case, source, config, env_file, temporary, artifact):
    from contextlib import ExitStack

    from tests.fork.support import processes
    from tests.fork.test_aave_v3_base_eoa import eoa_lifecycle
    from tests.strategy_fork.harness import (
        bind_workspace,
        load_assertions,
        resolve_config,
        retain,
        start_gateway,
    )

    if case.connector == ConnectorId("gmx_v2"):
        from tests.qa.gmx import eoa_lifecycle

    author = resolve_config(eoa_fork_configuration(case, config, env_file), env_file, {})
    artifact.mkdir()
    record = {
        "strategy": case.reference,
        "account_profile": "eoa",
        "phases": [],
        "status": "failed",
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(author, env_file, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            nodes.prepare(author, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            eoa_lifecycle(
                source, node, load_assertions(source), journal, arguments, launch, record["phases"]
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def eoa_case(case, source, config, env_file, temporary, artifact):
    from tests.strategy_fork.harness import retain
    from tests.strategy_fork.test_production_rehearsals import eoa_funding, exercise

    record = {"strategy": case.reference, "profile": "eoa", "phases": [], "status": "failed"}
    artifact.mkdir()
    try:
        author = eoa_funding(author_fork_configuration(case, config, env_file))
        exercise(source, author, env_file, temporary, record["phases"])
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def harness_case(case, source, config, env_file, temporary, artifact, *, configure_eoa, exercise):
    """Run one fork harness lane with the shared author, EOA, record and retention scaffold."""
    from tests.strategy_fork.harness import resolve_config, retain

    author = author_fork_configuration(case, config, env_file)
    if case.profile == "eoa" and not converted(case):
        author = configure_eoa(author)
    author = resolve_config(author, env_file, {})
    artifact.mkdir()
    record = {
        "strategy": case.reference,
        "account_profile": case.profile,
        "scenario": case.scenario,
        "phases": [],
        "status": "failed",
    }
    try:
        exercise(source, author, env_file, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


def curve_case(case, source, config, env_file, temporary, artifact):
    from tests.fork.test_aave_v3_base_eoa import eoa_configuration
    from tests.fork.test_curve_cleanup import exercise as cleanup
    from tests.fork.test_curve_eoa import exercise_eoa

    if case.scenario == "curve_cleanup":
        exercise = cleanup
    else:

        def exercise(source, author, env, temporary, record):
            exercise_eoa(source, author, env, temporary, artifact, record)

    harness_case(
        case,
        source,
        config,
        env_file,
        temporary,
        artifact,
        configure_eoa=eoa_configuration,
        exercise=exercise,
    )


def uniswap_cleanup_case(case, source, config, env_file, temporary, artifact):
    from tests.fork.test_uniswap_lp_cleanup import eoa_lp_configuration
    from tests.fork.test_uniswap_lp_cleanup import exercise as cleanup

    harness_case(
        case,
        source,
        config,
        env_file,
        temporary,
        artifact,
        configure_eoa=lambda author: eoa_lp_configuration(case.reference, author),
        exercise=cleanup,
    )


OPERATION_ACTIONS = {
    "swap": "swap_exact_input",
    "mint": "open_liquidity",
    "collect": "collect_liquidity_fees",
    "burn": "close_liquidity",
    "revoke": "revoke_exchange",
    "revoke_permit2": "revoke_exchange",
    "revoke_erc20": "revoke_exchange",
}


def operation_action(connector, kind):
    if connector in (
        ConnectorId("curve"),
        ConnectorId("uniswap_v3"),
        ConnectorId("pancakeswap_v3"),
        ConnectorId("aerodrome_slipstream"),
        ConnectorId("aerodrome"),
    ):
        liquidity = {
            "increase": "increase_liquidity",
            "decrease": "decrease_liquidity",
            "close": "close_liquidity",
        }
        if kind in liquidity:
            return liquidity[kind]
    return OPERATION_ACTIONS.get(kind, kind)


def fork_operation_action(connector, operation):
    if connector == ConnectorId("morpho_blue") and operation["kind"] == "repay":
        return "repay" if operation["shares"] else "repay_amount"
    if connector in (
        ConnectorId("aave_v3"),
        ConnectorId("sparklend"),
        ConnectorId("compound_v3"),
        ConnectorId("morpho_blue"),
    ) and operation["kind"] in (
        "withdraw",
        "repay",
    ):
        return operation["kind"] if operation["full"] else operation["kind"] + "_amount"
    if (
        connector
        in (
            ConnectorId("curve"),
            ConnectorId("uniswap_v3"),
            ConnectorId("pancakeswap_v3"),
            ConnectorId("aerodrome_slipstream"),
            ConnectorId("aerodrome"),
        )
        and operation["kind"] == "approve"
        and operation["amount"] == 0
    ):
        return "revoke_exchange"
    return operation_action(connector, operation["kind"])


def fork_actions(outcomes, allocations, *, connector=None, variant=None):
    actions = set()
    for item in outcomes:
        operation = item["operation"]
        if operation["target"]["allocation"] not in allocations or not item["success"]:
            continue
        if not matches_variant(connector, operation["target"], variant):
            continue
        actions.add(fork_operation_action(connector, operation))
        intent = item.get("decision", {}).get("intent")
        if intent is not None and intent["kind"] == "exit_exchange":
            actions.add(intent["kind"])
    return actions


def author_scopes(config):
    """One (name, chain, connector) triple per author position, in either root format.

    Retained evidence carries the author file its own run used, so the root
    format of that file selects the reader instead of the current case.
    """
    document = load(config)
    if not public_root(document.value):
        author = read_author(config).fork_config()
        return tuple(
            (item.name, item.chain.chain_id, allocation_connector(item))
            for item in (*author.allocations, *getattr(author, "debts", ()))
        )
    # Retained author files may name fields the current interface removed; scopes need
    # only names, markets and chains.
    current = replace(document, value=without_retired_fields(document.value))
    inputs = parse_inputs(current).inputs
    return tuple(
        (
            name,
            inputs.markets[item.market].chain,
            ConnectorId(inputs.markets[item.market].connector),
        )
        for group in (inputs.strategy.allocations, inputs.strategy.debts)
        for name, item in group.items()
    )


def assert_fork_actions(case, state, config):
    scopes = author_scopes(config)
    for check in case.checks():
        for chain in check.chains:
            allocations = {
                name
                for name, scoped, connector in scopes
                if scoped == chain and (check.connector is None or connector == check.connector)
            }
            for variant in check.variants or (None,):
                if check.connector == ConnectorId("gmx_v2"):
                    from tests.qa.gmx import actions

                    observed = actions(state, allocations, chain)
                else:
                    observed = fork_actions(
                        state["outcomes"], allocations, connector=check.connector, variant=variant
                    )
                assert set(check.actions) <= observed, (
                    f"{check.connector}/{chain}/{variant}: "
                    f"missing actions {set(check.actions) - observed}"
                )
    write_json(
        config.parent / "observed-actions.json",
        {
            "actions": list(case.actions),
            "chains": list(case.chains),
            "checkpoint": "evidence",
            "variants": case.variants,
            "scopes": [asdict(scope) for scope in case.scopes],
        },
    )


def prepare_dataset(directory, common, provider, timeout=1800):
    dataset = directory / "dataset"
    write_json(directory / "preparation-policy.json", {"timeout_seconds": timeout})
    run(
        cli(["backtest", "prepare", *common, *provider, "--output", dataset]),
        directory,
        "cold-prepare",
        timeout=timeout,
    )
    run(
        cli(["backtest", "prepare", *common, "--output", dataset]),
        directory,
        "cached-prepare",
        offline=True,
    )
    return ["--dataset", dataset]


def compare_model(case, directory, env_file):
    if case.model_mode != "economic":
        return
    if case.scopes:
        from tests.qa.composed_comparisons import compare_components

        compare_components(directory, env_file)
    else:
        compare = pool_comparison(case.connector)
        if compare is not None:
            compare(directory, env_file)


def pool_comparison(connector):
    from tests.qa.aerodrome_comparisons import compare_pool as compare_aerodrome
    from tests.qa.comparisons import compare_pool
    from tests.qa.curve_comparisons import compare_pool as compare_curve
    from tests.qa.gmx_comparisons import compare_pool as compare_gmx
    from tests.qa.pancake_comparisons import compare_pool as compare_pancake
    from tests.qa.slipstream_comparisons import compare_pool as compare_slipstream
    from tests.qa.v3_comparisons import compare_pool as compare_v3

    return {
        ConnectorId("uniswap_v4"): compare_pool,
        ConnectorId("uniswap_v3"): compare_v3,
        ConnectorId("aerodrome_slipstream"): compare_slipstream,
        ConnectorId("aerodrome"): compare_aerodrome,
        ConnectorId("pancakeswap_v3"): compare_pancake,
        ConnectorId("curve"): compare_curve,
        ConnectorId("gmx_v2"): compare_gmx,
    }.get(connector)


def backtest_arguments(case, directory, config, settings):
    """A converted reference binds its historical providers through the operator file."""
    strategy = ROOT / "strategies" / case.reference / "strategy.py"
    common = ["--strategy", strategy, "--config", config, "--backtest-config", settings]
    if converted(case):
        common.extend(["--gateway-config", directory / "operator.yaml"])
    return common


def backtest_case(case, directory, config, env_file, now, timeout=3600):
    settings = resolved_history(case, directory, now)
    common = backtest_arguments(case, directory, config, settings)
    economic = case.model_mode == "economic"
    provider = ["--env-file", env_file] if env_file is not None else []
    dataset_args = (
        prepare_dataset(directory, common, provider, timeout=preparation_timeout(case))
        if economic
        else provider
    )
    output = directory / "run"
    run(
        cli(["backtest", "run", *common, *dataset_args, "--output", output]),
        directory,
        "execute",
        timeout=timeout,
        offline=economic,
    )
    assert_backtest(case, output)
    raw = json.loads((output / "manifest.json").read_text())
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(without_exchange_budgets(raw)))
    state = Journal(output / "state.sqlite", type(manifest.initial)).load()
    for check in case.checks():
        assert_economic_direct(check, state, manifest.config)
    run(
        cli(["backtest", "reproduce", "--run", output]),
        directory,
        "reproduce",
        timeout=timeout,
        offline=True,
    )
    assert json.loads((output / "reproduction.json").read_text())["reproduced"]
    compare_model(case, directory, env_file)


def assert_backtest(case, directory):
    report = json.loads((directory / "report.json").read_text())
    assert report["closed"], "backtest did not close its obligations"
    assert_report_scopes(case, report)
    raw = json.loads((directory / "manifest.json").read_text())
    assert_model_claim(case, raw)
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(without_exchange_budgets(raw)))
    state = Journal(directory / "state.sqlite", type(manifest.initial)).load()
    assert_completed(state)
    for check in case.checks():
        for chain in check.chains:
            for variant in check.variants or (None,):
                actions = completed_actions(
                    state, chain, connector=check.connector, variant=variant
                )
                assert set(check.actions) <= actions, (
                    f"{check.connector}/{chain}/{variant}: "
                    f"missing economic actions {set(check.actions) - actions}"
                )
    write_json(
        directory / "qa-assertions.json",
        {
            "closed": True,
            "actions": list(case.actions),
            "chains": list(case.chains),
            "evaluations": state.evaluations,
            "completed": len(state.completed),
            "variants": case.variants,
            "scopes": [asdict(scope) for scope in case.scopes],
        },
    )


def assert_report_scopes(case, report):
    if not case.scopes:
        assert_measurements(case, report)
        return
    assert set(report["components"]) == {str(scope.connector) for scope in case.scopes}
    for check in case.checks():
        component = report["components"][str(check.connector)]
        assert component["model"] == check.model, "component model differs from the QA proof claim"
        assert_measurements(check, component)


def assert_completed(state):
    assert state.mode == "finished"
    assert state.pending is None and state.continuation is None
    assert state.completed and state.delivered == len(state.completed)


def assert_model_claim(case, manifest):
    if case.model_mode == "economic":
        assert manifest["model"] == case.model, "retained model differs from the QA proof claim"
        return
    observed = {
        ChainId(capability["chain"]["chain_id"]): capability["model"]
        for capability in manifest["capabilities"]
        if capability["connector"] == str(case.connector)
    }
    assert observed == dict.fromkeys(case.chains, case.model), (
        "retained historical models differ from the QA proof claim"
    )


def assert_measurements(case, report):
    assert report["positions"], "backtest has no measured positions"
    if case.connector == ConnectorId("curve"):
        assert not report["missing_inputs"] and not report["failures"]
        for position in report["positions"]:
            assert position["measurement_status"] == "modeled"
            assert position["observation"] is not None and position["missing_input"] is None
    elif case.connector in (
        ConnectorId("aave_v3"),
        ConnectorId("uniswap_v3"),
        ConnectorId("sparklend"),
        ConnectorId("compound_v3"),
        ConnectorId("morpho_blue"),
        ConnectorId("pancakeswap_v3"),
        ConnectorId("aerodrome_slipstream"),
        ConnectorId("aerodrome"),
    ):
        assert not report["missing_inputs"]
        for position in report["positions"]:
            assert position["measurement_status"] == "modeled"
            assert position["observation"] is not None and position["missing_input"] is None
    elif str(case.connector) == "uniswap_v4":
        assert all(p["complete"] for p in report["positions"])
    elif case.connector == ConnectorId("gmx_v2"):
        assert not report["missing_inputs"] and not report["failures"]
        for position in report["positions"]:
            assert position["measurement_status"] == "modeled"
            assert position["observation"] is not None and position["missing_input"] is None
            assert position["remaining_obligations"] is False
    elif case.model_mode == "economic":
        assert report["accounting_complete"]
    else:
        assert all(p["marked_assets"] is not None for p in report["positions"])


def completed_actions(state, chain, *, connector=None, variant=None):
    actions = set()
    for item in state.completed:
        if item.pending.intent.target.fund.chain.chain_id != chain:
            continue
        if connector is not None and target_connector(item.pending.intent.target) != connector:
            continue
        if variant is not None and not matches_variant(
            connector, item.pending.intent.target.model_dump(mode="json"), variant
        ):
            continue
        if not getattr(item.transition.result, "success", True):
            continue
        actions.add(item.pending.intent.kind)
        actions.update(
            operation_action(connector, step)
            for step in getattr(item.transition.result, "steps", ())
        )
    return actions


def pytest_case(case, directory, env_file, workers, timeout=3600):
    from tests.qa.results import assert_junit

    junit = directory / "pytest.xml"
    arguments = [
        sys.executable,
        "-m",
        "pytest",
        *case.nodes,
        "-n",
        str(workers),
        "--junitxml",
        str(junit),
    ]
    if case.lane == "faults":
        arguments.extend(["--fork-env", str(env_file), "--maxfail=1"])
    else:
        arguments.extend(["-m", "not fork"])
    if case.lane == "offline":
        for name, command in (
            ("lint", ["ruff", "check", "src", "tests", "strategies", "scripts"]),
            ("format", ["ruff", "format", "--check", "src", "tests", "strategies", "scripts"]),
            ("types", ["mypy"]),
        ):
            run([str(Path(sys.executable).parent / command[0]), *command[1:]], directory, name)
        arguments = [sys.executable, "scripts/check_crap.py", *arguments[3:]]
    run(arguments, directory, "pytest", timeout=timeout)
    assert_junit(junit)


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--origins", type=Path, required=True)
    parser.add_argument("--now", type=int, required=True)
    parser.add_argument("--workers", type=pytest_workers, default="auto")
    parser.add_argument("--timeout", type=int, default=3600)
    return parser.parse_args()


def main():
    args = arguments()
    signal.signal(signal.SIGTERM, interrupt)
    case = next(c for c in catalog() if c.id == args.case)
    dispatch(case, args)


def dispatch(case, args):
    if case.blocked_reason:
        raise ValueError(case.blocked_reason)
    if case.lane in ("offline", "pytest", "faults"):
        pytest_case(case, args.directory, args.env_file, args.workers, args.timeout)
        return
    origins = {ChainId(k): v for k, v in json.loads(args.origins.read_text())["resolved"].items()}
    config = resolved_author(case, args.directory, origins)
    if case.lane == "fork":
        fork_case(case, args.directory, config, args.env_file)
    else:
        backtest_case(case, args.directory, config, args.env_file, args.now, args.timeout)


if __name__ == "__main__":
    main()
