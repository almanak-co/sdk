"""One pre-admission author check pipeline with honest readiness stages."""

import json
import subprocess
import tempfile
from contextlib import ExitStack
from pathlib import Path

from almanak.configuration.parameter_codec import AuthorBase
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.support import RunMode
from almanak.runtime import (
    sandboxed_evaluation,
    strategy_capabilities,
    strategy_worker,
    validation_result,
)
from almanak.sandbox import worker_command
from almanak.strategy import DataDependencyRequest, StrategyDefinition, ValidationRequest
from almanak.typed_parameters import model_fields
from almanak.worker_session import preloaded_workers, started_workers

STAGES = (
    "configuration",
    "sandbox",
    "operator_configuration",
    "resolution",
    "parameters",
    "strategy",
    "capabilities",
    "data_dependencies",
    "online_admission",
)


def selected_paths(args):
    directory = getattr(args, "directory", None) or Path(".")
    config = getattr(args, "config", None) or directory / "config.yaml"
    strategy = getattr(args, "strategy", None) or directory / "strategy.py"
    if config.suffix not in (".yaml", ".yml"):
        raise ValueError(
            "author configuration must be YAML; runtime artifacts use service commands"
        )
    args.config, args.strategy = config.resolve(), strategy.resolve()
    return args.config, args.strategy


def author_inputs(args, mode=None, overrides=None, fields=...):
    """The public author root and strategy source of one invocation.

    Production refuses a root without `strategy.gas` here because that rule
    needs no provider; it would otherwise surface only during online resolution. `fields`
    defaults to the typed parameter model's fields read from the strategy.
    """
    from almanak.configuration.parameters import gas_policies

    config, strategy = selected_paths(args)
    fields = model_fields(strategy) if fields is ... else fields
    document = read_inputs(config, overrides, fields)
    if mode == RunMode.PRODUCTION:
        gas_policies(document, mode)
    return document, StrategyDefinition.read(strategy)


class CheckFailure(ConfigurationError):
    def __init__(self, error, stages, file):
        detail = (
            error.diagnostic()
            if isinstance(error, ConfigurationError)
            else {
                "code": getattr(error, "check_code", "check_stage_failed"),
                "file": str(file),
                "line": None,
                "field": stages[-1]["stage"],
                "message": str(error),
                "remedy": getattr(error, "remedy", "Correct the failed stage and run check again."),
            }
        )
        super().__init__(
            str(error), file=detail["file"], field=detail["field"], line=detail["line"]
        )
        names = {s["stage"] for s in stages}
        remaining = [
            stage(name, "not_checked", "Stopped after the failed stage.")
            for name in STAGES
            if name not in names and name != "operator_configuration"
        ]
        self.detail = {**detail, "valid": False, "ready": False, "stages": [*stages, *remaining]}

    def diagnostic(self):
        return self.detail


def checked(stages, name, file, action):
    from almanak.gateway.ethereum import DataError

    try:
        result = action()
    except (
        ValueError,
        RuntimeError,
        OSError,
        SyntaxError,
        subprocess.SubprocessError,
        DataError,
    ) as error:
        stages.append(stage(name, "failed", str(error)))
        raise CheckFailure(error, stages, file) from error
    stages.append(stage(name))
    return result


def check_capabilities(parameters, definition):
    if not isinstance(parameters, AuthorBase):
        return strategy_capabilities(definition, parameters)
    request = ValidationRequest(strategy=definition, config=parameters)
    output = sandboxed_evaluation(request, ["--author-check"])
    if output.strip() != "null":
        raise ValueError("strategy capability check returned an invalid result")


def sandbox_workers(workers: ExitStack):
    """The sandbox stage: isolation must exist and, on Linux, start the preloaded worker.

    The hook stages then share that one worker, each evaluation in a fresh forked
    child, instead of starting one sandboxed interpreter per hook.
    """
    worker_command()
    workers.enter_context(preloaded_workers())


def check_parameters(parameters, definition, stages=None):
    stages = [] if stages is None else stages
    with ExitStack() as workers:
        checked(stages, "sandbox", definition.filename, lambda: sandbox_workers(workers))
        checked(
            stages,
            "strategy",
            definition.filename,
            lambda: check_validation(parameters, definition),
        )
        capabilities = checked(
            stages,
            "capabilities",
            definition.filename,
            lambda: check_capabilities(parameters, definition),
        )
        dependencies = checked(
            stages,
            "data_dependencies",
            definition.filename,
            lambda: json.loads(
                strategy_worker(DataDependencyRequest(strategy=definition, config=parameters))
            ),
        )
    return capabilities, dependencies


def check_validation(parameters, definition):
    from almanak.configuration.parameter_codec import hook_parameters

    request = ValidationRequest(strategy=definition, config=hook_parameters(parameters))
    validation_result(strategy_worker(request), definition)


def stage(name, status="passed", message=""):
    return {"stage": name, "status": status, "message": message}


def selected_mode(args) -> RunMode | None:
    if getattr(args, "backtest_config", None) is not None:
        if getattr(args, "fork", False) or getattr(args, "production", False):
            raise ValueError("--backtest-config cannot be combined with --fork or --production")
        return RunMode.BACKTEST
    if getattr(args, "fork", False):
        return RunMode.FORK
    if getattr(args, "production", False):
        return RunMode.PRODUCTION
    return None


def require_online_inputs(args, mode):
    if getattr(args, "gateway_config", None) is None or getattr(args, "env_file", None) is None:
        raise ValueError("online checking requires --gateway-config and --env-file")
    if mode is None:
        raise ValueError("online checking requires --fork, --production or --backtest-config")


def backtest_inputs(document, args):
    """The backtest file parsed and bound to the author accounts, without any I/O."""
    from almanak.configuration.backtest_inputs import read_backtest_inputs

    backtest = read_backtest_inputs(args.backtest_config)
    backtest.require_accounts(document.inputs)
    return backtest


def operator_context(document, args, mode, backtest=None):
    """Bind the authored choices to the operator file without any I/O."""
    from almanak.configuration.fork_preparation import require_fork_policy
    from almanak.gateway.operator_config import read_operator_config
    from almanak.supervisor_preparation import selected_anchors

    operator = read_operator_config(args.gateway_config)
    if mode is None:
        raise ValueError("--gateway-config requires --fork, --production or --backtest-config")
    if mode == RunMode.FORK:
        require_fork_policy(document, operator)
    if backtest is not None:
        anchors = backtest.inputs.anchors(document.inputs)
    else:
        anchors = selected_anchors(operator, mode, document.inputs)
    return operator, document.inputs.context(operator, mode, anchors)


def check(args):
    stages: list[dict[str, str]] = []
    mode = selected_mode(args)
    document, definition = checked(
        stages,
        "configuration",
        getattr(args, "config", None) or getattr(args, "directory", "."),
        lambda: author_inputs(args, mode),
    )
    if getattr(args, "online", False):
        require_online_inputs(args, mode)
    backtest = None
    if mode == RunMode.BACKTEST:
        backtest = checked(
            stages,
            "backtest_configuration",
            args.backtest_config,
            lambda: backtest_inputs(document, args),
        )
    warnings = author_warnings(document, mode)
    defaults = gas_defaults(document)
    try:
        report = staged_check(document, definition, args, stages, mode, backtest)
    except CheckFailure as failure:
        prepend_warnings(failure.detail, warnings)
        failure.detail.update(defaults)
        raise
    prepend_warnings(report, warnings)
    report.update(defaults)
    return report


def gas_defaults(document) -> dict:
    """The gas policy values the SDK supplied because the author file omits them."""
    from almanak.configuration.gas import DEFAULTED_FIELDS

    authored = (document.source.value.get("strategy") or {}).get("gas") or {}
    entries = [
        {"field": f"strategy.gas.{name}.{field}", "value": getattr(policy, field)}
        for name, policy in document.inputs.strategy.gas.items()
        for field in DEFAULTED_FIELDS
        if field not in (authored.get(name) or {})
    ]
    return {"gas_defaults": entries} if entries else {}


def prepend_warnings(report, warnings):
    if warnings:
        report["warnings"] = [*warnings, *report.get("warnings", ())]


def staged_check(document, definition, args, stages, mode, backtest):
    from almanak.configuration.preparation import check_preparation

    normalized = getattr(args, "normalized", False)
    if getattr(args, "gateway_config", None) is None:
        return check_preparation(document, definition, normalized=normalized, stages=stages)
    operator, context = checked(
        stages,
        "operator_configuration",
        args.gateway_config,
        lambda: operator_context(document, args, mode, backtest),
    )
    if not getattr(args, "online", False):
        return check_preparation(document, definition, normalized=normalized, stages=stages)
    return online_check(document, definition, operator, context, args, stages, backtest)


def author_warnings(document, mode):
    """Advisory findings from the author file alone; a check without a mode also names
    the mode-bound rules it cannot refuse."""
    from almanak.check_warnings import decision_interval_warnings, offline_warnings, warning

    result = []
    if mode in (None, RunMode.PRODUCTION):
        # Fork and backtest runs advance or simulate time; only live runs pay per decision.
        result.extend(decision_interval_warnings(document.inputs))
    if mode is None and not document.inputs.strategy.gas:
        result.append(
            warning(
                "production_gas",
                "strategy.gas",
                "Production requires an explicit author gas policy; "
                "check with --production to enforce it.",
            )
        )
    return result + offline_warnings(document.inputs)


def online_check(document, definition, operator, context, args, stages, backtest=None):
    """Resolve current facts through one read-only gateway; hooks run once on them."""
    from almanak.configuration.bootstrap import read_progress, resolve_configuration
    from almanak.configuration.preparation import prepared_report
    from almanak.configuration.read_gateway import ReadGateway

    # The sandbox stage claims the preload worker; it boots while the gateway does.
    with tempfile.TemporaryDirectory(prefix="almanak-check-") as temporary, started_workers():
        private = Path(temporary) / "private"
        gateway = ReadGateway(private, args.gateway_config, context, args.env_file, context.mode)
        with gateway as client:
            path = private / "resolution.json"
            checked(
                stages,
                "resolution",
                args.gateway_config,
                lambda: resolve_configuration(path, context, client),
            )
            progress = read_progress(path)
            report = prepared_report(
                document, definition, progress, stages, normalized=args.normalized
            )
            findings = market_findings(
                report["_prepared"].parameters, operator, args, progress.context.mode
            )
            prepend_warnings(report, findings)
            try:
                mode_stage(stages, args, operator, progress, report, client, backtest, definition)
            except CheckFailure as failure:
                # Funding depends only on the resolved parameters, which passed, so a
                # refused operator policy or fixture still reports what to fund.
                if "funding" in report:
                    failure.detail["funding"] = report["funding"]
                prepend_warnings(failure.detail, findings)
                raise
    report.pop("_prepared")
    stages.append(
        stage(
            "online_admission",
            "not_checked",
            "Operator execution ceilings, fixture and installed authority remain unadmitted.",
        )
    )
    report["preview"] = preview(operator, progress)
    return report


def market_findings(parameters, operator, args, mode):
    """Gas price warnings at the market read now through the operator's providers.

    Backtests have no current market. These reads run outside any stage, so nothing
    they raise may escape: an unexpected failure, including closing a reader, becomes
    one warning and keeps every finding made before it, as a failed read never refuses
    a check.
    """
    from almanak.check_warnings import market_gas_warnings, warning

    warnings: list[dict] = []
    if mode == RunMode.BACKTEST:
        return warnings
    try:
        with ExitStack() as stack:
            reads = MarketReads(stack, operator, args.env_file)
            warnings = market_gas_warnings(
                getattr(parameters, "gas", ()), reads.gas_price, reads.window_bid, reads.l1_fee
            )
    except Exception as error:
        warnings = warnings + [
            warning(
                "market_read",
                "strategy",
                f"the current gas price was not fully checked: {type(error).__name__}: {error}",
            )
        ]
    return warnings


class MarketReads:
    """Latest-block reads through the operator's providers, one reader per chain."""

    def __init__(self, stack, operator, env_file):
        self.stack, self.operator, self.env_file = stack, operator, env_file
        self.readers: dict = {}
        self.latest: dict = {}

    def reader(self, chain):
        from almanak.gateway.providers import provider_endpoint
        from almanak.gateway.read_endpoints import mainnet_reader

        if chain not in self.readers:
            endpoint = provider_endpoint(self.operator.network(chain).binding(), self.env_file)
            self.readers[chain] = self.stack.enter_context(mainnet_reader(chain, endpoint))
        return self.readers[chain]

    def pinned(self, chain):
        if chain not in self.latest:
            self.latest[chain] = self.reader(chain).pin_block("latest")
        return self.reader(chain), self.latest[chain]

    def gas_price(self, policy) -> int:
        """The legacy gas price execution would quote now under this policy."""
        from almanak.gateway.production_fees import quoted_price

        reader, block = self.pinned(policy.asset.chain)
        return quoted_price(reader, policy, block)[0]

    def window_bid(self, policy) -> int:
        """The highest bid of this policy over the check's window ending at the pinned block."""
        from almanak.check_warnings import WINDOW_BLOCKS
        from almanak.gateway.production_fees import window_bid

        reader, block = self.pinned(policy.asset.chain)
        return window_bid(reader, policy, block, WINDOW_BLOCKS)

    def l1_fee(self, policy) -> int:
        """The OP Stack L1 data and operator fee bound admission applies to a transaction of
        max_gas_limit gas, sized for L1_DATA_BYTES of calldata at the pinned block."""
        from almanak.check_warnings import L1_DATA_BYTES
        from almanak.gateway.op_fees import exposure_bound

        reader, block = self.pinned(policy.asset.chain)
        multiplier = policy.base_fee_multiplier_bps
        return exposure_bound(reader, block, policy.max_gas_limit, L1_DATA_BYTES, multiplier)


def mode_stage(stages, args, operator, progress, report, client, backtest=None, definition=None):
    if backtest is not None:
        settings = checked(
            stages,
            "backtest_model",
            args.backtest_config,
            lambda: backtest_model(backtest, operator, progress, report["_prepared"].parameters),
        )
        report["backtest"] = settings.model_dump(mode="json")
        return
    checked(
        stages,
        "pool_liquidity",
        args.gateway_config,
        lambda: pool_liquidity(progress.result.markets, operator, args.env_file),
    )
    if progress.context.mode == RunMode.FORK:
        from almanak.gateway.fixture_preparation import compile_fixture

        plan = checked(
            stages,
            "fixture",
            args.gateway_config,
            lambda: compile_fixture(progress.context, operator, progress.result.anchors),
        )
        report["fixture_digest"] = plan.fixture_digest
        checked(
            stages,
            "fork_compatibility",
            args.gateway_config,
            lambda: fork_compatibility(plan, progress, args.env_file),
        )
        return
    production_stage(stages, args, progress, report, client, definition)


def pool_liquidity(markets, operator, env_file):
    """Refuse a Curve market whose pool is unfunded at the latest block.

    A backtest refuses the same pool when it prepares (`curve/pool_report.py`); fork and
    production checks read it here through the operator's providers at the chain head,
    the state a new deployment would enter. Other markets read nothing.
    """
    from almanak.connectors.curve.pool_report import require_funded
    from almanak.connectors.curve.resolution import ResolvedPool

    curve = [market for market in markets if isinstance(market.resolved, ResolvedPool)]
    with ExitStack() as stack:
        reads = MarketReads(stack, operator, env_file)
        for market in curve:
            reader, block = reads.pinned(market.resolved.block.chain.chain_id)
            require_funded(reader, market.name, market.resolved, block)


def fork_compatibility(plan, progress, env_file):
    """Refuse, before admission, a contract the local fork cannot execute."""
    from almanak.gateway.fork_compatibility import check_provider_code

    check_provider_code(plan, progress.result.markets, env_file)


def production_stage(stages, args, progress, report, client, definition):
    """Operator gas compatibility, then the Safe permissions admission will require."""
    from almanak.gateway.policy_resolution import GasCompatibility
    from almanak.production_setup import requires_permissions

    prepared = report["_prepared"]

    def policy():
        parameters = prepared.parameters
        result = GasCompatibility.model_validate_json(
            json.dumps(
                client.request(
                    "resolution-gas-policy",
                    context_digest=progress.context.digest,
                    policies=[p.model_dump(mode="json") for p in parameters.gas],
                )
            )
        )
        result.require(progress.context, parameters.accounts, parameters.gas)
        return result

    compatibility = checked(stages, "policy", args.gateway_config, policy)
    report["ceilings"] = [c.model_dump(mode="json") for c in compatibility.ceilings]
    if requires_permissions(prepared.parameters):
        report["permissions"] = checked(
            stages,
            "permissions",
            args.gateway_config,
            lambda: permission_statement(
                args.env_file, definition, progress, prepared, compatibility
            ),
        )


def permission_statement(env_file, definition, progress, prepared, compatibility):
    """Generate Safe permissions as admission will, over this check's resolution and policy.

    The configuration is the one provisioning would freeze from the same preparation, and
    generation reuses the capabilities the hook already returned. Nothing is exported or
    retained: the owner installs from the statement, and admission generates and
    verifies again against the deployment's own resolution.
    """
    from almanak.configuration.admission import PreparedDeployment
    from almanak.configuration.prepared import PreparationProgress
    from almanak.connectors.composition import connector_for
    from almanak.gateway.permissions import generate, statement
    from almanak.gateway.production_cli import production_routes

    preparation = PreparationProgress(
        prepared=prepared, prepared_digest=prepared.digest, policy=compatibility
    )
    config = PreparedDeployment(resolution=progress, preparation=preparation).configuration()
    backend = production_routes(config, env_file)
    try:
        manifest = generate(
            config,
            definition,
            backend,
            connector=connector_for(config).permissions,
            prepared=prepared,
        )
    finally:
        backend.close()
    return statement(manifest)


def backtest_model(backtest, operator, progress, parameters):
    """The connector's initial simulation state over the resolved parameters."""
    from almanak.configuration.backtest_inputs import history_bindings, resolve_backtest
    from almanak.connectors.composition import connector_for
    from almanak.triggers import IntervalSelection

    chains = tuple(anchor.chain for anchor in progress.context.anchors)
    settings = resolve_backtest(
        backtest, parameters, history_bindings(operator, chains), operator.vendors
    )
    interval = IntervalSelection(
        configured=parameters.decision_trigger,
        override_seconds=settings.schedule.decision_interval_override_seconds,
    )
    binding = connector_for(parameters).backtest(settings.market_model)
    config = binding.parameters(parameters, settings, interval)
    binding.initial(settings, config, interval)
    return settings


def preview(operator, progress):
    """Resolved identities only; evidence bodies and credentials never appear."""
    result = progress.result
    bindings = {b.name: b for b in result.binding.accounts.bindings}
    return {
        "anchors": [c.model_dump(mode="json") for c in result.anchors.checkpoints],
        "markets": [
            {"name": market.name, **market.resolved.summary()} for market in result.markets
        ],
        "accounts": [
            {
                "name": requirement.name,
                "chain": str(requirement.chain),
                "binding": bindings[requirement.name].profile.kind,
                "fund": bindings[requirement.name].profile.fund.address,
            }
            for requirement in progress.context.accounts
        ],
        "operator_digest": operator.digest,
    }
