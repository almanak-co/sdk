"""Freeze one invocation's inputs before validation or provider access.

Public author roots resolve market facts through the read gateway once per
invocation and retain resolution.json beside every run or dataset that consumes
those facts, so offline continuation replays exactly what created the run.
"""

from pathlib import Path
from typing import Any, NamedTuple

from almanak.configuration.backtest_inputs import (
    BacktestDocument,
    history_bindings,
    read_backtest_inputs,
    resolve_backtest,
)
from almanak.configuration.bootstrap import (
    ResolutionProgress,
    read_progress,
    resolve_configuration,
)
from almanak.configuration.parameters import construct_parameters, replay_facts
from almanak.configuration.read_gateway import ReadGateway
from almanak.configuration.resolution import ReadContext
from almanak.configuration.unresolved import InputDocument, read_inputs
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import ResolutionResult
from almanak.gateway.operator_config import OperatorConfig, read_operator_config
from almanak.models import Model
from almanak.strategy import StrategyDefinition
from almanak.typed_parameters import model_fields
from almanak.workspace import save


class HarnessSelection(NamedTuple):
    """Typed archived fixture inputs whose parameters are already resolved."""

    author: Any
    settings: Any
    strategy: StrategyDefinition | None

    @property
    def market_model(self):
        return self.settings.market_model

    @property
    def connector(self):
        from almanak.connectors.composition import connector_for

        return connector_for(self.author)

    @property
    def resolution_facts(self):
        return None

    @property
    def resolution_identity(self):
        return None

    def settings_view(self):
        return self.settings.model_dump(mode="json")

    def schedule_view(self):
        return self.settings.schedule.model_dump(mode="json")

    def authored_view(self):
        return self.author.model_dump(mode="json")

    def resolve(self, directory, env_file):
        return self.author, self.settings


class BacktestSelection:
    """A public root: facts resolve once per invocation and stay retained with the run."""

    def __init__(
        self,
        document: InputDocument,
        backtest: BacktestDocument,
        operator: OperatorConfig,
        definition: StrategyDefinition | None,
        gateway_config: Path,
        dataset: Path | None = None,
    ):
        backtest.require_accounts(document.inputs)
        self.document, self.backtest, self.operator = document, backtest, operator
        self.strategy, self.gateway_config, self.dataset = definition, gateway_config, dataset
        self.anchors = backtest.inputs.anchors(document.inputs)
        self.progress: ResolutionProgress | None = None
        self.result: ResolutionResult | None = None
        self.resolved: tuple[Model, Any] | None = None

    @property
    def market_model(self):
        return self.backtest.inputs.mode

    @property
    def connector(self):
        """One connector when the markets agree; otherwise the composed codec."""
        from almanak.connectors.composition import connector_from_wire

        tags = {market.connector for market in self.document.inputs.markets.values()}
        return connector_from_wire({"connector": tags.pop() if len(tags) == 1 else None})

    @property
    def resolution_facts(self):
        return None if self.result is None else self.result.facts_digest

    @property
    def resolution_identity(self):
        """The complete retained result identity, separate from reusable market facts."""
        return None if self.result is None else self.result.digest

    def settings_view(self):
        inputs = self.backtest.inputs
        return {
            "market_model": inputs.mode,
            "schedule": inputs.schedule(),
            "inventory": inputs.inventory.model_dump(mode="json"),
            "models": inputs.execution.models,
            "data": inputs.data,
            "costs": inputs.costs,
            "operator": self.operator.digest,
        }

    def schedule_view(self):
        return self.backtest.inputs.schedule()

    def authored_view(self):
        return self.document.inputs.model_dump(mode="json")

    def resolve(self, directory, env_file) -> tuple[Model, Any]:
        directory = Path(directory).resolve()
        if self.resolved is None:
            progress = self.acquire(directory, env_file)
            parameters = construct_parameters(self.document, progress)
            self.progress, self.result = progress, progress.result
            chains = tuple(anchor.chain for anchor in self.anchors)
            history = history_bindings(self.operator, chains)
            from almanak.backtest.strategy_prices import with_strategy_prices

            settings = resolve_backtest(self.backtest, parameters, history, self.operator.vendors)
            settings = with_strategy_prices(
                settings, self.strategy, parameters, self.operator.vendors
            )
            self.resolved = (parameters, settings)
        self.retain(directory / "resolution.json")
        return self.resolved

    def acquire(self, directory: Path, env_file) -> ResolutionProgress:
        for path in self.retained_paths(directory):
            if path.exists():
                progress = self.retained(path)
                if progress.result is not None:
                    return progress
                if env_file is None:
                    raise ValueError(
                        "incomplete retained resolution requires --env-file to continue"
                    )
                return self.continue_resolution(directory, path, progress.context, env_file)
        if env_file is None:
            raise ValueError(
                "a public author root requires --env-file to resolve market facts, "
                "or a retained resolution.json"
            )
        context = self.document.inputs.context(self.operator, RunMode.BACKTEST, self.anchors)
        path = directory / "resolution.json"
        return self.continue_resolution(directory, path, context, env_file)

    def continue_resolution(
        self, directory: Path, path: Path, context: ReadContext, env_file
    ) -> ResolutionProgress:
        gateway = ReadGateway(
            directory / "private", self.gateway_config, context, env_file, RunMode.BACKTEST
        )
        with gateway as client:
            resolve_configuration(path, context, client)
        return read_progress(path)

    def retained_paths(self, directory: Path):
        paths = [directory / "resolution.json"]
        if self.dataset is not None:
            paths.append(Path(self.dataset).resolve() / "resolution.json")
        return paths

    def retained(self, path: Path) -> ResolutionProgress:
        progress = read_progress(path)
        try:
            if (
                progress.context.mode != RunMode.BACKTEST
                or progress.context.anchors != self.anchors
            ):
                raise ValueError("retained resolution belongs to another mode or backtest window")
            self.document.inputs.require_context(progress.context)
            progress.context.match_operator(self.operator, RunMode.BACKTEST)
        except ValueError as error:
            raise ValueError(stale(error, path)) from error
        return progress

    def retain(self, path: Path):
        """Every directory consuming the facts keeps a copy with identical facts."""
        if path.exists():
            result = read_progress(path).result
            if (
                result is None
                or result.facts_digest != self.resolution_facts
                or result.digest != self.resolution_identity
            ):
                raise ValueError(
                    stale("retained resolution differs from the resolved result", path)
                )
            return
        assert self.progress is not None
        path.parent.mkdir(parents=True, exist_ok=True)
        save(path, self.progress.model_dump(mode="json"))


def stale(error, path: Path) -> str:
    """Name the recovery for a resolution an earlier or interrupted preparation left."""
    directory = path.parent
    return (
        f"{error}: {path} was retained by an earlier or interrupted preparation with other "
        f"inputs. Prepare into a fresh --output directory, or remove {directory} and "
        "prepare again."
    )


def retained(args):
    existing = getattr(args, "_backtest_inputs", None)
    if existing is None or isinstance(existing, (HarnessSelection, BacktestSelection)):
        return existing
    return HarnessSelection(*existing)


def selected(args):
    existing = retained(args)
    if existing is not None:
        return existing
    config = Path(args.config)
    gateway_config = getattr(args, "gateway_config", None)
    if gateway_config is None:
        raise ValueError("backtesting requires --gateway-config with operator settings")
    selection: HarnessSelection | BacktestSelection = BacktestSelection(
        read_inputs(config, fields=model_fields(getattr(args, "strategy", None))),
        read_backtest_inputs(Path(args.backtest_config)),
        read_operator_config(Path(gateway_config)),
        strategy_definition(args),
        Path(gateway_config).resolve(),
        getattr(args, "dataset", None),
    )
    args._backtest_inputs = selection
    return selection


def strategy_definition(args):
    path = getattr(args, "strategy", None)
    return StrategyDefinition.read(path) if path is not None else None


def strategy_source(args):
    existing = retained(args)
    return existing.strategy if existing is not None else StrategyDefinition.read(args.strategy)


def frozen(args, author, settings):
    """Direct callers supply resolved inputs; one selection then serves the invocation."""
    existing = retained(args)
    if existing is None:
        existing = HarnessSelection(author, settings, strategy_source(args))
        args._backtest_inputs = existing
    return existing


def require_facts(directory, manifest):
    """A frozen run continues only over facts that replay exactly from retained evidence."""
    if manifest.resolution_facts is None:
        if manifest.resolution_identity is not None:
            raise ValueError("frozen run has a resolution identity without resolved facts")
        return
    if manifest.resolution_identity is None:
        raise ValueError("frozen run lacks its retained resolution identity")
    path = Path(directory) / "resolution.json"
    if not path.exists():
        raise ValueError("frozen run requires its retained resolution.json")
    result = read_progress(path).result
    if result is None:
        raise ValueError("retained resolution is incomplete")
    replay_facts(result)
    if result.facts_digest != manifest.resolution_facts:
        raise ValueError("retained resolution facts differ from the frozen run")
    if result.digest != manifest.resolution_identity:
        raise ValueError("retained resolution identity differs from the frozen run")
    require_positions(manifest.config, result)


def require_positions(config, result):
    """Frozen positions stay inside the retained market chains and asset precisions."""
    chains = {market.resolved.block.chain for market in result.markets}
    for position in config.positions:
        if position.chain not in chains:
            raise ValueError("frozen positions differ from the retained resolved markets")
    for market in result.markets:
        require_precision(config, market.resolved.assets)


def require_precision(config, assets):
    for asset in assets:
        metadata = config.asset_metadata.get(asset.identifier)
        if metadata is None or metadata.decimals != asset.decimals:
            raise ValueError("frozen positions differ from the retained resolved markets")
