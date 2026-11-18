"""Small trusted connector contracts; dispatch never imports strategy-selected code."""

from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Protocol

from pydantic import TypeAdapter

from almanak.caip import ChainId
from almanak.capabilities import CapabilityCodec
from almanak.connectors.gas_lifecycle import GasLifecycle
from almanak.connectors.identifiers import ProtocolVariant
from almanak.connectors.support import ConnectorChainSupport, RunMode
from almanak.effects import AssetDelta
from almanak.gateway.accounts import ContractCall
from almanak.models import Block, Model


class Lifecycle(Protocol):
    def initial(self, config, strategy, capabilities, permissions) -> Model: ...

    def validate_initial(self, state, observations) -> None: ...

    def validate_owned(self, state, observations) -> None: ...

    def commit_owned(self, owned, outcome) -> tuple: ...

    def ownership(self, state, outcome=None) -> dict: ...

    def validate_closure(self, state, observations) -> None: ...

    def increases_exposure(self, intent) -> bool: ...


class ForkNodes(Protocol):
    """A fork test's nodes: each chain's fork RPC, origin block and origin-chain reader,
    and the record of each venue process the workspace runs beside them."""

    def rpc(self, chain) -> Any: ...

    def origin(self, chain) -> Any: ...

    def reader(self, chain) -> Any: ...

    def venue(self, name: str) -> dict: ...


class Requests(Protocol):
    """Connector-owned asynchronous work retained after transaction settlement."""

    def unresolved(self, state) -> bool: ...

    def reconcile(self, backend, state) -> Model | None: ...

    def fork_keeper(self, forks: ForkNodes, output) -> Any:
        """A fork test's settler of requests the protocol's own keepers would execute."""
        ...

    def cancellation_plan(
        self, backend, state, attempt_id: str, *, reference=None
    ) -> Model | None: ...

    def is_cancellation(self, state, operation) -> bool: ...

    def project(self, state, chain) -> dict: ...

    def merge(self, state, changed, chain) -> dict: ...

    def audit(self, backend, state) -> None: ...


class Execution(Protocol):
    def validate_replacement(self, config, operation) -> None: ...

    def requirements(self, config, account) -> tuple: ...

    gas_lifecycle: GasLifecycle

    def entry_transactions(self, config, account) -> int:
        """Transactions one entry of the account's positions can submit, through the last
        that adds exposure."""
        ...

    def preflight(self, config, account, observations) -> tuple: ...

    def validate_config(self, config) -> None: ...

    def snapshot(self, backend, name: str, block: Block, reference=None) -> Model: ...

    def operation(self, config, state, intent, before) -> Model: ...

    def call(self, config, operation) -> ContractCall: ...

    def validate_operation(self, backend, state, operation, before) -> None: ...

    def validate_simulation(self, operation, result: str, before) -> None: ...

    def validate_current(self, backend, plan, current) -> None: ...

    def plan_evidence(self, backend, operation, call, before) -> dict: ...

    def current_checks(self, backend, plan, block=None) -> None: ...

    def reconcile(self, backend, pending, receipt: dict, block: Block) -> Model: ...

    def increases_exposure(self, config, operation) -> bool: ...


class ExternalEffects(Protocol):
    """Exact canonical scan pages and unsolicited effects, without submitted intents."""

    page_type: Any

    def reconcile(self, backend, state, *, reference=None) -> Model | None: ...

    def audit(self, backend, state) -> None: ...


class Historical(Protocol):
    settings_type: Any
    state_type: Any
    manifest_type: Any

    def parameters(self, author, settings, interval): ...

    def initial(self, settings, config, interval): ...

    def capability(self, chain): ...

    def contracts(self, settings, config): ...

    def model(self, config, settings, strategy, histories, baselines=(), authority=None): ...

    def baselines(self, model, initial): ...

    def report(self, state, model): ...


class Prepared(Protocol):
    settings_type: Any
    decision_type: Any

    def request_type(self, mode): ...

    def prepare(self, args): ...

    def create(self, args, author, settings): ...

    def resume(self, args): ...

    def reproduce(self, args): ...

    def dataset_directory(self, args, author, settings): ...


class Projection(Protocol):
    def dashboard(self, state, *, captured_at=None): ...

    def report(self, state) -> dict: ...


class WireModel(Protocol):
    """Construct or decode one trusted model, including a typed model union."""

    def __call__(self, **values: Any) -> Model: ...

    def model_validate_json(self, value: str) -> Model: ...


@dataclass(frozen=True)
class Codec:
    parameters: TypeAdapter
    parameter_type: Any
    config: type[Model]
    checkpoint: type[Model]
    snapshot: type[Model] | WireModel
    intent: Any
    decision: type[Model]
    decision_input: type[Model]
    outcome_input: type[Model]
    decision_record: type[Model] | WireModel
    plan: type[Model] | WireModel
    pending: type[Model] | WireModel
    outcome: type[Model] | WireModel
    capability: CapabilityCodec
    schema_version: int
    actions: Callable[[Any], Any]
    legacy_schemas: dict[int, bool] = field(default_factory=dict)

    def request_type(self, mode):
        from almanak.strategy import (
            DataDependencyRequest,
            DecisionRequest,
            OutcomeRequest,
            ValidationRequest,
        )

        return {
            "decision": DecisionRequest.__class_getitem__(self.decision_input),
            "outcome": OutcomeRequest.__class_getitem__(self.outcome_input),
            "validate": ValidationRequest.__class_getitem__(self.parameter_type),
            "dependencies": DataDependencyRequest.__class_getitem__(self.parameter_type),
        }[mode]


@dataclass(frozen=True)
class Connector:
    codec: Codec
    lifecycle: Lifecycle
    execution: Execution
    permissions: Any
    projection: Projection
    fork: Any
    # Backtest bindings as "module:class" (the economic one a `Prepared`), imported only
    # when a backtest selects one, so a live or fork decision never imports their dataset,
    # journal and replay modules.
    historical: str | None = None
    economic: str | None = None
    chain_support: tuple[ConnectorChainSupport, ...] = ()
    execution_effects: Callable[[Any], tuple[AssetDelta, ...]] | None = None
    requests: Requests | None = None
    external_effects: ExternalEffects | None = None
    variant: Callable[[Any], ProtocolVariant | None] = lambda target: None

    def require_target(self, chain, target, mode, action=None, account_profile=None, *, model=None):
        support = self.support_for(chain)
        support.require(mode, action, account_profile, model=model)
        support.require_variant(self.variant(target), model=model)

    def backtest(self, mode):
        if mode not in ("historical_path", "economic"):
            raise ValueError("unsupported backtest model selection")
        binding = self.historical if mode == "historical_path" else self.economic
        if binding is None:
            raise ValueError(f"connector has no {mode} backtest implementation")
        module, name = binding.split(":")
        return getattr(import_module(module), name)()

    def support_for(self, chain: ChainId) -> ConnectorChainSupport:
        if not isinstance(chain, ChainId):
            raise ValueError("connector support requires a validated CAIP-2 chain")
        for support in self.chain_support:
            if support.chain == chain:
                return support
        raise ValueError(f"connector does not support {chain}")

    def supported_chains(self, mode: RunMode) -> tuple[ChainId, ...]:
        if not isinstance(mode, RunMode):
            raise ValueError("connector support requires a typed run mode")
        return tuple(s.chain for s in self.chain_support if mode in s.modes)
