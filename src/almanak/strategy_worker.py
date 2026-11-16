import json
import os
import sys
from typing import Any

from almanak.connectors.composition import connector_from_wire
from almanak.models import Model
from almanak.strategy import (
    DataDependencyRequest,
    EconomicDecisionRequest,
    EconomicOutcomeRequest,
    EconomicValidationRequest,
    EventRequest,
    OutcomeRequest,
    ValidationRequest,
)
from almanak.strategy_api import Memory
from almanak.strategy_refusal import author_traceback, refusal, refusal_document, register_source
from almanak.worker_policy import restrict_author


def request_kind(flag):
    return {
        "--validate-config": "validate",
        "--author-check": "validate",
        "--outcome": "outcome",
        "--event": "event",
        "--economic-outcome": "outcome",
        "--economic-validate": "validate",
        "--data-dependencies": "dependencies",
        "--economic-data-dependencies": "dependencies",
    }.get(flag, "decision")


def request_config(wire, kind):
    return wire["config"] if kind in ("validate", "dependencies") else wire["context"]["config"]


def request_binding(config, flag):
    """Resolve the trusted connector binding; this never evaluates strategy source."""
    economic = flag.startswith("--economic") or flag == "--event"
    connector = connector_from_wire(config)
    codec = connector.codec
    return codec, connector.backtest("economic") if economic else codec


def read_request():
    """The mode, the raw request and its connector binding, read before any limit applies.

    Importing the connector is trusted SDK work, so it runs before the author's budget starts.
    """
    flag = sys.argv[1] if len(sys.argv) > 1 else ""
    raw = sys.stdin.read()
    return flag, raw, prepared(flag, raw)


def prepared(flag, raw):
    """The request's connector binding, or None when it is answered without one."""
    if flag == "--parameter-fields":
        return None
    kind = request_kind(flag)
    config = request_config(json.loads(raw), kind)
    if kind in ("validate", "dependencies") and "author_accounts" in config:
        return None
    return request_binding(config, flag)


def evaluate_request(flag, raw, bound):
    """Print the mode's answer, or the typed model's refusal from any mode that types it."""
    from almanak.typed_parameters import ParametersRefused

    try:
        answer_request(flag, raw, bound)
    except ParametersRefused as exc:
        print(json.dumps(refusal_document(str(exc), field=exc.field)))


def answer_request(flag, raw, bound):
    if flag == "--parameter-fields":
        print(parameter_fields(raw))
        return
    kind = request_kind(flag)
    if bound is None:
        validate_author(
            json.loads(raw),
            dependencies=kind == "dependencies",
            capabilities=flag == "--author-check",
        )
        return
    codec, binding = bound
    request = binding.request_type(kind).model_validate_json(raw)
    definition = request.strategy
    namespace = load(definition)
    if isinstance(request, DataDependencyRequest):
        print(dependencies_for(namespace, request.config).model_dump_json())
        return
    if isinstance(request, (ValidationRequest, EconomicValidationRequest)):
        print(validate(namespace, request.config))
        return
    print(evaluate(namespace, request, codec, binding).model_dump_json())


def parameter_fields(raw) -> str:
    """The fields of the strategy's typed parameter model, or `null` without one.

    A strategy that fails to load or to select one model reads as untyped here; its
    strategy stage reports the failure.
    """
    from almanak.strategy import StrategyDefinition
    from almanak.typed_parameters import parameters_type

    try:
        selected = parameters_type(load(StrategyDefinition.model_validate_json(raw)))
    except Exception:
        return "null"
    return json.dumps(None if selected is None else sorted(selected.model_fields))


# The loaded strategy's file name, so a crash report can start at its first frame.
AUTHOR_FILE: list[str] = []


def load(definition) -> dict[str, Any]:
    from almanak.strategy import install_modules

    AUTHOR_FILE[:] = [definition.filename]
    modules = install_modules(definition)
    namespace: dict[str, Any] = {
        "__name__": "__strategy__",
        "__file__": definition.filename,
        "__author_modules__": modules,
    }
    register_source(definition)
    exec(compile(definition.source, definition.filename, "exec"), namespace)
    return namespace


def evaluate(namespace, request, codec, binding):
    expected: type[Model]
    from almanak.memory_codec import freeze, hydrate
    from almanak.typed_parameters import typed_context

    context = typed_context(namespace, request.context)
    context = context.model_copy(update={"memory": hydrate(namespace, context.memory)})
    if isinstance(request, (OutcomeRequest, EventRequest, EconomicOutcomeRequest)):
        hook = namespace.get("on_event" if isinstance(request, EventRequest) else "on_outcome")
        result = freeze(namespace, context.memory if hook is None else hook(context))
        expected = Memory
    else:
        result = namespace["decide"](context)
        result = result.model_copy(update={"memory": freeze(namespace, result.memory)})
        expected = codec.decision
        if isinstance(request, EconomicDecisionRequest):
            expected = binding.decision_type
            if not isinstance(result, (codec.decision, expected)):
                raise TypeError("economic strategy must return a typed decision")
            result = expected.model_validate_json(result.model_dump_json())
    if not isinstance(result, expected):
        raise TypeError(f"strategy must return {expected.__name__}")
    return result


def validate(namespace, config) -> str:
    """`null` for accepted parameters, or the author's `invalid_config` refusal document."""
    from almanak.memory_codec import freeze, hydrate
    from almanak.typed_parameters import typed

    freeze(namespace, hydrate(namespace, Memory()))
    config = typed(namespace, config)
    if "validate_config" not in namespace:
        return "null"
    hook = namespace["validate_config"]
    if not callable(hook):
        raise TypeError("validate_config must be callable")
    try:
        result = hook(config)
    except ValueError as exc:
        if (document := refusal(exc, hook, namespace["__file__"])) is None:
            raise
        return json.dumps(document)
    if result is not None:
        raise TypeError("validate_config must return None or raise an exception")
    return "null"


def main():
    try:
        request = read_request()
        restrict_author()
        evaluate_request(*request)
    except Exception as exc:
        # A crash is reported from the strategy's first frame; the SDK frames above it
        # are the same for every strategy and push the author's line out of the tail.
        sys.stdout.flush()
        sys.stderr.write(author_traceback(exc, AUTHOR_FILE[0] if AUTHOR_FILE else None))
        sys.stderr.flush()
        os._exit(1)


def validate_author(wire, *, dependencies=False, capabilities=False):
    from almanak.configuration.parameter_codec import parameters_from_wire
    from almanak.strategy import StrategyDefinition

    definition = StrategyDefinition.model_validate(wire["strategy"])
    namespace = load(definition)
    config = parameters_from_wire(wire["config"])
    if dependencies:
        print(dependencies_for(namespace, config).model_dump_json())
        return
    if not capabilities:
        print(validate(namespace, config))
        return
    validate_capabilities(namespace, config)
    print("null")


def dependencies_for(namespace, config):
    from almanak.backtest.dependencies import DataDependencies
    from almanak.typed_parameters import typed

    hook = namespace.get("data_dependencies")
    result = DataDependencies() if hook is None else hook(typed(namespace, config))
    if not isinstance(result, DataDependencies):
        raise TypeError("data_dependencies must return DataDependencies")
    return result


def validate_capabilities(namespace, config):
    from almanak.author_capabilities import DeclaredCapabilities
    from almanak.typed_parameters import typed

    hook = namespace.get("capabilities")
    if not callable(hook):
        raise TypeError("strategy must define capabilities(config)")
    declaration = hook(typed(namespace, config))
    if not isinstance(declaration, DeclaredCapabilities):
        raise TypeError("author capabilities must use the public declared(config) helper")
    declaration.validate_config(config)


if __name__ == "__main__":
    main()
