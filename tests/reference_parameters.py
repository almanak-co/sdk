"""A reference's own `strategy.parameters`, for tests that build its config by hand."""

import json
from collections.abc import Mapping
from pathlib import Path

import yaml

from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.typed_parameters import TypedParameters, parameters_type, typed

STRATEGIES = Path(__file__).parents[1] / "strategies"


def tupled(values: Mapping) -> StrategyParameters:
    """`values` as resolution passes them: lists arrive as tuples."""
    return StrategyParameters(
        {key: tuple(value) if isinstance(value, list) else value for key, value in values.items()}
    )


def authored_values(name: str, filename: str = "config.yaml") -> StrategyParameters:
    """The values `config.yaml` declares, as resolution passes them."""
    raw = yaml.safe_load((STRATEGIES / name / filename).read_text())
    return tupled(raw["strategy"].get("parameters") or {})


def authored_parameters(
    name: str, filename: str = "config.yaml", **overrides
) -> StrategyParameters | TypedParameters:
    """The values `config.yaml` declares, as each hook reads them.

    A reference with a typed parameter model gets its validated instance, which still
    dumps as the authored values; any other gets the mapping, lists as tuples.
    """
    values = tupled({**authored_values(name, filename), **overrides})
    source = (STRATEGIES / name / "strategy.py").read_text()
    namespace: dict = {"__name__": "__strategy__"}
    exec(compile(source, "strategy.py", "exec"), namespace)
    selected = parameters_type(namespace)
    if selected is None:
        return values
    model = selected.model_validate_json(json.dumps(dict(values)))
    model._values = values
    return model


class TypedHooks:
    """A reference module whose functions get its typed parameters, as the sandbox passes them.

    Tests that call a reference in-process skip the worker, which is where a strategy's
    `TypedParameters` model replaces the mapping. Every function called through this
    gets each config argument (or a context's config) with the model in place, so a
    value the model refuses raises `ParametersRefused`, a ValueError naming the field.
    Pydantic configs are copied with `model_copy`; test stand-ins by their attributes.
    """

    def __init__(self, module):
        self._module = module

    def typed_config(self, config):
        if not isinstance(getattr(config, "parameters", None), Mapping):
            return config
        if hasattr(config, "model_copy"):
            return typed(vars(self._module), config)
        holder = typed(vars(self._module), _Holder(config.parameters))
        return type(config)(**{**vars(config), "parameters": holder.parameters})

    def typed_argument(self, value):
        if isinstance(getattr(value, "parameters", None), Mapping):
            return self.typed_config(value)
        config = getattr(value, "config", None)
        if not isinstance(getattr(config, "parameters", None), Mapping):
            return value
        if hasattr(value, "model_copy"):
            return value.model_copy(update={"config": self.typed_config(config)})
        return type(value)(**{**vars(value), "config": self.typed_config(config)})

    def __setattr__(self, name, value):
        # A test that patches the reference patches the module its functions read.
        if name == "_module":
            object.__setattr__(self, name, value)
        else:
            setattr(self._module, name, value)

    def __getattr__(self, name):
        value = getattr(self._module, name)
        if not callable(value) or isinstance(value, type):
            return value

        def call(*args, **options):
            return value(*(self.typed_argument(a) for a in args), **options)

        return call


class _Holder:
    def __init__(self, parameters):
        self.parameters = (
            parameters if isinstance(parameters, StrategyParameters) else tupled(parameters)
        )

    def model_copy(self, update):
        result = _Holder({})
        result.parameters = update["parameters"]
        return result
