"""An optional typed model for `strategy.parameters`, validated before any hook runs.

A strategy that subclasses `TypedParameters` once, in parameters.py (shared with
dashboard.py) or strategy.py, gets its validated
instance as `config.parameters` in every sandboxed hook, instead of the read-only
mapping. A value the model refuses is the strategy's final `invalid_config`, naming
`strategy.parameters.<field>`. Without a subclass, nothing changes.
"""

import ast
import builtins
import importlib
import json
import sys
from collections.abc import MutableMapping, MutableSequence, MutableSet
from decimal import Decimal
from pathlib import Path
from types import UnionType
from typing import Annotated, Union, get_args, get_origin

from pydantic import ConfigDict, Field, PrivateAttr, ValidationError, field_validator

from almanak.models import Model, refusals
from almanak.strategy_api import InvalidConfig

PositiveDecimal = Annotated[Decimal, Field(gt=0)]
UnitFraction = Annotated[Decimal, Field(gt=0, lt=1)]


class TypedParameters(Model):
    """Subclass once in parameters.py or strategy.py, as `StrategyParams`, to type
    `strategy.parameters`.

    Declare it as a top-level class: the SDK reads its fields from the source, and loads
    the strategy to find a model defined any other way. Each YAML scalar converts to its
    field's type the way pydantic's lax JSON mode does: `"1"` or `1.0` to an `int`, `0.2`
    or `"0.2"` to a `Decimal`, `3` to a `str`; `1.5` for an `int`, infinities and NaN
    are refused. As without a model, a `bool` field takes only `true` or `false`, and a
    number field refuses a boolean and a `_` in its digits; a tuple's elements convert
    laxly. Undeclared keys are refused, and a missing key takes its default or is
    refused when it has none. A list field is a `tuple[...]`: a mutable one is refused,
    since a hook could change it for the next.
    """

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=True, allow_inf_nan=False)

    # The authored values, so a dump of the typed config matches the untyped one.
    _values: dict = PrivateAttr(default_factory=dict)

    def authored(self) -> dict:
        return dict(self._values)

    @field_validator("*", mode="before")
    @classmethod
    def _as_without_a_model(cls, value, info):
        kind = scalar_type(cls.model_fields[info.field_name].annotation)
        if kind is bool:
            return yaml_bool(value)
        if kind in (int, float, Decimal) and (
            isinstance(value, bool) or isinstance(value, str) and "_" in value
        ):
            raise ValueError(f"Input should be a number, not {value!r}")
        return value

    def __eq__(self, other):
        # The authored values only ride along for dumps; the typed values are the identity.
        return type(other) is type(self) and self.__dict__ == other.__dict__

    __hash__ = Model.__hash__


def scalar_type(annotation):
    """The one type besides None of a union, else the annotation itself."""
    if get_origin(annotation) not in (Union, UnionType):
        return annotation
    options = [option for option in get_args(annotation) if option is not type(None)]
    return options[0] if len(options) == 1 else None


def yaml_bool(value):
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"Input should be true or false, not {value!r}")


def parameters_type(namespace) -> type[TypedParameters] | None:
    """The strategy's model: the one `TypedParameters` subclass no other one extends.

    It is found in the hook's own namespace or in the shared parameters.py module.

    A shared base the model extends, defined in strategy.py or imported, is not a second
    model.
    """
    shared = namespace.get("__author_modules__", {}).get("parameters")
    values = [*namespace.values(), *(vars(shared).values() if shared else ())]
    found = {
        value
        for value in values
        if isinstance(value, type)
        and issubclass(value, TypedParameters)
        and value is not TypedParameters
    }
    leaves = [c for c in found if not any(o is not c and issubclass(o, c) for o in found)]
    if len(leaves) > 1:
        names = ", ".join(sorted(c.__name__ for c in leaves))
        raise TypeError(
            f"strategy.py must declare one TypedParameters model, not several unrelated ones: "
            f"{names}"
        )
    return leaves[0] if leaves else None


def model_fields(path) -> frozenset[str] | None:
    """The fields of the model `parameters_type` selects, or None without one.

    Read from strategy.py's syntax tree without running it, resolving bases defined in the
    file or imported from the SDK. A base imported from anywhere else can only be
    resolved by loading the strategy, which happens in the sandbox worker. A missing or
    unparsable file is left to the strategy read that reports it.
    """
    if path is not None and (shared := Path(path).with_name("parameters.py")).is_file():
        fields = model_fields_of(shared)
        if fields is not None:
            return fields
    return model_fields_of(path)


def model_fields_of(path) -> frozenset[str] | None:
    try:
        source = Path(path).read_text()
        tree = ast.parse(source)
    except (TypeError, OSError, SyntaxError, ValueError):
        return None
    try:
        fields = _ModelTree(tree).fields()
    except _Unresolved:
        return sandboxed_fields(Path(path).name, source)
    if fields is None and "TypedParameters" in source:
        # A model under `if` or `try`, or built by type(...), only exists once loaded.
        return sandboxed_fields(Path(path).name, source)
    return fields


FOREIGN = frozenset({"pydantic", "pydantic_core", "typing_extensions", "numpy", "talib"})


class _Unresolved(Exception):
    """A class base the syntax tree alone cannot classify."""


class _ModelTree:
    def __init__(self, tree):
        self.classes = {n.name: n for n in tree.body if isinstance(n, ast.ClassDef)}
        self.imports: dict[str, tuple[str, str | None]] = {}
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module and not node.level:
                for alias in node.names:
                    self.imports[alias.asname or alias.name] = (node.module, alias.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    bound = alias.asname or alias.name.partition(".")[0]
                    self.imports[bound] = (alias.name if alias.asname else bound, None)
        self.models: dict[str, frozenset[str]] = {}
        self.seen: set[str] = set()

    def fields(self) -> frozenset[str] | None:
        models = {n: f for n in self.classes if (f := self.own_model(n)) is not None}
        bases = {b for n in models for b in self.base_names(self.classes[n])}
        leaves = [n for n in models if n not in bases]
        return models[leaves[0]] if len(leaves) == 1 else None

    @staticmethod
    def base_names(node):
        return {b.id for b in node.bases if isinstance(b, ast.Name)}

    def own_model(self, name) -> frozenset[str] | None:
        """The fields of in-file class `name` when it is a model, else None."""
        if name in self.models:
            return self.models[name]
        if name in self.seen:
            return None
        self.seen.add(name)
        node = self.classes[name]
        inherited = [self.base(b) for b in node.bases]
        models = [f for f in inherited if f is not None]
        if not models:
            return None
        if len(models) < len(inherited):
            # pydantic also takes fields from a plain mixin, which only the loaded class shows.
            raise _Unresolved
        own = {
            item.target.id
            for item in node.body
            if isinstance(item, ast.AnnAssign)
            and isinstance(item.target, ast.Name)
            and not item.target.id.startswith("_")
            and "ClassVar" not in ast.unparse(item.annotation)
        }
        self.models[name] = frozenset(own).union(*models)
        return self.models[name]

    def base(self, node) -> frozenset[str] | None:
        """The fields a base contributes when it is a model, None when it isn't one."""
        if isinstance(node, ast.Subscript):
            return self.base(node.value)
        if isinstance(node, ast.Name) and node.id in self.classes:
            return self.own_model(node.id)
        if isinstance(node, ast.Name) and node.id in self.imports:
            module, attribute = self.imports[node.id]
            return self.imported(module, attribute)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id in self.imports and self.imports[node.value.id][1] is None:
                module = self.imports[node.value.id][0]
                return self.imported(module, node.attr)
        if isinstance(node, ast.Name) and hasattr(builtins, node.id):
            return None
        raise _Unresolved

    @staticmethod
    def imported(module, attribute) -> frozenset[str] | None:
        root = module.partition(".")[0]
        # Neither the standard library nor these dependencies can extend TypedParameters.
        if root in sys.stdlib_module_names or root in FOREIGN:
            return None
        if root != "almanak":
            raise _Unresolved
        try:
            value = getattr(importlib.import_module(module), attribute or "", None)
        except Exception:
            # Such as a V2 module: loading the strategy reports it in the strategy stage.
            raise _Unresolved from None
        if not isinstance(value, type) or not issubclass(value, TypedParameters):
            return None
        return frozenset(value.model_fields)


def sandboxed_fields(filename, source) -> frozenset[str] | None:
    """The selected model's fields, from the strategy loaded in the sandbox worker."""
    from almanak.strategy import StrategyDefinition
    from almanak.worker_process import run_worker

    definition = StrategyDefinition(filename=filename, source=source)
    output = run_worker(
        "almanak.strategy_worker", definition.model_dump_json(), ["--parameter-fields"]
    )
    fields = json.loads(output)
    return None if fields is None else frozenset(fields)


class ParametersRefused(InvalidConfig):
    """The typed model refused `strategy.parameters`; `field` is the first of `refusals`."""

    def __init__(self, message: str, field: str):
        super().__init__(message)
        self.field = field

    @classmethod
    def from_error(cls, error: ValidationError) -> "ParametersRefused":
        errors = refusals(error)
        return cls("; ".join(f"{_dotted(e)}: {_reason(e)}" for e in errors), _dotted(errors[0]))


def parameter_base_units(parameters, name: str, decimals: int) -> int:
    """`parameters.<name>`, a quantity in an asset's units, in its base units.

    A quantity finer than the asset's `decimals` is refused naming the field, never
    truncated: call it from `validate_config` so the refusal comes before any action.
    """
    units = Decimal(getattr(parameters, name)).scaleb(decimals)
    if units != units.to_integral_value():
        field = f"strategy.parameters.{name}"
        raise ParametersRefused(f"{field}: exceeds the asset's {decimals} decimal places", field)
    return int(units)


def _dotted(item) -> str:
    return ".".join(("strategy.parameters", *(str(p) for p in item["loc"])))


def _reason(item) -> str:
    if item["type"] == "extra_forbidden":
        return "is not a declared parameter"
    return item["msg"].removeprefix("Value error, ")


def mutable(annotation) -> bool:
    origin = get_origin(annotation) or annotation
    if isinstance(origin, type) and issubclass(
        origin, (MutableSequence, MutableSet, MutableMapping)
    ):
        return True
    return any(mutable(argument) for argument in get_args(annotation))


def refuse_unsupported(selected):
    """Refuse an alias, since `--param` and `test_parameters` check the field's own name,
    and a mutable collection, which one hook could change under the next."""
    for name, info in selected.model_fields.items():
        field = f"strategy.parameters.{name}"
        alias = info.validation_alias or info.alias
        if alias is not None and alias != name:
            raise ParametersRefused(
                f"{field}: declares the alias {alias!r}; a TypedParameters field takes no "
                f"alias, so remove it and write the parameter as {name}",
                field,
            )
        if mutable(info.annotation):
            raise ParametersRefused(
                f"{field}: a TypedParameters field must be immutable; declare a list as tuple[...]",
                field,
            )


def typed(namespace, config):
    """`config` with its parameters validated into the strategy's model, if it has one.

    Each sandboxed evaluation is its own process that validates once, so a cache could
    not outlive it.
    """
    from almanak.configuration.strategy_parameters import authored

    selected = parameters_type(namespace)
    if selected is None or isinstance(config.parameters, selected):
        return config
    refuse_unsupported(selected)
    try:
        model = selected.model_validate_json(json.dumps(authored(config.parameters)))
    except ValidationError as error:
        raise ParametersRefused.from_error(error) from None
    model._values = authored(config.parameters)
    return config.model_copy(update={"parameters": model})


def typed_context(namespace, context):
    return context.model_copy(update={"config": typed(namespace, context.config)})
