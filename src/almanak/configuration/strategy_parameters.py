"""Author knobs the strategy reads from `strategy.parameters`.

The SDK checks only their shape; the strategy validates the values in
`validate_config`. They are never resolved, priced or part of capabilities. An
empty set is omitted from every dump, so a root without parameters keeps its
digests.
"""

import math
from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Annotated, Any, cast

import yaml
from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr
from pydantic_core import core_schema
from yaml.nodes import ScalarNode

from almanak.configuration.yaml import ConfigurationError, Loader, decode

NAME = r"^[a-z][a-z0-9_]*$"
Scalar = StrictBool | StrictInt | StrictFloat | StrictStr | None
Value = Scalar | tuple[Scalar, ...]
Values = dict[Annotated[str, Field(pattern=NAME)], Value]


class StrategyParameters(Mapping[str, Value]):
    """A read-only mapping of parameter names to scalars; a list arrives as a tuple."""

    def __init__(self, values: Mapping[str, Value] | None = None):
        self._values = dict(values or {})

    def __getitem__(self, key: str) -> Value:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"StrategyParameters({self._values!r})"

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler) -> core_schema.CoreSchema:
        from almanak.typed_parameters import TypedParameters

        values = handler.generate_schema(Values)
        decoded = core_schema.no_info_after_validator_function(
            cls, core_schema.no_info_before_validator_function(shape, values)
        )
        # Inside the sandbox a strategy's typed model replaces the mapping; it dumps as authored.
        instance = core_schema.is_instance_schema((cls, TypedParameters))
        return core_schema.json_or_python_schema(
            json_schema=decoded,
            python_schema=core_schema.union_schema([instance, decoded]),
            serialization=core_schema.plain_serializer_function_ser_schema(
                authored, return_schema=values
            ),
        )


def authored(value) -> dict:
    """The authored values of the mapping or of the typed model that replaced it."""
    return value.authored() if hasattr(value, "authored") else dict(value)


def unset(value) -> bool:
    return not authored(value)


if TYPE_CHECKING:
    # A strategy's typed model replaces the mapping in its hooks, so checkers allow either.
    ParametersField = Any
else:
    ParametersField = StrategyParameters


def shape(value: Any) -> Any:
    """Name the parameter at fault instead of every union member's mismatch."""
    if isinstance(value, StrategyParameters):
        value = dict(value)
    for key, item in value.items() if isinstance(value, dict) else ():
        items = item if isinstance(item, (list, tuple)) else (item,)
        if any(isinstance(element, (dict, list, tuple)) for element in items):
            raise ValueError(f"parameter {key!r} must be a scalar or a flat list of scalars")
    if not isinstance(value, dict):
        return value
    # The inner validation is strict; a decoded JSON or YAML list becomes the read-only tuple.
    return {key: tuple(item) if isinstance(item, list) else item for key, item in value.items()}


class Written(float):
    """A `--param` float that keeps its text, which a string or integer knob reads."""

    text: str


def overrides(pairs) -> dict[str, Value]:
    """`KEY=VALUE` pairs, each value read as one YAML scalar of the author file."""
    result: dict[str, Value] = {}
    for pair in pairs or ():
        key, separator, text = pair.partition("=")
        field = f"strategy.parameters.{key}"
        if not separator or not key:
            raise ConfigurationError(
                f"--param {pair!r} is not KEY=VALUE",
                field="strategy.parameters",
                code="invalid_parameter_override",
                remedy="Write each override as --param KEY=VALUE.",
            )
        if key in result:
            raise ConfigurationError(
                "--param repeats a parameter", field=field, code="invalid_parameter_override"
            )
        try:
            node = yaml.compose(text, Loader=Loader)
        except yaml.YAMLError as error:
            raise ConfigurationError(
                "--param value is not valid YAML", field=field, code="invalid_parameter_override"
            ) from error
        if node is not None and not isinstance(node, ScalarNode):
            raise ConfigurationError(
                "--param takes one scalar value",
                field=field,
                code="invalid_parameter_override",
                remedy="Override a list parameter in config.yaml instead.",
            )
        value = decode(node, field, {}, "--param", 0)
        if isinstance(value, float):
            value = Written(value)
            value.text = text.strip()
        result[key] = value
    return result


def _unknown_key(key, configured, fields, source, field, code):
    """The refusal of a key that neither config.yaml nor the strategy's model declares."""
    if fields is None:
        listed = ", ".join(configured) or "none"
        return ConfigurationError(
            f"{source} {key!r} is not in strategy.parameters. Configured parameters: {listed}.",
            field=field,
            code=code,
            remedy="Override only a parameter that config.yaml declares.",
        )
    listed = ", ".join(sorted(fields)) or "none"
    return ConfigurationError(
        f"{source} {key!r} is not a field of the strategy's TypedParameters model. Model "
        f"fields: {listed}.",
        field=field,
        code=code,
        remedy="Override only a field the strategy's TypedParameters model declares.",
    )


def _checked(key, item, declared, fields, source, field, code) -> Value:
    """`item` as written for the typed model to check, else in config.yaml's type."""
    if key not in (declared if fields is None else fields):
        raise _unknown_key(key, declared, fields, source, field, code)
    return item if fields is not None else like(declared[key], item, key, field, source)


def apply_overrides(
    value: dict, requested: Mapping[str, Value], fields: frozenset[str] | None = None
) -> dict:
    """A copy of the author root with the requested parameters replaced.

    Without a typed parameter model each key must be in config.yaml and its value takes
    the authored type. With one (`fields`, the names the model declares), any model field
    may be set, including one config.yaml leaves to its default, and the YAML scalar is
    kept for the model to convert and check.
    """
    strategy = dict(value.get("strategy") or {})
    current = strategy.get("parameters")
    current = dict(current) if isinstance(current, dict) else {}
    for key, item in requested.items():
        field = f"strategy.parameters.{key}"
        current[key] = _checked(
            key, item, current, fields, "--param", field, "unknown_parameter_override"
        )
    return {**value, "strategy": {**strategy, "parameters": current}}


def test_values(strategy, fields: frozenset[str] | None = None) -> dict[str, Value]:
    """`strategy.test_parameters` in the declared types: the fork test's default overrides.

    Every key must be a declared parameter, or with a typed parameter model (`fields`) a
    field the model declares; then the values stay as written and the model checks them.
    Only `almanak test run` applies them; the parsed inputs drop them, so no strategy hook,
    backtest or digest sees them.
    """
    source, code = "strategy.test_parameters", "unknown_test_parameter"
    declared = strategy.parameters
    return {
        key: _checked(key, item, declared, fields, source, f"{source}.{key}", code)
        for key, item in strategy.test_parameters.items()
    }


def _as_bool(item: bool | int | float | str) -> bool:
    if isinstance(item, bool):
        return item
    if isinstance(item, str) and item.strip().lower() in ("true", "false"):
        return item.strip().lower() == "true"
    raise ValueError


def _as_int(item: bool | int | float | str) -> int:
    if isinstance(item, bool):
        raise ValueError
    if isinstance(item, int):
        return item
    if isinstance(item, str):
        item = item.strip()
        if "_" in item or "e" in item.lower():
            raise ValueError
        try:
            return int(item)
        except ValueError:
            pass
    try:
        number = Decimal(item)
    except InvalidOperation:
        raise ValueError from None
    # Exact: 1.0000000000000001 is not an integer and 9007199254740993.0 keeps its digits.
    if not math.isfinite(float(number)) or number != number.to_integral_value():
        raise ValueError
    return int(number)


def _as_float(item: bool | int | float | str) -> float:
    if isinstance(item, bool) or isinstance(item, str) and "_" in item:
        raise ValueError
    try:
        number = float(item.strip() if isinstance(item, str) else item)
    except OverflowError:
        raise ValueError from None
    if not math.isfinite(number):
        raise ValueError
    return number


def _as_str(item: bool | int | float | str) -> str:
    if isinstance(item, bool):
        return "true" if item else "false"
    return str(item)


# bool before int: a bool is an int in Python.
_COERCIONS: tuple[tuple[type, Callable[[Any], Value]], ...] = (
    (bool, _as_bool),
    (int, _as_int),
    (float, _as_float),
    (str, _as_str),
)


def _like_list(declared: Any, item: Value, key: str, field: str, source: str) -> Value:
    """A list of one element type takes that type element-wise; any other list replaces it."""
    if not isinstance(item, tuple):
        raise ConfigurationError(
            f"{source} {key}={item!r} is not a list, and config.yaml declares a list",
            field=field,
            code="invalid_parameter_override",
            remedy="Give a list parameter a list value.",
        )
    kinds = {type(value) for value in declared if value is not None}
    if len(kinds) != 1:
        return item
    exemplar = next(value for value in declared if value is not None)
    return tuple(
        None if value is None else cast(Scalar, like(exemplar, value, key, field, source))
        for value in item
    )


def like(
    declared: Any, item: Value, key: str, field: str | None = None, source: str = "--param"
) -> Value:
    """`item` in the type config.yaml declares for the parameter.

    A `--param` float converts from its text, so `label=1.10` stays `'1.10'`.
    """
    field = field or f"strategy.parameters.{key}"
    if declared is None:
        return item
    if item is None:
        raise ConfigurationError(
            f"{source} {key} is null, and config.yaml declares {declared!r}",
            field=field,
            code="invalid_parameter_override",
            remedy="Give a value of the same type as config.yaml declares.",
        )
    if isinstance(declared, (list, tuple)):
        return _like_list(declared, item, key, field, source)
    if isinstance(item, tuple):
        raise ConfigurationError(
            f"{source} {key} takes one scalar value",
            field=field,
            code="invalid_parameter_override",
        )
    coerce = next((c for kind, c in _COERCIONS if isinstance(declared, kind)), None)
    if coerce is None:
        return item
    try:
        return coerce(item.text if isinstance(item, Written) else item)
    except ValueError:
        raise ConfigurationError(
            f"{source} {key}={item!r} does not match the configured {type(declared).__name__} "
            f"value {declared!r}",
            field=field,
            code="invalid_parameter_override",
            remedy="Give a value of the same type as config.yaml declares.",
        ) from None
