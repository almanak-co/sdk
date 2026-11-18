"""Render the strategy-author methods each connector binds to `ctx.actions`.

The bound object is discovered through the same codec hook `DecisionContext.actions`
calls, so the page follows the code: an Actions class lists its public methods, and a
binding that hands back the configuration lists the configuration methods that build
one of the connector's intents.
"""

import inspect
import re
import types
import typing

HEADER = "## Strategy actions"
MODULE_PATH = re.compile(r"\b(?:[a-z_]\w*\.)+")


class _Probe:
    config = object()


def installed():
    from almanak.connectors.composed.connector import connector as composed
    from almanak.connectors.composition import installed_connectors

    return (
        *((str(key), binding) for key, binding in installed_connectors()),
        ("composed", composed()),
    )


def _classes(annotation):
    """The intent classes an annotation names, unwrapping aliases, unions and metadata."""
    origin = typing.get_origin(annotation)
    if isinstance(annotation, typing.TypeAliasType):
        return _classes(annotation.__value__)
    if isinstance(origin, typing.TypeAliasType):
        return _classes(origin.__value__)
    if origin is typing.Annotated:
        return _classes(typing.get_args(annotation)[0])
    if origin in (typing.Union, types.UnionType):
        return set().union(*(_classes(arg) for arg in typing.get_args(annotation)))
    return {_origin(annotation)} if inspect.isclass(annotation) else set()


def _origin(cls):
    metadata = getattr(cls, "__pydantic_generic_metadata__", None) or {}
    return metadata.get("origin") or cls


def _builds_intent(function, intents):
    try:
        returned = typing.get_type_hints(function).get("return")
    except (NameError, TypeError):
        return False
    return bool(_classes(returned) & intents)


def _takes_author_inputs(function):
    """Methods that take a retained observation are steps another action delegates to."""
    return "observed" not in inspect.signature(function).parameters


def bound_methods(codec):
    """Public methods reachable on `ctx.actions` for one connector codec."""
    bound = codec.actions(_Probe())
    if bound is _Probe.config:
        owner = codec.config
        intents = _classes(codec.intent)
        keep = lambda function: _builds_intent(function, intents)  # noqa: E731
    else:
        owner = type(bound)
        keep = _takes_author_inputs
    return [
        (name, function)
        for name, function in sorted(inspect.getmembers(owner, inspect.isfunction))
        if not name.startswith("_") and keep(function)
    ]


def _annotation(value):
    return MODULE_PATH.sub("", inspect.formatannotation(value))


def _parameter(parameter):
    text = parameter.name
    if parameter.kind is parameter.VAR_POSITIONAL:
        text = f"*{text}"
    elif parameter.kind is parameter.VAR_KEYWORD:
        text = f"**{text}"
    if parameter.annotation is not parameter.empty:
        text += f": {_annotation(parameter.annotation)}"
    if parameter.default is not parameter.empty:
        separator = " = " if parameter.annotation is not parameter.empty else "="
        text += f"{separator}{parameter.default!r}"
    return text


def signature(name, function):
    parts, starred = [], False
    for parameter in list(inspect.signature(function).parameters.values())[1:]:
        if parameter.kind is parameter.VAR_POSITIONAL:
            starred = True
        elif parameter.kind is parameter.KEYWORD_ONLY and not starred:
            parts.append("*")
            starred = True
        parts.append(_parameter(parameter))
    return f"{name}({', '.join(parts)})"


def summary(function):
    """The method's short docstring as one line; its paragraphs are joined in order."""
    return " ".join((inspect.getdoc(function) or "").split())


def collect(bindings=None):
    return {
        key: [(signature(name, f), summary(f)) for name, f in bound_methods(binding.codec)]
        for key, binding in (bindings if bindings is not None else installed())
    }


def render(actions):
    lines = [
        HEADER,
        "",
        "Methods a strategy calls on `ctx.actions` for each connector, read from the",
        "binding the connector installs. The first argument is the configured allocation name.",
        "Each entry gives the signature and what the action does: for example that an `exit`",
        "teardown closes liquidity, converts residual inventory to the funding asset and",
        "revokes allowances, how `swap` picks its quote, and `repay` versus `repay_amount`.",
        "",
    ]
    for key in sorted(actions):
        names = ", ".join(f"`{text.split('(')[0]}`" for text, _ in actions[key])
        lines += [
            f"### {key} actions",
            "",
            f"Strategy actions on `ctx.actions` for the `{key}` connector: {names}.",
            "",
        ]
        for text, doc in actions[key]:
            lines.append(f"- `{text}`" + (f": {doc}" if doc else ""))
        lines.append("")
    return lines
