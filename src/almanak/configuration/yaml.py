"""Bounded YAML input with exact scalars and source-aware diagnostics."""

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


class ConfigurationError(ValueError):
    def __init__(
        self,
        message,
        *,
        file="<configuration>",
        field="",
        line=None,
        code="invalid_configuration",
        remedy="Correct the indicated field and run check again.",
    ):
        self.file, self.field, self.line = str(file), field, line
        self.code, self.remedy = code, remedy
        super().__init__(message)

    def at(self, *, file, field, line):
        return ConfigurationError(
            str(self), file=file, field=field, line=line, code=self.code, remedy=self.remedy
        )

    def diagnostic(self):
        return {
            "code": self.code,
            "file": self.file,
            "line": self.line,
            "field": self.field,
            "message": str(self),
            "remedy": self.remedy,
        }


class Loader(yaml.BaseLoader):
    """BaseLoader avoids YAML 1.1 boolean, timestamp and hexadecimal coercion."""


@dataclass(frozen=True)
class Document:
    value: dict[str, Any]
    locations: dict[str, int]
    file: str

    def error(self, message, field=""):
        return ConfigurationError(
            message, file=self.file, field=field, line=self.locations.get(field)
        )


def load(path: Path) -> Document:
    if path.stat().st_size > 1024 * 1024:
        raise ConfigurationError("configuration exceeds 1 MiB", file=path)
    return parse(path.read_text(), file=str(path))


def parse(text: str, *, file="<configuration>") -> Document:
    locations: dict[str, int] = {}
    try:
        for event in yaml.parse(text, Loader=Loader):
            if isinstance(event, yaml.AliasEvent) or getattr(event, "anchor", None):
                raise ConfigurationError(
                    "YAML anchors and aliases are not supported; use named markets",
                    file=file,
                    line=event.start_mark.line + 1,
                )
        node = yaml.compose(text, Loader=Loader)
        value = decode(node, "", locations, file, 0)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        raise ConfigurationError(
            "invalid YAML syntax", file=file, line=mark.line + 1 if mark else None
        ) from exc
    if not isinstance(value, dict):
        raise ConfigurationError("configuration must be a mapping", file=file)
    return Document(value, locations, file)


def decode(node: Node | None, field, locations, file, depth):
    if node is None:
        return None
    locations[field] = node.start_mark.line + 1
    if depth > 40:
        raise ConfigurationError("configuration nesting exceeds 40", file=file, field=field)
    allowed = {"str", "int", "bool", "null", "float", "seq", "map"}
    if node.tag not in {"tag:yaml.org,2002:" + tag for tag in allowed}:
        raise ConfigurationError("YAML tags are not supported", file=file, field=field)
    if isinstance(node, MappingNode):
        return mapping(node, field, locations, file, depth)
    if isinstance(node, SequenceNode):
        return [
            decode(item, f"{field}.{i}".strip("."), locations, file, depth + 1)
            for i, item in enumerate(node.value)
        ]
    if not isinstance(node, ScalarNode):
        raise ConfigurationError("unsupported YAML node", file=file, field=field)
    return scalar(node, file, field)


def mapping(node, field, locations, file, depth):
    result = {}
    for key, item in node.value:
        if not isinstance(key, ScalarNode) or key.value == "<<":
            raise ConfigurationError("mapping keys must be names; merges are forbidden", file=file)
        name = key.value
        path = f"{field}.{name}".strip(".")
        if name in result:
            raise ConfigurationError(
                "duplicate mapping key", file=file, field=path, line=key.start_mark.line + 1
            )
        result[name] = decode(item, path, locations, file, depth + 1)
    return result


def scalar(node, file, field):
    text = node.value
    if node.style in ("'", '"', "|", ">"):
        return text
    if text in ("null", "~", ""):
        return None
    if text in ("true", "false"):
        return text == "true"
    if re.fullmatch(r"-?(0|[1-9][0-9]*)", text):
        return int(text)
    if text.lower() in (".nan", ".inf", "-.inf", "+.inf"):
        raise ConfigurationError("non-finite numbers are forbidden", file=file, field=field)
    if re.fullmatch(r"[+-]?[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", text):
        number = float(text)
        if not math.isfinite(number):
            raise ConfigurationError("non-finite numbers are forbidden", file=file, field=field)
        return number
    return text
