"""Unit tests for VIB-3703: template alias resolution and friendly errors.

Edge sends `templateMatch` values like `"swap"` and `"bridge"` that do not
match the canonical StrategyTemplate enum. parse_template() bridges the gap
and produces an actionable error when neither name nor alias matches.
"""

from __future__ import annotations

import pytest

from almanak.framework.cli.new_strategy import (
    TEMPLATE_ALIASES,
    StrategyTemplate,
    UnknownTemplateError,
    parse_template,
)


@pytest.mark.parametrize("template", list(StrategyTemplate))
def test_parse_template_accepts_canonical_value(template: StrategyTemplate) -> None:
    assert parse_template(template.value) is template


def test_parse_template_swap_alias_resolves_to_ta_swap() -> None:
    assert parse_template("swap") is StrategyTemplate.TA_SWAP


def test_parse_template_bridge_alias_resolves_to_multi_step() -> None:
    assert parse_template("bridge") is StrategyTemplate.MULTI_STEP


def test_parse_template_is_case_insensitive_for_canonical() -> None:
    assert parse_template("TA_SWAP") is StrategyTemplate.TA_SWAP
    assert parse_template("Dynamic_LP") is StrategyTemplate.DYNAMIC_LP


def test_parse_template_is_case_insensitive_for_alias() -> None:
    assert parse_template("SWAP") is StrategyTemplate.TA_SWAP
    assert parse_template("Bridge") is StrategyTemplate.MULTI_STEP


def test_parse_template_strips_whitespace() -> None:
    assert parse_template(" ta_swap ") is StrategyTemplate.TA_SWAP


def test_parse_template_unknown_raises_helpful_error() -> None:
    with pytest.raises(UnknownTemplateError) as excinfo:
        parse_template("not_a_template")
    msg = str(excinfo.value)
    # Should list every canonical value so the caller can self-correct
    for template in StrategyTemplate:
        assert template.value in msg
    # And should advertise every alias so AlmanakCode-style callers learn the mapping
    for alias, target in TEMPLATE_ALIASES.items():
        assert alias in msg
        assert target.value in msg


def test_parse_template_non_string_raises_unknown_template_error() -> None:
    with pytest.raises(UnknownTemplateError):
        parse_template(None)  # type: ignore[arg-type]


def test_unknown_template_error_subclasses_value_error() -> None:
    """Existing callers that catch ValueError keep working."""
    assert issubclass(UnknownTemplateError, ValueError)


def test_template_aliases_targets_exist_in_enum() -> None:
    """Aliases must point at a real enum member; otherwise the alias map rots."""
    for alias, target in TEMPLATE_ALIASES.items():
        assert isinstance(target, StrategyTemplate), f"Alias {alias!r} is not StrategyTemplate"
