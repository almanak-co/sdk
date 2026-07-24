"""Unit tests for state_service._attr_text.

Pins the helper's or-default contract: it must behave exactly like the
inline ``entry.name or default`` expressions it replaced in
``_ledger_entry_to_proto`` — falsy attribute values collapse to the
default, and a missing attribute raises AttributeError (two-arg getattr,
no silent fallback).
"""

from types import SimpleNamespace

import pytest

from almanak.gateway.services.state_service import _attr_text


def test_truthy_attribute_is_returned():
    assert _attr_text(SimpleNamespace(chain="arbitrum"), "chain") == "arbitrum"


@pytest.mark.parametrize("falsy", [None, "", 0, 0.0, False, []])
def test_falsy_attribute_collapses_to_default(falsy):
    assert _attr_text(SimpleNamespace(value=falsy), "value") == ""


def test_custom_default_is_used_for_falsy_attribute():
    assert _attr_text(SimpleNamespace(value=None), "value", "{}") == "{}"


def test_custom_default_is_ignored_for_truthy_attribute():
    assert _attr_text(SimpleNamespace(value="x"), "value", "{}") == "x"


def test_missing_attribute_raises_attribute_error():
    with pytest.raises(AttributeError):
        _attr_text(SimpleNamespace(), "absent")
