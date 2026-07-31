"""Canonical intent-type identity and runtime-boundary parsing."""

from __future__ import annotations

from almanak.core.intent_types import IntentType


def test_framework_vocabulary_reexports_canonical_identity() -> None:
    from almanak.framework.intents.vocabulary import IntentType as FrameworkIntentType

    assert FrameworkIntentType is IntentType


def test_try_parse_accepts_runtime_strings_without_changing_enum_semantics() -> None:
    assert IntentType.try_parse(IntentType.SWAP) is IntentType.SWAP
    assert IntentType.try_parse(" swap ") is IntentType.SWAP
    assert IntentType.try_parse("not_an_intent") is None
    assert IntentType.try_parse(object()) is None
    assert IntentType.SWAP != "SWAP"
    assert str(IntentType.SWAP) == "IntentType.SWAP"
