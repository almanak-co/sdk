"""Branch-complete tests for intent strategy inspection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from almanak.framework.cli import intent_debug as ide


@dataclass
class _ExampleIntent:
    kind: str

    def serialize(self) -> dict[str, str]:
        return {"type": self.kind, "marker": "example"}


class _MetadataStrategy:
    STRATEGY_NAME = "Metadata Strategy"
    STRATEGY_METADATA = {
        "name": "metadata-strategy",
        "intent_types": ["SWAP", "HOLD", "FUTURE_INTENT"],
    }


class _UnnamedStrategy:
    pass


def test_inspect_strategy_returns_exact_load_error(tmp_path: Path) -> None:
    strategy_path = tmp_path / "missing.py"

    with patch.object(ide, "load_strategy_from_file", return_value=(None, "exact load failure")):
        result = ide.inspect_strategy(strategy_path, chain="base")

    assert result.to_dict() == {
        "strategy_name": "unknown",
        "strategy_path": str(strategy_path),
        "metadata": None,
        "intent_types": [],
        "intent_details": [],
        "state_diagrams": {},
        "action_bundles": {},
        "errors": ["exact load failure"],
    }


def test_inspect_strategy_preserves_ast_metadata_and_error_output(tmp_path: Path) -> None:
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text(
        "def decide():\n    Intent.swap()\n    Intent.hold()\n    Intent.not_a_registered_intent()\n",
        encoding="utf-8",
    )

    def create_example(intent_type: str) -> _ExampleIntent:
        if intent_type == "FUTURE_INTENT":
            raise RuntimeError("example unavailable")
        return _ExampleIntent(intent_type)

    def compile_example(intent: _ExampleIntent, chain: str) -> dict[str, str]:
        return {"status": "SUCCESS", "type": intent.kind, "chain": chain}

    with (
        patch.object(ide, "load_strategy_from_file", return_value=(_MetadataStrategy, None)),
        patch.object(ide, "generate_state_diagram", side_effect=lambda intent_type: f"diagram:{intent_type.value}"),
        patch.object(ide, "create_example_intent", side_effect=create_example),
        patch.object(ide, "compile_example_intent", side_effect=compile_example),
    ):
        result = ide.inspect_strategy(strategy_path, chain="base")

    assert result.to_dict() == {
        "strategy_name": "Metadata Strategy",
        "strategy_path": str(strategy_path),
        "metadata": _MetadataStrategy.STRATEGY_METADATA,
        "intent_types": ["FUTURE_INTENT", "HOLD", "SWAP"],
        "intent_details": [
            {"type": "HOLD", "example": {"type": "HOLD", "marker": "example"}},
            {"type": "SWAP", "example": {"type": "SWAP", "marker": "example"}},
        ],
        "state_diagrams": {
            "HOLD": "diagram:HOLD",
            "SWAP": "diagram:SWAP",
        },
        "action_bundles": {
            "HOLD": {"status": "SUCCESS", "note": "HOLD intents require no transactions"},
            "SWAP": {"status": "SUCCESS", "type": "SWAP", "chain": "base"},
        },
        "errors": [
            "Unknown intent type: FUTURE_INTENT",
            "Error creating example for FUTURE_INTENT: example unavailable",
        ],
    }


def test_inspect_strategy_falls_back_to_every_intent_type(tmp_path: Path) -> None:
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def decide():\n    return None\n", encoding="utf-8")
    expected_types = [intent_type.value for intent_type in ide.IntentType]

    with (
        patch.object(ide, "load_strategy_from_file", return_value=(_UnnamedStrategy, None)),
        patch.object(ide, "generate_state_diagram", side_effect=lambda intent_type: f"diagram:{intent_type.value}"),
        patch.object(ide, "create_example_intent", side_effect=lambda intent_type: _ExampleIntent(intent_type)),
        patch.object(
            ide,
            "compile_example_intent",
            side_effect=lambda intent, chain: {"status": "SUCCESS", "type": intent.kind, "chain": chain},
        ) as compile_example,
    ):
        result = ide.inspect_strategy(strategy_path, chain="arbitrum")

    assert result.strategy_name == "_UnnamedStrategy"
    assert result.intent_types == expected_types
    assert result.errors == ["Could not detect intent types from source, showing all possible types"]
    assert result.state_diagrams == {intent_type: f"diagram:{intent_type}" for intent_type in expected_types}
    assert result.action_bundles["HOLD"] == {
        "status": "SUCCESS",
        "note": "HOLD intents require no transactions",
    }
    assert compile_example.call_count == len(expected_types) - 1


def test_unimplemented_builtin_example_preserves_hold_fallback() -> None:
    example = ide.create_example_intent("SUPPLY")

    assert example.intent_type is ide.IntentType.HOLD
    assert example.reason == "No action needed"
