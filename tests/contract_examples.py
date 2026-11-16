"""The committed example documents of the JSON contracts the SDK emits.

Each example is a real producer's output with the run-specific members its index entry
names replaced by fixed placeholders. With ALMANAK_WRITE_CONTRACT_EXAMPLES=1 the producer
tests rewrite the examples instead of comparing them; review the diff before committing.
"""

import json
import os
from pathlib import Path

ROOT = Path(__file__).parents[1]
CONTRACTS = ROOT / "docs/contracts"
DIRECTORY = CONTRACTS / "examples"
WRITE = "ALMANAK_WRITE_CONTRACT_EXAMPLES"


def index() -> dict:
    return json.loads((DIRECTORY / "index.json").read_text())


def example_path(contract: str, case: str) -> Path:
    return DIRECTORY / contract / f"{case}.json"


def load(contract: str, case: str) -> dict:
    return json.loads(example_path(contract, case).read_text())


def placeholders(contract: str, case: str) -> dict:
    """JSON pointers whose producer values vary between runs, with their fixed values."""
    entry = index()["contracts"][contract]
    return {**entry.get("placeholders", {}), **entry["examples"][case].get("placeholders", {})}


def replaced(value, parts: list[str], placeholder):
    """``value`` with the member at the pointer ``parts`` set to ``placeholder``, if present."""
    if not parts:
        return placeholder
    if not isinstance(value, dict) or parts[0] not in value:
        return value
    return {**value, parts[0]: replaced(value[parts[0]], parts[1:], placeholder)}


def normalized(contract: str, case: str, document) -> dict:
    result = json.loads(json.dumps(document))
    for pointer, value in placeholders(contract, case).items():
        result = replaced(result, pointer.split("/")[1:], value)
    return result


def text(document) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def assert_example(contract: str, case: str, document):
    """The producer's ``document`` equals the committed example, or rewrites it on request."""
    path = example_path(contract, case)
    current = normalized(contract, case, document)
    if os.environ.get(WRITE) == "1":
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text(current))
        return
    assert path.is_file(), f"{path} is missing; run `make contract-examples`"
    expected = json.loads(path.read_text())
    assert current == expected, (
        f"{contract}/{case}.json no longer matches its producer. If the contract change is "
        "intended, run `make contract-examples` and review the diff."
    )
