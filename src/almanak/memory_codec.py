"""Author memory types are instantiated only by sandbox entry points."""

import hashlib
import json
import math

from almanak.strategy_api import Memory, StrategyState


def state_type(namespace):
    selected = namespace.get("State")
    if selected is None:
        return None
    if not isinstance(selected, type) or not issubclass(selected, StrategyState):
        raise TypeError("State must derive from almanak.api.StrategyState")
    return selected


def identity(selected):
    schema = json.dumps(selected.model_json_schema(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(schema.encode()).hexdigest()


def hydrate(namespace, memory):
    selected = state_type(namespace)
    if selected is None:
        if memory.schema_id is not None:
            raise ValueError("typed memory requires its captured State definition")
        return memory
    digest = identity(selected)
    if memory.schema_id is None:
        if memory.values:
            raise ValueError("typed memory cannot replace existing untyped state")
        return selected()
    if memory.schema_id != digest:
        raise ValueError("memory schema differs from captured State")
    result = selected.model_validate_json(json.dumps({**memory.values, "version": memory.version}))
    if result.version != selected().version:
        raise ValueError("memory version differs from captured State")
    return result


def refuse_non_finite(value, path):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"memory field {path} is {value}; non-finite floats cannot be persisted")
    if isinstance(value, dict):
        for key, item in value.items():
            refuse_non_finite(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple, set, frozenset)):
        for index, item in enumerate(value):
            refuse_non_finite(item, f"{path}[{index}]")


def persisted(memory):
    """The JSON memory is stored as, refusing any inf, -inf or nan it would carry.

    JSON has no such values, so storing them would hand the strategy something other than
    what it returned. Memory writes them as JSON constants, which catches serializers that
    produce them only for JSON. Nested models keep their own null setting, so the Python
    dump is checked as well.
    """
    text = memory.model_dump_json()
    for fields in (memory.model_dump(), json.loads(text)):
        for name, value in fields.items():
            refuse_non_finite(value, name)
    return text


def freeze(namespace, memory):
    selected = state_type(namespace)
    if selected is None:
        return Memory.model_validate_json(persisted(memory))
    if not isinstance(memory, selected):
        raise TypeError("strategy must return its declared State memory")
    validated = selected.model_validate_json(persisted(memory))
    if validated.values or validated.schema_id is not None:
        raise ValueError("State uses typed fields; values and schema_id are reserved")
    if validated.version != selected().version:
        raise ValueError("State version cannot change during a run")
    return Memory(
        version=validated.version,
        schema_id=identity(selected),
        values=validated.model_dump(mode="json", exclude={"version", "values", "schema_id"}),
    )
