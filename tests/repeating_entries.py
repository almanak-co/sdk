"""Retained exchange runs predate repeating entries (blueprint 27 section 7).

Before it, an allocation was offered an entry quote only until its first exposure. Now
the quote is offered whenever funding inventory remains, so an observation taken after
the first entry also carries one. The reference strategies that produced the retained
runs enter once and ignore that quote. `first_entry_only` removes exactly those later
quotes from a reproduced value, so every other retained fact still has to reproduce
exactly.
"""

import json


def without_repeated_entry_quotes(value):
    if isinstance(value, list):
        return [without_repeated_entry_quotes(item) for item in value]
    if isinstance(value, dict):
        return mapping_without_repeated_entry_quotes(value)
    return value


def mapping_without_repeated_entry_quotes(value):
    result = {name: without_repeated_entry_quotes(item) for name, item in value.items()}
    if repeated_entry_quote(value):
        result["entry_quote"] = None
    return result


def repeated_entry_quote(value):
    """`value` carries an entry quote for an allocation whose entry is already complete."""
    owned = value.get("owned")
    return "entry_quote" in value and isinstance(owned, dict) and bool(owned.get("entry_complete"))


def first_entry_only(model):
    """The reproduced model with the entry quotes the one-shot rule would not offer removed."""
    value = without_repeated_entry_quotes(json.loads(model.model_dump_json()))
    return type(model).model_validate_json(json.dumps(value))
