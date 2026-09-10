"""Strict JSON projections for durable state with exact decimal quantities."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def copy_json_state(value: Any) -> Any:
    """Detach JSON state, encoding finite decimals without accepting arbitrary objects."""

    def encode(item: Any) -> str:
        if isinstance(item, Decimal) and item.is_finite():
            return str(item)
        raise TypeError("State contains a non-serializable value")

    return json.loads(json.dumps(value, default=encode, allow_nan=False))
