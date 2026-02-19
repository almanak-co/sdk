"""Action-related types for the Almanak Strategy Framework.

This module contains the base action types that are used across multiple
modules to avoid circular imports.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class AvailableAction(StrEnum):
    """Actions that can be taken on a strategy."""

    BUMP_GAS = "BUMP_GAS"
    CANCEL_TX = "CANCEL_TX"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    EMERGENCY_UNWIND = "EMERGENCY_UNWIND"


@dataclass
class SuggestedAction:
    """A suggested action with description and optional parameters."""

    action: AvailableAction
    description: str
    priority: int = 1
    params: dict[str, Any] = field(default_factory=dict)
    is_recommended: bool = False
