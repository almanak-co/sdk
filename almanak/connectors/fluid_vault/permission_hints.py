"""Permission hints for ``fluid_vault`` — convention-import re-export.

The permission system discovers hints via
``almanak.connectors.{protocol}.permission_hints`` (convention-based
import, ``framework/permissions/hints.py``); the IMPLEMENTATION lives with
the rest of the vault code in ``almanak.connectors.fluid.vault_permission_hints``
(ADR §7 — one codebase, two manifests). This module only re-exports the
two convention-named attributes.
"""

from __future__ import annotations

from almanak.connectors.fluid.vault_permission_hints import (
    PERMISSION_HINTS,
    build_discovery_vectors,
)

__all__ = ["PERMISSION_HINTS", "build_discovery_vectors"]
