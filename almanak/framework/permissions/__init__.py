"""Zodiac Roles permission manifest generation.

Automatically derives minimum-privilege permission manifests from
strategy metadata by compiling synthetic intents and inspecting
the resulting transactions.
"""

from .generator import discover_teardown_protocols, generate_manifest  # noqa: F401
from .models import ContractPermission, FunctionPermission, PermissionManifest  # noqa: F401
