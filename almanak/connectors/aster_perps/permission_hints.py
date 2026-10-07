"""Permission discovery hints.

Aster Pro orders are off-chain API requests signed by the gateway; no strategy
intent produces an on-chain call, so there is nothing to grant.
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints()
