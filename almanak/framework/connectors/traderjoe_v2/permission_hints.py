"""TraderJoe V2 permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    supports_standalone_fee_collection=True,
)
