"""GMX V2 permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    selector_labels={
        "0xac9650d8": "multicall(bytes[])",
    },
)
