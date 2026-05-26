"""Permission discovery hints.

This connector currently has no special-case hints — the IntentCompiler
discovers every `(target, selector)` via synthetic intent compilation.
Extend ``PERMISSION_HINTS`` (e.g. with ``synthetic_market_id``,
``synthetic_fee_tier``, or ``static_permissions``) if discovery gaps are
found. See ``.claude/skills/sdk-integrator/SKILL.md`` Phase 6 for patterns.
"""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints()
