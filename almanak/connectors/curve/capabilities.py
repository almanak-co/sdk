"""Curve protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "curve": {
        "operations": ["swap", "lp_open", "lp_close"],
        # LPCloseIntent's pool-coin exit selectors (``coin_index`` for a
        # single-sided close, ``imbalanced_amounts`` for an exact-amounts
        # close) are compiled only by connectors declaring this flag. The
        # intent-layer guard in ``framework.intents.vocabulary`` reads it;
        # protocols without the flag reject those fields at construction.
        "lp_close_exit_selectors": True,
    },
}
