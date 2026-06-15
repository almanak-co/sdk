"""Backward-compatible re-export shim (VIB-2378).

The implementation moved to ``almanak.framework.anvil.token_overrides`` so the
Anvil funding path (``fork_manager.fund_tokens``) no longer imports the
``backtesting`` package — that package eagerly imports ``report_generator``
(which hard-requires ``jinja2``) and the heavy paper-trading engine, neither of
which is needed to fund a fork.

This shim preserves the old import path for existing callers and tests.
"""

from almanak.framework.anvil.token_overrides import (
    CONFIG_PATH,
    TokenOverride,
    load_token_overrides,
)

__all__ = [
    "CONFIG_PATH",
    "TokenOverride",
    "load_token_overrides",
]
