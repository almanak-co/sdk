"""Jupiter Lend Connector — EXPERIMENTAL / NOT PRODUCTION-READY.

The Solana lending compiler (``compiler_solana.py``) routes SUPPLY / BORROW /
REPAY / WITHDRAW intents to ``JupiterLendAdapter`` when ``protocol == "jupiter_lend"``,
but no demo, no incubating strategy, and no on-chain intent test exercises
this path. The folded compiler integration is unverified.

This connector is intentionally:
- Omitted from ``ConnectorRegistry`` (see deregistration block at end of file)
- Removed from ``almanak strat matrix`` (no longer added in
  ``almanak/framework/cli/support_matrix.py``)
- Removed from public docs (``docs/api/connectors/`` + ``mkdocs.yml`` nav)

See ``docs/internal/plans/connector-status-audit-2026-05-23.html`` for the
audit that flagged the lack of demo / intent-test coverage. Re-register only
once at least one on-chain Solana intent test exercises the full lending
lifecycle against a live Jupiter Lend vault.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import JupiterLendAdapter
    from .client import (
        U64_MAX,
        JupiterLendClient,
        JupiterLendConfig,
    )
    from .exceptions import (
        JupiterLendAPIError,
        JupiterLendConfigError,
        JupiterLendError,
        JupiterLendValidationError,
    )
    from .models import (
        JupiterLendTransactionResponse,
        JupiterLendVault,
    )
    from .receipt_parser import JupiterLendReceiptParser

__all__ = [
    "JupiterLendAPIError",
    "JupiterLendAdapter",
    "JupiterLendClient",
    "JupiterLendConfig",
    "JupiterLendConfigError",
    "JupiterLendError",
    "JupiterLendReceiptParser",
    "JupiterLendTransactionResponse",
    "JupiterLendValidationError",
    "JupiterLendVault",
    "U64_MAX",
]

_LAZY: dict[str, tuple[str, str]] = {
    "JupiterLendAPIError": (".exceptions", "JupiterLendAPIError"),
    "JupiterLendAdapter": (".adapter", "JupiterLendAdapter"),
    "JupiterLendClient": (".client", "JupiterLendClient"),
    "JupiterLendConfig": (".client", "JupiterLendConfig"),
    "JupiterLendConfigError": (".exceptions", "JupiterLendConfigError"),
    "JupiterLendError": (".exceptions", "JupiterLendError"),
    "JupiterLendReceiptParser": (".receipt_parser", "JupiterLendReceiptParser"),
    "JupiterLendTransactionResponse": (".models", "JupiterLendTransactionResponse"),
    "JupiterLendValidationError": (".exceptions", "JupiterLendValidationError"),
    "JupiterLendVault": (".models", "JupiterLendVault"),
    "U64_MAX": (".client", "U64_MAX"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
