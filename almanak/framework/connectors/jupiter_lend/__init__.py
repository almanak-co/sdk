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

from .adapter import JupiterLendAdapter
from .client import U64_MAX, JupiterLendClient, JupiterLendConfig
from .exceptions import (
    JupiterLendAPIError,
    JupiterLendConfigError,
    JupiterLendError,
    JupiterLendValidationError,
)
from .models import JupiterLendTransactionResponse, JupiterLendVault
from .receipt_parser import JupiterLendReceiptParser

__all__ = [
    # Client
    "JupiterLendClient",
    "JupiterLendConfig",
    "U64_MAX",
    # Adapter
    "JupiterLendAdapter",
    # Receipt Parser
    "JupiterLendReceiptParser",
    # Models
    "JupiterLendVault",
    "JupiterLendTransactionResponse",
    # Exceptions
    "JupiterLendError",
    "JupiterLendAPIError",
    "JupiterLendValidationError",
    "JupiterLendConfigError",
]

# Connector registration intentionally OMITTED.
#
# Folded into ``compiler_solana.py`` but completely unexercised: zero demo,
# zero incubating strategy, zero on-chain intent test. Registering the
# connector would pin four (jupiter_lend, SUPPLY/BORROW/REPAY/WITHDRAW, solana)
# cells in the intent-coverage required-set with no path to satisfy them.
#
# The adapter / client / receipt parser stay (above) so the compiler routing
# in ``compiler_solana.py`` keeps working for anyone hand-driving the SDK.
# Re-add the ``register_connector(...)`` call once an on-chain intent test
# covers the full lending lifecycle against a live Jupiter Lend vault.
