"""Radiant V2 connector — Aave V2 fork lending protocol."""

from almanak.framework.connectors.radiant_v2.receipt_parser import RadiantV2ReceiptParser

__all__ = ["RadiantV2ReceiptParser"]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="radiant_v2",
    intents=(
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.REPAY,
        IntentType.WITHDRAW,
    ),
    chains=("ethereum",),
)
