"""Euler V2 lending protocol connector for Avalanche.

Euler V2 uses ERC-4626 vaults with the Ethereum Vault Connector (EVC)
for cross-vault collateral/borrow relationships.

Supported operations: SUPPLY, WITHDRAW, BORROW, REPAY
"""

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

from .adapter import (  # noqa: F401
    EULER_V2_VAULTS,
    EVAULT_FACTORY_ADDRESS,
    EVC_ADDRESS,
    MAX_UINT256,
    VAULT_LENS_ADDRESS,
    EulerV2Adapter,
    EulerV2Config,
    EulerV2VaultInfo,
    TransactionResult,
)
from .receipt_parser import (  # noqa: F401
    BORROW_TOPIC,
    DEPOSIT_TOPIC,
    REPAY_TOPIC,
    WITHDRAW_TOPIC,
    EulerV2ParseResult,
    EulerV2ReceiptParser,
)

register_connector(
    name="euler_v2",
    intents=(
        IntentType.SUPPLY,
        IntentType.BORROW,
        IntentType.REPAY,
        IntentType.WITHDRAW,
    ),
    chains=(
        "ethereum",
        "avalanche",
    ),
)
