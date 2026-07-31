"""Euler V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LendingReadDecl,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="euler_v2",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.euler_v2.receipt_parser_provider",
        attribute="EulerV2ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.euler_v2.compiler",
        attribute="EulerV2Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("euler_v2",),
        module="almanak.connectors.euler_v2.capabilities",
    ),
    # Bespoke vault/EVC reader (VIB-4966): market-scoped, synthetic market ids; see lending_read.py.
    lending_read=LendingReadDecl(
        account_state=ImportRef(module="almanak.connectors.euler_v2.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"),
        market_table=ImportRef(
            module="almanak.connectors.euler_v2.lending_read", attribute="EULER_V2_ACCOUNT_STATE_MARKETS"
        ),
    ),
    # TD-14 post-close on-chain closure verifier (VIB-5795): supply value /
    # debt ≤ dust per position leg, read from the connector's own vault tables.
    teardown_post_condition=ImportRef(
        module="almanak.connectors.euler_v2.teardown_post_condition",
        attribute="euler_v2_teardown_post_condition",
    ),
    strategy_intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, AVALANCHE, BASE, ARBITRUM)),
)

__all__ = ["CONNECTOR"]
