"""Cross-cutting connector types — strategy-safe leaf of the foundation.

Imports nothing from inside ``_base/`` so it is unambiguously the leaf of
the foundation's import graph. ``Chain`` and ``ChainFamily`` are owned by
``almanak.core.enums`` and re-exported here per PR 2169 spec §6.2.4
(canonical home stays in ``core``).
"""

from __future__ import annotations

from enum import Enum
from typing import NewType

from almanak.core.enums import Chain, ChainFamily

__all__ = ["Chain", "ChainFamily", "ProtocolKind", "ProtocolName"]


ProtocolName = NewType("ProtocolName", str)
"""Stable canonical protocol identifier (e.g. ``ProtocolName('aave_v3')``).

Used as the registry key. Aliases (``aave`` → ``aave_v3``) are resolved
to the canonical name by ``almanak.connectors._strategy_base.protocol_aliases``
before reaching the registry.
"""


class ProtocolKind(Enum):
    """Static category a connector declares for the dashboard classifier.

    Replaces the ``deployment_id`` substring sniff in
    ``almanak/gateway/services/dashboard_service.py`` once Phase 2 lands —
    each connector declares its own kind rather than the dashboard
    matching ``"aave" in deployment_id_lower``.

    See PR 2169 spec §6.2 (``connector-self-containment-spec.md``).
    """

    LENDING = "lending"
    LP = "lp"
    PERP = "perp"
    SWAP = "swap"
    YIELD_TRADING = "yield_trading"
    BRIDGE = "bridge"
    PREDICTION_MARKET = "prediction_market"
    VAULT = "vault"
    CROSS_CHAIN_SWAP = "cross_chain_swap"
    FLASH_LOAN = "flash_loan"
