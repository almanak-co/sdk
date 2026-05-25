"""Gateway-side connector base class.

Each protocol's gateway-side code lives under
``almanak/connectors/<protocol>/gateway/`` (or, during the migration,
``almanak/framework/connectors/<protocol>/gateway/``) and exports a single
``GatewayConnector`` subclass. The gateway boot loop discovers these via
``GatewayConnectorRegistry`` and inspects them for capabilities through
``isinstance(connector, Gateway*Capability)`` checks.

Strategy-side code MUST NOT import this module. The import-graph lint
(``tests/static/test_strategy_import_boundary.py``) hard-fails on any
strategy-side file that touches ``_base.gateway_*``.
"""

from __future__ import annotations

from abc import ABC
from typing import ClassVar

from .types import ProtocolKind, ProtocolName


class GatewayConnector(ABC):
    """Gateway-side base class for a single protocol.

    Subclasses declare which capabilities they implement by also
    inheriting from the relevant ``Gateway*Capability`` runtime-checkable
    Protocols. The registry inspects ``isinstance(connector, Cap)`` at
    boot to route capability-keyed requests.

    Required class attributes:

    * ``protocol`` — canonical ``ProtocolName`` for this connector.
    * ``kind`` — static ``ProtocolKind`` for dashboard classification.

    Example::

        class AaveV3GatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
            protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
            kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

            def market_lookup(self) -> ProtocolTokenLookup:
                return AaveV3MarketLookup(...)
    """

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]
