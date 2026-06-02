"""Strategy-side protocol-family registration site (VIB-4928 PR-3b).

Sibling of :mod:`almanak.connectors._strategy_contract_role_registry`, scoped to
named protocol-family membership (``AAVE_COMPATIBLE_PROTOCOLS`` /
``UNIV3_LP_GROUPING_PROTOCOLS``). Adding a connector to a family is one
``protocol_family.py`` data module + one import line below.

Registration order is irrelevant (set-union membership). Lives one level up from
``_strategy_base/`` because it imports concrete connectors; ``_strategy_base/``
stays protocol-clean (no concrete connector imports).

The completeness invariant — every connector shipping a ``protocol_family``
module MUST register here — is enforced by
``tests/unit/connectors/test_protocol_family_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``protocol_family`` modules it imports are pure data.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.protocol_family_registry import (
    PROTOCOL_FAMILY_REGISTRY,
    ProtocolFamily,
    ProtocolFamilyRegistry,
    ProtocolFamilySpec,
)

__all__ = [
    "PROTOCOL_FAMILY_REGISTRY",
    "ProtocolFamily",
    "ProtocolFamilyRegistry",
    "ProtocolFamilySpec",
]


def _register_all() -> None:
    """Register every strategy-side connector's protocol-family membership."""
    from almanak.connectors.aave_v3.protocol_family import PROTOCOL_FAMILY as _aave_v3
    from almanak.connectors.aerodrome.protocol_family import (
        PROTOCOL_FAMILY as _aerodrome,
    )
    from almanak.connectors.pancakeswap_v3.protocol_family import (
        PROTOCOL_FAMILY as _pancakeswap_v3,
    )
    from almanak.connectors.sushiswap_v3.protocol_family import (
        PROTOCOL_FAMILY as _sushiswap_v3,
    )
    from almanak.connectors.uniswap_v3.protocol_family import (
        PROTOCOL_FAMILY as _uniswap_v3,
    )

    for spec in (_uniswap_v3, _sushiswap_v3, _pancakeswap_v3, _aerodrome, _aave_v3):
        PROTOCOL_FAMILY_REGISTRY.register(spec)


_register_all()
