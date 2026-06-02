"""Strategy-side swap-classification registration site (VIB-4928 PR-3b).

Sibling of :mod:`almanak.connectors._strategy_contract_role_registry`, scoped to
the swap-router-ABI + fee-tier classification concern. The intent compiler's
five swap-classification symbols (``SWAP_FEE_TIERS``, ``DEFAULT_SWAP_FEE_TIER``,
``SWAP_ROUTER_V1_PROTOCOLS``, ``SWAP_ROUTER_V1_CHAIN_OVERRIDES``,
``SWAP_ROUTER_ALGEBRA_PROTOCOLS``) fan out over this registry instead of
hand-importing each DEX connector's ``swap_classification`` module — so adding a
V3-fork or Algebra DEX means one ``swap_classification.py`` data module + one
import line below, not edits across ``compiler_constants.py``.

Lives one level up from ``_strategy_base/`` because it imports concrete DEX
connectors' ``swap_classification`` modules; ``_strategy_base/`` stays
protocol-clean (no concrete connector imports).

Registration order is NOT load-bearing for the derived symbols — every consumer
does ``protocol in X`` / ``X.get(protocol)``, never an ordered iteration (unlike
the contract-role address tables). The order below mirrors the pre-PR-3b
``_swap_constants_sources`` order only for diagnostic determinism.

The completeness invariant — every connector shipping a ``swap_classification``
module MUST register here — is enforced by
``tests/unit/connectors/test_swap_classification_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``swap_classification`` modules it imports are pure data.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
    SwapClassificationRegistry,
    SwapClassificationSpec,
)

__all__ = [
    "SWAP_CLASSIFICATION_REGISTRY",
    "SwapClassificationRegistry",
    "SwapClassificationSpec",
]


def _register_all() -> None:
    """Register every strategy-side DEX connector's swap classification.

    Imports are local to the function so loading this module does not
    transitively import each connector's ``swap_classification`` data module
    until the registry is actually constructed (mirrors
    ``_strategy_contract_role_registry._register_all``).
    """
    from almanak.connectors.camelot.swap_classification import (
        SWAP_CLASSIFICATION as _camelot,
    )
    from almanak.connectors.pancakeswap_v3.swap_classification import (
        SWAP_CLASSIFICATION as _pancakeswap_v3,
    )
    from almanak.connectors.sushiswap_v3.swap_classification import (
        SWAP_CLASSIFICATION as _sushiswap_v3,
    )
    from almanak.connectors.uniswap_v3.swap_classification import (
        SWAP_CLASSIFICATION as _uniswap_v3,
    )

    ordered_specs: tuple[tuple[SwapClassificationSpec, ...], ...] = (
        _uniswap_v3,  # uniswap_v3 + agni_finance (adjacent)
        _sushiswap_v3,
        _pancakeswap_v3,
        _camelot,
    )
    for specs in ordered_specs:
        for spec in specs:
            SWAP_CLASSIFICATION_REGISTRY.register(spec)


_register_all()
