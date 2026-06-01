"""Strategy-side flash-loan provider registration site (VIB-4837).

Sibling of :mod:`almanak.connectors._strategy_receipt_registry`, scoped to the
flash-loan-provider concern.

Lives one level up from ``_strategy_base/`` because it imports every
flash-loan connector's ``flash_loan`` + ``flash_loan_provider`` modules — and
``_strategy_base/`` must stay protocol-clean (no concrete connector imports).
Adding a new connector that provides flash loans means one import block + one
``FLASH_LOAN_PROVIDER_REGISTRY.register`` line below — no edit anywhere in the
framework.

The completeness invariant — every connector that ships a
``flash_loan_provider.py`` MUST register here — is enforced statically by
``tests/unit/connectors/test_flash_loan_registry_completeness.py``.

Registration order (aave, balancer, morpho) is load-bearing: it fixes the
selector's candidate order and the compiler's ``"Supported providers: …"``
error string. Keep it stable unless intentionally changing that surface.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.flash_loan_registry import (
    FLASH_LOAN_PROVIDER_REGISTRY,
    FlashLoanProviderRegistration,
)

__all__ = ["FLASH_LOAN_PROVIDER_REGISTRY"]


def _register_all() -> None:
    """Register every strategy-side flash-loan provider.

    Imports are local to the function so that loading this module does not
    transitively import each connector's transaction builders until the
    registry is actually constructed (they pull in connector-side address
    tables and ABI helpers we don't want loaded just to know "this connector
    exists").
    """
    from almanak.connectors.aave_v3.flash_loan import build_aave_flash_loan
    from almanak.connectors.aave_v3.flash_loan_provider import AaveFlashLoanProvider
    from almanak.connectors.balancer_v2.flash_loan import build_balancer_flash_loan
    from almanak.connectors.balancer_v2.flash_loan_provider import BalancerFlashLoanProvider
    from almanak.connectors.morpho_blue.flash_loan import build_morpho_flash_loan
    from almanak.connectors.morpho_blue.flash_loan_provider import MorphoFlashLoanProvider

    FLASH_LOAN_PROVIDER_REGISTRY.register(
        FlashLoanProviderRegistration(
            name="aave",
            make_provider=AaveFlashLoanProvider,
            build=build_aave_flash_loan,
        )
    )
    FLASH_LOAN_PROVIDER_REGISTRY.register(
        FlashLoanProviderRegistration(
            name="balancer",
            make_provider=BalancerFlashLoanProvider,
            build=build_balancer_flash_loan,
        )
    )
    FLASH_LOAN_PROVIDER_REGISTRY.register(
        FlashLoanProviderRegistration(
            name="morpho",
            make_provider=MorphoFlashLoanProvider,
            build=build_morpho_flash_loan,
        )
    )


_register_all()
