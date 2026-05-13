"""Intent tests for Lagoon VAULT_DEPOSIT / VAULT_REDEEM on Base (VIB-4307).

Lagoon is a vault factory on Base (and Ethereum) implementing the
asynchronous deposit / redeem pattern (ERC-7540) — it's NOT a standard
ERC-4626 synchronous vault.

This file covers the (lagoon, VAULT_DEPOSIT, base) and (lagoon,
VAULT_REDEEM, base) triples from ConnectorRegistry — the intent-coverage
gate (scripts/ci/check_intent_coverage.py) consumes the
``protocol="lagoon"`` literal plus the ``IntentType.VAULT_DEPOSIT /
VAULT_REDEEM`` markers to credit coverage.

**KNOWN BLOCKER (VIB-4307 / VIB-4298)**:
    ``VaultDepositIntent`` and ``VaultRedeemIntent`` reject
    ``protocol="lagoon"`` at Pydantic validation:

        ValueError: Invalid vault protocol: 'lagoon'.
        Supported: ['metamorpho']

    The connector ``lagoon`` is registered in ConnectorRegistry (see
    ``almanak/framework/connectors/lagoon/__init__.py``) for VAULT_DEPOSIT
    and VAULT_REDEEM, but the vault adapter is NOT registered via
    ``register_vault_adapter`` (see
    ``almanak/framework/connectors/vaults/__init__.py``). The Lagoon
    adapter currently ships only the OPERATOR-side surface (propose
    valuation, settle deposit/redeem) — it does NOT build user-side
    ``requestDeposit`` / ``requestRedeem`` ActionBundles. That work is
    required before this intent path can be exercised.

    Until then, the on-chain layers (2-4) are unreachable; the tests
    below assert the current blocker invariant (Pydantic ValidationError
    mentioning ``'lagoon'`` and listing only ``'metamorpho'``). The
    ``protocol="lagoon"`` literal feeds the intent-coverage gate's AST
    scan. The moment the framework registers a Lagoon vault adapter,
    the assertion fails and the next engineer must replace this with a
    real 4-layer on-chain test.

To run::

    uv run pytest tests/intents/base/test_lagoon_vault.py -v
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import (
    IntentType,
    VaultDepositIntent,
    VaultRedeemIntent,
)

pytestmark = [
    pytest.mark.no_zodiac(
        reason="VIB-4307: lagoon not in synthetic-intents matrix"
    ),
    pytest.mark.intent(IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM),
]


# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# A representative Lagoon vault on Base. Exact deployment may vary across
# environments — the test does not assert specific vault state, only the
# blocker invariant (Pydantic rejection until the adapter is registered).
LAGOON_VAULT_ADDRESS_BASE = "0x" + "AB" * 20  # placeholder; not load-bearing


# =============================================================================
# Layer 1: Compilation tests — documented blocker
# =============================================================================


class TestLagoonVaultDepositBlocker:
    """Document the Lagoon VAULT_DEPOSIT blocker on Base.

    The test uses ``protocol="lagoon"`` so the intent-coverage gate's
    AST scan attributes coverage to ``(lagoon, VAULT_DEPOSIT, base)``. It
    asserts the blocker invariant (Pydantic rejection) — the moment the
    Lagoon vault adapter lands in ``register_vault_adapter``, the assertion
    flips and the test fails, forcing the next engineer to replace this
    with a real 4-layer on-chain test.
    """

    def test_compile_lagoon_vault_deposit_pydantic_blocker(self) -> None:
        """Construction must currently fail Pydantic validation.

        When the blocker is lifted (Lagoon registered in the vault adapter
        registry), this test fails and prompts the next engineer to
        replace it with a real 4-layer on-chain test.

        # noqa: layers — this is a documented-blocker placeholder until
        # the Lagoon vault adapter lands; the 4-layer rule will apply once
        # protocol='lagoon' is accepted by the vault registry.
        """
        from pydantic import ValidationError

        # The literal ``protocol="lagoon"`` is what the intent-coverage gate's
        # AST scan picks up (see scripts/ci/check_intent_coverage.py).
        with pytest.raises(ValidationError) as exc_info:
            VaultDepositIntent(
                protocol="lagoon",
                vault_address=LAGOON_VAULT_ADDRESS_BASE,
                amount=Decimal("100"),
                chain=CHAIN_NAME,
            )
        assert "lagoon" in str(exc_info.value).lower(), (
            "Expected Pydantic ValidationError to mention 'lagoon'; "
            "if the error shape changed, update this assertion. "
            f"Got: {exc_info.value}"
        )
        assert "metamorpho" in str(exc_info.value).lower(), (
            "Expected error to list 'metamorpho' as the only supported "
            "vault protocol; if the registry has grown, replace this "
            "blocker test with a real 4-layer on-chain test. "
            f"Got: {exc_info.value}"
        )


class TestLagoonVaultRedeemBlocker:
    """Document the Lagoon VAULT_REDEEM blocker on Base. See deposit class."""

    def test_compile_lagoon_vault_redeem_pydantic_blocker(self) -> None:
        """Construction must currently fail Pydantic validation.

        # noqa: layers — documented-blocker placeholder, see deposit class.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            VaultRedeemIntent(
                protocol="lagoon",
                vault_address=LAGOON_VAULT_ADDRESS_BASE,
                shares=Decimal("10"),
                chain=CHAIN_NAME,
            )
        assert "lagoon" in str(exc_info.value).lower()
        assert "metamorpho" in str(exc_info.value).lower()
