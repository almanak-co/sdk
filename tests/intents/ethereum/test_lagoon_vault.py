"""Intent tests for Lagoon VAULT_DEPOSIT / VAULT_REDEEM on Ethereum (VIB-4307).

See ``tests/intents/base/test_lagoon_vault.py`` for the full blocker
context — both chains share the same root cause (no Lagoon vault adapter
registered in ``register_vault_adapter``; VaultDepositIntent and
VaultRedeemIntent reject ``protocol="lagoon"`` at Pydantic validation).

This file covers the (lagoon, VAULT_DEPOSIT, ethereum) and (lagoon,
VAULT_REDEEM, ethereum) triples from ConnectorRegistry.

To run::

    uv run pytest tests/intents/ethereum/test_lagoon_vault.py -v
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

CHAIN_NAME = "ethereum"

# Placeholder Lagoon vault address. Exact deployment not load-bearing for
# the blocker assertion below.
LAGOON_VAULT_ADDRESS_ETHEREUM = "0x" + "CD" * 20


# =============================================================================
# Layer 1: Compilation tests — documented blocker
# =============================================================================


class TestLagoonVaultDepositBlockerEthereum:
    """Document the Lagoon VAULT_DEPOSIT blocker on Ethereum.

    See ``tests/intents/base/test_lagoon_vault.py`` for the full
    blocker explanation. When the blocker is lifted, both ethereum and
    base xfail tests in this family must be upgraded together.
    """

    def test_compile_lagoon_vault_deposit_pydantic_blocker(self) -> None:
        """Construction must currently fail Pydantic validation.

        # noqa: layers — documented-blocker placeholder until the Lagoon
        # vault adapter is registered in the vault registry.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            VaultDepositIntent(
                protocol="lagoon",
                vault_address=LAGOON_VAULT_ADDRESS_ETHEREUM,
                amount=Decimal("100"),
                chain=CHAIN_NAME,
            )
        assert "lagoon" in str(exc_info.value).lower()
        assert "metamorpho" in str(exc_info.value).lower()


class TestLagoonVaultRedeemBlockerEthereum:
    """Document the Lagoon VAULT_REDEEM blocker on Ethereum."""

    def test_compile_lagoon_vault_redeem_pydantic_blocker(self) -> None:
        """Construction must currently fail Pydantic validation.

        # noqa: layers — documented-blocker placeholder.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            VaultRedeemIntent(
                protocol="lagoon",
                vault_address=LAGOON_VAULT_ADDRESS_ETHEREUM,
                shares=Decimal("10"),
                chain=CHAIN_NAME,
            )
        assert "lagoon" in str(exc_info.value).lower()
        assert "metamorpho" in str(exc_info.value).lower()
