"""Fail-closed guard against bundled collateralized lending borrows.

A lending ``BorrowIntent`` carrying ``collateral_amount > 0`` (or the chained
``"all"`` form) supplies AND borrows on-chain in one action, but the accounting
layer writes exactly one ``accounting_events`` row per intent. The supply leg
collapses into the single BORROW event -- no standalone SUPPLY event and no
``supply:<position_key>`` FIFO cost-basis lot -- corrupting principal/interest
attribution. Until a compiler-level decomposition exists, the production-safe
behaviour is to reject the bundled form loudly and steer callers to the
accounting-correct two-intent pattern (supply, then standalone borrow).

See ``docs/internal/FollowUp-13June15.md`` §D1 and VIB-3586 (``9d982cadf``).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents import (
    BundledCollateralBorrowError,
    Intent,
)
from almanak.framework.intents.lending_intents import BorrowIntent, SupplyIntent
from almanak.framework.intents.perp_intents import PerpOpenIntent

# A Morpho Blue market id is required for morpho_blue borrows; any 32-byte hex works.
_MORPHO_MARKET_ID = "0x" + "b3" * 32


# ---------------------------------------------------------------------------
# (a) Bundled lending borrow -> raises the clear, accounting-aware error
# ---------------------------------------------------------------------------


class TestBundledBorrowRejected:
    def test_aave_v3_positive_collateral_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("0.01"),
                borrow_token="USDC",
                borrow_amount=Decimal("5"),
            )
        msg = str(exc_info.value)
        assert "Bundled collateralized borrow is not supported" in msg
        # The error must name the accounting-correct two-intent pattern.
        assert "Intent.supply" in msg
        assert "use_as_collateral=True" in msg
        assert 'collateral_amount=Decimal("0")' in msg
        assert "docs/internal/archive/reports/bundled-collateral-borrow-migration.md" in msg

    def test_morpho_blue_positive_collateral_rejected(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Intent.borrow(
                protocol="morpho_blue",
                collateral_token="wstETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1500"),
                market_id=_MORPHO_MARKET_ID,
            )
        assert "Bundled collateralized borrow is not supported" in str(exc_info.value)

    def test_chained_all_collateral_rejected(self) -> None:
        """``collateral_amount="all"`` resolves to a positive supply -> rejected."""
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount="all",
                borrow_token="USDC",
                borrow_amount=Decimal("5"),
            )

    def test_direct_construction_also_rejected(self) -> None:
        """The guard lives on the model validator, so direct construction is covered too."""
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            BorrowIntent(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("2"),
                borrow_token="USDC",
                borrow_amount=Decimal("5"),
            )

    def test_error_type_is_bundled_collateral_borrow_error(self) -> None:
        """Pydantic wraps the ValueError; the underlying cause is our typed error."""
        with pytest.raises(ValidationError) as exc_info:
            BorrowIntent(
                protocol="compound_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("0.5"),
                borrow_token="USDC",
                borrow_amount=Decimal("5"),
            )
        # The typed error carries the prefix used for stable string matching.
        assert BundledCollateralBorrowError.ERROR_PREFIX in str(exc_info.value)


# ---------------------------------------------------------------------------
# (b) The accounting-correct two-intent form still compiles fine
# ---------------------------------------------------------------------------


class TestTwoIntentFormAllowed:
    def test_supply_then_standalone_borrow_aave_v3(self) -> None:
        supply = Intent.supply(
            protocol="aave_v3",
            token="WETH",
            amount=Decimal("0.01"),
            use_as_collateral=True,
        )
        assert isinstance(supply, SupplyIntent)
        assert supply.use_as_collateral is True

        borrow = Intent.borrow(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("5"),
        )
        assert isinstance(borrow, BorrowIntent)
        assert borrow.collateral_amount == Decimal("0")
        assert borrow.intent_type.value == "BORROW"

    def test_supply_then_standalone_borrow_morpho_blue(self) -> None:
        supply = Intent.supply(
            protocol="morpho_blue",
            token="wstETH",
            amount=Decimal("1"),
            market_id=_MORPHO_MARKET_ID,
        )
        assert isinstance(supply, SupplyIntent)

        borrow = Intent.borrow(
            protocol="morpho_blue",
            collateral_token="wstETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1500"),
            market_id=_MORPHO_MARKET_ID,
        )
        assert isinstance(borrow, BorrowIntent)
        assert borrow.collateral_amount == Decimal("0")

    def test_zero_collateral_keeps_collateral_token_metadata(self) -> None:
        """collateral_token is retained as metadata in the standalone form."""
        borrow = Intent.borrow(
            protocol="compound_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("5"),
        )
        assert borrow.collateral_token == "WETH"
        assert borrow.collateral_amount == Decimal("0")


# ---------------------------------------------------------------------------
# (c) Perp opens with collateral_amount are UNAFFECTED (separate intent type)
# ---------------------------------------------------------------------------


class TestPerpOpenUnaffected:
    def test_perp_open_positive_collateral_allowed(self) -> None:
        """Perps legitimately bundle collateral -- the lending guard must not touch them."""
        perp = Intent.perp_open(
            protocol="gmx_v2",
            market="SOL/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("200"),
            leverage=Decimal("2"),
            is_long=True,
            chain="arbitrum",
        )
        assert isinstance(perp, PerpOpenIntent)
        assert perp.collateral_amount == Decimal("100")
        assert perp.intent_type.value == "PERP_OPEN"

    def test_perp_open_is_a_distinct_class_from_borrow(self) -> None:
        """Structural confirmation the guard cannot reach perps: different class/validator."""
        assert PerpOpenIntent is not BorrowIntent
        # The bundled-collateral guard is a method on BorrowIntent only.
        assert hasattr(BorrowIntent, "_reject_bundled_collateral")
        assert not hasattr(PerpOpenIntent, "_reject_bundled_collateral")


# ---------------------------------------------------------------------------
# Permission discovery bypass: synthetic, never-executed intents may bundle.
# ---------------------------------------------------------------------------


class TestAtomicVaultExemption:
    """fluid_vault opens atomically (operate() mints NFT-CDP + supply + borrow),
    so it opts out of the guard via the supports_bundled_collateral_borrow
    capability. This pins the exemption through the REAL validator (not
    model_construct) so a capability-key rename re-breaks here, loudly."""

    def test_fluid_vault_bundled_collateral_allowed(self) -> None:
        intent = Intent.borrow(
            protocol="fluid_vault",
            collateral_token="WETH",
            collateral_amount=Decimal("0.2"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            market_id="0xvault0000000000000000000000000000000000",
        )
        assert isinstance(intent, BorrowIntent)
        assert intent.collateral_amount == Decimal("0.2")
        assert intent.protocol == "fluid_vault"

    def test_fluid_vault_chained_all_collateral_allowed(self) -> None:
        intent = Intent.borrow(
            protocol="fluid_vault",
            collateral_token="WETH",
            collateral_amount="all",
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            market_id="0xvault0000000000000000000000000000000000",
        )
        assert intent.collateral_amount == "all"

    def test_separable_protocol_does_not_inherit_the_exemption(self) -> None:
        """Sanity: the exemption is fluid_vault-specific, not a global default."""
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("0.2"),
                borrow_token="USDC",
                borrow_amount=Decimal("100"),
            )


class TestPermissionDiscoveryBypass:
    def test_for_permission_discovery_allows_bundled_collateral(self) -> None:
        """Discovery vectors intentionally bundle to enumerate the collateral approval."""
        intent = BorrowIntent.for_permission_discovery(
            protocol="morpho_blue",
            collateral_token="WETH",
            collateral_amount=Decimal("1"),
            borrow_token="USDC",
            borrow_amount=Decimal("100"),
            market_id=_MORPHO_MARKET_ID,
            chain="ethereum",
        )
        assert intent.collateral_amount == Decimal("1")
        assert intent.intent_type.value == "BORROW"
        # Field defaults are still applied by model_construct.
        assert intent.intent_id
        assert intent.created_at is not None
