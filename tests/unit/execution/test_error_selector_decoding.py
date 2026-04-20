"""Unit tests for custom error selector decoding in the revert diagnostics pipeline.

Validates that Aave V3 and Compound V3 custom error selectors are correctly
mapped and decoded by the KNOWN_CUSTOM_ERRORS registry.
"""

import pytest

from almanak.framework.execution.submitter.public import KNOWN_CUSTOM_ERRORS


class TestAaveV3ErrorSelectors:
    """Validate Aave V3 Pool custom error selectors are registered."""

    @pytest.mark.parametrize(
        "selector, expected_name",
        [
            ("0x2c5211c6", "InvalidAmount()"),
            ("0x90cd6f24", "ReserveInactive()"),
            ("0xd37f5f1c", "ReservePaused()"),
            ("0x6d305815", "ReserveFrozen()"),
            ("0x77a6a896", "BorrowCapExceeded()"),
            ("0xf58f733a", "SupplyCapExceeded()"),
            ("0xcdd36a97", "CallerNotPoolAdmin()"),
            ("0x930bb771", "HealthFactorNotBelowThreshold()"),
            ("0x979b5ce8", "CollateralCannotBeLiquidated()"),
            ("0x3a23d825", "InsufficientCollateral()"),
            ("0xf0788fb2", "NoDebtOfSelectedType()"),
            ("0xdff88f51", "SameBlockBorrowRepay()"),
        ],
    )
    def test_aave_v3_selector_registered(self, selector: str, expected_name: str) -> None:
        """Each Aave V3 error selector must be in KNOWN_CUSTOM_ERRORS."""
        assert selector in KNOWN_CUSTOM_ERRORS, f"Missing Aave V3 selector {selector} ({expected_name})"
        assert KNOWN_CUSTOM_ERRORS[selector] == expected_name


class TestCompoundV3ErrorSelectors:
    """Validate Compound V3 Comet custom error selectors are registered."""

    @pytest.mark.parametrize(
        "selector, expected_name",
        [
            ("0xe273b446", "BorrowTooSmall()"),
            ("0x14c5f7b6", "NotCollateralized()"),
            ("0x945e9268", "InsufficientReserves()"),
            ("0x9e87fac8", "Paused()"),
            ("0x82b42900", "Unauthorized()"),
            ("0xe7a3dfa0", "TransferInFailed()"),
            ("0xcefaffeb", "TransferOutFailed()"),
            ("0xfd1ee349", "BadPrice()"),
            ("0xfa6ad355", "TooMuchSlippage()"),
        ],
    )
    def test_compound_v3_selector_registered(self, selector: str, expected_name: str) -> None:
        """Each Compound V3 error selector must be in KNOWN_CUSTOM_ERRORS."""
        assert selector in KNOWN_CUSTOM_ERRORS, f"Missing Compound V3 selector {selector} ({expected_name})"
        assert KNOWN_CUSTOM_ERRORS[selector] == expected_name


class TestSelectorIntegrity:
    """Validate selector format and uniqueness."""

    def test_all_selectors_are_valid_hex(self) -> None:
        """All selectors must be valid 0x-prefixed 4-byte hex strings (or empty '0x')."""
        for selector in KNOWN_CUSTOM_ERRORS:
            assert selector.startswith("0x"), f"Selector {selector} must start with 0x"
            hex_part = selector[2:]
            if hex_part:  # skip "0x" (EmptyRevertData)
                assert len(hex_part) == 8, f"Selector {selector} must be 4 bytes (8 hex chars)"
                int(hex_part, 16)  # validates hex

    def test_no_duplicate_selectors(self) -> None:
        """No two error names should map to the same selector (dict handles this, but verify count)."""
        # Dict naturally deduplicates keys, but verify the error names are unique per selector
        selectors = list(KNOWN_CUSTOM_ERRORS.keys())
        assert len(selectors) == len(set(selectors)), "Duplicate selectors found"
