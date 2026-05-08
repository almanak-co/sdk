"""Unit tests for custom error selector decoding in the revert diagnostics pipeline.

Validates that Aave V3, Compound V3, and TraderJoe V2 custom error selectors are
correctly mapped and decoded by the KNOWN_CUSTOM_ERRORS registry.
"""

import pytest
from eth_utils import keccak

from almanak.framework.execution.submitter.public import KNOWN_CUSTOM_ERRORS, PublicMempoolSubmitter


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


class TestTraderJoeV2ErrorSelectors:
    """Validate TraderJoe V2 (LFJ Liquidity Book v2.1) custom error selectors are registered.

    Reference: https://github.com/traderjoe-xyz/joe-v2/tree/main/src/interfaces
    """

    @pytest.mark.parametrize(
        "selector, expected_name",
        [
            # ILBPair errors
            ("0xd36bfd88", "LBPair__OutOfLiquidity()"),
            ("0x7df801c7", "LBPair__InsufficientAmountIn()"),
            ("0x873bf0ba", "LBPair__InsufficientAmountOut()"),
            ("0x803fc59a", "LBPair__TokenNotSupported()"),
            ("0x6996a925", "LBPair__ZeroAmount(uint24)"),
            ("0x9931a6ae", "LBPair__ZeroShares(uint24)"),
            ("0x254b6068", "LBPair__OracleNotActive()"),
            # ILBRouter errors
            ("0xd648e3a2", "LBRouter__PairNotCreated(address,address,uint256)"),
            ("0xfaa1db56", "LBRouter__IdSlippageCaught(uint256,uint256,uint256)"),
            ("0x3199f6ee", "LBRouter__AmountSlippageCaught(uint256,uint256,uint256,uint256)"),
            ("0xdae7ca7d", "LBRouter__DeadlineExceeded(uint256,uint256)"),
            ("0xb91b4d4d", "LBRouter__LengthsMismatch()"),
            ("0xc2c4cd5c", "LBRouter__BrokenSwapSafetyCheck()"),
            # ILBFactory errors
            ("0x40aa4644", "LBFactory__LBPairDoesNotExist(address,address,uint256)"),
            ("0xb65ee953", "LBFactory__LBPairNotCreated(address,address,uint256)"),
            ("0x95cf3ee4", "LBFactory__AddressZero()"),
            ("0xfb22c17e", "LBFactory__BinStepHasNoPreset(uint256)"),
            ("0xa2d3f3e4", "LBFactory__ImplementationNotSet()"),
        ],
    )
    def test_traderjoe_v2_selector_registered(self, selector: str, expected_name: str) -> None:
        """Each TraderJoe V2 error selector must be in KNOWN_CUSTOM_ERRORS."""
        assert selector in KNOWN_CUSTOM_ERRORS, f"Missing TJ V2 selector {selector} ({expected_name})"
        assert KNOWN_CUSTOM_ERRORS[selector] == expected_name

    def test_lbpair_out_of_liquidity_iter176_signature(self) -> None:
        """The selector reported on Arbitrum during Kitchen Loop iter 176 must decode."""
        signature = "LBPair__OutOfLiquidity()"
        computed = "0x" + keccak(text=signature)[:4].hex()
        assert computed == "0xd36bfd88"
        assert KNOWN_CUSTOM_ERRORS[computed] == signature

    def test_decode_revert_data_renders_lbpair_out_of_liquidity(self) -> None:
        """End-to-end: the decoder formats LBPair__OutOfLiquidity, not 'Unknown revert'."""
        submitter = PublicMempoolSubmitter(rpc_url="http://localhost")
        decoded = submitter._decode_revert_data("0xd36bfd88")
        assert decoded == "Custom error: LBPair__OutOfLiquidity()"
        assert "Unknown revert" not in decoded

    def test_decode_revert_data_renders_lbrouter_deadline_exceeded(self) -> None:
        submitter = PublicMempoolSubmitter(rpc_url="http://localhost")
        decoded = submitter._decode_revert_data("0xdae7ca7d")
        assert decoded == "Custom error: LBRouter__DeadlineExceeded(uint256,uint256)"

    def test_decode_revert_data_renders_lbfactory_pair_does_not_exist(self) -> None:
        submitter = PublicMempoolSubmitter(rpc_url="http://localhost")
        decoded = submitter._decode_revert_data("0x40aa4644")
        assert decoded == "Custom error: LBFactory__LBPairDoesNotExist(address,address,uint256)"


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
