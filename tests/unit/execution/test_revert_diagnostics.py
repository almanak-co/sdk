"""Tests for revert diagnostics.

Verifies that revert error messages are correctly categorized with accurate
diagnostic causes and actionable suggestions.

Regression test for VIB-305: "Gas price cap exceeded" errors were incorrectly
classified as "Slippage or price check failed" because the generic "price"
check fired before the gas-cap-specific check.

VIB-2116: Revert diagnostic WETH address must be chain-aware, not hardcoded Arbitrum.
"""

from decimal import Decimal

from almanak.framework.execution.revert_diagnostics import (
    BalanceCheck,
    _get_weth_address,
    determine_likely_cause,
)


def _get_cause(error: str) -> tuple[str, list[str]]:
    """Helper to get the likely cause from revert diagnostics."""
    return determine_likely_cause(balance_checks=[], raw_error=error, gas_warnings=[])


class TestGasPriceCapDiagnostic:
    """VIB-305: Gas price cap errors should produce accurate diagnostics."""

    def test_gas_price_cap_exceeded_produces_correct_cause(self):
        """Gas price cap error should produce gas-specific cause, not slippage."""
        cause, suggestions = _get_cause(
            "Gas price cap exceeded: Transaction 0: gas price 336.9 gwei exceeds limit 100 gwei"
        )
        assert cause == "Gas price cap exceeded"
        assert any("ALMANAK_MAX_GAS_PRICE_GWEI" in s for s in suggestions)

    def test_gas_price_cap_suggestions_include_env_var(self):
        """Suggestions should tell the user to set ALMANAK_MAX_GAS_PRICE_GWEI."""
        _, suggestions = _get_cause("gas price cap exceeded: 500 gwei exceeds limit 10 gwei")
        assert any("ALMANAK_MAX_GAS_PRICE_GWEI" in s for s in suggestions)

    def test_gas_price_cap_not_classified_as_slippage(self):
        """Gas price cap errors must NOT be classified as slippage failures."""
        cause, _ = _get_cause(
            "Gas price cap exceeded: Transaction 0: gas price 250.0 gwei exceeds limit 100 gwei"
        )
        assert "slippage" not in cause.lower()
        assert "price check" not in cause.lower()

    def test_slippage_error_still_produces_slippage_cause(self):
        """Existing slippage errors should still produce the correct slippage cause."""
        cause, suggestions = _get_cause("UniswapV2: slippage exceeded")
        assert "slippage" in cause.lower() or "price" in cause.lower()
        assert any("slippage" in s.lower() for s in suggestions)

    def test_price_check_error_still_produces_price_cause(self):
        """Generic price check failures should still produce the price cause."""
        cause, _ = _get_cause("Too little received: price impact too high")
        assert "slippage" in cause.lower() or "price" in cause.lower()

    def test_gas_price_cap_case_insensitive(self):
        """Gas price cap check should work regardless of case."""
        cause, _ = _get_cause("GAS PRICE CAP exceeded: 500 gwei over limit")
        assert cause == "Gas price cap exceeded"


class TestChainAwareWethAddress:
    """VIB-2116: WETH address in diagnostic must match the chain, not hardcoded Arbitrum."""

    ARBITRUM_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
    BASE_WETH = "0x4200000000000000000000000000000000000006"
    ETHEREUM_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    def test_get_weth_address_arbitrum(self):
        """Arbitrum should return the well-known Arbitrum WETH address."""
        addr = _get_weth_address("arbitrum")
        assert addr.lower() == self.ARBITRUM_WETH.lower()

    def test_get_weth_address_base(self):
        """Base chain should return the Base WETH address (not Arbitrum)."""
        addr = _get_weth_address("base")
        assert addr.lower() == self.BASE_WETH.lower()

    def test_get_weth_address_ethereum(self):
        """Ethereum should return Ethereum mainnet WETH."""
        addr = _get_weth_address("ethereum")
        assert addr.lower() == self.ETHEREUM_WETH.lower()

    def test_get_weth_address_fallback_on_unknown_chain(self):
        """Unknown chain should return None rather than a wrong-chain address."""
        addr = _get_weth_address("nonexistent_chain_xyz")
        assert addr is None

    def test_diagnostic_uses_chain_specific_weth_in_suggestion(self):
        """When WETH is insufficient on Base, the cast send suggestion must use the Base WETH address."""
        weth_check = BalanceCheck(
            symbol="WETH",
            required=Decimal("1.0"),
            actual=Decimal("0.0"),
            sufficient=False,
            shortfall=Decimal("1.0"),
        )
        _, suggestions = determine_likely_cause(
            balance_checks=[weth_check],
            raw_error=None,
            chain="base",
        )
        # The Arbitrum WETH address should NOT appear in suggestions for Base
        wrap_suggestions = [s for s in suggestions if "cast send" in s]
        assert len(wrap_suggestions) == 1
        assert self.ARBITRUM_WETH not in wrap_suggestions[0], (
            f"Diagnostic used hardcoded Arbitrum WETH on Base chain: {wrap_suggestions[0]}"
        )

    def test_diagnostic_unknown_chain_uses_generic_message(self):
        """When WETH resolution fails (unknown chain), emit a generic message -- not a wrong cast send."""
        weth_check = BalanceCheck(
            symbol="WETH",
            required=Decimal("1.0"),
            actual=Decimal("0.0"),
            sufficient=False,
            shortfall=Decimal("1.0"),
        )
        _, suggestions = determine_likely_cause(
            balance_checks=[weth_check],
            raw_error=None,
            chain="nonexistent_chain_xyz",
        )
        # No cast send suggestion — resolution failed
        cast_suggestions = [s for s in suggestions if "cast send" in s]
        assert len(cast_suggestions) == 0, f"Should not emit cast send for unknown chain: {cast_suggestions}"
        # A generic suggestion should still be present
        assert any("WETH" in s or "ETH" in s for s in suggestions)

    def test_diagnostic_arbitrum_still_uses_arbitrum_weth(self):
        """Arbitrum chain should still show the correct Arbitrum WETH address."""
        weth_check = BalanceCheck(
            symbol="WETH",
            required=Decimal("1.0"),
            actual=Decimal("0.0"),
            sufficient=False,
            shortfall=Decimal("1.0"),
        )
        cause, suggestions = determine_likely_cause(
            balance_checks=[weth_check],
            raw_error=None,
            chain="arbitrum",
        )
        wrap_suggestions = [s for s in suggestions if "cast send" in s]
        assert len(wrap_suggestions) == 1
        assert self.ARBITRUM_WETH.lower() in wrap_suggestions[0].lower()
