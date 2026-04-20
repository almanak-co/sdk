"""Tests for revert diagnostics.

Verifies that revert error messages are correctly categorized with accurate
diagnostic causes and actionable suggestions.

Regression test for VIB-305: "Gas price cap exceeded" errors were incorrectly
classified as "Slippage or price check failed" because the generic "price"
check fired before the gas-cap-specific check.
"""

from almanak.framework.execution.revert_diagnostics import determine_likely_cause


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
