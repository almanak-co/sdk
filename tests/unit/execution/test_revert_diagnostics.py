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
    NativeETHCheck,
    _get_weth_address,
    determine_likely_cause,
    extract_token_requirements,
)
from almanak.framework.intents.vocabulary import Intent


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


def _insufficient(symbol: str, shortfall: Decimal = Decimal("1.0")) -> BalanceCheck:
    """Build an insufficient balance check for the given symbol."""
    return BalanceCheck(
        symbol=symbol,
        required=shortfall,
        actual=Decimal("0"),
        sufficient=False,
        shortfall=shortfall,
    )


class TestDetermineLikelyCauseCharacterization:
    """Characterization tests locking in the current classification behaviour.

    These tests pin the full priority-ordered rule ladder of
    ``determine_likely_cause`` so the rule-table refactor (Phase 7.4)
    can be verified without changing semantics.
    """

    # --- PRIORITY 0: signing / address mismatch --------------------------------

    def test_signing_mismatch_takes_priority_over_balances(self):
        """Signer mismatch is a configuration error and outranks balance / ETH checks."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("USDC")],
            raw_error="from_address 0xabc does not match signer 0xdef",
            native_eth_check=NativeETHCheck(
                required=Decimal("0.01"),
                actual=Decimal("0"),
                sufficient=False,
                shortfall=Decimal("0.01"),
                breakdown="gas",
            ),
        )
        assert cause == "Configuration error: wallet address mismatch"
        assert any("ALMANAK_PRIVATE_KEY" in s for s in suggestions)
        # The "does not match" branch appends the error details
        assert any("Details:" in s for s in suggestions)

    def test_signing_mismatch_appends_gas_warning_notes(self):
        """When signer mismatch fires, gas warnings should still be surfaced as notes."""
        _, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error="signing failed: address mismatch",
            gas_warnings=["tx 1: execution reverted: STF"],
        )
        assert any("gas estimation also detected" in s for s in suggestions)

    # --- PRIORITY 0.5: compilation errors -------------------------------------

    def test_compilation_error_classified_before_balance_checks(self):
        """Compilation-failure strings short-circuit the ladder."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("USDC")],
            raw_error="intent compilation error: missing pool",
        )
        assert cause == "Intent compilation error"
        assert any("could not be compiled" in s for s in suggestions)

    # --- PRIORITY 1: native ETH insufficient ----------------------------------

    def test_insufficient_native_eth_takes_priority_over_token_balances(self):
        """Native ETH shortfall outranks ERC-20 shortfalls when both present."""
        native_check = NativeETHCheck(
            required=Decimal("0.005"),
            actual=Decimal("0.001"),
            sufficient=False,
            shortfall=Decimal("0.004"),
            breakdown="gas (~0.0005 ETH) + gmx_v2 keeper execution fee (~0.001 ETH)",
        )
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("USDC")],
            raw_error=None,
            native_eth_check=native_check,
            chain="arbitrum",
        )
        assert cause == "Insufficient native ETH for gas + execution fees"
        # GMX-specific note should be appended when breakdown mentions gmx
        assert any("GMX V2" in s for s in suggestions)

    def test_insufficient_native_eth_on_polygon_uses_matic_symbol(self):
        """Native symbol should come from the chain map."""
        native_check = NativeETHCheck(
            required=Decimal("0.1"),
            actual=Decimal("0"),
            sufficient=False,
            shortfall=Decimal("0.1"),
            breakdown="gas",
        )
        cause, _ = determine_likely_cause(
            balance_checks=[],
            raw_error=None,
            native_eth_check=native_check,
            chain="polygon",
        )
        assert cause == "Insufficient native MATIC for gas + execution fees"

    # --- PRIORITY 2: insufficient token balance -------------------------------

    def test_insufficient_weth_known_chain_emits_cast_send(self):
        """WETH shortfall on a known chain produces a cast-send suggestion with that chain's WETH."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("WETH", Decimal("2.5"))],
            raw_error=None,
            chain="base",
        )
        assert cause == "Insufficient balance for: WETH"
        assert any("cast send" in s for s in suggestions)

    def test_insufficient_stable_emits_acquire_via_swap_suggestion(self):
        """Known stablecoins get a swap-or-bridge suggestion."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("USDC.e", Decimal("100"))],
            raw_error=None,
            chain="arbitrum",
        )
        assert cause == "Insufficient balance for: USDC.e"
        assert any("via swap or bridge" in s for s in suggestions)

    def test_insufficient_generic_token_emits_acquire_more_suggestion(self):
        """Unknown tokens fall through to the generic acquire-more suggestion."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[_insufficient("ARB", Decimal("50"))],
            raw_error=None,
            chain="arbitrum",
        )
        assert cause == "Insufficient balance for: ARB"
        assert any("Acquire" in s and "ARB" in s for s in suggestions)

    def test_multiple_insufficient_balances_concatenated_in_cause(self):
        """Cause string lists all insufficient symbols joined by commas."""
        cause, _ = determine_likely_cause(
            balance_checks=[
                _insufficient("WETH"),
                _insufficient("USDC"),
            ],
            raw_error=None,
            chain="arbitrum",
        )
        assert cause == "Insufficient balance for: WETH, USDC"

    # --- balances OK: STF with balances sufficient ----------------------------

    def test_stf_with_sufficient_balances_flags_approval_issue(self):
        """STF revert with balances-OK branch points at approvals, not balance."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error="execution reverted: STF",
        )
        assert cause == "Token transfer failed (STF) - balances OK, likely an approval issue"
        assert any("approval" in s.lower() for s in suggestions)

    def test_stf_with_sufficient_balances_appends_gas_warnings(self):
        """Gas warnings should be appended to STF-approval suggestions."""
        _, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error="STF",
            gas_warnings=["tx 2/3: execution reverted"],
        )
        assert any("gas estimation also detected" in s for s in suggestions)

    # --- common error-string patterns (raw_error, balances OK) ----------------

    def test_deadline_expired_error_maps_to_deadline_cause(self):
        cause, _ = determine_likely_cause(
            balance_checks=[],
            raw_error="Transaction deadline expired",
        )
        assert cause == "Transaction deadline expired"

    def test_liquidity_error_maps_to_liquidity_cause(self):
        cause, _ = determine_likely_cause(
            balance_checks=[],
            raw_error="INSUFFICIENT_LIQUIDITY",
        )
        assert cause == "Insufficient liquidity in pool"

    def test_safetransferfrom_error_maps_to_generic_stf_cause(self):
        """Raw 'SafeTransferFrom' (no 'stf' substring) hits the generic STF rule."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error="execution reverted: SafeTransferFrom failed",
        )
        assert cause == "Token transfer failed (STF) - likely insufficient balance or approval"
        assert any("approval" in s.lower() for s in suggestions)

    # --- gas-warning-only branch ----------------------------------------------

    def test_gas_warnings_with_stf_maps_to_approval_cause(self):
        """No raw_error, balances OK, but gas warnings mention STF -> approval cause."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error=None,
            gas_warnings=["gas estimation failed: STF"],
        )
        assert cause == "Gas estimation detected STF revert - likely an approval issue"
        assert any("approvals" in s.lower() for s in suggestions)

    def test_gas_warnings_generic_maps_to_unknown_with_warnings(self):
        """Non-STF gas warnings fall through to the unknown-with-warnings branch."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error=None,
            gas_warnings=["some unrelated gas warning"],
        )
        assert cause == "Unknown - balances appear sufficient but gas estimation detected issues"
        assert any("unrelated gas warning" in s for s in suggestions)

    # --- fallthrough / defaults -----------------------------------------------

    def test_none_error_empty_checks_returns_default_unknown(self):
        """No error, no warnings, no balance problems -> default unknown branch."""
        cause, suggestions = determine_likely_cause(
            balance_checks=[],
            raw_error=None,
        )
        assert cause == "Unknown - balances appear sufficient"
        assert any("approvals" in s.lower() for s in suggestions)

    def test_empty_string_error_returns_default_unknown(self):
        """Empty-string error is falsy and hits the default branch."""
        cause, _ = determine_likely_cause(
            balance_checks=[],
            raw_error="",
        )
        assert cause == "Unknown - balances appear sufficient"

    def test_unrecognised_error_returns_default_unknown(self):
        """An error that matches no known pattern falls through to default."""
        cause, _ = determine_likely_cause(
            balance_checks=[],
            raw_error="some totally unrelated runtime error",
        )
        assert cause == "Unknown - balances appear sufficient"


class TestExtractTokenRequirementsLPOpen:
    """VIB-5154: revert diagnostics must report token requirements for both the
    legacy amount0/amount1 Curve path and the new pool-coin-aligned coin_amounts
    path (non-leading multi-coin deposits)."""

    def test_legacy_amount0_amount1_unchanged(self):
        """When coin_amounts is None, idx0/idx1 mapping is unchanged."""
        intent = Intent.lp_open(
            pool="WETH/USDC/500",
            amount0=Decimal("1"),
            amount1=Decimal("2000"),
            range_lower=Decimal("1800"),
            range_upper=Decimal("2200"),
            protocol="uniswap_v3",
        )
        reqs = extract_token_requirements(intent, "ethereum")
        assert [(r.symbol, r.amount) for r in reqs] == [
            ("WETH", Decimal("1")),
            ("USDC", Decimal("2000")),
        ]

    def test_coin_amounts_targets_non_leading_coins(self):
        """A Curve 3pool deposit of USDC.e (idx1) + USDT (idx2), DAI (idx0) zero,
        reports exactly the two funded coins — not an empty list."""
        intent = Intent.lp_open(
            pool="DAI/USDC.e/USDT",
            coin_amounts=[Decimal("0"), Decimal("500"), Decimal("500")],
            protocol="curve",
        )
        reqs = extract_token_requirements(intent, "polygon")
        assert [(r.symbol, r.amount) for r in reqs] == [
            ("USDC.e", Decimal("500")),
            ("USDT", Decimal("500")),
        ]

    def test_coin_amounts_skips_zero_entries(self):
        """Zero-amount coins produce no requirement entry."""
        intent = Intent.lp_open(
            pool="DAI/USDC.e/USDT",
            coin_amounts=[Decimal("100"), Decimal("0"), Decimal("0")],
            protocol="curve",
        )
        reqs = extract_token_requirements(intent, "polygon")
        assert [(r.symbol, r.amount) for r in reqs] == [("DAI", Decimal("100"))]
