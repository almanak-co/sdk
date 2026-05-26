"""Tests for Drift collateral validation in the PERP_OPEN compile path.

Drift is a cross-margin perpetuals DEX on Solana. Unlike GMX V2 (which binds
each market to a fixed ``longToken``/``shortToken`` pair), Drift's margin
engine nets a user's spot positions across every perp market — so the
collateral rule is a single global allow-list of registered Drift
spot-market mints.

If a PERP_OPEN intent is submitted with a ``collateral_token`` that is not
one of Drift's registered spot markets, the Solana transaction would fail
opaquely at submission time (margin calc against a non-existent spot
market). Validating the mint at intent-compile time surfaces a clean,
actionable error to the strategy author before any transaction is built.

This test suite exercises three levels:

1. The rules module's pure-function API
   (:mod:`almanak.connectors.drift.market_rules`).
2. The compiler's Step 1.5 dispatch for ``protocol="drift"`` — proving that
   validation fires BEFORE the Drift adapter is instantiated.
3. Error-shape contract: the raised
   :class:`InvalidCollateralForMarketError` must expose the allowed
   collateral set for programmatic callers.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.drift.market_rules import (
    ALLOWED_COLLATERAL_MINTS,
    is_supported_collateral,
    validate_drift_collateral,
)
from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError
from almanak.framework.intents.vocabulary import PerpOpenIntent

# =============================================================================
# Fixtures / Helpers
# =============================================================================


def _make_mock_compiler(chain: str = "solana") -> IntentCompiler:
    """Create a compiler with minimal mocking for PERP_OPEN testing.

    Mirrors the helper used in the GMX V2 collateral-validation tests so that
    test style stays consistent across the perp compile-path suites.
    """
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "11111111111111111111111111111112"  # base58 system program pk
    compiler.rpc_url = "http://localhost:8899"
    compiler._approve_cache = {}
    compiler._allowance_cache = {}
    compiler._gateway_client = None
    compiler._cached_drift_adapter = None
    compiler.default_protocol = "drift"
    compiler._token_resolver = None
    compiler._config = IntentCompilerConfig(allow_placeholder_prices=True)
    compiler._using_placeholders = False
    compiler._placeholder_warning_logged = False
    compiler.price_oracle = None
    compiler.default_deadline_seconds = 600
    return compiler


def _make_perp_open_intent(
    collateral_token: str = "USDC",
    collateral_amount: Decimal = Decimal("100"),
    market: str = "SOL-PERP",
    size_usd: Decimal = Decimal("500"),
    is_long: bool = True,
) -> PerpOpenIntent:
    """Create a minimal PerpOpenIntent for the Drift compile path."""
    return PerpOpenIntent(
        market=market,
        collateral_token=collateral_token,
        collateral_amount=collateral_amount,
        size_usd=size_usd,
        is_long=is_long,
        leverage=Decimal("5"),
        protocol="drift",
    )


# =============================================================================
# Pure-function tests — the rules module itself
# =============================================================================


class TestDriftMarketRulesPureFunctions:
    """The rule table is the single source of truth; make sure lookups work."""

    def test_usdc_is_supported_collateral(self):
        assert is_supported_collateral("USDC") is True

    def test_sol_is_supported_collateral(self):
        assert is_supported_collateral("SOL") is True

    def test_msol_is_supported_collateral(self):
        # mSOL is Drift spot-market index 2 — the Drift SDK's canonical symbol
        # uses mixed case ("mSOL"). The validator MUST treat it as supported
        # regardless of caller-supplied casing.
        assert is_supported_collateral("mSOL") is True
        assert is_supported_collateral("MSOL") is True
        assert is_supported_collateral("msol") is True

    def test_unknown_collateral_not_supported(self):
        assert is_supported_collateral("FOO") is False

    def test_cross_chain_symbol_not_supported(self):
        # A WETH-variant symbol that belongs to an EVM chain (e.g. Arbitrum
        # bridged notation) must NOT be accepted as a Drift collateral — it
        # is not a Drift spot market.
        assert is_supported_collateral("WETH-on-arbitrum") is False

    def test_allowed_collateral_mints_is_frozenset(self):
        """Allow-list must be frozen so it can't be mutated at runtime."""
        assert isinstance(ALLOWED_COLLATERAL_MINTS, frozenset)
        # Sanity: the canonical Drift spot-market heavyweights are present.
        assert "USDC" in ALLOWED_COLLATERAL_MINTS
        assert "SOL" in ALLOWED_COLLATERAL_MINTS
        assert "MSOL" in ALLOWED_COLLATERAL_MINTS
        assert "WBTC" in ALLOWED_COLLATERAL_MINTS
        assert "WETH" in ALLOWED_COLLATERAL_MINTS
        assert "USDT" in ALLOWED_COLLATERAL_MINTS

    def test_allow_list_is_uppercased_for_comparison(self):
        """Every entry in the comparison set is uppercase so lookups are O(1)."""
        assert all(entry == entry.upper() for entry in ALLOWED_COLLATERAL_MINTS)

    def test_validate_accepts_supported_collaterals(self):
        # These must all pass silently.
        validate_drift_collateral("USDC")
        validate_drift_collateral("SOL")
        validate_drift_collateral("mSOL")
        validate_drift_collateral("wBTC")

    def test_validate_is_case_insensitive(self):
        validate_drift_collateral("usdc")
        validate_drift_collateral("Usdc")
        validate_drift_collateral("sol")
        validate_drift_collateral("MSOL")
        validate_drift_collateral("msol")

    def test_validate_rejects_unknown_symbol(self):
        with pytest.raises(InvalidCollateralForMarketError) as exc_info:
            validate_drift_collateral("WETH-on-arbitrum")
        err = exc_info.value
        # Cross-margin: market is the wildcard sentinel, not a per-perp key.
        assert err.market == "*"
        assert err.collateral == "WETH-on-arbitrum"
        assert err.chain == "solana"
        assert err.protocol == "drift"
        # Allowed list must include the canonical Drift spot markets.
        allowed_upper = {a.upper() for a in err.allowed_collaterals}
        assert "USDC" in allowed_upper
        assert "SOL" in allowed_upper
        assert "MSOL" in allowed_upper

    def test_validate_rejects_garbage_string(self):
        with pytest.raises(InvalidCollateralForMarketError):
            validate_drift_collateral("!!!")

    def test_validate_rejects_empty_string(self):
        with pytest.raises(InvalidCollateralForMarketError):
            validate_drift_collateral("")

    def test_validate_rejects_whitespace_only(self):
        with pytest.raises(InvalidCollateralForMarketError):
            validate_drift_collateral("   ")

    def test_validate_skips_address_shaped_inputs(self):
        """0x-prefixed EVM-style addresses are deferred to downstream resolution.

        The symbol validator's job is to catch *symbol* misconfiguration; raw
        address-shaped inputs go through the adapter's own resolution path.
        """
        # EVM-style address
        validate_drift_collateral("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
        # Uppercase 0X prefix
        validate_drift_collateral("0X82aF49447D8a07e3bd95BD0d56f35241523fBab1")

    def test_validate_skips_long_non_symbol_inputs(self):
        """Long base58-looking strings are treated as raw mints (permissive)."""
        # Looks like a Solana mint pubkey. The validator should not reject
        # it; the Drift adapter's own address resolution handles this case.
        validate_drift_collateral("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

    def test_validate_rejects_malformed_0x_prefixed_garbage(self):
        """Malformed ``0x``-prefixed strings must NOT bypass symbol validation.

        Regression guard: an earlier implementation only checked for the
        ``0x`` prefix, which let garbage like ``"0xfoo"`` slip through as
        if it were a valid address. The tightened check requires exactly
        42 chars and all-hex body; shorter/non-hex inputs now fall through
        to symbol validation and get a proper error.
        """
        with pytest.raises(InvalidCollateralForMarketError):
            validate_drift_collateral("0xfoo")
        with pytest.raises(InvalidCollateralForMarketError):
            validate_drift_collateral("0x")
        with pytest.raises(InvalidCollateralForMarketError):
            # Right length (42) but contains non-hex character 'Z'.
            validate_drift_collateral("0xZZ2AF49447D8a07e3bd95BD0d56f35241523fBab")
        with pytest.raises(InvalidCollateralForMarketError):
            # Too short (41 chars total).
            validate_drift_collateral("0x82aF49447D8a07e3bd95BD0d56f35241523fBab")

    def test_error_message_lists_allowed_set(self):
        """The human-readable message must call out what IS allowed."""
        with pytest.raises(InvalidCollateralForMarketError) as exc_info:
            validate_drift_collateral("NOTATOKEN")
        msg = str(exc_info.value)
        assert "NOTATOKEN" in msg
        assert "drift" in msg
        assert "USDC" in msg


# =============================================================================
# Integration with the compiler — Step 1.5 dispatch for protocol="drift"
# =============================================================================


class TestDriftPerpOpenCompilerCollateralValidation:
    """End-to-end checks on `_compile_perp_open` for protocol='drift'.

    The validation gate must fire BEFORE the Drift compiler branch is
    reached — otherwise an invalid collateral would reach the Drift adapter
    and build transactions that fail at Solana submission time.
    """

    def test_invalid_collateral_rejects_before_drift_adapter_builds(self):
        """(Drift, unsupported collateral) must fail compilation up-front."""
        compiler = _make_mock_compiler(chain="solana")

        # Patch the connector adapter class and assert it is never constructed
        # when collateral is invalid.
        with patch("almanak.connectors.drift.compiler.DriftAdapter") as mock_drift_adapter:
            intent = _make_perp_open_intent(
                collateral_token="WETH-on-arbitrum",
                market="SOL-PERP",
            )
            result = compiler.compile(intent)

            assert mock_drift_adapter.called is False, (
                "Drift collateral validation must short-circuit before the Drift "
                "adapter is constructed; otherwise invalid configs reach tx building."
            )

        assert result.status.value == "FAILED"
        assert result.error is not None
        assert "WETH-on-arbitrum" in result.error
        assert "drift" in result.error

    def test_garbage_collateral_rejects_before_drift_adapter_builds(self):
        """Garbage collateral symbols are rejected up-front too."""
        compiler = _make_mock_compiler(chain="solana")

        with patch("almanak.connectors.drift.compiler.DriftAdapter") as mock_drift_adapter:
            intent = _make_perp_open_intent(
                collateral_token="!!!",
                market="SOL-PERP",
            )
            result = compiler.compile(intent)

            assert mock_drift_adapter.called is False

        assert result.status.value == "FAILED"
        assert "!!!" in result.error

    def test_valid_collateral_dispatches_to_drift_compile(self):
        """Valid collateral must reach the Drift compiler branch."""
        compiler = _make_mock_compiler(chain="solana")

        bundle = MagicMock()
        bundle.metadata = {}

        with patch("almanak.connectors.drift.compiler.DriftAdapter") as mock_drift_adapter:
            mock_drift_adapter.return_value.compile_perp_open_intent.return_value = bundle
            intent = _make_perp_open_intent(
                collateral_token="USDC",
                market="SOL-PERP",
            )
            result = compiler.compile(intent)

            assert mock_drift_adapter.called is True
            assert mock_drift_adapter.return_value.compile_perp_open_intent.call_args.args[0] is intent
            assert result.status.value == "SUCCESS"
            assert result.action_bundle is bundle

    def test_valid_collateral_case_insensitive_dispatches(self):
        """Case-variant valid symbols must still dispatch to the Drift branch."""
        compiler = _make_mock_compiler(chain="solana")

        bundle = MagicMock()
        bundle.metadata = {}

        with patch("almanak.connectors.drift.compiler.DriftAdapter") as mock_drift_adapter:
            mock_drift_adapter.return_value.compile_perp_open_intent.return_value = bundle
            # "msol" (lowercase) is the same as Drift's canonical "mSOL".
            intent = _make_perp_open_intent(
                collateral_token="msol",
                market="SOL-PERP",
            )
            result = compiler.compile(intent)

            assert mock_drift_adapter.called is True
            assert result.status.value == "SUCCESS"
            assert result.action_bundle is bundle

    def test_error_structure_exposes_allowed_collaterals(self):
        """Business contract: callers can programmatically read the allowed set."""
        err = InvalidCollateralForMarketError(
            market="*",
            collateral="WETH-on-arbitrum",
            allowed_collaterals=sorted(ALLOWED_COLLATERAL_MINTS),
            chain="solana",
            protocol="drift",
        )
        assert err.market == "*"
        assert err.collateral == "WETH-on-arbitrum"
        assert err.chain == "solana"
        assert err.protocol == "drift"
        assert "USDC" in err.allowed_collaterals
        assert "SOL" in err.allowed_collaterals
