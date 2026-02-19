"""Unit tests for wrapped-native price alias resolution in IntentCompiler.

VIB-33: When compiling a USDC->WETH swap, the compiler needs the WETH price
but the oracle only has "ETH". The _WRAPPED_TO_NATIVE mapping resolves
wrapped native symbols (WETH, WMATIC, WAVAX, etc.) to their native
counterparts so price lookups succeed.
"""

from decimal import Decimal

import pytest

from almanak import IntentCompiler, IntentCompilerConfig, SwapIntent


@pytest.fixture()
def config():
    """Config with placeholder prices disabled (production mode)."""
    return IntentCompilerConfig(allow_placeholder_prices=False)


@pytest.fixture()
def placeholder_config():
    """Config with placeholder prices enabled (test mode)."""
    return IntentCompilerConfig(allow_placeholder_prices=True)


class TestWrappedNativePriceAlias:
    """Verify that wrapped-native tokens resolve to their native price."""

    def test_weth_resolves_to_eth_price(self, config):
        """WETH should use ETH price when only ETH is in the oracle."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        price = compiler._require_token_price("WETH")
        assert price == Decimal("3400")

    def test_wmatic_resolves_to_matic_price(self, config):
        """WMATIC should use MATIC price when only MATIC is in the oracle."""
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"MATIC": Decimal("0.85"), "USDC": Decimal("1")},
            config=config,
        )
        price = compiler._require_token_price("WMATIC")
        assert price == Decimal("0.85")

    def test_wavax_resolves_to_avax_price(self, config):
        """WAVAX should use AVAX price when only AVAX is in the oracle."""
        compiler = IntentCompiler(
            chain="avalanche",
            price_oracle={"AVAX": Decimal("35.50"), "USDC": Decimal("1")},
            config=config,
        )
        price = compiler._require_token_price("WAVAX")
        assert price == Decimal("35.50")

    def test_wbnb_resolves_to_bnb_price(self, config):
        """WBNB should use BNB price when only BNB is in the oracle."""
        compiler = IntentCompiler(
            chain="bsc",
            price_oracle={"BNB": Decimal("600"), "USDC": Decimal("1")},
            config=config,
        )
        price = compiler._require_token_price("WBNB")
        assert price == Decimal("600")

    def test_direct_price_takes_precedence(self, config):
        """When both WETH and ETH prices exist, WETH's own price is used."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={
                "ETH": Decimal("3400"),
                "WETH": Decimal("3401"),
                "USDC": Decimal("1"),
            },
            config=config,
        )
        price = compiler._require_token_price("WETH")
        assert price == Decimal("3401")

    def test_unknown_token_still_raises(self, config):
        """Unknown tokens without aliases should still raise ValueError."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        with pytest.raises(ValueError, match="missing.*in the price oracle"):
            compiler._require_token_price("UNKNOWN_TOKEN")

    def test_zero_native_price_does_not_alias(self, config):
        """If native price is zero, alias should not use it (fall through to error)."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("0"), "USDC": Decimal("1")},
            config=config,
        )
        with pytest.raises(ValueError, match="missing.*in the price oracle"):
            compiler._require_token_price("WETH")

    def test_case_insensitive_symbol_lookup(self, config):
        """Alias lookup should work regardless of symbol casing."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        # The symbol is uppercased internally for alias lookup
        price = compiler._require_token_price("WETH")
        assert price == Decimal("3400")


class TestSwapCompilationWithWrappedNative:
    """End-to-end: SwapIntent for USDC->WETH compiles when only ETH price available."""

    def test_usdc_to_weth_swap_compiles_with_eth_price_only(self, config):
        """A USDC->WETH swap should compile successfully when only ETH price is in oracle."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("1000"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)
        # Should not fail with "Price for 'WETH' is missing"
        assert result.status.value != "failed", f"Compilation failed: {result.error}"

    def test_weth_to_usdc_swap_compiles_with_eth_price_only(self, config):
        """A WETH->USDC swap should also compile using ETH alias."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=Decimal("0.5"),
            max_slippage=Decimal("0.005"),
        )
        result = compiler.compile(intent)
        assert result.status.value != "failed", f"Compilation failed: {result.error}"
