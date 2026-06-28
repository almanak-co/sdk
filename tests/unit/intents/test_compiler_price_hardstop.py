"""Unit tests for the compiler price HARD STOP gate (VIB-2928 / VIB-5475).

``IntentCompiler.assert_prices_available`` is the canonical price-validation
seam the teardown lane uses to refuse compiling a price-dependent leg (a swap)
on a fake/placeholder/zero price that swap adapters would otherwise silently
substitute with ``$1``.

The gate MUST:
- pass for tokens that resolve to a real price,
- pass for known stablecoins (legitimate $1) — a false positive would strand
  funds by blocking a safe unwind,
- pass for wrapped-native aliases (WETH -> ETH price),
- raise loudly listing every token that has no usable price,
- treat a placeholder-mode compiler as "everything unpriced".
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
)

_WALLET = "0x1111111111111111111111111111111111111111"


def _priced_compiler(prices: dict[str, Decimal]) -> IntentCompiler:
    """A production-shaped compiler (placeholders DISABLED) with a real oracle."""
    return IntentCompiler(
        chain="ethereum",
        wallet_address=_WALLET,
        price_oracle=prices,
        config=IntentCompilerConfig(allow_placeholder_prices=False),
    )


class TestAssertPricesAvailable:
    def test_passes_for_real_prices(self) -> None:
        compiler = _priced_compiler({"WETH": Decimal("3000"), "USDC": Decimal("1")})
        # No raise == gate passed.
        compiler.assert_prices_available(["WETH", "USDC"])

    def test_passes_for_known_stablecoin_not_in_oracle(self) -> None:
        # USDC missing from the oracle but it is a known stablecoin → $1 is a
        # legitimate value, NOT a placeholder. Must not trip the gate.
        compiler = _priced_compiler({"WETH": Decimal("3000")})
        compiler.assert_prices_available(["USDC"])

    def test_passes_for_wrapped_native_alias(self) -> None:
        # WETH not in the oracle but ETH is — the wrapped-native alias resolves
        # 1:1, so the gate must accept WETH.
        compiler = _priced_compiler({"ETH": Decimal("3000")})
        compiler.assert_prices_available(["WETH"])

    def test_raises_for_missing_token(self) -> None:
        compiler = _priced_compiler({"WETH": Decimal("3000")})
        with pytest.raises(ValueError, match="WBTC"):
            compiler.assert_prices_available(["WETH", "WBTC"])

    def test_raises_for_zero_price(self) -> None:
        compiler = _priced_compiler({"WETH": Decimal("3000"), "PEPE": Decimal("0")})
        with pytest.raises(ValueError, match="PEPE"):
            compiler.assert_prices_available(["PEPE"])

    def test_error_lists_every_missing_token_once(self) -> None:
        compiler = _priced_compiler({"WETH": Decimal("3000")})
        with pytest.raises(ValueError) as exc:
            compiler.assert_prices_available(["AAA", "BBB", "AAA"])
        msg = str(exc.value)
        assert "AAA" in msg and "BBB" in msg
        # Deduped: AAA appears once.
        assert msg.count("AAA") == 1

    def test_skips_empty_tokens(self) -> None:
        compiler = _priced_compiler({"WETH": Decimal("3000")})
        # Empty/None entries are ignored (an LP_CLOSE leg may have no to_token).
        compiler.assert_prices_available(["WETH", "", None])

    def test_placeholder_mode_reports_everything_missing(self) -> None:
        # A placeholder compiler returns a fake $1 for everything — the gate
        # must NOT trust that and must report the tokens as unpriced.
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        assert compiler._using_placeholders is True
        with pytest.raises(ValueError, match="WETH"):
            compiler.assert_prices_available(["WETH"])

    def test_raises_for_unpriceable_address(self) -> None:
        # An address that resolves to no symbol / no price must hard-stop
        # rather than be priced at $1.
        compiler = _priced_compiler({"WETH": Decimal("3000")})
        bogus = "0x000000000000000000000000000000000000dEaD"
        with pytest.raises(ValueError):
            compiler.assert_prices_available([bogus])

    def test_passes_for_address_resolving_to_priced_symbol(self) -> None:
        # A token identified by *address/mint* (not a symbol) must be resolved
        # to its symbol and priced — covers both EVM 0x addresses and non-EVM
        # base58 mints, so a priceable token is not falsely rejected. The oracle
        # is keyed by symbol only, so without the resolve+retry the raw address
        # lookup would hard-stop a perfectly priceable swap and strand funds.
        compiler = _priced_compiler({"WETH": Decimal("3000")})

        class _Info:
            symbol = "WETH"

        # A Solana-style base58 mint (does not start with 0x); the resolver
        # maps it to WETH, which is priced.
        mint = "So11111111111111111111111111111111111111112"
        compiler._resolve_token = lambda token, chain=None: _Info()  # type: ignore[method-assign]
        compiler.assert_prices_available([mint])  # no raise == passed
