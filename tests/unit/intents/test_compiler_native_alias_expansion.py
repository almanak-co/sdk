"""Tests for price oracle native <-> wrapped alias expansion (VIB-3136).

Background: ``MarketSnapshot.get_price_oracle_dict()`` returns only the symbols
the strategy's ``decide()`` actually touched (usually the native symbol, e.g.
``POL``). DEX adapters (UniswapV3, SushiswapV3, Aerodrome) then look up the
*wrapped* symbol (``WPOL``) because ``resolve_for_swap`` auto-wraps native
tokens for routing. Before this fix the adapters silently fell back to
``Decimal("1")`` on miss, producing broken slippage (26 WPOL valued as $26
instead of $2.30).

The compiler now expands its price oracle dict bidirectionally on __init__ /
update_prices / restore_prices so adapters consuming the dict by value see
both the native AND wrapped keys.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak import IntentCompilerConfig
from almanak.framework.intents.compiler import IntentCompiler


@pytest.fixture()
def config() -> IntentCompilerConfig:
    """Config with placeholder prices disabled (production mode)."""
    return IntentCompilerConfig(allow_placeholder_prices=False)


class TestNativeToWrappedExpansion:
    """Dict carrying only the native symbol fills in the wrapped counterpart."""

    def test_pol_fills_wpol(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"POL": Decimal("0.08"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WPOL"] == Decimal("0.08")

    def test_matic_fills_wmatic(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"MATIC": Decimal("0.80"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WMATIC"] == Decimal("0.80")

    def test_eth_fills_weth(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"ETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WETH"] == Decimal("3400")

    def test_avax_fills_wavax(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="avalanche",
            price_oracle={"AVAX": Decimal("35"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WAVAX"] == Decimal("35")


class TestWrappedToNativeExpansion:
    """Dict carrying only the wrapped symbol fills in the native counterpart."""

    def test_wpol_fills_pol(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"WPOL": Decimal("0.08"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["POL"] == Decimal("0.08")

    def test_weth_fills_eth(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"WETH": Decimal("3400"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["ETH"] == Decimal("3400")


class TestExpansionDoesNotOverwrite:
    """If both keys are already present and non-zero, leave each alone."""

    def test_both_present_distinct_values_not_overwritten(
        self, config: IntentCompilerConfig
    ) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={
                "POL": Decimal("0.08"),
                "WPOL": Decimal("0.081"),  # tiny arbitrage gap — don't clobber
                "USDC": Decimal("1"),
            },
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["POL"] == Decimal("0.08")
        assert compiler.price_oracle["WPOL"] == Decimal("0.081")

    def test_zero_wrapped_is_filled_from_native(
        self, config: IntentCompilerConfig
    ) -> None:
        """Zero counts as missing — zeros are unsafe for slippage math."""
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={
                "POL": Decimal("0.08"),
                "WPOL": Decimal("0"),
                "USDC": Decimal("1"),
            },
            config=config,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WPOL"] == Decimal("0.08")


class TestExpansionViaUpdatePrices:
    """Prices updated after construction must also get expanded."""

    def test_update_prices_expands(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"POL": Decimal("0.08"), "USDC": Decimal("1")},
            config=config,
        )
        compiler.update_prices({"ETH": Decimal("3400"), "USDC": Decimal("1")})
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WETH"] == Decimal("3400")

    def test_restore_prices_expands(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"POL": Decimal("0.08"), "USDC": Decimal("1")},
            config=config,
        )
        compiler.restore_prices(
            {"MATIC": Decimal("0.50"), "USDC": Decimal("1")},
            original_using_placeholders=False,
        )
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WMATIC"] == Decimal("0.50")


class TestExpansionIsSafeWithoutOracle:
    """Edge cases — empty / None / unrelated symbols."""

    def test_none_oracle_is_noop(self, config: IntentCompilerConfig) -> None:
        """Compiler without an oracle (placeholder path) still works."""
        compiler = IntentCompiler(
            chain="polygon",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        # Placeholder path yields a populated oracle with ETH/WETH/MATIC etc.
        # No ValueError should be raised — expansion is tolerant.
        assert compiler.price_oracle is not None

    def test_unrelated_symbols_passthrough(
        self, config: IntentCompilerConfig
    ) -> None:
        """Symbols not in the wrapped/native map are left untouched."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={
                "USDC": Decimal("1"),
                "ARB": Decimal("1.20"),
                "GMX": Decimal("24"),
            },
            config=config,
        )
        assert compiler.price_oracle is not None
        # Keys we didn't provide and that aren't in the alias map stay absent.
        assert "WGMX" not in compiler.price_oracle
        assert "WARB" not in compiler.price_oracle
        # Provided keys preserved.
        assert compiler.price_oracle["USDC"] == Decimal("1")
        assert compiler.price_oracle["ARB"] == Decimal("1.20")


class TestCallerDictNotMutated:
    """VIB-3136: Caller-provided dicts must not be mutated by the compiler.

    Regression guard — the compiler previously assigned the caller's dict by
    reference and then mutated it in ``_expand_native_aliases_in_price_oracle``,
    leaking wrapped/native aliases back into a shared ``MarketSnapshot`` cache
    or test fixture. The compiler now copies on assignment so callers retain
    whatever they passed in.
    """

    def test_constructor_does_not_mutate_caller_dict(self, config: IntentCompilerConfig) -> None:
        caller_dict: dict[str, Decimal] = {"POL": Decimal("0.08"), "USDC": Decimal("1")}
        snapshot = dict(caller_dict)
        compiler = IntentCompiler(chain="polygon", price_oracle=caller_dict, config=config)
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WPOL"] == Decimal("0.08")
        # Caller's dict should be unchanged.
        assert caller_dict == snapshot
        assert "WPOL" not in caller_dict

    def test_update_prices_does_not_mutate_caller_dict(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"USDC": Decimal("1")},
            config=config,
        )
        caller_dict: dict[str, Decimal] = {"ETH": Decimal("3400"), "USDC": Decimal("1")}
        snapshot = dict(caller_dict)
        compiler.update_prices(caller_dict)
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WETH"] == Decimal("3400")
        assert caller_dict == snapshot
        assert "WETH" not in caller_dict

    def test_restore_prices_does_not_mutate_caller_dict(self, config: IntentCompilerConfig) -> None:
        compiler = IntentCompiler(
            chain="polygon",
            price_oracle={"USDC": Decimal("1")},
            config=config,
        )
        caller_dict: dict[str, Decimal] = {"MATIC": Decimal("0.50"), "USDC": Decimal("1")}
        snapshot = dict(caller_dict)
        compiler.restore_prices(caller_dict, original_using_placeholders=False)
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["WMATIC"] == Decimal("0.50")
        assert caller_dict == snapshot
        assert "WMATIC" not in caller_dict
