"""ALM-3183: kill production-reachable placeholder pricing.

The defect was a chain of five small permissions that together let a real
strategy size a real trade off a hardcoded price table:

1. the runner enabled placeholder pricing whenever the iteration's price oracle
   was empty;
2. the price pre-fetch swallowed every exception, so a price-service outage was
   *exactly* what emptied the oracle;
3. the table itself is, by its own docstring, 40-60%% wrong (ETH=$2000,
   WBTC=$45000, anything unknown=$1);
4. placeholder mode also disables the price-impact guard, so the one check that
   would have caught the resulting bad quote was off;
5. missing compiler config defaulted to the *unsafe* mode via
   ``getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))``.

Each test below is a negative control for one link: revert that link and the
test fails. The two carve-outs that must keep working -- Safe/Zodiac permission
discovery, and unit tests -- are asserted here too, because a fix that breaks
permission discovery ships an EMPTY Zodiac manifest, and an empty manifest makes
every Safe call revert at ``execTransactionWithRole``.
"""

from __future__ import annotations

import ast
import logging
from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import (
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.compiler_queries import (
    PLACEHOLDER_ESCAPE_HATCH_ENV,
    PlaceholderPriceUse,
    get_placeholder_prices,
    placeholder_escape_hatch_enabled,
)
from almanak.framework.intents.vocabulary import Intent

_WALLET = "0x1111111111111111111111111111111111111111"


def _empty_oracle_compiler(chain: str = "ethereum") -> IntentCompiler:
    """A production-shaped compiler with a REAL-BUT-EMPTY oracle.

    This is the shape the runner now builds when the market snapshot priced
    nothing: placeholders disallowed, oracle present, oracle empty.
    """
    return IntentCompiler(
        chain=chain,
        wallet_address=_WALLET,
        price_oracle={},
        config=IntentCompilerConfig(allow_placeholder_prices=False),
    )


# =============================================================================
# (d) One canonical table, reachable only by naming why
# =============================================================================


class TestCanonicalTableRequiresExplicitUse:
    def test_calling_without_use_is_a_type_error(self) -> None:
        """``use`` is keyword-ONLY and required.

        This is the "explicit flag" the fix turns on. An omitted argument must
        fail at the call site, loudly, rather than silently yielding a table.
        """
        with pytest.raises(TypeError):
            get_placeholder_prices()  # type: ignore[call-arg]

    def test_a_bare_string_is_not_a_use(self) -> None:
        """``PlaceholderPriceUse`` is a ``str`` enum, so a plain string would
        compare equal to a member -- but it must not be *accepted*. Only a real
        enum member counts, so the set of sanctioned uses stays closed."""
        with pytest.raises(TypeError):
            get_placeholder_prices(use="unit_test")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "use",
        [PlaceholderPriceUse.UNIT_TEST, PlaceholderPriceUse.PERMISSION_DISCOVERY],
    )
    def test_sanctioned_uses_return_the_table(self, use: PlaceholderPriceUse) -> None:
        table = get_placeholder_prices(use=use)
        assert table["ETH"] == Decimal("2000")
        assert table["USDC"] == Decimal("1")

    def test_legacy_escape_hatch_use_is_refused_without_the_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The enum member alone must not buy a fabricated price.

        Without this, ``LEGACY_ESCAPE_HATCH`` would be a second, quieter way to
        reach the table -- the exact shape of the defect being removed.
        """
        monkeypatch.delenv(PLACEHOLDER_ESCAPE_HATCH_ENV, raising=False)
        with pytest.raises(RuntimeError, match=PLACEHOLDER_ESCAPE_HATCH_ENV):
            get_placeholder_prices(use=PlaceholderPriceUse.LEGACY_ESCAPE_HATCH)

    def test_legacy_escape_hatch_use_works_when_the_env_var_is_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(PLACEHOLDER_ESCAPE_HATCH_ENV, "true")
        assert get_placeholder_prices(use=PlaceholderPriceUse.LEGACY_ESCAPE_HATCH)["ETH"] == Decimal("2000")

    def test_table_is_internally_consistent_across_native_and_wrapped(self) -> None:
        """Wrapped and native must price identically.

        The deleted connector copies are how this stopped being true: BNB was
        $300 in pancakeswap_v3/sushiswap_v3 and $600 in the framework. Nothing
        detected it, because a second table is invisible to a test of the first.
        """
        table = get_placeholder_prices(use=PlaceholderPriceUse.UNIT_TEST)
        for native, wrapped in [
            ("ETH", "WETH"),
            ("BNB", "WBNB"),
            ("AVAX", "WAVAX"),
            ("MATIC", "WMATIC"),
            ("S", "WS"),
            ("MNT", "WMNT"),
        ]:
            assert table[native] == table[wrapped], f"{native}/{wrapped} disagree"

    def test_table_absorbed_every_symbol_from_the_deleted_connector_copies(self) -> None:
        """Deleting a copy must not lose symbol coverage.

        These are the symbols that existed ONLY in the pancakeswap_v3 /
        sushiswap_v3 tables before ALM-3183 folded them in. Drop one and a swap
        on that token silently reverts to the ``$1`` unknown-symbol fallback.
        """
        table = get_placeholder_prices(use=PlaceholderPriceUse.UNIT_TEST)
        for symbol in ("BUSD", "BTCB", "CAKE", "SUSHI", "WETH.e", "DAI.e", "WBTC.e"):
            assert symbol in table, f"{symbol} was lost when the connector copies were deleted"


class TestConnectorsDelegateToTheCanonicalTable:
    def test_sushiswap_v3_delegates_rather_than_owning_a_table(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Negative control for the deletion — asserts DELEGATION, not equality.

        Value equality was the weaker form CodeRabbit caught on PR #3640: a
        reinstated connector-local dict holding the same numbers passes it. That
        is the one case worth catching, since copies start identical and drift
        later — which is exactly how BNB became $300 here and $600 upstream.
        """
        from almanak.connectors.sushiswap_v3.adapter import SushiSwapV3Adapter
        from almanak.framework.intents import compiler_queries

        sentinel = {"WBNB": Decimal("600")}
        uses: list[object] = []

        def fake_get_placeholder_prices(*, use):
            uses.append(use)
            return sentinel

        # Patch the DEFINING module: the adapter resolves the function through a
        # function-scoped import, so patching the adapter namespace binds nothing.
        monkeypatch.setattr(compiler_queries, "get_placeholder_prices", fake_get_placeholder_prices)
        adapter = SushiSwapV3Adapter.__new__(SushiSwapV3Adapter)

        assert adapter._get_placeholder_prices() is sentinel
        assert uses == [PlaceholderPriceUse.UNIT_TEST]


# =============================================================================
# The escape hatch (carve-out 3)
# =============================================================================


class TestEscapeHatch:
    def test_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(PLACEHOLDER_ESCAPE_HATCH_ENV, raising=False)
        assert placeholder_escape_hatch_enabled() is False

    @pytest.mark.parametrize("raw", ["true", "TRUE", "1", "yes", "on", " true "])
    def test_truthy_spellings_enable_it(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        monkeypatch.setenv(PLACEHOLDER_ESCAPE_HATCH_ENV, raw)
        assert placeholder_escape_hatch_enabled() is True

    @pytest.mark.parametrize("raw", ["", "0", "false", "no", "off", "maybe", "   "])
    def test_falsey_and_unrecognised_spellings_do_not(self, monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
        """Fail CLOSED on anything unrecognised.

        A typo'd value must leave the safe behaviour in place; the dangerous
        setting is the one that has to be spelled correctly.
        """
        monkeypatch.setenv(PLACEHOLDER_ESCAPE_HATCH_ENV, raw)
        assert placeholder_escape_hatch_enabled() is False

    def test_enabling_it_logs_at_error_every_time(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Loud on EVERY read, not once.

        A once-per-process warning scrolls off the top of a long-running
        deployment's log and the condition then looks normal.
        """
        monkeypatch.setenv(PLACEHOLDER_ESCAPE_HATCH_ENV, "1")
        with caplog.at_level(logging.ERROR, logger="almanak.framework.intents.compiler_queries"):
            placeholder_escape_hatch_enabled()
            placeholder_escape_hatch_enabled()
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert len(errors) == 2
        assert PLACEHOLDER_ESCAPE_HATCH_ENV in errors[0].getMessage()


# =============================================================================
# (c) Fail-loud config access
# =============================================================================


class TestConfigAccessFailsLoud:
    def test_require_config_raises_when_unset(self) -> None:
        """Negative control for the fail-open default.

        Restore ``getattr(self, "_config", IntentCompilerConfig(allow_placeholder_prices=True))``
        and this passes silently while handing every connector compiler context
        permission to fabricate prices.
        """
        compiler = IntentCompiler.__new__(IntentCompiler)
        with pytest.raises(RuntimeError, match="_config is unset"):
            compiler._require_config()

    def test_require_config_returns_the_assigned_config(self) -> None:
        compiler = IntentCompiler.__new__(IntentCompiler)
        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler._config = config
        assert compiler._require_config() is config

    def test_context_builders_do_not_invent_a_permissive_config(self) -> None:
        """The three call sites that used the permissive default must raise.

        This is the property that matters: not that a helper raises, but that no
        code path can reach ``allow_placeholder_prices=True`` without someone
        having written it down.
        """
        compiler = IntentCompiler.__new__(IntentCompiler)
        compiler.chain = "ethereum"
        compiler.wallet_address = _WALLET
        for builder in (
            lambda: compiler._base_compiler_context_kwargs(resolve_rpc_url=False),
            compiler._swap_compiler_context_kwargs,
        ):
            with pytest.raises(RuntimeError, match="_config is unset"):
                builder()


# =============================================================================
# (a) An empty oracle is fatal to a money-bearing compile, and only to that
# =============================================================================


class TestEmptyOracleCompiler:
    def test_empty_oracle_is_not_placeholder_mode(self) -> None:
        """``_using_placeholders`` stays False, which re-arms the price-impact
        guard: that guard is skipped whenever placeholder mode is on."""
        compiler = _empty_oracle_compiler()
        assert compiler._using_placeholders is False
        assert compiler.price_oracle == {}

    def test_unpriceable_token_raises_instead_of_returning_a_fabricated_price(self) -> None:
        compiler = _empty_oracle_compiler()
        with pytest.raises(ValueError, match="ETH"):
            compiler._require_token_price("ETH")

    def test_swap_compilation_fails_with_a_clear_no_price_error(self) -> None:
        """The whole point: a money-bearing intent refuses to compile.

        Revert the runner change and this compiler is built with
        ``allow_placeholder_prices=True`` and no oracle instead, at which point
        ETH prices at $2000 and the swap compiles a slippage floor off it.
        """
        compiler = _empty_oracle_compiler(chain="arbitrum")
        result = compiler.compile(Intent.swap(from_token="USDC", to_token="WETH", amount=Decimal("1000")))
        assert result.status.name == "FAILED"
        assert result.error is not None
        assert "price" in result.error.lower()

    def test_price_irrelevant_intent_still_compiles(self) -> None:
        """HOLD needs no price and must not be collateral damage."""
        compiler = _empty_oracle_compiler(chain="arbitrum")
        result = compiler.compile(Intent.hold(reason="no signal"))
        assert result.status.name == "SUCCESS"

    def test_assert_prices_available_reports_the_unpriceable_token(self) -> None:
        compiler = _empty_oracle_compiler()
        with pytest.raises(ValueError, match="WETH"):
            compiler.assert_prices_available(["WETH"])


# =============================================================================
# Carve-out 1: Safe / Zodiac permission discovery must keep working
# =============================================================================


class TestPermissionDiscoveryCarveOut:
    """Offline permission discovery compiles synthetic intents purely to
    enumerate the ``(target, selector)`` pairs a Safe must authorise. That
    calldata is never signed or submitted, and discovery runs with no oracle by
    construction. Break this and ``generate_manifest`` returns an EMPTY manifest
    -- and an empty manifest makes every Safe call revert at
    ``execTransactionWithRole`` rather than fail loudly."""

    def test_discovery_compiler_still_gets_placeholder_prices(self) -> None:
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=_WALLET,
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                permission_discovery=True,
            ),
        )
        assert compiler._using_placeholders is True
        assert compiler.price_oracle is not None
        assert compiler.price_oracle["ETH"] == Decimal("2000")

    def test_discovery_use_is_attributed_in_the_log(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The log must say WHICH sanctioned use served the table, so a
        production log can be read after the fact."""
        monkeypatch.delenv(PLACEHOLDER_ESCAPE_HATCH_ENV, raising=False)
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=_WALLET,
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                permission_discovery=True,
            ),
        )
        with caplog.at_level(logging.WARNING, logger="almanak.framework.intents.compiler_queries"):
            compiler._get_placeholder_prices()
        assert any("permission_discovery" in r.getMessage() for r in caplog.records)

    def test_real_manifest_generation_still_produces_permissions(self) -> None:
        """End-to-end proof, not a proxy: a real discovery run must still emit
        targets. A mocked assertion here would pass against an empty manifest.

        Two corrections from the PR #3640 review, both worth stating because the
        original version of this test was WORSE than no test:

        1. ``discover_permissions`` returns ``(permissions, warnings)``. The
           first version asserted on the 2-tuple, which is truthy even when zero
           permissions were discovered — a vacuous guard on precisely the
           property (a non-empty Zodiac manifest) whose absence makes every Safe
           call revert at ``execTransactionWithRole``. It is unpacked now.
        2. ``curve`` rather than ``uniswap_v3``: curve declares
           ``PermissionHints.offline_discovery=True``, so discovery resolves no
           implicit RPC and issues no ``eth_call``s. Uniswap's discovery reaches
           a quoter, which makes this test both slow and a function of network
           weather in CI (VIB-6046 D5 is the same reasoning that made curve opt
           in). Offline also makes the count deterministic.
        """
        from almanak.framework.intents.vocabulary import IntentType
        from almanak.framework.permissions.discovery import discover_permissions

        permissions, _warnings = discover_permissions(
            chain="ethereum",
            protocols=["curve"],
            intent_types=[IntentType.SWAP],
        )
        assert len(permissions) > 0, (
            "permission discovery produced NO permissions -- the Zodiac manifest would be "
            "empty and every Safe call would revert at execTransactionWithRole"
        )


# =============================================================================
# (d) The CI gate that keeps the table single
# =============================================================================


class TestPlaceholderPriceGateDetection:
    """Unit-level negative control for ``scripts/ci/check_placeholder_prices.py``.

    A gate nobody proves fires is a gate that silently stops firing.
    """

    @staticmethod
    def _first_dict(source: str) -> ast.Dict:
        node = next(n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Dict))
        return node

    def test_detects_a_planted_connector_local_price_table(self) -> None:
        from scripts.ci.check_placeholder_prices import _is_price_table

        planted = """
prices = {
    "WETH": Decimal("2000"),
    "WBTC": Decimal("45000"),
    "USDC": Decimal("1"),
    "WBNB": Decimal("300"),
}
"""
        assert _is_price_table(self._first_dict(planted)) is True

    @pytest.mark.parametrize(
        ("label", "source"),
        [
            (
                "gas estimates (int values, lowercase keys)",
                'x = {"approve": 50_000, "swap": 200_000, "add_liquidity": 700_000, "collect_fees": 200_000}',
            ),
            (
                "liquidation discounts (all values < 1)",
                'x = {"WETH": Decimal("0.05"), "WBTC": Decimal("0.05"), "USDC": Decimal("0.03"), "LINK": Decimal("0.08")}',
            ),
            (
                "computed values, not literals",
                'x = {"WETH": Decimal(str(a)), "WBTC": Decimal(str(b)), "USDC": Decimal(str(c)), "DAI": Decimal(str(d))}',
            ),
            (
                "too small to be a table",
                'x = {"WETH": Decimal("2000"), "USDC": Decimal("1")}',
            ),
        ],
    )
    def test_does_not_flag_lookalikes(self, label: str, source: str) -> None:
        """False positives are how a gate gets allowlisted into uselessness."""
        from scripts.ci.check_placeholder_prices import _is_price_table

        assert _is_price_table(self._first_dict(source)) is False, label

    def test_repo_is_currently_clean(self) -> None:
        """Every remaining connector-local table is allowlisted with a reason."""
        from scripts.ci.check_placeholder_prices import find_violations

        assert find_violations() == []

    def test_scientific_notation_decimals_are_still_prices(self) -> None:
        """CodeRabbit on PR #3640: a digits-only regex rejected ``Decimal("1e-8")``
        and ``Decimal("2E3")``, so a table written in scientific notation walked
        straight through the gate. Revert to the regex and this fails."""
        from scripts.ci.check_placeholder_prices import _is_price_table

        source = """
prices = {
    "WETH": Decimal("2E3"),
    "SHIB": Decimal("1e-8"),
    "USDC": Decimal("1"),
    "WBTC": Decimal("4.5E4"),
}
"""
        assert _is_price_table(self._first_dict(source)) is True

    def test_non_finite_decimals_are_not_prices(self) -> None:
        """``Decimal("NaN")``/``Decimal("Infinity")`` parse but are not prices."""
        from scripts.ci.check_placeholder_prices import _is_price_table

        source = 'x = {"WETH": Decimal("NaN"), "WBTC": Decimal("Infinity"), "USDC": Decimal("NaN"), "DAI": Decimal("NaN")}'
        assert _is_price_table(self._first_dict(source)) is False
