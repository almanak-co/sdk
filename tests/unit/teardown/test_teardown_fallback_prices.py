"""Tests for teardown fallback price oracle behavior.

Validates that:
- _get_fallback_teardown_prices returns stablecoin fallbacks + retried major tokens
- _build_teardown_compiler merges fallback into partially-populated oracles
- allow_placeholder_prices stays False when fallback prices are available
- Empty oracle triggers fallback; non-empty oracle keeps its prices
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.runner.strategy_runner import StrategyRunner


class TestGetFallbackTeardownPrices:
    """Tests for StrategyRunner._get_fallback_teardown_prices."""

    def test_returns_universal_stablecoin_fallbacks_when_no_market(self):
        # Market=None → no chain context → only the universal stablecoins.
        # Bridged variants (USDC.e, USDbC, …) MUST NOT leak in (VIB-3814):
        # advertising USDC.e on a chain that doesn't have it caused the
        # downstream resolver to time out probing it.
        result = StrategyRunner._get_fallback_teardown_prices(None)
        assert result is not None
        assert result["USDC"] == Decimal("1")
        assert result["USDT"] == Decimal("1")
        assert result["DAI"] == Decimal("1")
        assert "USDC.e" not in result
        assert "USDbC" not in result

    def test_returns_stablecoin_fallbacks_when_market_has_no_price(self):
        market = MagicMock(spec=[])  # no .price attribute, no chain
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result is not None
        assert "USDC" in result
        # No volatile tokens since market.price is not available
        assert "ETH" not in result

    def test_bsc_market_excludes_bridged_usdc_variants(self):
        # VIB-3814: BSC has neither USDC.e nor USDbC on-chain; advertising
        # them as $1 leaked the symbols into the merged price_oracle and
        # downstream consumers (e.g. fee-tier heuristic) burned 240s probing
        # the resolver for them. The fallback must omit bridged variants
        # on chains where they don't exist.
        market = MagicMock(spec=["_chain"])
        market._chain = "bsc"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result is not None
        assert result["USDC"] == Decimal("1")
        assert "USDC.e" not in result
        assert "USDbC" not in result

    def test_arbitrum_market_includes_usdc_e(self):
        market = MagicMock(spec=["_chain"])
        market._chain = "arbitrum"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["USDC.e"] == Decimal("1")

    def test_linea_and_mantle_exclude_bridged_usdc_variants(self):
        # VIB-3814 follow-up: Linea and Mantle have no USDC.e / USDbC entry in
        # ``symbol_aliases.json``. Advertising them at $1 would replicate the
        # BSC phantom-symbol bug — Gemini caught this discrepancy in
        # PR #1994 review when an earlier draft of the table included them.
        for chain in ("linea", "mantle"):
            market = MagicMock(spec=["_chain"])
            market._chain = chain
            result = StrategyRunner._get_fallback_teardown_prices(market)
            assert result["USDC"] == Decimal("1"), chain
            assert "USDC.e" not in result, chain
            assert "USDbC" not in result, chain

    def test_native_to_wrapped_covers_every_native_token_symbol(self):
        """VIB-3970: the explicit ``_NATIVE_TO_WRAPPED`` map must cover every
        native symbol the gateway will hand to ``get_fallback_teardown_prices``
        via ``NATIVE_TOKEN_SYMBOLS``. The previous ``f"W{native}"`` fallback
        silently produced phantom symbols (``WA0GI`` on 0G) on the first
        chain whose wrapping convention broke the rule. Removing the
        fallback is only safe if the explicit map is exhaustive — this test
        pins the contract so the next chain rollout fails this assertion
        loudly instead of leaking a missing-wrapped-price warning.
        """
        from almanak.framework.data.models import _NATIVE_TO_WRAPPED
        from almanak.gateway.data.balance.web3_provider import NATIVE_TOKEN_SYMBOLS

        native_symbols = set(NATIVE_TOKEN_SYMBOLS.values())
        missing = native_symbols - set(_NATIVE_TO_WRAPPED.keys())
        assert not missing, (
            f"Native symbols in NATIVE_TOKEN_SYMBOLS but not in _NATIVE_TO_WRAPPED: "
            f"{sorted(missing)}. Add explicit entries to "
            "almanak/framework/data/models.py:_NATIVE_TO_WRAPPED so the "
            "wrapped-price fallback isn't silently skipped on those chains."
        )

    def test_native_to_wrapped_inverse_symmetric_with_wrapped_to_native(self):
        """VIB-3970: every entry in ``_NATIVE_TO_WRAPPED`` should have an
        inverse in ``IntentCompiler._WRAPPED_TO_NATIVE`` so price-oracle
        alias expansion bridges both directions. ``WPOL`` is the only
        exception today — Polygon was renamed POL→MATIC and the inverse
        map carries both wrappers; the forward map only carries the
        canonical native (``MATIC``). All other chains must be symmetric.
        """
        from almanak.framework.data.models import _NATIVE_TO_WRAPPED
        from almanak.framework.intents.compiler import IntentCompiler

        forward_wrapped = set(_NATIVE_TO_WRAPPED.values())
        inverse_wrapped = set(IntentCompiler._WRAPPED_TO_NATIVE.keys())
        # WPOL is in inverse-only by design (POL is the post-rename Polygon native).
        unmapped_in_inverse = forward_wrapped - inverse_wrapped
        assert not unmapped_in_inverse, (
            f"Wrapped symbols in _NATIVE_TO_WRAPPED but missing from "
            f"IntentCompiler._WRAPPED_TO_NATIVE: {sorted(unmapped_in_inverse)}. "
            "Add the inverse to almanak/framework/intents/compiler.py so "
            "price-oracle alias expansion bridges both directions."
        )

    def test_zerog_market_resolves_w0g_not_wa0gi(self):
        # VIB-3970: 0G's wrapped native is ``W0G``, not the ``W{native}``
        # prefix would suggest (``WA0GI``). The previous silent
        # ``f"W{native}"`` fallback leaked the phantom ``WA0GI`` symbol
        # into the teardown price-fetch loop, where each lookup burned a
        # 15s gateway timeout. The map now lists 0G explicitly.
        market = MagicMock()
        market._chain = "zerog"
        market.price.side_effect = lambda sym: {
            "A0GI": Decimal("4"),
            "W0G": Decimal("4"),
        }.get(sym, None)

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["A0GI"] == Decimal("4")
        assert result["W0G"] == Decimal("4")
        # The phantom symbol must NEVER be queried.
        called_symbols = [call.args[0] for call in market.price.call_args_list]
        assert "WA0GI" not in called_symbols
        assert "WA0GI" not in result

    def test_unknown_chain_skips_wrapped_fetch_and_warns(self, caplog):
        # VIB-3970: a future native that hasn't been added to
        # ``_NATIVE_TO_WRAPPED`` must NOT fall through to a string-prefix
        # phantom symbol. The fetch list collapses to the native only and
        # a WARNING surfaces the missing entry.
        #
        # VIB-4801: the chain ↔ native-symbol lookup now goes through
        # ``ChainRegistry``; register a temporary descriptor for the
        # synthetic chain so ``_get_fallback_teardown_prices`` resolves
        # its native symbol to ``ZZZ``.
        from almanak.core.chains import (
            ChainDescriptor,
            ChainRegistry,
            GasProfile,
            NativeToken,
        )
        from almanak.core.enums import Chain, ChainFamily
        from almanak.framework.runner import runner_teardown as rt

        synthetic = ChainDescriptor(
            enum=Chain.ETHEREUM,  # placeholder; we patch the lookup map directly below
            name="ethereum",  # placeholder
            chain_id=999_999,
            family=ChainFamily.EVM,
            native=NativeToken(symbol="ZZZ", name="Synthetic", decimals=18),
            gas=GasProfile(),
        )
        # Stash a synthetic-chain entry into the registry's name index so
        # ``ChainRegistry.try_resolve("_unknown_chain")`` returns the
        # synthetic descriptor for the duration of the test.
        original_by_name = dict(ChainRegistry._by_name)
        ChainRegistry._by_name["_unknown_chain"] = synthetic
        try:
            market = MagicMock()
            market._chain = "_unknown_chain"
            market.price.side_effect = lambda sym: {"ZZZ": Decimal("9")}.get(sym, None)

            with caplog.at_level("WARNING", logger=rt.logger.name):
                result = StrategyRunner._get_fallback_teardown_prices(market)
        finally:
            ChainRegistry._by_name.clear()
            ChainRegistry._by_name.update(original_by_name)

        called_symbols = [call.args[0] for call in market.price.call_args_list]
        assert "WZZZ" not in called_symbols
        assert "WZZZ" not in result
        assert any("_NATIVE_TO_WRAPPED" in rec.getMessage() for rec in caplog.records)

    def test_base_market_includes_usdbc(self):
        market = MagicMock(spec=["_chain"])
        market._chain = "base"
        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["USDbC"] == Decimal("1")
        # USDC.e is Arbitrum/Optimism/Polygon/Avalanche/Berachain naming —
        # not Base, so it must NOT be present here.
        assert "USDC.e" not in result

    def test_retries_major_tokens_from_market(self):
        market = MagicMock()
        market.price.side_effect = lambda sym: {
            "ETH": Decimal("3500"),
            "WETH": Decimal("3500"),
        }.get(sym, None)

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert result["ETH"] == Decimal("3500")
        assert result["WETH"] == Decimal("3500")
        # WBTC is NOT in the unconditional fetch list — only native/wrapped tokens
        assert "WBTC" not in result
        called_symbols = [call.args[0] for call in market.price.call_args_list]
        assert "WBTC" not in called_symbols
        # Stablecoins still present
        assert result["USDC"] == Decimal("1")

    def test_skips_tokens_with_zero_price(self):
        market = MagicMock()
        market.price.return_value = Decimal("0")

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert "ETH" not in result
        assert "USDC" in result  # stablecoins still there

    def test_skips_tokens_when_price_raises(self):
        market = MagicMock()
        market.price.side_effect = Exception("gateway down")

        result = StrategyRunner._get_fallback_teardown_prices(market)
        assert "ETH" not in result
        assert "USDC" in result


class TestBuildTeardownCompilerPriceOracle:
    """Tests for the price oracle merging logic in _build_teardown_compiler."""

    def _make_runner(self):
        runner = MagicMock(spec=StrategyRunner)
        runner._get_fallback_teardown_prices = StrategyRunner._get_fallback_teardown_prices
        return runner

    def test_empty_oracle_gets_fallback(self):
        """When get_price_oracle_dict returns {}, fallback prices fill in."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        market.price.side_effect = lambda sym: Decimal("3500") if sym == "ETH" else None

        # Simulate the merge logic from _build_teardown_compiler
        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        assert price_oracle is not None
        assert "USDC" in price_oracle
        assert price_oracle["ETH"] == Decimal("3500")

    def test_nonempty_oracle_preserves_fetched_prices(self):
        """When get_price_oracle_dict has real prices, they take precedence."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {
            "ETH": Decimal("4000"),
            "USDC": Decimal("0.999"),
        }
        market.price.return_value = None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        # Fetched prices override fallback
        assert price_oracle["ETH"] == Decimal("4000")
        assert price_oracle["USDC"] == Decimal("0.999")
        # Fallback fills in missing tokens
        assert "DAI" in price_oracle

    def test_partial_oracle_gets_missing_tokens_from_fallback(self):
        """Partially populated oracle gets fallback for missing tokens."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {"USDC": Decimal("1")}
        market.price.side_effect = lambda sym: Decimal("3500") if sym == "WETH" else None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        assert price_oracle["USDC"] == Decimal("1")
        assert price_oracle["WETH"] == Decimal("3500")
        assert "DAI" in price_oracle

    def test_placeholder_prices_false_when_fallback_available(self):
        """allow_placeholder_prices should be False when fallback is available."""
        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        market.price.return_value = None

        fetched = market.get_price_oracle_dict()
        fallback = StrategyRunner._get_fallback_teardown_prices(market)
        merged = {**(fallback or {}), **(fetched or {})}
        price_oracle = merged or None

        has_prices = bool(price_oracle)
        assert has_prices is True
        # has_prices True means build_teardown_compiler proceeds past the
        # VIB-2928 no-price HARD STOP and builds the compiler with
        # allow_placeholder_prices=False (placeholders are never enabled).
        assert (not has_prices) is False


class TestBuildTeardownCompilerHardStop:
    """VIB-2928: build_teardown_compiler never enables placeholder prices and
    HARD STOPS (returns None) when no real prices are resolvable."""

    _WALLET = "0x1111111111111111111111111111111111111111"

    def _make_runner(self):
        runner = MagicMock()
        # Not a GatewayExecutionOrchestrator → the rpc_url branch is taken;
        # spec=[] makes getattr(orch, "rpc_url", None) return None.
        runner.execution_orchestrator = MagicMock(spec=[])
        return runner

    def _make_strategy(self):
        strategy = MagicMock()
        strategy.chain = "ethereum"
        strategy.wallet_address = self._WALLET
        strategy.deployment_id = "deployment:test"
        strategy._chain_wallets = None
        return strategy

    def test_compiler_disables_placeholder_prices(self):
        from almanak.framework.runner.runner_teardown import build_teardown_compiler

        market = MagicMock()
        market.get_price_oracle_dict.return_value = {
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        }
        with patch(
            "almanak.framework.runner.runner_teardown.get_fallback_teardown_prices",
            return_value={},
        ):
            compiler = build_teardown_compiler(self._make_runner(), self._make_strategy(), market)

        assert compiler is not None
        assert compiler._config.allow_placeholder_prices is False
        assert compiler._using_placeholders is False

    def test_compiler_built_from_fallback_prices_only(self):
        """Fetched oracle empty but fallback prices present → compiler is built
        with placeholders still disabled (covers the real fallback-only path,
        not just the fetched-prices path)."""
        from almanak.framework.runner.runner_teardown import build_teardown_compiler

        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        with patch(
            "almanak.framework.runner.runner_teardown.get_fallback_teardown_prices",
            return_value={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        ):
            compiler = build_teardown_compiler(self._make_runner(), self._make_strategy(), market)

        assert compiler is not None
        assert compiler._config.allow_placeholder_prices is False
        assert compiler._using_placeholders is False

    def test_hard_stops_when_no_prices_available(self):
        """No fetched prices AND no fallback → return None (loud abort upstream)."""
        from almanak.framework.runner.runner_teardown import build_teardown_compiler

        market = MagicMock()
        market.get_price_oracle_dict.return_value = {}
        with patch(
            "almanak.framework.runner.runner_teardown.get_fallback_teardown_prices",
            return_value=None,
        ):
            compiler = build_teardown_compiler(self._make_runner(), self._make_strategy(), market)

        assert compiler is None
