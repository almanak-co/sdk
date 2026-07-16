"""Regression tests for ALM-2696: documented strategy-facing MarketSnapshot API.

The bug surfaced when ``ArbTASwapRSIStrategy`` called ``market.pool_price(...)``
in a hosted deployment and crashed with::

    'MarketSnapshot' object has no attribute 'pool_price'

Root cause: the SDK shipped two ``MarketSnapshot`` classes — a data-layer one
with the full ``pool_*`` / ``twap`` / ``lwap`` / ``ohlcv`` / ``il_*`` /
``prediction`` / ``realized_vol`` API, and a strategy-facing one (the one the
runner hands ``decide()``) that lacked all of those methods. The
``almanak-strategy-builder`` skill documented the full surface, so any author
following the docs verbatim hit ``AttributeError`` at runtime.

These tests pin the contract: every method documented in
``almanak/skills/almanak-strategy-builder/SKILL.md`` must exist on the
strategy-facing :class:`MarketSnapshot`. When a provider is missing, the
method must raise a clear :class:`ValueError` (not :class:`AttributeError`)
so strategies fail loudly with an actionable error rather than silently
disabling logic via ``hasattr`` guards.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

import pytest

from almanak.framework.market import MarketSnapshot
# =============================================================================
# Documented public API on the strategy-facing MarketSnapshot
# =============================================================================
#
# Source of truth: ``almanak/skills/almanak-strategy-builder/SKILL.md``
# (sections "Pool and DEX Data", "Price Aggregation and Slippage", "OHLCV Data",
# "Multi-Token Queries", "Lending and Funding Rates", "Impermanent Loss",
# "Prediction Markets", "Rate History", "Position Health", "LST Exchange Rates",
# "Risk Metrics", "Yield and Analytics").
#
# This is the canonical public surface a strategy author can reach for in
# ``decide(market)``. If a method is removed/renamed, update both sources
# (skill doc + this list) in lockstep.
DOCUMENTED_METHODS: tuple[str, ...] = (
    # Core data
    "price",
    "price_data",
    "balance",
    "balance_usd",
    "collateral_value_usd",
    "total_portfolio_usd",
    # ``prices`` and ``balances`` (batch-fetcher methods on the deprecated
    # data-layer class) are deliberately deferred to VIB-4065 / GH#2126 —
    # they collide with legacy ``hasattr(market, "prices")`` /
    # ``market.balances.get(...)`` callers in runner_state.py and a handful
    # of tests. Use ``price()`` / ``balance()`` per token until Phase 2
    # migrates those call sites in lockstep.
    # Indicators
    "rsi",
    "macd",
    "bollinger_bands",
    "stochastic",
    "atr",
    "sma",
    "ema",
    "adx",
    "obv",
    "cci",
    "ichimoku",
    # OHLCV / gas / health
    "ohlcv",
    "gas_price",
    "health",
    # Pools and DEX
    "pool_price",
    "pool_price_by_pair",
    "pool_reserves",
    "pool_history",
    "pool_analytics",
    "best_pool",
    # Price aggregation and slippage
    "twap",
    "lwap",
    "liquidity_depth",
    "estimate_slippage",
    "price_across_dexs",
    "best_dex_price",
    # Rates
    "lending_rate",
    "best_lending_rate",
    "funding_rate",
    "funding_rate_spread",
    "lending_rate_history",
    "funding_rate_history",
    # IL
    "il_exposure",
    "projected_il",
    # Prediction markets
    "prediction",
    "prediction_price",
    "prediction_positions",
    "prediction_orders",
    # Risk metrics
    "realized_vol",
    "vol_cone",
    "portfolio_risk",
    "rolling_sharpe",
    # Yield
    "yield_opportunities",
    # LSTs (Solana)
    "lst_exchange_rate",
    "lst_all_rates",
    # Position health
    "position_health",
    "pt_position_health",
    # Wallet activity
    "wallet_activity",
)


# =============================================================================
# Tests
# =============================================================================


@pytest.fixture
def bare_snapshot() -> MarketSnapshot:
    """A snapshot with NO providers wired — mimics a strategy author who
    constructs ``MarketSnapshot`` directly in a unit test, or a deployment
    runtime where a particular provider hasn't been configured yet.
    """
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x0000000000000000000000000000000000000000",
    )


class TestDocumentedMethodsExist:
    """Pin the public method surface against the documented API.

    Reproduces the ALM-2696 failure mode: on ``main`` before the fix, calling
    ``market.pool_price(...)`` raised ``AttributeError`` because the method
    didn't exist on the strategy-facing ``MarketSnapshot``. After the fix,
    every documented method exists and is callable.
    """

    @pytest.mark.parametrize("method_name", DOCUMENTED_METHODS)
    def test_method_exists_on_class(self, method_name: str) -> None:
        """Every documented method must be defined on the class."""
        assert hasattr(MarketSnapshot, method_name), (
            f"MarketSnapshot.{method_name} is documented in the strategy-builder "
            f"skill but missing from the runtime class. This is the ALM-2696 "
            f"AttributeError class — adding the method (even as a delegating "
            f"stub that raises ValueError when no provider is wired) is required."
        )

    @pytest.mark.parametrize("method_name", DOCUMENTED_METHODS)
    def test_method_is_callable(self, method_name: str, bare_snapshot: MarketSnapshot) -> None:
        """Every documented method must be a bound, callable attribute on
        an instance — not a property, not a class variable, not a stub."""
        attr = getattr(bare_snapshot, method_name)
        assert callable(attr), (
            f"MarketSnapshot.{method_name} exists but is not callable "
            f"(got {type(attr).__name__}). Strategy authors call these "
            f"as ``market.{method_name}(...)``."
        )


class TestPoolPriceContractALM2696:
    """The exact reproduction the ALM-2696 reporter hit.

    The reporter's ``_pool_has_liquidity`` did
    ``market.pool_price(self.pool_address, chain=self.execution_chain)`` and
    crashed with ``'MarketSnapshot' object has no attribute 'pool_price'``.

    These tests pin the precise call-shape and error contract.
    """

    def test_pool_price_exists_and_is_callable(self, bare_snapshot: MarketSnapshot) -> None:
        """``market.pool_price(...)`` must exist as a bound method."""
        assert hasattr(bare_snapshot, "pool_price")
        assert callable(bare_snapshot.pool_price)

    def test_pool_price_raises_value_error_when_provider_missing(
        self, bare_snapshot: MarketSnapshot
    ) -> None:
        """When no pool reader registry is wired, ``pool_price`` must raise
        a clear ``ValueError`` — NOT ``AttributeError``, NOT silently return,
        NOT ``None``. The point of the fix is that strategy authors never get
        ``AttributeError`` for this method, and never silently lose
        liquidity-gate logic the way the reporter's ``hasattr`` workaround did.
        """
        with pytest.raises(ValueError, match=r"[Nn]o pool reader registry"):
            bare_snapshot.pool_price(
                "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
                chain="arbitrum",
            )

    def test_pool_price_signature_matches_data_layer(self) -> None:
        """The call shape must match the documented (and data-layer) signature
        — ``pool_price(pool_address, chain=None)`` — so strategies that follow
        the skill verbatim work without modification across both classes.

        We pin not just the parameter *names* but also the optionality of
        ``chain`` (default ``None``, positional-or-keyword): making ``chain``
        required, keyword-only, or non-``None``-defaulted would still pass a
        names-only check while breaking the documented call shape
        ``market.pool_price(pool_address, chain=...)`` at runtime.
        """
        sig = inspect.signature(MarketSnapshot.pool_price)
        params = sig.parameters
        names = list(params)
        # 'self' + the documented positional/keyword params
        assert names == ["self", "pool_address", "chain"], (
            f"pool_price signature changed unexpectedly: got params={names}. "
            f"The strategy-builder skill documents "
            f"``market.pool_price(pool_address, chain=...)``."
        )
        # ``pool_address`` is required and positional-or-keyword.
        assert params["pool_address"].default is inspect.Parameter.empty, (
            "pool_address must remain a required argument; making it default "
            "to None would silently re-introduce the ALM-2696 hasattr-style "
            "'gracefully degrade' anti-pattern at the call site."
        )
        assert params["pool_address"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        # ``chain`` is optional with a None default, positional-or-keyword.
        assert params["chain"].default is None, (
            "chain must default to None so strategies can rely on the "
            "snapshot's chain when omitted; changing this default would "
            "break the documented call shape."
        )
        assert params["chain"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


class TestAaveHealthFactorAccessor:
    """``aave_health_factor`` is a strategy-facing accessor that multi-chain
    leverage strategies (e.g. ``leverage_loop_cross_chain``) call as
    ``market.aave_health_factor(chain=...)``.

    Before this method existed, the call raised ``'MarketSnapshot' object has
    no attribute 'aave_health_factor'`` — the same ALM-2696 ``AttributeError``
    class as ``pool_price``. It was worse here because the leverage strategy's
    ``decide()`` calls it un-guarded (so the missing method crashed the
    decision loop) while its teardown path swallowed the error and silently
    reported no Aave positions. These tests pin the contract: the method
    exists, returns ``None`` (a soft signal) when no provider is wired,
    delegates to the wired ``(chain) -> Decimal | None`` provider, and never
    silently swallows a provider error.
    """

    def test_exists_and_is_callable(self, bare_snapshot: MarketSnapshot) -> None:
        assert hasattr(bare_snapshot, "aave_health_factor")
        assert callable(bare_snapshot.aave_health_factor)

    def test_returns_none_when_provider_unconfigured(self, bare_snapshot: MarketSnapshot) -> None:
        """Soft-signal contract: no provider wired -> ``None`` (NOT a raise,
        NOT ``AttributeError``). Mirrors ``prediction_price`` so ``decide()``
        can branch with ``if hf is None``."""
        assert bare_snapshot.aave_health_factor(chain="arbitrum") is None

    def test_delegates_to_provider_with_resolved_chain(self) -> None:
        """When wired, delegate to the provider and return its value, passing
        the resolved chain through (proves a real wire-up, not a stub)."""
        seen: list[str] = []

        def _provider(chain: str) -> Decimal:
            seen.append(chain)
            return Decimal("1.75")

        snapshot = MarketSnapshot(
            chains=("base", "arbitrum"),
            wallet_address="0x0000000000000000000000000000000000000000",
            aave_health_factor_provider=_provider,
        )

        assert snapshot.aave_health_factor(chain="arbitrum") == Decimal("1.75")
        assert seen == ["arbitrum"]

    def test_returns_none_when_provider_reports_no_position(self) -> None:
        """A wired provider returning ``None`` (no live position) passes
        through unchanged."""
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            aave_health_factor_provider=lambda _chain: None,
        )

        assert snapshot.aave_health_factor(chain="arbitrum") is None

    def test_provider_errors_propagate(self) -> None:
        """A failing provider must NOT be coerced to ``None`` — a gateway
        outage mistaken for "no position" would let a cross-chain strategy
        stack leverage on an existing position. Fail loud."""

        def _boom(_chain: str) -> Decimal:
            raise RuntimeError("gateway unavailable")

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            aave_health_factor_provider=_boom,
        )

        with pytest.raises(RuntimeError, match="gateway unavailable"):
            snapshot.aave_health_factor(chain="arbitrum")

    def test_signature_is_keyword_only_chain(self) -> None:
        """Pin the documented call shape ``aave_health_factor(chain=...)``:
        ``chain`` is keyword-only with a ``None`` default, matching
        ``balance`` / ``price`` (PRD §4.2)."""
        sig = inspect.signature(MarketSnapshot.aave_health_factor)
        params = sig.parameters
        assert list(params) == ["self", "chain"]
        assert params["chain"].default is None
        assert params["chain"].kind is inspect.Parameter.KEYWORD_ONLY

    def test_raises_ambiguous_chain_error_on_multichain_without_chain(self) -> None:
        """On a multi-chain snapshot, calling ``aave_health_factor`` without an
        explicit ``chain`` must raise ``AmbiguousChainError`` (PRD §4.2). A
        provider is wired so resolution is reached -- the no-provider branch
        short-circuits to ``None`` *before* ``_resolve_chain`` (see
        ``test_returns_none_when_provider_unconfigured``)."""
        from almanak.framework.market.errors import AmbiguousChainError

        snapshot = MarketSnapshot(
            chains=("base", "arbitrum"),
            wallet_address="0x0000000000000000000000000000000000000000",
            aave_health_factor_provider=lambda _chain: Decimal("1.5"),
        )

        with pytest.raises(AmbiguousChainError, match="chain=None on a multi-chain snapshot"):
            snapshot.aave_health_factor()

    def test_raises_chain_not_configured_error_on_unconfigured_chain(self) -> None:
        """Calling ``aave_health_factor`` with a chain not in the configured set
        must raise ``ChainNotConfiguredError`` (PRD §4.2). Provider wired for the
        same reason as above."""
        from almanak.framework.market.errors import ChainNotConfiguredError

        snapshot = MarketSnapshot(
            chains=("base", "arbitrum"),
            wallet_address="0x0000000000000000000000000000000000000000",
            aave_health_factor_provider=lambda _chain: Decimal("1.5"),
        )

        with pytest.raises(ChainNotConfiguredError, match="not in configured chains"):
            snapshot.aave_health_factor(chain="ethereum")


class TestProviderlessMethodsRaiseValueError:
    """Provider-driven methods must raise ``ValueError`` (not ``AttributeError``,
    not silently return) when the runner has not wired their provider.

    Why this matters: the bug-report's local fix used
    ``hasattr(market, "pool_price")`` + ``try/except AttributeError`` to
    "gracefully degrade". That made liquidity sanity checks **silently
    disabled** in production — the strategy then traded blind. The contract
    here is loud-fail: a missing provider raises a clear, named error so
    the strategy holds (or the operator wires the provider), but never
    quietly skips a safety check.
    """

    @pytest.mark.parametrize(
        ("method_name", "args", "kwargs", "needle"),
        [
            ("pool_price", ("0xpool",), {}, "pool reader registry"),
            ("pool_price_by_pair", ("WETH", "USDC"), {}, "pool reader registry"),
            ("pool_reserves", ("0xpool",), {}, "pool reader"),
            ("twap", ("WETH/USDC",), {}, "price aggregator"),
            ("lwap", ("WETH/USDC",), {}, "price aggregator"),
            ("pool_history", ("0xpool",), {"protocol": "uniswap_v3"}, "pool history reader"),
            ("liquidity_depth", ("0xpool",), {}, "liquidity depth reader"),
            (
                "estimate_slippage",
                ("WETH", "USDC", Decimal("100")),
                {},
                "slippage estimator",
            ),
            ("pool_analytics", ("0xpool",), {}, "pool analytics reader"),
            ("best_pool", ("WETH", "USDC"), {}, "pool analytics reader"),
            ("yield_opportunities", ("USDC",), {}, "yield aggregator"),
            (
                "lending_rate_history",
                ("aave_v3", "USDC"),
                {},
                "rate history reader",
            ),
            (
                "funding_rate_history",
                ("hyperliquid", "ETH-USD"),
                {},
                "rate history reader",
            ),
            (
                "il_exposure",
                ("position-1",),
                {},
                "IL calculator",
            ),
            (
                "projected_il",
                ("WETH", "USDC", Decimal("10")),
                {},
                "IL calculator",
            ),
            ("realized_vol", ("WETH",), {}, "volatility calculator"),
            ("vol_cone", ("WETH",), {}, "volatility calculator"),
            (
                "portfolio_risk",
                ([0.01, -0.02, 0.03],),
                {},
                "risk calculator",
            ),
            (
                "rolling_sharpe",
                ([0.01, -0.02, 0.03],),
                {},
                "risk calculator",
            ),
            ("gas_price", (), {}, "gas oracle"),
            ("ohlcv", ("WETH",), {}, "OHLCV"),
            ("prediction", ("market-1",), {}, "prediction provider"),
            # SKILL.md documents these as accepting a market_id; the impl
            # makes it optional but a strategy author following the doc
            # verbatim passes one. Align with the documented call shape so
            # this matrix keeps verifying ValueError if the signature ever
            # tightens.
            ("prediction_positions", ("market-1",), {}, "prediction provider"),
            ("prediction_orders", ("market-1",), {}, "prediction provider"),
            ("lst_exchange_rate", ("jitoSOL",), {}, "Solana LST"),
            ("lst_all_rates", (), {}, "Solana LST"),
            # ``price_across_dexs`` / ``best_dex_price`` are excluded from this
            # parametrize set because they raise ``NotImplementedError`` (their
            # historical contract) rather than ``ValueError`` when no
            # multi_dex_service is wired. See
            # ``test_multi_dex_methods_raise_not_implemented`` below.
            ("funding_rate", ("hyperliquid", "ETH-USD"), {}, "funding rate"),
            (
                "funding_rate_spread",
                ("ETH-USD", "binance", "hyperliquid"),
                {},
                "funding rate",
            ),
            ("lending_rate", ("aave_v3", "USDC"), {}, "rate monitor"),
            ("best_lending_rate", ("USDC",), {}, "rate monitor"),
        ],
    )
    def test_raises_value_error(
        self,
        bare_snapshot: MarketSnapshot,
        method_name: str,
        args: tuple,
        kwargs: dict,
        needle: str,
    ) -> None:
        method = getattr(bare_snapshot, method_name)
        with pytest.raises(ValueError) as exc_info:
            method(*args, **kwargs)
        # Match case-insensitively so we don't pin to the exact wording.
        assert needle.lower() in str(exc_info.value).lower(), (
            f"{method_name}({args}, {kwargs}) raised ValueError "
            f"but message did not mention '{needle}': {exc_info.value!r}"
        )

    def test_prediction_price_returns_none_when_unconfigured(
        self, bare_snapshot: MarketSnapshot
    ) -> None:
        """``prediction_price`` is the one historical exception: it returns
        ``None`` (not raises) when no provider is configured, since
        strategies use it as a soft signal. Pin that behaviour so the
        ``ValueError``-everywhere refactor doesn't accidentally tighten it.
        """
        assert bare_snapshot.prediction_price("market-1", "YES") is None

    def test_wallet_activity_returns_empty_list_when_unconfigured(
        self, bare_snapshot: MarketSnapshot
    ) -> None:
        """Same pattern for ``wallet_activity`` — graceful empty list."""
        assert bare_snapshot.wallet_activity() == []

    @pytest.mark.parametrize(
        "method_name",
        ["price_across_dexs", "best_dex_price"],
    )
    def test_multi_dex_methods_raise_not_implemented(
        self, bare_snapshot: MarketSnapshot, method_name: str
    ) -> None:
        """``price_across_dexs`` and ``best_dex_price`` raise
        ``MultiDexUnavailableError`` when no MultiDexService is wired — a
        dual-contract error inheriting BOTH ``NotImplementedError`` (the
        pinned legacy contract; existing catches keep working) and
        ``ValueError`` (so standard data-unavailable catches degrade to
        HOLD instead of crashing, ALM-2951).
        """
        method = getattr(bare_snapshot, method_name)
        with pytest.raises(NotImplementedError, match=r"[Mm]ulti-DEX") as excinfo:
            method("WETH", "USDC", Decimal("1"))
        # Dual contract (ALM-2951): the same error must satisfy standard
        # data-unavailable catches too.
        assert isinstance(excinfo.value, ValueError)


class TestProvidersAreWired:
    """When a provider is passed at construction, the corresponding method
    actually delegates to it (not a no-op stub).
    """

    def test_pool_price_delegates_to_registry(self) -> None:
        """``pool_price`` reads from the registry's reader, not a hardcoded
        stub. Ensures the fix is a real wire-up, not a placeholder."""
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        sentinel = object()

        class _StubReader:
            def read_pool_price(self, pool_address: str, chain: str) -> object:
                # Returning a sentinel proves the snapshot delegated to us.
                return sentinel

        class _StubRegistry:
            def protocols_for_chain(self, chain: str) -> list[str]:
                return ["uniswap_v3"]

            def get_reader(self, chain: str, protocol: str) -> _StubReader:
                return _StubReader()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=_StubRegistry(),
        )
        result = ms.pool_price("0xpool", chain="arbitrum")
        assert result is sentinel

        # And: a registry that fails for all protocols surfaces a
        # PoolPriceUnavailableError, not a silent skip.
        class _FailingReader:
            def read_pool_price(self, pool_address: str, chain: str) -> object:
                raise RuntimeError("rpc down")

        class _FailingRegistry:
            def protocols_for_chain(self, chain: str) -> list[str]:
                return ["uniswap_v3"]

            def get_reader(self, chain: str, protocol: str) -> _FailingReader:
                return _FailingReader()

        ms2 = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=_FailingRegistry(),
        )
        with pytest.raises(PoolPriceUnavailableError):
            ms2.pool_price("0xpool", chain="arbitrum")


class TestTwapDecimalDerivation:
    """``twap`` must NOT silently assume token decimals when the caller
    supplies ``pool_address`` directly.

    Before this fix, the explicit-pool branch hardcoded
    ``token0_decimals=18`` / ``token1_decimals=6``, which is correct for
    WETH/USDC but produces TWAP values off by powers of ten for pools like
    WBTC/WETH (8/18) or USDC/USDT (6/6). Since TWAP is classified
    EXECUTION_GRADE and is consumed by execution-blocking decisions, a
    silent power-of-ten error is unacceptable. The contract is now:

    1. Caller supplies ``token0_decimals`` / ``token1_decimals`` →
       use them verbatim, no metadata roundtrip.
    2. Caller omits decimals but a ``pool_reader_registry`` is wired →
       resolve via the reader's pool metadata.
    3. Neither path available → raise ``ValueError`` with an explicit
       remediation hint (NOT a silent guess).
    """

    def _aggregator_capturing(self) -> object:
        """Return a stub PriceAggregator that records the kwargs it was
        passed, so tests can assert decimals propagated correctly."""

        class _Captured:
            def __init__(self) -> None:
                self.last_kwargs: dict | None = None

            def twap(self, **kwargs: object) -> object:
                self.last_kwargs = kwargs
                return object()  # any sentinel — we don't introspect

        return _Captured()

    def test_explicit_pool_no_registry_no_decimals_raises_value_error(self) -> None:
        """Without decimals AND without a registry, ``twap`` must raise
        ``ValueError`` rather than fall through to a hardcoded 18/6."""
        agg = self._aggregator_capturing()
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            price_aggregator=agg,
            # no pool_reader_registry
        )
        with pytest.raises(ValueError, match=r"Cannot derive token decimals"):
            ms.twap(
                "WBTC/WETH",
                pool_address="0x2f5e87C9312fa29aed5c179E456625D79015299c",  # WBTC/WETH
                window_seconds=60,
            )
        assert agg.last_kwargs is None, (
            "twap should not have called the aggregator at all when decimals "
            "cannot be derived — the ValueError must short-circuit before any "
            "off-by-powers-of-ten computation reaches the aggregator."
        )

    def test_explicit_pool_with_caller_decimals_skips_metadata_lookup(self) -> None:
        """When the caller supplies decimals, ``twap`` must use them
        verbatim and must NOT consult the registry — even if one is wired.
        This lets authors pass known decimals without paying for an RPC
        roundtrip."""
        agg = self._aggregator_capturing()

        class _AssertingRegistry:
            """Registry that fails the test if ``twap`` reaches it."""

            def get_reader(self, chain: str, protocol: str) -> object:  # pragma: no cover
                raise AssertionError(
                    "twap must NOT consult the registry when caller supplies decimals"
                )

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            price_aggregator=agg,
            pool_reader_registry=_AssertingRegistry(),
        )
        ms.twap(
            "WBTC/WETH",
            pool_address="0x2f5e87C9312fa29aed5c179E456625D79015299c",
            window_seconds=60,
            token0_decimals=8,
            token1_decimals=18,
        )
        assert agg.last_kwargs is not None
        assert agg.last_kwargs["token0_decimals"] == 8
        assert agg.last_kwargs["token1_decimals"] == 18

    def test_explicit_pool_with_registry_resolves_decimals(self) -> None:
        """When decimals are not supplied but a registry IS wired, ``twap``
        must read pool metadata and pass the resolved decimals."""
        agg = self._aggregator_capturing()

        class _StubReader:
            def _get_pool_metadata(self, pool_address: str, chain: str) -> tuple[int, int, int]:
                # WBTC = 8, WETH = 18, fee = 500
                return (8, 18, 500)

        class _StubRegistry:
            def get_reader(self, chain: str, protocol: str) -> _StubReader:
                return _StubReader()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            price_aggregator=agg,
            pool_reader_registry=_StubRegistry(),
        )
        ms.twap(
            "WBTC/WETH",
            pool_address="0x2f5e87C9312fa29aed5c179E456625D79015299c",
            window_seconds=60,
        )
        assert agg.last_kwargs is not None
        assert agg.last_kwargs["token0_decimals"] == 8
        assert agg.last_kwargs["token1_decimals"] == 18

    def test_explicit_pool_registry_metadata_failure_raises_value_error(self) -> None:
        """If the registry is wired but metadata fetch fails, ``twap`` must
        surface a ``ValueError`` (not silently fall back to 18/6)."""
        agg = self._aggregator_capturing()

        class _FailingReader:
            def _get_pool_metadata(self, pool_address: str, chain: str) -> tuple[int, int, int]:
                raise RuntimeError("rpc timeout")

        class _FailingRegistry:
            def get_reader(self, chain: str, protocol: str) -> _FailingReader:
                return _FailingReader()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            price_aggregator=agg,
            pool_reader_registry=_FailingRegistry(),
        )
        with pytest.raises(ValueError, match=r"Cannot derive token decimals"):
            ms.twap(
                "WBTC/WETH",
                pool_address="0x2f5e87C9312fa29aed5c179E456625D79015299c",
                window_seconds=60,
            )
        assert agg.last_kwargs is None


class TestPoolHistoryDefaultBoundsAreSnapshotTimestamp:
    """``pool_history`` must default the upper bound to the snapshot's
    iteration ``timestamp``, not ``datetime.now(UTC)``.

    Why this matters: ``MarketSnapshot`` is the unit of per-iteration
    determinism — every other read on it (price, balance, indicators,
    OHLCV) is anchored to ``self._timestamp``. Defaulting ``end_date`` to
    ``datetime.now(UTC)`` in ``pool_history`` would silently leak future
    data in backtests, paper runs, and historical snapshot replays, and
    make the method internally inconsistent with the rest of the snapshot
    state. Pinned per CodeRabbit review on PR #2125.
    """

    def _capturing_history_reader(self) -> object:
        """Return a stub PoolHistoryReader that records the bounds it was
        called with, so the test can assert the snapshot timestamp was used."""

        class _Captured:
            def __init__(self) -> None:
                self.last_kwargs: dict | None = None

            def get_pool_history(self, **kwargs: object) -> object:
                self.last_kwargs = kwargs
                return object()  # any sentinel — we don't introspect

        return _Captured()

    def test_default_end_is_snapshot_timestamp_not_now(self) -> None:
        """When neither ``start_date`` nor ``end_date`` is supplied, the
        upper bound must be the snapshot's iteration timestamp."""
        from datetime import datetime, timedelta, timezone

        reader = self._capturing_history_reader()
        # A timestamp explicitly NOT equal to "now" so the assertion has
        # discriminating power even if the test runs near a clock tick.
        snapshot_ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            timestamp=snapshot_ts,
            pool_history_reader=reader,
        )
        ms.pool_history("0xpool", protocol="uniswap_v3")

        assert reader.last_kwargs is not None
        assert reader.last_kwargs["end_date"] == snapshot_ts, (
            "pool_history default end_date must be the snapshot timestamp, "
            "not datetime.now(UTC) — leaking 'now' breaks deterministic "
            "replay (backtests / paper / historical snapshots)."
        )
        # Default start_date is end_date - 90 days, not now - 90 days.
        assert reader.last_kwargs["start_date"] == snapshot_ts - timedelta(days=90)

    def test_explicit_end_date_overrides_default(self) -> None:
        """Caller-supplied ``end_date`` must be respected verbatim."""
        from datetime import datetime, timezone

        reader = self._capturing_history_reader()
        snapshot_ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        explicit_end = datetime(2023, 12, 1, tzinfo=timezone.utc)
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            timestamp=snapshot_ts,
            pool_history_reader=reader,
        )
        ms.pool_history("0xpool", end_date=explicit_end, protocol="uniswap_v3")

        assert reader.last_kwargs is not None
        assert reader.last_kwargs["end_date"] == explicit_end


class TestVolConeOhlcvFetchSizeCap:
    """The implicit OHLCV fetch size in ``vol_cone`` / ``realized_vol`` must
    be bounded so sub-hourly timeframes don't blow up the per-iteration
    fetch / DataFrame build.

    Without this guard, ``vol_cone(timeframe="1m")`` on the default 90-day
    window asks for ~388,800 candles per call, which is unsafe in both
    local runs and hosted multi-tenant runners. The contract: when the
    computed limit would exceed the implicit cap and the caller has NOT
    set ``ohlcv_limit`` explicitly, raise ``ValueError`` with a clear
    remediation hint. An explicit ``ohlcv_limit`` overrides the cap (the
    caller has measured the cost).
    """

    def _stub_volatility_calculator(self) -> object:
        class _StubVol:
            def vol_cone(self, **kwargs: object) -> object:  # pragma: no cover
                raise AssertionError(
                    "vol_cone calculator must NOT be reached when the OHLCV "
                    "fetch would exceed the implicit cap — the cap should "
                    "short-circuit before any fetch."
                )

            def realized_vol(self, **kwargs: object) -> object:  # pragma: no cover
                raise AssertionError("same as above for realized_vol")

        return _StubVol()

    def test_vol_cone_at_1m_with_default_windows_raises_cap_error(self) -> None:
        """``vol_cone(timeframe="1m")`` on default windows would ask for
        ~388k candles; the cap must reject this with an actionable error."""
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            volatility_calculator=self._stub_volatility_calculator(),
        )
        with pytest.raises(ValueError, match=r"safe implicit cap"):
            ms.vol_cone("WETH", timeframe="1m")

    def test_vol_cone_explicit_ohlcv_limit_bypasses_cap(self) -> None:
        """An explicit ``ohlcv_limit`` is opt-in — the cap must NOT apply.
        We assert by reaching the calculator (which raises in the stub)."""

        class _ReachableVol:
            def vol_cone(self, **kwargs: object) -> object:
                # The fact that we got here means the cap did not fire.
                raise RuntimeError("reached calculator")

        # We also need ohlcv() to produce an empty df so we don't hit the
        # full pipeline; instead we expect VolatilityUnavailableError or
        # the RuntimeError above. Either signals the cap was bypassed.
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            volatility_calculator=_ReachableVol(),
        )
        # Without an OHLCV router, ``self.ohlcv(...)`` would itself raise.
        # That's fine — the assertion is that we got *past* the cap into
        # the OHLCV fetch / calculator path. The cap must NOT be the
        # first error.
        with pytest.raises(Exception) as excinfo:
            ms.vol_cone("WETH", timeframe="1m", ohlcv_limit=500_000)
        msg = str(excinfo.value)
        assert "safe implicit cap" not in msg, (
            "Explicit ohlcv_limit must bypass the cap — got cap error "
            f"instead: {msg!r}"
        )

    def test_vol_cone_at_1h_with_default_windows_does_not_hit_cap(self) -> None:
        """A reasonable hourly window must NOT trip the cap. Default
        windows (max=90 days) * 3 = 270 days at 1h = 6480 candles, well
        under the 10_000 limit."""

        class _ReachableVol:
            def vol_cone(self, **kwargs: object) -> object:
                raise RuntimeError("reached calculator")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            volatility_calculator=_ReachableVol(),
        )
        with pytest.raises(Exception) as excinfo:
            ms.vol_cone("WETH", timeframe="1h")
        assert "safe implicit cap" not in str(excinfo.value)

    @pytest.mark.parametrize("bad_limit", [0, -1, -100])
    def test_vol_cone_rejects_non_positive_explicit_ohlcv_limit(self, bad_limit: int) -> None:
        """Explicit ``ohlcv_limit`` must be ``> 0``. ``0`` and negatives are
        caller bugs that previously slipped through the truthiness check
        (``ohlcv_limit or default``) — ``0`` was silently treated as "use
        the default" and negatives propagated to the OHLCV fetch.
        """
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            volatility_calculator=self._stub_volatility_calculator(),
        )
        with pytest.raises(ValueError, match=r"ohlcv_limit must be > 0"):
            ms.vol_cone("WETH", timeframe="1h", ohlcv_limit=bad_limit)


class TestChainCasingNormalization:
    """All provider-driven methods must lowercase the chain string before
    handing it to the underlying provider. Mixed-case caller inputs (e.g.,
    ``chain="Arbitrum"``) otherwise produce avoidable lookup failures —
    most callers normalize, but a couple of the methods lifted in ALM-2696
    (``pool_reserves``, ``gas_price``) used to skip ``.lower()``.
    """

    def test_pool_reserves_normalizes_chain(self) -> None:
        """``pool_reserves(chain="Arbitrum")`` must reach the reader with
        ``"arbitrum"``."""
        captured: dict[str, str] = {}

        class _StubPoolReader:
            def get_pool_reserves(self, pool_address: str, chain: str):  # noqa: ANN201
                captured["pool"] = pool_address
                captured["chain"] = chain

                async def _coro():
                    return None  # we only assert on the captured args

                return _coro()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader=_StubPoolReader(),
        )
        # Pass mixed-case explicitly. The fact that the underlying coroutine
        # returns ``None`` doesn't matter — we only care that ``chain`` was
        # normalized before reaching the reader.
        try:
            ms.pool_reserves("0xPool", chain="Arbitrum")
        except Exception:  # noqa: BLE001
            pass
        assert captured["chain"] == "arbitrum", (
            f"pool_reserves did not lowercase chain: got {captured['chain']!r}"
        )

    def test_gas_price_normalizes_chain(self) -> None:
        """``gas_price(chain="Arbitrum")`` must reach the oracle with
        ``"arbitrum"``."""
        captured: dict[str, str] = {}

        class _StubGasOracle:
            def get_gas_price(self, chain: str):  # noqa: ANN201
                captured["chain"] = chain

                async def _coro():
                    return None

                return _coro()

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            gas_oracle=_StubGasOracle(),
        )
        try:
            ms.gas_price(chain="Arbitrum")
        except Exception:  # noqa: BLE001
            pass
        assert captured["chain"] == "arbitrum", (
            f"gas_price did not lowercase chain: got {captured['chain']!r}"
        )


class TestOhlcvLegacyFallbackRejectsPoolAddress:
    """The legacy ``OHLCVModule`` is strictly token-scoped (CEX tape) and has
    no ``pool_address`` parameter. When only the legacy module is wired and
    the caller passes ``pool_address=...``, the snapshot must NOT silently
    drop it and return candles for a different market — that's the worst
    failure mode for an indicator-driven strategy. Instead, raise
    ``ValueError`` and tell the caller to wire the OHLCV router.
    """

    def test_pool_address_with_legacy_ohlcv_module_raises_value_error(self) -> None:
        class _StubOhlcvModule:
            def get_ohlcv(self, **kwargs: object):  # noqa: ANN201, pragma: no cover
                raise AssertionError(
                    "Legacy OHLCV module must NOT be reached when pool_address "
                    "is set — the snapshot should raise ValueError before the "
                    "fallback runs."
                )

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_module=_StubOhlcvModule(),
        )
        with pytest.raises(ValueError, match=r"pool_address requires an OHLCV router"):
            ms.ohlcv("WETH", timeframe="1h", limit=10, pool_address="0xPool")

    def test_legacy_ohlcv_module_still_works_without_pool_address(self) -> None:
        """Regression: the legacy fallback must still fire normally when the
        caller does NOT pass ``pool_address``."""
        captured: dict[str, object] = {}

        class _StubOhlcvModule:
            def get_ohlcv(self, **kwargs: object):  # noqa: ANN201
                captured.update(kwargs)
                import pandas as pd

                df = pd.DataFrame()
                return df

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_module=_StubOhlcvModule(),
        )
        ms.ohlcv("WETH", timeframe="1h", limit=10)
        assert captured.get("token") == "WETH"
        assert "pool_address" not in captured, (
            "Legacy OHLCV module must NOT receive pool_address (it doesn't "
            "accept the kwarg)."
        )


class TestPtPositionHealthFailsFastWithoutTransport:
    """``pt_position_health`` must fail fast with a clear error when neither
    a connected ``GatewayClient`` nor an explicit ``rpc_url`` is available.

    Without this guard, ``PositionHealthProvider`` is constructed with
    ``rpc_url=""`` and a missing/disconnected gateway, and the failure
    surfaces as a less specific downstream provider error. The contract
    is to raise ``HealthUnavailableError`` immediately so callers get an
    actionable contract error pointing at the missing transport.
    """

    def test_no_rpc_url_no_gateway_raises_health_unavailable(self) -> None:
        from almanak.framework.data.market_snapshot import HealthUnavailableError

        ms = MarketSnapshot(
            chain="ethereum",
            wallet_address="0x0000000000000000000000000000000000000000",
            # no gateway_client, no rpc_url
        )
        with pytest.raises(HealthUnavailableError, match=r"connected GatewayClient or an explicit rpc_url"):
            ms.pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
            )

    def test_disconnected_gateway_no_rpc_url_raises_health_unavailable(self) -> None:
        """A GatewayClient instance whose ``is_connected`` is False is the
        same as no client at all — the provider has no transport."""
        from almanak.framework.data.market_snapshot import HealthUnavailableError

        class _DisconnectedGateway:
            is_connected = False

        ms = MarketSnapshot(
            chain="ethereum",
            wallet_address="0x0000000000000000000000000000000000000000",
            gateway_client=_DisconnectedGateway(),
        )
        with pytest.raises(HealthUnavailableError, match=r"connected GatewayClient or an explicit rpc_url"):
            ms.pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
            )

    def test_explicit_rpc_url_bypasses_fail_fast(self) -> None:
        """An explicit ``rpc_url`` provides transport — the fail-fast guard
        must NOT fire. We assert by getting past the guard and into the
        provider's own error path (which raises HealthUnavailableError
        too, but with a different message)."""
        from almanak.framework.data.market_snapshot import HealthUnavailableError

        ms = MarketSnapshot(
            chain="ethereum",
            wallet_address="0x0000000000000000000000000000000000000000",
        )
        # Explicit rpc_url means the guard skips; PositionHealthProvider
        # will then fail later when it tries to actually use the bogus url
        # — that downstream error is fine (not the contract error we just
        # added). The assertion is that the contract error message does
        # NOT match the new fail-fast hint.
        with pytest.raises(HealthUnavailableError) as excinfo:
            ms.pt_position_health(
                morpho_market_id="0xmarket",
                pendle_market_address="0xpendle",
                rpc_url="http://127.0.0.1:1",
            )
        assert "connected GatewayClient or an explicit rpc_url" not in str(excinfo.value)


class TestPoolPriceByPairBranchCoverage:
    """Cover the multi-protocol resolution + failover branches of
    ``pool_price_by_pair``. The function tries every protocol registered
    for the chain (or the single one explicitly requested) and falls
    through on resolver-returns-None or reader-raises. Without these
    tests, only the no-registry-configured branch is exercised, leaving
    the function at CRAP=42 (cc=8, cov=19%).
    """

    def _make_envelope(self, source: str = "stub_reader") -> object:
        from datetime import UTC, datetime

        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )
        from almanak.framework.data.pools.reader import PoolPrice

        return DataEnvelope(
            value=PoolPrice(
                price=Decimal("3000"),
                tick=0,
                liquidity=0,
                fee_tier=3000,
                block_number=0,
                timestamp=datetime.now(UTC),
                pool_address="0xPool",
                token0_decimals=18,
                token1_decimals=6,
            ),
            meta=DataMeta(
                source=source,
                observed_at=datetime.now(UTC),
                finality="off_chain",
                staleness_ms=0,
                latency_ms=0,
                confidence=1.0,
                cache_hit=False,
            ),
            classification=DataClassification.INFORMATIONAL,
        )

    def _make_registry(self, behaviour: dict[str, object], chain_protocols: list[str]) -> object:
        """Return a stub PoolReaderRegistry whose readers behave per
        ``behaviour`` (mapping protocol -> ('ok'|'no_pool'|'raise', envelope?)).
        """

        outer_self = self

        class _StubReader:
            def __init__(self, mode: str, env: object | None) -> None:
                self.mode = mode
                self.env = env

            def resolve_pool_address(
                self,
                token_a: str,
                token_b: str,
                chain: str,
                fee_tier: int,
            ) -> str | None:
                if self.mode == "no_pool":
                    return None
                if self.mode == "raise":
                    raise RuntimeError(f"reader for {token_a}/{token_b} blew up")
                return "0xPool"

            def read_pool_price(self, pool_address: str, chain: str) -> object:
                return self.env or outer_self._make_envelope()

        class _StubRegistry:
            def protocols_for_chain(self, chain: str) -> list[str]:
                return list(chain_protocols)

            def get_reader(self, chain: str, protocol: str):  # noqa: ANN201
                mode_env = behaviour.get(protocol, ("no_pool", None))
                mode, env = mode_env  # type: ignore[misc]
                return _StubReader(mode, env)

        return _StubRegistry()

    def test_no_protocols_for_chain_raises_pool_price_unavailable(self) -> None:
        """If the registry has no protocols for the requested chain, the
        method must raise ``PoolPriceUnavailableError`` with an actionable
        message — NOT silently return None or a stale envelope.
        """
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=self._make_registry({}, chain_protocols=[]),
        )
        with pytest.raises(PoolPriceUnavailableError, match=r"No pool reader protocols"):
            ms.pool_price_by_pair("WETH", "USDC")

    def test_explicit_protocol_takes_precedence_over_registry(self) -> None:
        """When ``protocol="uniswap_v3"`` is passed, only that protocol is
        tried — the registry's full chain list is bypassed. Verifies the
        ``[protocol] if protocol else ...`` branch."""
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=self._make_registry(
                # Sushiswap (the registry default) would raise; we should
                # never reach it because the explicit protocol takes
                # precedence and resolves cleanly.
                {
                    "uniswap_v3": ("ok", self._make_envelope("uniswap_v3")),
                    "sushiswap_v3": ("raise", None),
                },
                chain_protocols=["sushiswap_v3", "uniswap_v3"],
            ),
        )
        env = ms.pool_price_by_pair("WETH", "USDC", protocol="uniswap_v3")
        assert env.meta.source == "uniswap_v3"

    def test_falls_through_when_resolver_returns_none(self) -> None:
        """A protocol whose resolver returns ``None`` (no pool deployed)
        is skipped without error; the next protocol in the registry's
        ordered list is tried."""
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=self._make_registry(
                {
                    "sushiswap_v3": ("no_pool", None),
                    "uniswap_v3": ("ok", self._make_envelope("uniswap_v3")),
                },
                chain_protocols=["sushiswap_v3", "uniswap_v3"],
            ),
        )
        env = ms.pool_price_by_pair("WETH", "USDC")
        assert env.meta.source == "uniswap_v3"

    def test_catches_reader_exception_and_falls_through(self) -> None:
        """A reader that raises (network blip, RPC failure, etc.) is
        caught and the next protocol is tried. The exception is captured
        as ``last_error`` so the final raise carries triage context."""
        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=self._make_registry(
                {
                    "sushiswap_v3": ("raise", None),
                    "uniswap_v3": ("ok", self._make_envelope("uniswap_v3")),
                },
                chain_protocols=["sushiswap_v3", "uniswap_v3"],
            ),
        )
        env = ms.pool_price_by_pair("WETH", "USDC")
        assert env.meta.source == "uniswap_v3"

    def test_raises_pool_price_unavailable_when_all_protocols_exhausted(self) -> None:
        """When every protocol either returns no pool or raises, the
        method must raise ``PoolPriceUnavailableError`` and include the
        last reader exception in the message for triage."""
        from almanak.framework.data.market_snapshot import PoolPriceUnavailableError

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            pool_reader_registry=self._make_registry(
                {
                    "sushiswap_v3": ("no_pool", None),
                    "uniswap_v3": ("raise", None),
                },
                chain_protocols=["sushiswap_v3", "uniswap_v3"],
            ),
        )
        with pytest.raises(PoolPriceUnavailableError, match=r"No pool found.*WETH/USDC"):
            ms.pool_price_by_pair("WETH", "USDC")


class TestIlExposureBranchCoverage:
    """Cover the IL-exposure exception narrowing + price-prefetch path.

    ``il_exposure`` previously had only the no-calculator branch tested,
    leaving the price-prefetch ``except`` paths and the three calculator
    error classes (``PositionNotFoundError``, ``ILExposureUnavailableError``,
    generic ``Exception``) at coverage 23%. CRAP=37 on cc=8.
    """

    def _make_calculator(
        self,
        position: object | None = None,
        position_error: Exception | None = None,
        calc_result: object | None = None,
        calc_error: Exception | None = None,
    ) -> object:
        from almanak.framework.data.lp import PositionNotFoundError

        class _StubCalc:
            def get_position(self_, position_id: str) -> object:  # noqa: N805, ANN001
                if position_error is not None:
                    raise position_error
                if position is not None:
                    return position
                raise PositionNotFoundError(position_id)

            def calculate_il_exposure(  # noqa: ANN201
                self_,  # noqa: ANN001, N805
                *,
                position_id: str,
                current_price_a: object,
                current_price_b: object,
                fees_earned: Decimal,
            ):
                if calc_error is not None:
                    raise calc_error
                if calc_result is not None:
                    return calc_result
                # Return type is opaque — tests assert on raises only.
                from types import SimpleNamespace

                return SimpleNamespace(position_id=position_id)

        return _StubCalc()

    def _make_position(self) -> object:
        # ``il_exposure`` only reads ``position.token_a`` / ``token_b`` on the
        # returned object before delegating to the calculator. A namespace is
        # sufficient and avoids depending on the production LPPosition
        # dataclass shape, which evolves under VIB-3475.
        from types import SimpleNamespace

        return SimpleNamespace(token_a="WETH", token_b="USDC", position_id="alm-2696-pos")

    def test_position_not_found_raises_il_unavailable(self) -> None:
        from almanak.framework.data.lp import PositionNotFoundError
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            il_calculator=self._make_calculator(position_error=PositionNotFoundError("alm-2696-pos")),
        )
        with pytest.raises(ILExposureUnavailableError, match=r"Position not found"):
            ms.il_exposure("alm-2696-pos")

    def test_calculator_error_wrapped_into_il_unavailable(self) -> None:
        from almanak.framework.data.lp import (
            ILExposureUnavailableError as CalcErr,
        )
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            il_calculator=self._make_calculator(
                position=self._make_position(),
                calc_error=CalcErr("alm-2696-pos", "stub-calc-failure"),
            ),
        )
        with pytest.raises(ILExposureUnavailableError, match=r"stub-calc-failure"):
            ms.il_exposure("alm-2696-pos")

    def test_unexpected_error_wrapped_into_il_unavailable(self) -> None:
        from almanak.framework.data.market_snapshot import ILExposureUnavailableError

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            il_calculator=self._make_calculator(
                position=self._make_position(),
                calc_error=RuntimeError("network blip"),
            ),
        )
        with pytest.raises(ILExposureUnavailableError, match=r"Unexpected error: network blip"):
            ms.il_exposure("alm-2696-pos")

    def test_price_oracle_failures_swallowed_so_calculator_runs(self) -> None:
        """When the price oracle is wired but ``self.price()`` raises for
        either token, ``il_exposure`` must still call the calculator with
        ``None`` price slots — IL is computable from initial-state alone.
        """
        from almanak.framework.data.market_snapshot import PriceUnavailableError

        captured: dict[str, object] = {}

        # The price oracle is called like ``oracle(token, quote, chain)`` —
        # raising on call propagates to the inner ``self.price()``, which
        # catches it and re-raises as ``ValueError`` (caught by
        # ``il_exposure``'s ``(PriceUnavailableError, ValueError)`` handler).
        def _failing_oracle(*args: object, **kwargs: object) -> object:
            raise PriceUnavailableError("WETH", "no provider")

        class _StubCalc:
            def get_position(self, position_id: str) -> object:
                return TestIlExposureBranchCoverage()._make_position()

            def calculate_il_exposure(self, **kwargs: object) -> object:
                from types import SimpleNamespace

                captured.update(kwargs)
                # Return type doesn't matter — the test asserts only on
                # captured kwargs. A namespace bypasses ILExposure's strict
                # dataclass shape (which evolves under VIB-3475).
                return SimpleNamespace(position_id=kwargs["position_id"])

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            il_calculator=_StubCalc(),
            price_oracle=_failing_oracle,
        )
        # The oracle's failure should be swallowed; calculator runs with None
        # for both prices. We assert via captured kwargs.
        ms.il_exposure("alm-2696-pos")
        assert captured["current_price_a"] is None
        assert captured["current_price_b"] is None


class TestOhlcvHelpers:
    """Cover the OHLCV helpers extracted from ``ohlcv`` in the cc=19 →
    cc≈4 refactor: ``_fetch_ohlcv_via_router``, ``_envelope_to_ohlcv_df``,
    ``_fetch_ohlcv_legacy``. The router path's empty-candles branch and
    the gap_strategy ffill/drop branches are easy misses.
    """

    def _make_envelope(self, candles: list[object], source: str = "stub-router") -> object:
        from datetime import UTC, datetime

        from almanak.framework.data.models import (
            DataClassification,
            DataEnvelope,
            DataMeta,
        )

        return DataEnvelope(
            value=candles,
            meta=DataMeta(
                source=source,
                observed_at=datetime.now(UTC),
                finality="off_chain",
                staleness_ms=0,
                latency_ms=0,
                confidence=1.0,
                cache_hit=False,
            ),
            classification=DataClassification.INFORMATIONAL,
        )

    def _make_candle(self, ts: object, **kw: object) -> object:
        from almanak.framework.data.interfaces import OHLCVCandle

        defaults = {
            "open": Decimal("3000"),
            "high": Decimal("3100"),
            "low": Decimal("2950"),
            "close": Decimal("3050"),
            "volume": Decimal("1000"),
        }
        defaults.update(kw)
        return OHLCVCandle(timestamp=ts, **defaults)  # type: ignore[arg-type]

    def test_router_returns_empty_dataframe_when_no_candles(self) -> None:
        """Empty router envelope → empty DataFrame with the documented
        attrs (``base = token_str`` for the empty case)."""

        class _Router:
            def get_ohlcv(self_, **kwargs: object) -> object:  # noqa: N805, ANN001
                return TestOhlcvHelpers()._make_envelope([])

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        df = ms.ohlcv("WETH", timeframe="1h", limit=10)
        assert df.empty
        assert df.attrs["base"] == "WETH"
        assert df.attrs["quote"] == "USD"
        assert df.attrs["timeframe"] == "1h"
        assert df.attrs["source"] == "stub-router"

    def test_router_data_source_error_wraps_to_ohlcv_unavailable(self) -> None:
        from almanak.framework.data.interfaces import DataSourceError
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        class _Router:
            def get_ohlcv(self, **kwargs: object) -> object:
                raise DataSourceError("upstream timeout")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        with pytest.raises(OHLCVUnavailableError, match=r"upstream timeout"):
            ms.ohlcv("WETH", timeframe="1h", limit=10)

    def test_router_unexpected_error_wraps_to_ohlcv_unavailable(self) -> None:
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        class _Router:
            def get_ohlcv(self, **kwargs: object) -> object:
                raise RuntimeError("mystery")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        with pytest.raises(OHLCVUnavailableError, match=r"Unexpected error: mystery"):
            ms.ohlcv("WETH", timeframe="1h", limit=10)

    def test_router_envelope_with_candles_materializes_dataframe(self) -> None:
        """Happy path: candles in → DataFrame out with the documented
        columns + attrs. Volume `None` coerces to NaN."""
        import math

        from datetime import UTC, datetime as _dt

        ts = _dt(2024, 1, 1, tzinfo=UTC)
        candles = [
            self._make_candle(ts),
            self._make_candle(ts, volume=None),  # NaN volume path
        ]

        class _Router:
            def get_ohlcv(self_, **kwargs: object) -> object:  # noqa: N805, ANN001
                return TestOhlcvHelpers()._make_envelope(candles)

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        df = ms.ohlcv("WETH", timeframe="1h", limit=10)
        assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
        assert len(df) == 2
        assert math.isnan(df["volume"].iloc[1])
        assert df.attrs["base"] == "WETH"

    @pytest.mark.parametrize("strategy", ["ffill", "drop"])
    def test_gap_strategy_branches(self, strategy: str) -> None:
        """``gap_strategy`` ``ffill`` and ``drop`` both apply the documented
        DataFrame transform after row materialization."""
        from datetime import UTC, datetime as _dt

        ts = _dt(2024, 1, 1, tzinfo=UTC)
        candles = [self._make_candle(ts), self._make_candle(ts, volume=None)]

        class _Router:
            def get_ohlcv(self_, **kwargs: object) -> object:  # noqa: N805, ANN001
                return TestOhlcvHelpers()._make_envelope(candles)

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        df = ms.ohlcv("WETH", timeframe="1h", limit=10, gap_strategy=strategy)
        # ffill replaces the NaN volume with the previous row's volume; drop
        # discards the NaN row entirely.
        if strategy == "drop":
            assert len(df) == 1
        else:
            assert len(df) == 2
            assert df["volume"].iloc[1] == 1000.0  # forward-filled

    def test_base_symbol_derivation_for_instrument(self) -> None:
        """When token is an ``Instrument``, the ``base`` attr comes from
        ``instrument.base`` rather than the BASE/QUOTE string."""
        from datetime import UTC, datetime as _dt

        from almanak.framework.data.models import Instrument

        ts = _dt(2024, 1, 1, tzinfo=UTC)
        candles = [self._make_candle(ts)]

        class _Router:
            def get_ohlcv(self_, **kwargs: object) -> object:  # noqa: N805, ANN001
                return TestOhlcvHelpers()._make_envelope(candles)

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        instrument = Instrument(base="WBTC", quote="USDT", chain="arbitrum")
        df = ms.ohlcv(instrument, timeframe="1h", limit=10)
        assert df.attrs["base"] == "WBTC"

    def test_base_symbol_derivation_for_pair_string(self) -> None:
        """When token is a ``BASE/QUOTE`` string, the ``base`` attr is the
        substring before the slash."""
        from datetime import UTC, datetime as _dt

        ts = _dt(2024, 1, 1, tzinfo=UTC)
        candles = [self._make_candle(ts)]

        class _Router:
            def get_ohlcv(self_, **kwargs: object) -> object:  # noqa: N805, ANN001
                return TestOhlcvHelpers()._make_envelope(candles)

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_router=_Router(),
        )
        df = ms.ohlcv("WETH/USDC", timeframe="1h", limit=10)
        assert df.attrs["base"] == "WETH"

    def test_legacy_path_data_source_error_wraps(self) -> None:
        from almanak.framework.data.interfaces import DataSourceError
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        class _Mod:
            def get_ohlcv(self, **kwargs: object) -> object:
                raise DataSourceError("legacy-fail")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_module=_Mod(),
        )
        with pytest.raises(OHLCVUnavailableError, match=r"legacy-fail"):
            ms.ohlcv("WETH", timeframe="1h", limit=10)

    def test_legacy_path_unexpected_error_wraps(self) -> None:
        from almanak.framework.data.market_snapshot import OHLCVUnavailableError

        class _Mod:
            def get_ohlcv(self, **kwargs: object) -> object:
                raise RuntimeError("legacy-mystery")

        ms = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x0000000000000000000000000000000000000000",
            ohlcv_module=_Mod(),
        )
        with pytest.raises(OHLCVUnavailableError, match=r"Unexpected error: legacy-mystery"):
            ms.ohlcv("WETH", timeframe="1h", limit=10)
