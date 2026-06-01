"""VIB-4843 (Epic F, FR-5001/5003/5004): MarketSnapshot price-cache sharing.

These tests pin the price-call-reduction behaviour the PRD §Epic F targets:

* FR-5001 — pre-warm, decide(), and portfolio valuation share ONE
  MarketSnapshot instance (and its ``_price_cache``), so a 2-token iteration
  fetches each distinct price at most once. A per-iteration memo on the
  strategy is the mechanism; a short TTL bounds reuse across iterations.
* FR-5003 — ``MarketSnapshot.balance(price=...)`` computes ``balance_usd``
  from a supplied or already-cached price WITHOUT an oracle re-fetch.
* FR-5004 — the native gas-token is pre-warmed once (not re-fetched inside
  the valuation lane on every HOLD).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from almanak import IntentStrategy
from almanak.framework.market.models import TokenBalance
from almanak.framework.market.snapshot import MarketSnapshot

# ---------------------------------------------------------------------------
# Counting oracle / balance provider (no real egress; pure stubs)
# ---------------------------------------------------------------------------


class CountingOracle:
    """Callable price oracle that counts every (token, chain) fetch."""

    def __init__(self, prices: dict[str, Decimal]) -> None:
        self._prices = prices
        self.calls: list[tuple[str, str]] = []

    def __call__(self, token: str, quote: str = "USD") -> Decimal:
        self.calls.append((token, quote))
        try:
            return self._prices[token]
        except KeyError as exc:  # pragma: no cover - guard
            raise ValueError(f"no price for {token}") from exc

    @property
    def call_count(self) -> int:
        return len(self.calls)


class CountingBalanceProvider:
    """Callable balance provider that does NOT measure USD.

    Returns a bare ``Decimal`` (the legacy data-layer shape) so the snapshot
    coerces it to the *unmeasured* FR-5003 sentinel — the only provenance under
    which ``balance_usd`` may later be filled from a price. A provider that
    returns a ``TokenBalance`` is, by contrast, treated as having MEASURED
    ``balance_usd`` (Empty≠Zero) and is never overwritten.
    """

    def __init__(self, balances: dict[str, Decimal]) -> None:
        self._balances = balances
        self.calls: list[str] = []

    def __call__(self, token: str) -> Decimal:
        self.calls.append(token)
        return self._balances.get(token, Decimal("0"))


def _make_snapshot(oracle: CountingOracle, balance_provider: CountingBalanceProvider | None = None) -> MarketSnapshot:
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x" + "0" * 40,
        price_oracle=oracle,
        balance_provider=balance_provider,
    )


# ---------------------------------------------------------------------------
# Minimal strategy whose create_market_snapshot() returns a shared instance
# ---------------------------------------------------------------------------


@dataclass
class _Config:
    deployment_id: str = "rsi-test"
    strategy_name: str = "rsi-test"
    chain: str = "arbitrum"

    def to_dict(self) -> dict:
        return {}


class _RSIStrat(IntentStrategy):
    """2-token strategy that builds a snapshot wired to the counting oracle."""

    def __init__(self, oracle: CountingOracle, balance_provider: CountingBalanceProvider) -> None:
        # Bypass the heavyweight runner __init__; wire only what the
        # per-iteration memo + _build_market_snapshot touch.
        self._oracle = oracle
        self._balance_provider_stub = balance_provider
        self._chain = "arbitrum"
        self._wallet_address = "0x" + "0" * 40
        self._deployment_id = "rsi-test"
        self._ohlcv_dedup_provider = None
        self._cached_market_snapshot = None
        self._cached_market_snapshot_token = None
        self._cached_market_snapshot_at = None
        self.config = _Config()
        self.build_count = 0

    # Override the fresh-mint hook so the memo wraps a snapshot we control.
    def _build_market_snapshot(self) -> MarketSnapshot:
        self.build_count += 1
        return _make_snapshot(self._oracle, self._balance_provider_stub)

    def get_config(self, key, default=None):  # noqa: D401 - simple stub
        return default

    def _get_tracked_tokens(self) -> list[str]:
        return ["ARB", "USDC"]

    def decide(self, market):  # pragma: no cover - not exercised here
        return None

    def get_open_positions(self):  # pragma: no cover - not exercised here
        from almanak.framework.teardown.models import TeardownPositionSummary

        return TeardownPositionSummary.empty("rsi-test")

    def generate_teardown_intents(self, mode=None, market=None):  # pragma: no cover
        return []


# ---------------------------------------------------------------------------
# FR-5001 — one snapshot per iteration; call-count reduction
# ---------------------------------------------------------------------------


class TestSharedSnapshotPerIteration:
    def test_pre_warm_decide_and_valuation_share_one_instance(self) -> None:
        """All three create_market_snapshot() calls within an iteration return
        the SAME instance (id() identical) — the FR-5001 contract.
        """
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.10"), "USDC": Decimal("1.0")}), CountingBalanceProvider({}))
        strat.begin_market_snapshot_iteration("cycle-1")

        pre_warm = strat.create_market_snapshot()  # runner pre-warm seam
        decide = strat.create_market_snapshot()  # decide() seam
        valuation = strat.create_market_snapshot()  # portfolio valuation seam

        assert id(pre_warm) == id(decide) == id(valuation)
        assert strat.build_count == 1

    def test_two_token_iteration_fetches_each_price_once(self) -> None:
        """A 2-token RSI iteration that pre-warms then re-reads prices in
        decide() + valuation issues at most one oracle call per distinct token
        (target: <=3 incl. native). Before FR-5001 each create_market_snapshot()
        minted a cold cache and re-fetched.
        """
        oracle = CountingOracle({"ARB": Decimal("1.10"), "USDC": Decimal("1.0")})
        strat = _RSIStrat(oracle, CountingBalanceProvider({}))
        strat.begin_market_snapshot_iteration("cycle-1")

        # Pre-warm phase: prices fetched once each into the shared cache.
        warm = strat.create_market_snapshot()
        for token in strat._get_tracked_tokens():
            warm.price(token)
        assert oracle.call_count == 2

        # decide() re-reads the same prices on a fresh create_market_snapshot()
        # handle — must hit the warm cache, NOT the oracle.
        decide_market = strat.create_market_snapshot()
        decide_market.price("ARB")
        decide_market.price("USDC")

        # Portfolio valuation re-reads them yet again on its own handle.
        valuation_market = strat.create_market_snapshot()
        valuation_market.price("ARB")
        valuation_market.price("USDC")

        assert oracle.call_count == 2  # still 2 — no re-fetch across seams
        assert oracle.call_count <= 3

    def test_new_iteration_token_invalidates_memo(self) -> None:
        """A new iteration token forces a fresh mint (cold cache)."""
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.1"), "USDC": Decimal("1.0")}), CountingBalanceProvider({}))
        strat.begin_market_snapshot_iteration("cycle-1")
        first = strat.create_market_snapshot()

        strat.begin_market_snapshot_iteration("cycle-2")
        second = strat.create_market_snapshot()

        assert id(first) != id(second)
        assert strat.build_count == 2

    def test_begin_iteration_is_idempotent_for_same_token(self) -> None:
        """Re-stamping the current token must not drop a warm snapshot."""
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.1")}), CountingBalanceProvider({}))
        strat.begin_market_snapshot_iteration("cycle-1")
        first = strat.create_market_snapshot()
        strat.begin_market_snapshot_iteration("cycle-1")  # same token
        second = strat.create_market_snapshot()
        assert id(first) == id(second)
        assert strat.build_count == 1

    def test_token_scoped_memo_survives_past_ttl_within_one_iteration(self, monkeypatch) -> None:
        """Codex VIB-4843: when an iteration token is stamped, the memo MUST
        persist for the whole iteration even if more than the TTL elapses
        between pre-warm and portfolio valuation.

        A slow ``decide()`` (>5s) previously expired the TTL mid-iteration and
        re-minted a COLD snapshot for the valuation lane, discarding the
        pre-warmed ``_price_cache`` and defeating the dedup. The iteration
        token — not wall-clock — is the lifetime once stamped.
        """
        oracle = CountingOracle({"ARB": Decimal("1.10"), "USDC": Decimal("1.0")})
        strat = _RSIStrat(oracle, CountingBalanceProvider({}))
        clock = {"t": 1000.0}
        monkeypatch.setattr("time.monotonic", lambda: clock["t"])

        strat.begin_market_snapshot_iteration("cycle-1")
        pre_warm = strat.create_market_snapshot()
        for token in strat._get_tracked_tokens():
            pre_warm.price(token)
        assert oracle.call_count == 2

        # Simulate a slow decide() that runs well past the 5s default TTL.
        clock["t"] += 30.0

        decide_market = strat.create_market_snapshot()
        valuation_market = strat.create_market_snapshot()

        # Same instance across the whole iteration despite the elapsed TTL.
        assert id(pre_warm) == id(decide_market) == id(valuation_market)
        assert strat.build_count == 1

        # The pre-warmed price cache is still warm — no re-fetch.
        valuation_market.price("ARB")
        valuation_market.price("USDC")
        assert oracle.call_count == 2


# ---------------------------------------------------------------------------
# FR-5001 — TTL behaviour
# ---------------------------------------------------------------------------


class TestSnapshotTTL:
    def test_within_ttl_reuses_instance(self, monkeypatch) -> None:
        """Same token within TTL → cache hit (no fresh mint)."""
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.1")}), CountingBalanceProvider({}))
        # No iteration token stamped → TTL is the only guard.
        clock = {"t": 1000.0}
        monkeypatch.setattr("time.monotonic", lambda: clock["t"])

        first = strat.create_market_snapshot()
        clock["t"] += 1.0  # < default 5s TTL
        second = strat.create_market_snapshot()

        assert id(first) == id(second)
        assert strat.build_count == 1

    def test_after_ttl_rebuilds_instance(self, monkeypatch) -> None:
        """After TTL elapses → fresh mint (avoids serving stale prices)."""
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.1")}), CountingBalanceProvider({}))
        clock = {"t": 1000.0}
        monkeypatch.setattr("time.monotonic", lambda: clock["t"])

        first = strat.create_market_snapshot()
        clock["t"] += 6.0  # > default 5s TTL
        second = strat.create_market_snapshot()

        assert id(first) != id(second)
        assert strat.build_count == 2

    def test_ttl_is_configurable(self, monkeypatch) -> None:
        """A custom market_snapshot_cache_ttl_seconds is honoured."""
        strat = _RSIStrat(CountingOracle({"ARB": Decimal("1.1")}), CountingBalanceProvider({}))
        strat.get_config = lambda key, default=None: 0.5 if key == "market_snapshot_cache_ttl_seconds" else default

        clock = {"t": 1000.0}
        monkeypatch.setattr("time.monotonic", lambda: clock["t"])
        first = strat.create_market_snapshot()
        clock["t"] += 1.0  # > 0.5s custom TTL
        second = strat.create_market_snapshot()
        assert id(first) != id(second)


# ---------------------------------------------------------------------------
# FR-5003 — balance(price=...) computes balance_usd without an oracle call
# ---------------------------------------------------------------------------


class TestBalanceUsdFromPrice:
    def test_supplied_price_fills_balance_usd_without_fetch(self) -> None:
        oracle = CountingOracle({})  # any oracle call would be a bug here
        provider = CountingBalanceProvider({"ARB": Decimal("100")})
        snap = _make_snapshot(oracle, provider)

        tb = snap.balance("ARB", price=Decimal("1.25"))

        assert tb.balance == Decimal("100")
        assert tb.balance_usd == Decimal("125.00")
        assert oracle.call_count == 0  # price() was never called

    def test_cached_price_fills_balance_usd_without_fetch(self) -> None:
        oracle = CountingOracle({"ARB": Decimal("2.0")})
        provider = CountingBalanceProvider({"ARB": Decimal("10")})
        snap = _make_snapshot(oracle, provider)

        # Warm the price cache once (one oracle call).
        snap.price("ARB")
        assert oracle.call_count == 1

        # balance() with no explicit price should consult the warm cache.
        tb = snap.balance("ARB")
        assert tb.balance_usd == Decimal("20.0")
        assert oracle.call_count == 1  # no second fetch

    def test_no_price_available_leaves_sentinel(self) -> None:
        oracle = CountingOracle({})
        provider = CountingBalanceProvider({"ARB": Decimal("5")})
        snap = _make_snapshot(oracle, provider)

        tb = snap.balance("ARB")  # no price arg, cold cache
        assert tb.balance_usd == Decimal("0")  # unmeasured sentinel preserved
        assert oracle.call_count == 0  # MUST NOT fetch from balance lookup

    def test_provider_reported_usd_is_authoritative(self) -> None:
        """A provider that already set balance_usd is not overwritten."""
        oracle = CountingOracle({"ARB": Decimal("99")})

        class _UsdProvider:
            def __call__(self, token: str) -> TokenBalance:
                return TokenBalance(symbol=token, balance=Decimal("3"), balance_usd=Decimal("7.50"))

        snap = _make_snapshot(oracle, _UsdProvider())
        tb = snap.balance("ARB", price=Decimal("1.0"))
        assert tb.balance_usd == Decimal("7.50")  # provider value wins

    def test_couldnt_price_zero_on_nonzero_balance_is_recomputed(self) -> None:
        """Codex VIB-4843 re-audit (P2 #2): a provider ``balance_usd == 0`` on a
        NON-ZERO holding is a couldn't-price sentinel, NOT a measured zero.

        ``create_sync_balance_func`` in ``cli/run.py`` falls back to
        ``balance_usd=0`` when the price oracle raises — so a non-zero token
        would otherwise be reported as $0 even when an authoritative price is
        available. The snapshot must RECOMPUTE ``balance * price`` from a
        supplied price (and from a warm cache) instead of trusting that $0.
        """
        oracle = CountingOracle({"ARB": Decimal("99")})

        class _CouldntPriceProvider:
            def __call__(self, token: str) -> TokenBalance:
                # Non-zero balance, but the provider FAILED to price it → 0.
                return TokenBalance(symbol=token, balance=Decimal("42"), balance_usd=Decimal("0"))

        snap = _make_snapshot(oracle, _CouldntPriceProvider())

        # Supplied price recomputes the couldn't-price zero. Once filled, the
        # key is reclassified MEASURED and the value is stable for later reads
        # (it is not re-derived against a changing price mid-iteration).
        tb = snap.balance("ARB", price=Decimal("1.25"))
        assert tb.balance == Decimal("42")
        assert tb.balance_usd == Decimal("52.50")  # 42 * 1.25, recomputed once
        tb2 = snap.balance("ARB")  # cache hit → measured value preserved
        assert tb2.balance_usd == Decimal("52.50")

    def test_cache_hit_recomputes_couldnt_price_zero_from_warm_cache_supplied(self) -> None:
        """Companion to the cache-hit warm-cache case: a couldn't-price zero
        cached on a cold pass is recomputed once the price cache is warm —
        proving the unmeasured marker survives until a price is actually found.
        """
        oracle = CountingOracle({"ARB": Decimal("2.0")})

        class _CouldntPriceProvider:
            def __call__(self, token: str) -> TokenBalance:
                return TokenBalance(symbol=token, balance=Decimal("42"), balance_usd=Decimal("0"))

        snap = _make_snapshot(oracle, _CouldntPriceProvider())
        first = snap.balance("ARB")  # cold cache, no price → stays unmeasured $0
        assert first.balance_usd == Decimal("0")

        snap.price("ARB")  # warm the cache (1 oracle call)
        second = snap.balance("ARB")  # cache-hit → recompute from warm price
        assert second.balance_usd == Decimal("84.0")  # 42 * 2.0
        assert oracle.call_count == 1

    def test_provider_genuine_zero_holding_stays_zero(self) -> None:
        """Codex VIB-4843 re-audit (P2 #2 / test c): a provider ``balance == 0``
        with ``balance_usd == 0`` is a GENUINE measured zero (0 * price == 0
        regardless of price) and must stay $0 — never recomputed, never an
        oracle fetch.
        """
        oracle = CountingOracle({"ARB": Decimal("99")})

        class _ZeroHoldingProvider:
            def __call__(self, token: str) -> TokenBalance:
                return TokenBalance(symbol=token, balance=Decimal("0"), balance_usd=Decimal("0"))

        snap = _make_snapshot(oracle, _ZeroHoldingProvider())

        # Supplied price must NOT fabricate a non-zero USD on a zero holding.
        tb = snap.balance("ARB", price=Decimal("1.25"))
        assert tb.balance == Decimal("0")
        assert tb.balance_usd == Decimal("0")  # genuine measured zero

        # Cache-hit read with a warm price cache also stays $0.
        snap.set_price("ARB", Decimal("1.25"))
        tb2 = snap.balance("ARB")
        assert tb2.balance_usd == Decimal("0")
        assert oracle.call_count == 0  # never fetched from the balance lane

    def test_set_balance_clears_stale_unmeasured_marker(self) -> None:
        """Codex VIB-4843 re-audit (P2 #1 / test a): an unpriced provider read
        marks the key unmeasured; a later ``set_balance`` with a MEASURED value
        must clear that marker so a subsequent ``price=`` read does NOT recompute
        and clobber the measured value (incl. a measured ``Decimal("0")``).
        """
        oracle = CountingOracle({})  # any fetch would be a bug
        # Bare-Decimal provider → unmeasured sentinel marked on first read.
        provider = CountingBalanceProvider({"ARB": Decimal("100")})
        snap = _make_snapshot(oracle, provider)

        first = snap.balance("ARB")  # unpriced read → key marked unmeasured
        assert first.balance_usd == Decimal("0")

        # Caller now supplies a MEASURED balance_usd via set_balance.
        snap.set_balance("ARB", TokenBalance(symbol="ARB", balance=Decimal("100"), balance_usd=Decimal("250")))

        # A later read WITH a price must keep the measured value, not recompute
        # 100 * 1.25 = 125 over the top of the measured 250.
        tb = snap.balance("ARB", price=Decimal("1.25"))
        assert tb.balance_usd == Decimal("250")  # measured value preserved
        assert oracle.call_count == 0

    def test_set_balance_clears_stale_marker_for_measured_zero(self) -> None:
        """P2 #1 corollary: a measured ``Decimal("0")`` supplied via
        set_balance after an unpriced read is preserved (Empty≠Zero), not
        recomputed from a later available price.
        """
        oracle = CountingOracle({})
        provider = CountingBalanceProvider({"ARB": Decimal("100")})
        snap = _make_snapshot(oracle, provider)

        first = snap.balance("ARB")  # unpriced → marked unmeasured
        assert first.balance_usd == Decimal("0")

        # Caller measures USD as exactly 0 (authoritative).
        snap.set_balance("ARB", TokenBalance(symbol="ARB", balance=Decimal("100"), balance_usd=Decimal("0")))

        tb = snap.balance("ARB", price=Decimal("1.25"))
        assert tb.balance_usd == Decimal("0")  # measured zero, not 125
        assert oracle.call_count == 0

    def test_cache_hit_recomputes_couldnt_price_zero_from_warm_cache(self) -> None:
        """Codex VIB-4843 re-audit (P2 #2 / test b): a non-zero holding cached
        with ``balance_usd == 0`` (couldn't-price sentinel) is recomputed from a
        warm price cache on the cache-hit path.

        Exercises the per-chain ``cache_key`` cache-hit branch: the first read
        marks the key unmeasured (non-zero balance, $0), and a subsequent read
        once the price cache is warm fills USD from ``balance * price``.
        """
        oracle = CountingOracle({"ARB": Decimal("3.0")})

        class _CouldntPriceProvider:
            def __call__(self, token: str) -> TokenBalance:
                return TokenBalance(symbol=token, balance=Decimal("10"), balance_usd=Decimal("0"))

        snap = _make_snapshot(oracle, _CouldntPriceProvider())
        first = snap.balance("ARB")  # provider path → couldn't-price sentinel
        assert first.balance_usd == Decimal("0")

        snap.price("ARB")  # warm the price cache (1 oracle call)
        second = snap.balance("ARB")  # cache-hit path → recompute from warm cache
        assert second.balance == Decimal("10")
        assert second.balance_usd == Decimal("30.0")  # 10 * 3.0
        assert oracle.call_count == 1  # only the explicit price() warm-up

        # Third read must still return the measured USD. _fill_balance_usd
        # discards the cache_key from _balance_usd_unmeasured on the second read,
        # so without persisting the filled balance back into _balance_cache this
        # would treat USD as "measured" yet return the original $0-sentinel
        # balance (CodeRabbit VIB-4843 stale-USD regression).
        third = snap.balance("ARB")
        assert third.balance == Decimal("10")
        assert third.balance_usd == Decimal("30.0")  # not the stale $0 sentinel
        assert oracle.call_count == 1  # still no extra oracle fetch


# ---------------------------------------------------------------------------
# FR-5004 — native gas-token pre-warm (priced once, not re-fetched per HOLD)
# ---------------------------------------------------------------------------


class TestNativeGasPreWarm:
    @pytest.mark.asyncio
    async def test_pre_warm_includes_native_gas_token(self) -> None:
        """The runner pre-warms the chain's native gas token so the single
        valuation-required fetch lands outside the decide timeout.
        """
        from unittest.mock import MagicMock

        from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
            config=RunnerConfig(enable_state_persistence=False, enable_alerting=False),
        )

        strategy = MagicMock()
        strategy.chain = "arbitrum"
        strategy.is_multi_chain.return_value = False
        strategy._get_tracked_tokens.return_value = ["ARB", "USDC"]

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        await runner._do_pre_warm_prices(market, strategy)

        warmed = [c.args[0] for c in market.price.call_args_list]
        assert "ARB" in warmed
        assert "USDC" in warmed
        assert "ETH" in warmed  # arbitrum native gas token
        # No duplicate fetches.
        assert len(warmed) == len(set(warmed))

    @pytest.mark.asyncio
    async def test_native_skipped_for_multi_chain(self) -> None:
        from unittest.mock import MagicMock

        from almanak.framework.runner.strategy_runner import RunnerConfig, StrategyRunner

        runner = StrategyRunner(
            price_oracle=MagicMock(),
            balance_provider=MagicMock(),
            execution_orchestrator=MagicMock(),
            state_manager=MagicMock(),
            config=RunnerConfig(enable_state_persistence=False, enable_alerting=False),
        )

        strategy = MagicMock()
        strategy.chain = "arbitrum"
        strategy.is_multi_chain.return_value = True
        strategy._get_tracked_tokens.return_value = ["ARB", "USDC"]

        market = MagicMock()
        market.price.return_value = Decimal("1.0")

        await runner._do_pre_warm_prices(market, strategy)

        warmed = [c.args[0] for c in market.price.call_args_list]
        assert "ETH" not in warmed  # native is ambiguous on multi-chain
