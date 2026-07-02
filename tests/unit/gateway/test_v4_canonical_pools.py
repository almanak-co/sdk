"""Unit tests for the V4 canonical PoolKey seed registry (VIB-4534).

Three concerns exercised here:

1. ``CANONICAL_V4_PAIRS`` table shape — multi-chain, multi-fee-tier coverage
   so a passing run rules out the "tested one variant" anti-pattern.
2. ``register_canonical`` behaviour on :class:`V4PoolKeyCache` — idempotent
   on identical input, raises on conflicting input.
3. ``seed_canonical_pool_keys`` orchestration — robust to token-resolver
   misses, refuses unknown chains, and crucially does NOT open any network
   connection on the boot path.
4. Gateway wiring — :meth:`MarketService._get_pool_key_cache` invokes
   :meth:`UniswapV4GatewayConnector.build_cache` (which performs the seed)
   BEFORE the first ``LookupV4PoolKey`` RPC is served, so the seed is
   visible to the very first lookup.

No network access is performed; the suite is fully offline.
"""

from __future__ import annotations

from collections import Counter
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.connectors.uniswap_v4.hooks import compute_pool_id
from almanak.connectors.uniswap_v4.sdk import (
    NATIVE_CURRENCY,
)
from almanak.connectors.uniswap_v4.sdk import (
    PoolKey as FrameworkPoolKey,
)
from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.connectors.uniswap_v4.gateway.canonical_pools import (
    CANONICAL_V4_PAIRS,
    CanonicalV4Pair,
    SeedReport,
    V4CanonicalSeedConfigError,
    seed_canonical_pool_keys,
)
from almanak.connectors.uniswap_v4.gateway.pool_key_cache import (
    NO_HOOKS,
    CachedPoolKey,
    V4CanonicalSeedCollisionError,
    V4PoolKeyCache,
)

# --- Canonical Base addresses (real on-chain) ----------------------------
_WETH_BASE = "0x4200000000000000000000000000000000000006"
_USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
# Sorted: WETH (0x42...) < USDC (0x83...).
_BASE_WETH_USDC_3000_POOL_ID = (
    # From VIB-4534 description: poolId of the historically-deployed Base
    # WETH/USDC 0.3% pool whose Initialize event sits ~15M blocks behind
    # the gateway's bounded backfill window. The seed must reproduce this
    # exact hash from the framework's compute_pool_id helper.
    "0x1d8c55f347727c0fb4f5e1b65cdb93639e0c7102580a7d345e1144cd5a718f54"
)

# --- Native-currency Base ETH/USDC pool ids (VIB-4483) -------------------
# currency0 == 0x0 (native ETH sentinel), currency1 == USDC. These hash to a
# DIFFERENT poolId than the wrapped WETH/USDC pool above — that is the entire
# reason native pools need their own seed rows. The expected hashes are the
# on-chain poolIds verified against Base StateView.getSlot0 at fixture
# authoring (all four tiers carry live liquidity); they are recomputed here
# from compute_pool_id so a drift in the framework hash fails this test loud.
_BASE_NATIVE_ETH_USDC_500_POOL_ID = "0x96d4b53a38337a5733179751781178a2613306063c511b78cd02684739288c0a"
_BASE_NATIVE_ETH_USDC_3000_POOL_ID = "0xe070797535b13431808f8fc81fdbe7b41362960ed0b55bc2b6117c49c51b7eb9"


# =========================================================================
# CANONICAL_V4_PAIRS — table shape
# =========================================================================


class TestCanonicalPairsTable:
    def test_table_is_non_empty(self) -> None:
        assert len(CANONICAL_V4_PAIRS) > 0

    def test_table_covers_at_least_three_v4_chains(self) -> None:
        chains = {p.chain for p in CANONICAL_V4_PAIRS}
        # The framework's UNISWAP_V4 registry covers 7 chains as of
        # 2026-05; the seed should cover all of them. At minimum the
        # three the ticket calls out as load-bearing.
        assert {"ethereum", "base", "arbitrum"}.issubset(chains), chains

    def test_table_covers_all_four_canonical_fee_tiers(self) -> None:
        fees = {p.fee for p in CANONICAL_V4_PAIRS}
        assert fees == {100, 500, 3000, 10000}, fees

    def test_each_chain_carries_all_four_fee_tiers_for_weth_usdc(self) -> None:
        # The minimum AC: WETH/USDC at all 4 fee tiers on every V4 chain.
        by_chain: dict[str, set[int]] = {}
        for p in CANONICAL_V4_PAIRS:
            if p.token0_symbol == "WETH" and p.token1_symbol == "USDC":
                by_chain.setdefault(p.chain, set()).add(p.fee)
        for chain, fees in by_chain.items():
            assert fees == {100, 500, 3000, 10000}, (chain, fees)

    def test_no_duplicate_rows(self) -> None:
        # If two rows resolve to the same pool the table is malformed — the
        # dataclass equality would mask the duplicate. The native flag is part
        # of the identity: a native ETH/USDC row and a wrapped WETH/USDC row are
        # distinct pools. CRITICAL: for a native row (token0_native=True) the
        # canonical seed FORCES currency0=NATIVE_CURRENCY and ignores
        # token0_symbol (it's a cosmetic label), so two native rows that differ
        # ONLY by that label hash to the SAME poolId. The identity key must
        # therefore null out token0_symbol for native rows — otherwise a
        # label-only-divergent native duplicate slips past this guard.
        def _identity(p: CanonicalV4Pair) -> tuple:
            # Native rows ignore token0_symbol (currency0 is forced to 0x0), so
            # the cosmetic label must NOT be part of their identity.
            label = None if p.token0_native else p.token0_symbol
            return (p.chain, label, p.token1_symbol, p.fee, p.token0_native)

        counts = Counter(_identity(p) for p in CANONICAL_V4_PAIRS)
        dupes = {k: v for k, v in counts.items() if v > 1}
        assert not dupes, f"duplicate rows in CANONICAL_V4_PAIRS: {dupes}"


# =========================================================================
# Native-currency pairs (VIB-4483)
# =========================================================================


class TestNativeCurrencyPairs:
    """Native-ETH (currency0 == 0x0) V4 pools must be seeded as their own
    rows. A native pool hashes to a different poolId than the wrapped
    equivalent, so without these rows the gateway returns
    ``pool_key_not_found`` for native LP positions (the VIB-4483 blocker:
    LP_OPEN/LP_CLOSE accounting events are dropped because lp_open_data is
    gated behind a successful pool-key lookup).
    """

    def test_table_contains_native_rows(self) -> None:
        native = [p for p in CANONICAL_V4_PAIRS if p.token0_native]
        assert native, "CANONICAL_V4_PAIRS must contain native-currency rows"
        # native/USDC + native/USDT at 4 fee tiers, on every V4 chain.
        chains = {p.chain for p in native}
        assert {"ethereum", "base", "arbitrum"}.issubset(chains), chains
        # Each native chain carries both quote symbols at all four tiers.
        by_chain_quote: dict[tuple[str, str], set[int]] = {}
        for p in native:
            by_chain_quote.setdefault((p.chain, p.token1_symbol), set()).add(p.fee)
        for key, fees in by_chain_quote.items():
            assert fees == {100, 500, 3000, 10000}, (key, fees)

    def test_native_label_is_chain_native_gas_token(self) -> None:
        # The display label is cosmetic but must read honestly per chain —
        # ETH on Base/Arbitrum/Optimism/Ethereum, AVAX on Avalanche, BNB on
        # BSC, POL on Polygon. (The poolId depends only on currency0 == 0x0.)
        labels = {(p.chain, p.token0_symbol) for p in CANONICAL_V4_PAIRS if p.token0_native}
        assert ("base", "ETH") in labels
        assert ("avalanche", "AVAX") in labels
        assert ("bsc", "BNB") in labels
        assert ("polygon", "POL") in labels

    def test_native_and_wrapped_are_distinct_pool_ids(self) -> None:
        """The native ETH/USDC pool and the wrapped WETH/USDC pool at the
        SAME fee tier must hash to different poolIds. If they collided, the
        native seed would be a no-op and the bug would persist.
        """
        assert _BASE_NATIVE_ETH_USDC_3000_POOL_ID != _BASE_WETH_USDC_3000_POOL_ID

    def test_native_pool_id_currency0_is_zero_address(self) -> None:
        """Seeding a native row must produce currency0 == 0x0 (the V4 native
        sentinel), NOT a resolved WETH address. This is the byte-identity
        guarantee: the on-chain native pool keys on address(0).
        """
        cache = V4PoolKeyCache()
        seed_canonical_pool_keys(cache)
        base_idx = cache._index.get("base", {})
        assert _BASE_NATIVE_ETH_USDC_500_POOL_ID in base_idx, (
            f"native ETH/USDC fee=500 pool_id {_BASE_NATIVE_ETH_USDC_500_POOL_ID} "
            f"must be seeded; got {sorted(base_idx.keys())}"
        )
        key = base_idx[_BASE_NATIVE_ETH_USDC_500_POOL_ID]
        assert key.currency0 == NATIVE_CURRENCY, key.currency0
        assert key.currency1 == _USDC_BASE, key.currency1
        assert key.fee == 500
        assert key.hooks == NO_HOOKS

    def test_native_pool_id_matches_on_chain_hash(self) -> None:
        """The seeded native poolId must equal the on-chain-verified hash
        (recomputed from compute_pool_id over currency0=0x0). This is the
        load-bearing assertion: if the seed's hash drifts from what the
        connector/PoolManager computes, the gateway lookup misses.
        """
        fw_key = FrameworkPoolKey(
            currency0=NATIVE_CURRENCY,
            currency1=_USDC_BASE,
            fee=3000,
            tick_spacing=60,
            hooks=NATIVE_CURRENCY,
        )
        assert compute_pool_id(fw_key).lower() == _BASE_NATIVE_ETH_USDC_3000_POOL_ID

    def test_native_leg_not_resolved_through_token_resolver(self) -> None:
        """The native label MUST NOT be resolved to a wrapped address. We
        feed a native row whose label is a token that, IF resolved, would
        yield WETH — and assert the resulting currency0 is still 0x0.
        Guards against a regression where someone drops the token0_native
        short-circuit and the native leg silently becomes WETH (wrong pool).
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",  # would resolve to WETH if not short-circuited
                token1_symbol="USDC",
                fee=3000,
                token0_native=True,
            ),
        )
        cache = V4PoolKeyCache()
        seed_canonical_pool_keys(cache, pairs=pairs)
        assert _BASE_NATIVE_ETH_USDC_3000_POOL_ID in cache._index["base"]
        key = cache._index["base"][_BASE_NATIVE_ETH_USDC_3000_POOL_ID]
        assert key.currency0 == NATIVE_CURRENCY

    def test_native_row_skipped_when_quote_token_missing(self) -> None:
        """If the QUOTE (token1) symbol is missing on the chain, the native
        row is skipped like any wrapped row — not raised. The native leg
        being 0x0 must not bypass the token1 resolution miss.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="ETH",
                token1_symbol="DEFINITELY_NOT_A_REAL_TOKEN_XYZ",
                fee=3000,
                token0_native=True,
            ),
        )
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache, pairs=pairs)
        assert report.registered == 0
        assert len(report.skipped) == 1
        _, reason = report.skipped[0]
        assert "token_not_found" in reason


# =========================================================================
# V4PoolKeyCache.register_canonical
# =========================================================================


class TestRegisterCanonical:
    def _key(self, c0: str = _WETH_BASE, c1: str = _USDC_BASE, fee: int = 3000, ts: int = 60) -> CachedPoolKey:
        return CachedPoolKey(
            currency0=c0,
            currency1=c1,
            fee=fee,
            tick_spacing=ts,
            hooks=NO_HOOKS,
        )

    def test_first_call_registers_and_returns_registered(self) -> None:
        cache = V4PoolKeyCache()
        key = self._key()
        outcome = cache.register_canonical("base", _BASE_WETH_USDC_3000_POOL_ID, key)
        assert outcome == "registered"
        assert cache.known_pool_count("base") == 1

    def test_second_identical_call_is_idempotent(self) -> None:
        cache = V4PoolKeyCache()
        key = self._key()
        cache.register_canonical("base", _BASE_WETH_USDC_3000_POOL_ID, key)
        outcome = cache.register_canonical("base", _BASE_WETH_USDC_3000_POOL_ID, key)
        assert outcome == "already_present"
        assert cache.known_pool_count("base") == 1

    def test_conflicting_key_raises_collision(self) -> None:
        cache = V4PoolKeyCache()
        cache.register_canonical("base", _BASE_WETH_USDC_3000_POOL_ID, self._key(fee=3000, ts=60))
        # Same pool_id, different fee tier — must raise rather than overwrite.
        with pytest.raises(V4CanonicalSeedCollisionError) as exc_info:
            cache.register_canonical(
                "base",
                _BASE_WETH_USDC_3000_POOL_ID,
                self._key(fee=500, ts=10),
            )
        msg = str(exc_info.value)
        assert "base" in msg
        assert _BASE_WETH_USDC_3000_POOL_ID in msg
        # Cache must still hold the original entry — collision is rejection,
        # not partial mutation.
        assert cache.known_pool_count("base") == 1

    def test_chain_case_insensitive(self) -> None:
        cache = V4PoolKeyCache()
        cache.register_canonical("BASE", _BASE_WETH_USDC_3000_POOL_ID, self._key())
        # known_pool_count uses .lower() internally, so this proves the
        # registration was stored under the normalised chain key.
        assert cache.known_pool_count("base") == 1
        assert cache.known_pool_count("BASE") == 1


# =========================================================================
# seed_canonical_pool_keys — happy path
# =========================================================================


class TestCanonicalSeed:
    def test_seed_produces_known_base_weth_usdc_pool_id(self) -> None:
        """The Base WETH/USDC fee=3000 pool_id called out in VIB-4534 must
        appear in the cache after seeding. This is the load-bearing assertion
        — if it fails, the seed will not fix the LP_CLOSE attribution bug
        the ticket exists to solve.
        """
        cache = V4PoolKeyCache()
        seed_canonical_pool_keys(cache)
        base_idx = cache._index.get("base", {})
        assert _BASE_WETH_USDC_3000_POOL_ID in base_idx, (
            f"expected Base WETH/USDC fee=3000 pool_id "
            f"{_BASE_WETH_USDC_3000_POOL_ID} to be seeded; "
            f"got keys: {sorted(base_idx.keys())}"
        )
        key = base_idx[_BASE_WETH_USDC_3000_POOL_ID]
        assert key.fee == 3000
        assert key.tick_spacing == 60
        assert key.hooks == NO_HOOKS
        assert int(key.currency0, 16) < int(key.currency1, 16)

    def test_seed_pool_ids_match_framework_compute_pool_id(self) -> None:
        """For every successfully-seeded (chain, pair, fee), the pool_id
        stored in the cache must equal what
        :func:`compute_pool_id` produces on the same canonicalised inputs.
        Defends against drift between the seed's hash and the on-chain hash
        — if the framework's compute_pool_id ever changes, the seed must
        change with it.
        """
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache)

        # Walk the cache contents and recompute the pool_id from the stored
        # PoolKey. The two MUST agree, or the seed has lied about what it
        # registered.
        for chain, idx in cache._index.items():
            for stored_pid, stored_key in idx.items():
                fw_key = FrameworkPoolKey(
                    currency0=stored_key.currency0,
                    currency1=stored_key.currency1,
                    fee=stored_key.fee,
                    tick_spacing=stored_key.tick_spacing,
                    hooks=NATIVE_CURRENCY,
                )
                recomputed = compute_pool_id(fw_key).lower()
                assert stored_pid == recomputed, (chain, stored_pid, recomputed)

        # Sanity: registered + skipped == table size (no rows dropped silently).
        assert report.registered + report.already_present + len(report.skipped) == len(CANONICAL_V4_PAIRS)

    def test_seed_is_idempotent(self) -> None:
        cache = V4PoolKeyCache()
        first = seed_canonical_pool_keys(cache)
        second = seed_canonical_pool_keys(cache)
        # On the second invocation everything is already present; nothing new
        # gets registered; the cache size is unchanged.
        assert second.registered == 0
        assert second.already_present == first.registered
        sizes = {chain: len(idx) for chain, idx in cache._index.items()}
        third = seed_canonical_pool_keys(cache)
        assert third.registered == 0
        for chain, idx in cache._index.items():
            assert len(idx) == sizes[chain]


# =========================================================================
# seed_canonical_pool_keys — robustness (D3)
# =========================================================================


class TestSeedRobustness:
    def test_unresolvable_pair_skipped_silently(self) -> None:
        """A pair whose token symbol is not in the resolver for a given chain
        must be SKIPPED, not raised. The rest of the table must still
        populate, and the skipped pair must appear in the report.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="USDC",
                fee=3000,
            ),
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                # USDT is not in the static resolver for Base — real miss.
                token1_symbol="USDT",
                fee=3000,
            ),
        )
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache, pairs=pairs)
        assert report.registered == 1
        assert len(report.skipped) == 1
        skipped_pair, reason = report.skipped[0]
        assert skipped_pair.token1_symbol == "USDT"
        assert "token_not_found" in reason

    def test_unresolvable_pair_does_not_block_subsequent_pairs(self) -> None:
        """Explicit silent-error guard: a miss MUST NOT short-circuit the
        rest of the table. Mock the resolver so the FIRST pair raises and
        the second succeeds; assert the second is registered.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="MISSING",  # intentionally absent
                fee=3000,
            ),
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="USDC",  # real, must succeed
                fee=3000,
            ),
        )
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache, pairs=pairs)
        assert report.registered == 1
        assert cache.known_pool_count("base") == 1

    def test_collision_with_different_key_raises(self) -> None:
        """Manually pre-poison the cache with a wrong PoolKey under the
        canonical pool_id, then run the seed. The collision detector must
        raise V4CanonicalSeedCollisionError on the Base WETH/USDC row.
        """
        cache = V4PoolKeyCache()
        # Poison with a deliberately-wrong PoolKey (fee tier mismatch) under
        # the canonical Base WETH/USDC fee=3000 pool_id.
        cache._index.setdefault("base", {})[_BASE_WETH_USDC_3000_POOL_ID] = CachedPoolKey(
            currency0=_WETH_BASE,
            currency1=_USDC_BASE,
            fee=10000,  # wrong tier
            tick_spacing=200,
            hooks=NO_HOOKS,
        )
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="USDC",
                fee=3000,
            ),
        )
        with pytest.raises(V4CanonicalSeedCollisionError):
            seed_canonical_pool_keys(cache, pairs=pairs)

    def test_seed_does_not_open_network_connections(self) -> None:
        """The seed MUST be in-process only — no AsyncWeb3 instantiation,
        no socket open. Patch both and assert neither is touched. The trust
        statement says the seed adds zero egress to the gateway boot path.
        """
        cache = V4PoolKeyCache()
        # Patch every surface an accidental network call could route through:
        # the two AsyncWeb3 constructors AND the underlying socket primitive.
        # The socket patch catches raw-socket attempts that would bypass
        # AsyncWeb3 entirely (e.g. a hand-rolled HTTP client). The seed must
        # be strictly in-process — none of these constructors should fire.
        with (
            patch(
                "almanak.connectors.uniswap_v4.gateway.pool_key_cache.AsyncWeb3",
                side_effect=AssertionError("seed must not construct AsyncWeb3"),
            ) as web3_ctor,
            patch(
                "almanak.connectors.uniswap_v4.gateway.pool_key_cache.AsyncHTTPProvider",
                side_effect=AssertionError("seed must not construct AsyncHTTPProvider"),
            ) as provider_ctor,
            patch(
                "socket.socket",
                side_effect=AssertionError("seed must not open a socket"),
            ) as socket_ctor,
        ):
            report = seed_canonical_pool_keys(cache)
        # The seed completed normally...
        assert report.registered > 0
        # ...without touching the network constructors or opening a socket.
        web3_ctor.assert_not_called()
        provider_ctor.assert_not_called()
        socket_ctor.assert_not_called()

    def test_unknown_chain_rejected(self) -> None:
        """A row referencing a chain not in UNISWAP_V4 must raise rather
        than silently registering. Prevents the seed from inventing pool
        keys for chains that have no PoolManager.
        """
        pairs = (
            CanonicalV4Pair(
                chain="not_a_real_chain",
                token0_symbol="WETH",
                token1_symbol="USDC",
                fee=3000,
            ),
        )
        cache = V4PoolKeyCache()
        with pytest.raises(V4CanonicalSeedConfigError):
            seed_canonical_pool_keys(cache, pairs=pairs)

    def test_unknown_fee_tier_rejected(self) -> None:
        """A non-canonical fee tier (one not in TICK_SPACING) must raise.
        Defensive — the module-level table only generates canonical tiers,
        but a tester passing a custom table could trip this.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="USDC",
                fee=12345,  # not canonical
            ),
        )
        cache = V4PoolKeyCache()
        with pytest.raises(V4CanonicalSeedConfigError):
            seed_canonical_pool_keys(cache, pairs=pairs)

    def test_skip_includes_reason_for_observability(self) -> None:
        """The skip report MUST contain a reason string per skipped pair so
        an operator chasing a "why is my pool not seeded?" question can
        diff the report against the table without guessing.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                token0_symbol="WETH",
                token1_symbol="DEFINITELY_NOT_A_REAL_TOKEN_XYZ",
                fee=3000,
            ),
        )
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache, pairs=pairs)
        assert len(report.skipped) == 1
        _, reason = report.skipped[0]
        assert reason  # non-empty


# =========================================================================
# Gateway boot wiring (MarketService)
# =========================================================================


class TestMarketServiceSeedWiring:
    """The seed MUST be applied during the first ``_get_pool_key_cache``
    call, BEFORE the cache reference is published, so the first
    ``LookupV4PoolKey`` RPC sees the seed.
    """

    def _make_servicer(self):
        from almanak.gateway.services.market_service import MarketServiceServicer

        settings = MagicMock()
        settings.network = "mainnet"
        settings.chains = ["base"]
        settings.coingecko_api_key = ""
        return MarketServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_market_service_seeds_cache_on_first_lookup(self) -> None:
        """First call to ``_get_pool_key_cache`` invokes the connector's
        ``build_cache`` exactly once before returning. The returned cache
        must contain the canonical Base WETH/USDC pool_id.

        VIB-4818 — construction + seeding fold into one
        ``GatewayPoolKeyCacheCapability.build_cache`` call, so the spy
        patches the provider's ``build_cache`` (not a separate
        ``seed_pool_keys`` step that no longer exists).
        """
        from almanak.connectors.uniswap_v4.gateway.provider import (
            UniswapV4GatewayConnector,
        )

        servicer = self._make_servicer()

        observed_networks: list[str] = []
        original_build = UniswapV4GatewayConnector.build_cache

        def _tracking_build(self_: UniswapV4GatewayConnector, *, network: str) -> V4PoolKeyCache:
            observed_networks.append(network)
            return original_build(self_, network=network)

        with patch.object(
            UniswapV4GatewayConnector,
            "build_cache",
            _tracking_build,
        ):
            cache = await servicer._get_pool_key_cache()

        # ``_get_pool_key_cache`` returns ``PoolKeyCacheProtocol``; the
        # ``_index`` attribute access below requires the concrete V4
        # type. Assert it so the test fails loudly if the registered
        # provider ever swaps the impl class.
        assert isinstance(cache, V4PoolKeyCache)
        assert len(observed_networks) == 1, (
            "UniswapV4GatewayConnector.build_cache should be invoked exactly once on "
            f"first cache access; got {len(observed_networks)}"
        )
        assert observed_networks[0] == "mainnet"
        # And the canonical Base WETH/USDC pool_id is present.
        assert _BASE_WETH_USDC_3000_POOL_ID in cache._index.get("base", {})

    @pytest.mark.asyncio
    async def test_market_service_does_not_reseed_on_subsequent_lookups(self) -> None:
        """The seed is a boot-time operation. Subsequent
        ``_get_pool_key_cache`` invocations must NOT re-invoke
        ``build_cache``; the cache is already populated.
        """
        from almanak.connectors.uniswap_v4.gateway.provider import (
            UniswapV4GatewayConnector,
        )

        servicer = self._make_servicer()

        call_count = 0
        original_build = UniswapV4GatewayConnector.build_cache

        def _tracking_build(self_: UniswapV4GatewayConnector, *, network: str) -> V4PoolKeyCache:
            nonlocal call_count
            call_count += 1
            return original_build(self_, network=network)

        with patch.object(
            UniswapV4GatewayConnector,
            "build_cache",
            _tracking_build,
        ):
            cache1 = await servicer._get_pool_key_cache()
            cache2 = await servicer._get_pool_key_cache()
            cache3 = await servicer._get_pool_key_cache()

        assert cache1 is cache2 is cache3
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_lookup_v4_pool_key_resolves_canonical_pool_without_rpc(
        self,
    ) -> None:
        """End-to-end: with the seed in place, a ``LookupV4PoolKey`` RPC
        for the Base WETH/USDC fee=3000 pool MUST return the PoolKey body
        without triggering ``_refresh_chain`` (no eth_getLogs, no
        eth_blockNumber). This is the fix the ticket exists to ship.
        """
        from almanak.gateway.proto import gateway_pb2

        servicer = self._make_servicer()
        # Real seed; pre-warm the cache via _get_pool_key_cache.
        cache = await servicer._get_pool_key_cache()
        # ``_get_pool_key_cache`` returns ``PoolKeyCacheProtocol``; narrow
        # to the concrete impl so the ``_index`` introspection below is
        # well-typed and fails loudly on a swapped provider impl.
        assert isinstance(cache, V4PoolKeyCache)
        # If the seed wired up correctly the canonical pool_id is in the cache.
        assert _BASE_WETH_USDC_3000_POOL_ID in cache._index.get("base", {})

        # Now do the lookup and assert _refresh_chain is NEVER called.
        ctx = MagicMock()
        ctx.set_code = MagicMock()
        ctx.set_details = MagicMock()
        request = gateway_pb2.LookupV4PoolKeyRequest(
            pool_id=bytes.fromhex(_BASE_WETH_USDC_3000_POOL_ID[2:]),
            chain="base",
        )
        with patch.object(
            cache,
            "_refresh_chain",
            new=AsyncMock(side_effect=AssertionError("must not refresh")),
        ):
            resp = await servicer.LookupV4PoolKey(request, ctx)

        ctx.set_code.assert_not_called()
        assert resp.pool_key.currency0 == _WETH_BASE
        assert resp.pool_key.currency1 == _USDC_BASE
        assert resp.pool_key.fee == 3000
        assert resp.pool_key.tick_spacing == 60
        assert resp.pool_key.hooks == NO_HOOKS


# =========================================================================
# Edge cases — defensive
# =========================================================================


class TestSeedEdgeCases:
    def test_empty_pairs_returns_empty_report(self) -> None:
        cache = V4PoolKeyCache()
        report = seed_canonical_pool_keys(cache, pairs=())
        assert report.registered == 0
        assert report.already_present == 0
        assert report.skipped == []
        assert cache._index == {}

    def test_resolver_returns_unsorted_currencies_still_sorted_in_cache(self) -> None:
        """The seed must normalise currency order regardless of which symbol
        is "token0" in the registry — CachedPoolKey enforces sorted order.
        Test by registering a pair declared in reverse order.
        """
        pairs = (
            CanonicalV4Pair(
                chain="base",
                # USDC > WETH numerically, declared as token0 anyway.
                token0_symbol="USDC",
                token1_symbol="WETH",
                fee=3000,
            ),
        )
        cache = V4PoolKeyCache()
        seed_canonical_pool_keys(cache, pairs=pairs)
        # The resulting cache entry must carry sorted currencies AND the
        # canonical pool_id (which is identical to the WETH-first ordering).
        assert _BASE_WETH_USDC_3000_POOL_ID in cache._index["base"]
        key = cache._index["base"][_BASE_WETH_USDC_3000_POOL_ID]
        assert int(key.currency0, 16) < int(key.currency1, 16)


# =========================================================================
# Imports usability — ensures third-party can import the public API
# =========================================================================


def test_public_api_importable() -> None:
    # Intentional local re-import (already imported at module top) — this test
    # exists to prove the public API is importable by a third party, so the
    # redefinition (F811) and unused-binding (F401) are expected here.
    from almanak.connectors.uniswap_v4.gateway.canonical_pools import (  # noqa: F401,F811
        CANONICAL_V4_PAIRS,
        CanonicalV4Pair,
        SeedReport,
        V4CanonicalSeedConfigError,
        seed_canonical_pool_keys,
    )
    from almanak.connectors.uniswap_v4.gateway.pool_key_cache import (  # noqa: F401
        V4CanonicalSeedCollisionError,
    )


# Quick sanity assertion against unused imports — placate ruff.
_ = (TokenNotFoundError,)
