"""Gateway-backed dynamic Curve pool resolution (VIB-5628).

``resolve_pool_metadata`` reads an UNCURATED Curve pool's shape (coins /
decimals / lp_token / metapool / gamma-discriminated pool_type) live from
Curve's on-chain MetaRegistry, fail-closed on anything it can't fully and safely
resolve. These tests drive a FAKE gateway transport (no network) that answers
the MetaRegistry selectors, so every branch — the stable / crypto / tricrypto /
metapool families, the fail-closed sentinels, and the cache — is exercised
deterministically.

The gamma() discriminator is the safety-critical proof: a crypto pool whose
``asset_type`` would say "stable" MUST still classify crypto because gamma()
succeeds.
"""

from __future__ import annotations

import pytest

from almanak.connectors.curve import pool_resolver
from almanak.connectors.curve.pool_resolver import (
    _GAMMA_SEL,
    _GET_ADDRESS_SEL,
    _GET_BASE_POOL_SEL,
    _GET_COINS_SEL,
    _GET_DECIMALS_SEL,
    _GET_LP_TOKEN_SEL,
    _GET_N_COINS_SEL,
    _GET_UNDERLYING_COINS_SEL,
    _IS_META_SEL,
    resolve_pool_metadata,
)

# Real Ethereum mainnet addresses (opaque fixtures here).
DAI = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
WBTC = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
FRAX = "0x853d955aCEf822Db058eb8505911ED77F175b99e"
CRV3 = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"  # 3CRV LP (base LP token)

POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"  # 3pool address (fixture)
LP_TOKEN = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
META_REGISTRY = "0xF98B45FA17DE75FB1aD0e7aFD971b0ca00e379fC"
BASE_POOL = "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"
ZERO = "0x" + "0" * 40


def _addr_word(addr: str) -> str:
    return addr.lower().removeprefix("0x").zfill(64)


def _uint_word(value: int) -> str:
    return format(value, "064x")


def _addr_array(addresses: list[str]) -> str:
    """Fixed address[8] blob — trailing slots zero."""
    padded = list(addresses) + [ZERO] * (8 - len(addresses))
    return "".join(_addr_word(a) for a in padded[:8])


def _uint_array(values: list[int]) -> str:
    padded = list(values) + [0] * (8 - len(values))
    return "".join(_uint_word(v) for v in padded[:8])


class FakeMetaRegistryGateway:
    """Connected gateway that answers the MetaRegistry + gamma() reads.

    Configured with a pool's coins/decimals/lp_token/meta shape and whether
    ``gamma()`` succeeds. ``fail_selectors`` forces a selector to raise (revert
    simulation); ``meta_registry`` may be overridden to ``ZERO`` to exercise the
    unresolved-registry fail-closed path.
    """

    def __init__(
        self,
        *,
        coins: list[str],
        decimals: list[int],
        n_coins: int,
        lp_token: str = LP_TOKEN,
        is_meta: bool = False,
        base_pool: str = BASE_POOL,
        underlying: list[str] | None = None,
        gamma: int | None = None,
        gamma_blips: int = 0,
        meta_registry: str = META_REGISTRY,
        fail_selectors: frozenset[str] = frozenset(),
        confirm_healthy: bool = True,
    ) -> None:
        self.is_connected = True
        self._coins = coins
        self._decimals = decimals
        self._n = n_coins
        self._lp = lp_token
        self._is_meta = is_meta
        self._base_pool = base_pool
        self._underlying = underlying if underlying is not None else coins
        self._gamma = gamma
        # Number of initial gamma() reads that raise a TRANSPORT-style error before
        # the read recovers — models an isolated blip on the discriminator that the
        # gamma re-read must survive (VIB-5628 hardening).
        self._gamma_blips_remaining = gamma_blips
        self._meta_registry = meta_registry
        self._fail = fail_selectors
        # When False, the pool-independent transport-health probe
        # ``AddressProvider.get_address(0)`` raises (simulating a degraded
        # transport) while the id-7 MetaRegistry resolve still succeeds — so a
        # target-read failure can't be confirmed as a genuine revert.
        self._confirm_healthy = confirm_healthy
        self.calls = 0

    def eth_call(self, *, chain: str, to: str, data: str) -> str:
        self.calls += 1
        selector = data[:10]
        if selector == _GET_ADDRESS_SEL:
            arg = int(data[10:], 16) if len(data) > 10 else 0
            # id 0 = the StableSwap registry = the transport-health probe.
            if arg == 0 and not self._confirm_healthy:
                raise ValueError("transport error: connection reset")
            return "0x" + _addr_word(self._meta_registry)
        if selector in self._fail:
            raise ValueError("execution reverted: no registry")
        if selector == _GET_N_COINS_SEL:
            return "0x" + _uint_word(self._n)
        if selector == _GET_COINS_SEL:
            return "0x" + _addr_array(self._coins)
        if selector == _GET_DECIMALS_SEL:
            return "0x" + _uint_array(self._decimals)
        if selector == _GET_LP_TOKEN_SEL:
            return "0x" + _addr_word(self._lp)
        if selector == _IS_META_SEL:
            return "0x" + _uint_word(1 if self._is_meta else 0)
        if selector == _GET_UNDERLYING_COINS_SEL:
            return "0x" + _addr_array(self._underlying)
        if selector == _GET_BASE_POOL_SEL:
            return "0x" + _addr_word(self._base_pool)
        if selector == _GAMMA_SEL:
            if self._gamma_blips_remaining > 0:
                self._gamma_blips_remaining -= 1
                raise ValueError("transport error: gamma blip")
            if self._gamma is None:
                raise ValueError("execution reverted: no gamma")
            return "0x" + _uint_word(self._gamma)
        raise ValueError(f"unexpected selector {selector}")


@pytest.fixture(autouse=True)
def _clear_resolver_cache():
    pool_resolver._clear_cache()
    yield
    pool_resolver._clear_cache()


class TestHappyPath:
    def test_stableswap_3pool_shape(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None)
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "stableswap"
        assert meta.n_coins == 3
        assert [a.lower() for a in meta.coin_addresses] == [DAI.lower(), USDC.lower(), USDT.lower()]
        assert meta.coin_decimals == [18, 6, 6]
        assert meta.lp_token.lower() == LP_TOKEN.lower()
        assert meta.is_metapool is False
        assert meta.base_pool is None
        assert len(meta.coin_symbols) == 3

    def test_tricrypto_gamma_succeeds_n3(self) -> None:
        """asset_type would mislead — gamma() succeeding + n==3 is the crypto proof."""
        gw = FakeMetaRegistryGateway(coins=[USDC, WBTC, WETH], decimals=[6, 8, 18], n_coins=3, gamma=10**11)
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "tricrypto"
        assert meta.n_coins == 3

    def test_cryptoswap_gamma_succeeds_n2(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[WETH, USDC], decimals=[18, 6], n_coins=2, gamma=10**11)
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "cryptoswap"

    def test_metapool_base_coins(self) -> None:
        gw = FakeMetaRegistryGateway(
            coins=[FRAX, CRV3],
            decimals=[18, 18],
            n_coins=2,
            is_meta=True,
            base_pool=BASE_POOL,
            underlying=[FRAX, DAI, USDC, USDT],
            gamma=None,  # metapool base is stableswap
        )
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "stableswap"
        assert meta.is_metapool is True
        assert meta.base_pool is not None and meta.base_pool.lower() == BASE_POOL.lower()
        assert meta.base_pool_coin_addresses is not None
        assert [a.lower() for a in meta.base_pool_coin_addresses] == [DAI.lower(), USDC.lower(), USDT.lower()]
        # base_pool_coins (SYMBOLS) must be populated too — the valuer's
        # _classify_family / _build_metapool_position key the metapool_usd
        # classification on these; addresses-only would fall through (CodeRabbit #3191).
        assert meta.base_pool_coins is not None
        assert len(meta.base_pool_coins) == len(meta.base_pool_coin_addresses)


class TestGammaOrdering:
    def test_gamma_revert_after_healthy_reads_is_stableswap(self) -> None:
        """gamma()-None WITH a confirmed-healthy transport → genuine revert → stableswap."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None, confirm_healthy=True
        )
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "stableswap"

    def test_gamma_single_blip_then_recovers_is_crypto_not_stableswap(self) -> None:
        """An ISOLATED gamma() blip on a real crypto pool must NOT mis-classify as
        stableswap: the re-read recovers the gamma value → crypto. Guards the
        ~10^10 cached valuation mis-mark (crypto valued as stableswap)."""
        gw = FakeMetaRegistryGateway(
            coins=[USDC, WBTC, WETH], decimals=[6, 8, 18], n_coins=3,
            gamma=10**11, gamma_blips=1, confirm_healthy=True,
        )
        meta = pool_resolver.resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert meta is not None
        assert meta.pool_type == "tricrypto"

    def test_gamma_none_with_unhealthy_transport_returns_none_uncached(self) -> None:
        """gamma()-None AND the transport-health probe fails → ambiguous → None, NOT cached."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None, confirm_healthy=False
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        # The blip must NOT poison the per-process memo.
        assert ("ethereum", POOL.lower()) not in pool_resolver._METADATA_CACHE
        # ...and a subsequent healthy call self-heals to the genuine classification.
        healthy = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None, confirm_healthy=True
        )
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=healthy)
        assert meta is not None
        assert meta.pool_type == "stableswap"


class TestTransientTransport:
    def test_transient_read_failure_not_cached(self) -> None:
        """A required-read blip with unhealthy transport → None, uncached, self-heals."""
        blip = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT],
            decimals=[18, 6, 6],
            n_coins=3,
            fail_selectors=frozenset({_GET_N_COINS_SEL}),
            confirm_healthy=False,
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=blip) is None
        assert ("ethereum", POOL.lower()) not in pool_resolver._METADATA_CACHE
        # A later healthy call resolves — the transient failure was not cached.
        healthy = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None)
        meta = resolve_pool_metadata("ethereum", POOL, gateway_client=healthy)
        assert meta is not None
        assert meta.n_coins == 3

    def test_required_read_failure_with_healthy_transport_is_definitive_none(self) -> None:
        """A required-read revert with a HEALTHY transport → definitive not-a-pool None (cached)."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT],
            decimals=[18, 6, 6],
            n_coins=3,
            fail_selectors=frozenset({_GET_N_COINS_SEL}),
            confirm_healthy=True,
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        # A transport-confirmed "no registry" revert is definitive → cached (cheap miss).
        assert pool_resolver._METADATA_CACHE[("ethereum", POOL.lower())] is None

    def test_aave_gate_underlying_none_unhealthy_is_transient(self) -> None:
        """Non-meta underlying read None + unhealthy transport → None, uncached (transient)."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT],
            decimals=[18, 6, 6],
            n_coins=3,
            fail_selectors=frozenset({_GET_UNDERLYING_COINS_SEL}),
            confirm_healthy=False,
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        assert ("ethereum", POOL.lower()) not in pool_resolver._METADATA_CACHE

    def test_aave_gate_underlying_none_healthy_fails_closed(self) -> None:
        """Non-meta underlying read None + HEALTHY transport → fail-closed None (never plain)."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT],
            decimals=[18, 6, 6],
            n_coins=3,
            fail_selectors=frozenset({_GET_UNDERLYING_COINS_SEL}),
            confirm_healthy=True,
        )
        # Cannot confirm the pool is a plain (non-wrapped) pool → out of scope, None.
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        # Definitive (transport-confirmed) → cached.
        assert pool_resolver._METADATA_CACHE[("ethereum", POOL.lower())] is None

    def test_implausible_decimals_returns_none(self) -> None:
        """A coin decimal outside 0..36 (malformed read) → fail closed."""
        gw = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 99], n_coins=3, gamma=None)
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None


class TestFailClosed:
    def test_no_transport_returns_none(self) -> None:
        assert resolve_pool_metadata("ethereum", POOL) is None

    def test_address_provider_zero_returns_none(self) -> None:
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, meta_registry=ZERO
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None

    def test_metaregistry_revert_returns_none(self) -> None:
        """Non-pool address: get_n_coins reverts ('no registry') → None."""
        gw = FakeMetaRegistryGateway(
            coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, fail_selectors=frozenset({_GET_N_COINS_SEL})
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None

    def test_zero_coin_returns_none(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[DAI, ZERO, USDT], decimals=[18, 6, 6], n_coins=3)
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None

    def test_zero_lp_token_returns_none(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, lp_token=ZERO)
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None

    def test_implausible_n_coins_returns_none(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[DAI], decimals=[18], n_coins=99)
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None

    def test_aave_wrapped_pool_returns_none(self) -> None:
        """Non-meta pool whose underlying differs from coins (aTokens) → out of scope."""
        a_dai = "0x028171bCA77440897B824Ca71D1c56caC55b68A3"
        a_usdc = "0xBcca60bB61934080951369a648Fb03DF4F96263C"
        a_usdt = "0x3Ed3B47Dd13EC9a98b44e6204A523E766B225811"
        gw = FakeMetaRegistryGateway(
            coins=[a_dai, a_usdc, a_usdt],
            decimals=[18, 6, 6],
            n_coins=3,
            underlying=[DAI, USDC, USDT],
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None


class TestCache:
    def test_memoises_value(self) -> None:
        gw = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3)
        first = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        calls_after_first = gw.calls
        second = resolve_pool_metadata("ethereum", POOL, gateway_client=gw)
        assert first is second  # same cached object
        assert gw.calls == calls_after_first  # no new reads

    def test_memoises_none(self) -> None:
        gw = FakeMetaRegistryGateway(
            coins=[DAI], decimals=[18], n_coins=3, fail_selectors=frozenset({_GET_N_COINS_SEL})
        )
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        calls_after_first = gw.calls
        assert resolve_pool_metadata("ethereum", POOL, gateway_client=gw) is None
        assert gw.calls == calls_after_first  # None cached too


class TestAdapterFallback:
    def test_get_pool_info_returns_none_for_unknown_address(self) -> None:
        """adapter.get_pool_info returns None when the resolver returns None."""
        from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig

        gw = FakeMetaRegistryGateway(
            coins=[DAI], decimals=[18], n_coins=3, fail_selectors=frozenset({_GET_N_COINS_SEL})
        )
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            gateway_client=gw,
        )
        adapter = CurveAdapter(config)
        assert adapter.get_pool_info("0x1111111111111111111111111111111111111111") is None

    def test_get_pool_info_builds_dynamic_pool(self) -> None:
        """A resolvable uncurated address → a PoolInfo with the resolved shape."""
        from almanak.connectors.curve.adapter import CurveAdapter, CurveConfig, PoolType

        uncurated = "0x1111111111111111111111111111111111111111"
        gw = FakeMetaRegistryGateway(coins=[DAI, USDC, USDT], decimals=[18, 6, 6], n_coins=3, gamma=None)
        config = CurveConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            gateway_client=gw,
        )
        adapter = CurveAdapter(config)
        info = adapter.get_pool_info(uncurated, refresh=False)
        assert info is not None
        assert info.pool_type == PoolType.STABLESWAP
        assert info.n_coins == 3
        assert [a.lower() for a in info.coin_addresses] == [DAI.lower(), USDC.lower(), USDT.lower()]
        assert info.coin_decimals == [18, 6, 6]
