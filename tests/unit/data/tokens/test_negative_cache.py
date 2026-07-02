"""VIB-2715: TokenResolver negative cache.

Remembered misses must short-circuit the slow gateway path so the second
lookup for an unknown symbol returns instantly instead of hammering the
gateway again.

Also covers the CodeRabbit refinement on PR #1525: the cache only stores
DEFINITIVE misses ("gateway said this token doesn't exist"), never
transient failures (timeout / UNAVAILABLE / integrity reject).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.framework.data.tokens.models import BridgeType, ResolvedToken
from almanak.framework.data.tokens.resolver import TokenResolver


@pytest.fixture()
def fresh_resolver(tmp_path: Path):
    counter = {"n": 0}

    def _make(with_gateway: bool = True) -> TokenResolver:
        TokenResolver._instance = None
        counter["n"] += 1
        cache_file = tmp_path / f"cache_{counter['n']}.json"
        channel = MagicMock(name="grpc_channel") if with_gateway else None
        return TokenResolver(gateway_channel=channel, cache_file=str(cache_file))

    return _make


def _install_definitive_miss(resolver: TokenResolver, monkeypatch: pytest.MonkeyPatch, counter: dict):
    """Patch both gateway helpers so they count the call + mark the miss
    as DEFINITIVE (the real helpers do this on ``response.success == False``).

    The resolver only stores a negative-cache entry when ``definitive`` is
    True, so tests of the cached-miss path must simulate that signal.
    """

    def _symbol_miss(*_args, **_kwargs):
        counter["symbol"] = counter.get("symbol", 0) + 1
        resolver._gateway_miss_state.definitive = True
        return None

    def _address_miss(*_args, **_kwargs):
        counter["address"] = counter.get("address", 0) + 1
        resolver._gateway_miss_state.definitive = True
        return None

    monkeypatch.setattr(resolver, "_resolve_symbol_via_gateway", _symbol_miss)
    monkeypatch.setattr(resolver, "_resolve_via_gateway", _address_miss)
    monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)


def _install_transient_miss(resolver: TokenResolver, monkeypatch: pytest.MonkeyPatch, counter: dict):
    """Patch helpers so every call returns None WITHOUT setting
    ``definitive``. Mimics a gateway timeout or UNAVAILABLE error; the
    resolver must not negative-cache these."""

    def _symbol_transient(*_args, **_kwargs):
        counter["symbol"] = counter.get("symbol", 0) + 1
        resolver._gateway_miss_state.definitive = False
        return None

    def _address_transient(*_args, **_kwargs):
        counter["address"] = counter.get("address", 0) + 1
        resolver._gateway_miss_state.definitive = False
        return None

    monkeypatch.setattr(resolver, "_resolve_symbol_via_gateway", _symbol_transient)
    monkeypatch.setattr(resolver, "_resolve_via_gateway", _address_transient)
    monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)


class TestNegativeCacheSymbol:
    def test_second_lookup_short_circuits(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("DEFINITELY_NOT_A_REAL_SYMBOL", "ethereum")
        assert counter["symbol"] == 1, "first miss should attempt the gateway"

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("DEFINITELY_NOT_A_REAL_SYMBOL", "ethereum")
        assert counter["symbol"] == 1, (
            "second miss within TTL should NOT re-call the gateway -- that's the whole point"
        )

    def test_case_insensitive_short_circuit(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UnknownToken", "ethereum")
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("unknowntoken", "ethereum")
        assert counter["symbol"] == 1

    def test_per_chain_isolation(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        hit_chains: list[str] = []

        def _impl(_sym, chain):
            hit_chains.append(chain)
            resolver._gateway_miss_state.definitive = True
            return None

        monkeypatch.setattr(resolver, "_resolve_symbol_via_gateway", _impl)
        monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "arbitrum")

        assert hit_chains == ["ethereum", "arbitrum"]

    def test_ttl_expiry(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        resolver._negative_cache_ttl_seconds = 0.05
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        assert counter["symbol"] == 1

        import almanak.framework.data.tokens.resolver as resolver_mod

        base = resolver_mod.time.monotonic()
        monkeypatch.setattr(resolver_mod.time, "monotonic", lambda: base + 10.0)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        assert counter["symbol"] == 2, "gateway should be re-attempted after TTL expiry"

    def test_register_evicts_negative_cache(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("NEWTOKEN", "arbitrum")

        resolver.register(
            ResolvedToken(
                symbol="NEWTOKEN",
                address="0x1234567890abcdef1234567890abcdef12345678",
                decimals=18,
                chain="arbitrum",
                chain_id=42161,
                name="New Token",
                source="manual",
                is_verified=True,
                bridge_type=BridgeType.NATIVE,
            )
        )
        resolved = resolver.resolve("NEWTOKEN", "arbitrum")
        assert resolved.symbol == "NEWTOKEN"

    def test_no_cache_without_gateway(self, fresh_resolver) -> None:
        resolver = fresh_resolver(with_gateway=False)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("NEVER_EXISTS", "ethereum")

        assert not resolver._negative_cache


class TestNegativeCacheTransientFailureNotStored:
    """CodeRabbit-flagged behavior: timeouts / UNAVAILABLE / integrity
    rejects must NOT poison the cache for 5 minutes. Retry must hit the
    gateway again."""

    def test_transient_symbol_miss_not_cached(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_transient_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("TRANSIENT_SYMBOL", "ethereum")
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("TRANSIENT_SYMBOL", "ethereum")

        assert counter["symbol"] == 2, (
            "transient (timeout/UNAVAILABLE/integrity-reject) miss must NOT be cached; "
            "the second call should re-attempt the gateway"
        )
        assert not resolver._negative_cache, "transient failures must not populate the cache"

    def test_transient_address_miss_not_cached(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_transient_miss(resolver, monkeypatch, counter)

        addr = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        with pytest.raises(TokenNotFoundError):
            resolver.resolve(addr, "ethereum")
        with pytest.raises(TokenNotFoundError):
            resolver.resolve(addr, "ethereum")

        assert counter["address"] == 2
        assert not resolver._negative_cache


class TestNegativeCacheAddress:
    def test_address_miss_is_cached(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        addr = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        with pytest.raises(TokenNotFoundError):
            resolver.resolve(addr, "ethereum")
        assert counter["address"] == 1

        with pytest.raises(TokenNotFoundError):
            resolver.resolve(addr.upper().replace("X", "x"), "ethereum")
        assert counter["address"] == 1


class TestNegativeCacheStats:
    def test_hit_counter_increments(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        assert resolver._stats["negative_cache_hits"] == 0
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        assert resolver._stats["negative_cache_hits"] == 0

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        assert resolver._stats["negative_cache_hits"] == 1


class TestClearNegativeCache:
    def test_clear_api(self, fresh_resolver, monkeypatch) -> None:
        resolver = fresh_resolver()
        counter: dict = {}
        _install_definitive_miss(resolver, monkeypatch, counter)

        with pytest.raises(TokenNotFoundError):
            resolver.resolve("UNKNOWN", "ethereum")
        assert resolver._negative_cache

        resolver.clear_negative_cache()
        assert not resolver._negative_cache
