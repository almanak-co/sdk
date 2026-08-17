"""Contract tests for the canonical token-decimals resolver."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.data.tokens import (
    NATIVE_SENTINEL,
    TokenMeta,
    TokenNotFoundError,
    TokenResolutionError,
    resolve_token_decimals,
)


class StubResolver:
    def __init__(self, *outcomes: int | Exception) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, str]] = []

    def resolve(self, token: str, chain: str) -> SimpleNamespace:
        self.calls.append((token, chain))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return SimpleNamespace(decimals=outcome)


def test_uses_exact_address_hint_before_cache_and_resolver() -> None:
    address = "0x00000000000000000000000000000000000000Aa"
    resolver = StubResolver(18)
    cache = {("ethereum", address.lower()): 8}
    hints: dict[str, TokenMeta] = {
        address.lower(): TokenMeta(address=address.lower(), symbol="USDC", decimals=6),
    }

    assert resolve_token_decimals(address, "ETHEREUM", cache=cache, hints=hints, resolver=resolver) == 6
    assert resolver.calls == []
    assert cache[("ethereum", address.lower())] == 6


def test_positive_cache_avoids_resolution() -> None:
    address = "0x00000000000000000000000000000000000000aa"
    resolver = StubResolver(18)
    cache = {("base", address): 6}

    assert resolve_token_decimals(address, "base", cache=cache, resolver=resolver) == 6
    assert resolver.calls == []


def test_definitive_miss_is_negative_cached() -> None:
    address = "0x00000000000000000000000000000000000000aa"
    miss = TokenNotFoundError(token=address, chain="base")
    resolver = StubResolver(miss, 18)
    cache: dict[tuple[str, str], int | None] = {}

    with pytest.raises(TokenNotFoundError):
        resolve_token_decimals(address, "base", cache=cache, resolver=resolver)
    with pytest.raises(TokenNotFoundError, match="negatively cached"):
        resolve_token_decimals(address, "base", cache=cache, resolver=resolver)

    assert resolver.calls == [(address, "base")]


def test_transient_error_is_not_cached() -> None:
    address = "0x00000000000000000000000000000000000000aa"
    transient = TokenResolutionError(token=address, chain="base", reason="gateway unavailable")
    resolver = StubResolver(transient, 6)
    cache: dict[tuple[str, str], int | None] = {}

    with pytest.raises(TokenResolutionError, match="gateway unavailable"):
        resolve_token_decimals(address, "base", cache=cache, resolver=resolver)

    assert cache == {}
    assert resolve_token_decimals(address, "base", cache=cache, resolver=resolver) == 6


def test_invalid_hint_is_ignored_and_invalid_resolution_is_not_cached() -> None:
    address = "0x00000000000000000000000000000000000000aa"
    resolver = StubResolver(78)
    cache: dict[tuple[str, str], int | None] = {}

    with pytest.raises(TokenResolutionError, match="invalid decimals"):
        resolve_token_decimals(address, "base", cache=cache, hints={address: 99}, resolver=resolver)

    assert cache == {}


def test_evm_native_sentinel_has_protocol_invariant_decimals() -> None:
    resolver = StubResolver(6)

    assert resolve_token_decimals(NATIVE_SENTINEL, "arbitrum", resolver=resolver) == 18
    assert resolver.calls == []


def test_solana_cache_keys_preserve_mint_case() -> None:
    mint = "So11111111111111111111111111111111111111112"
    resolver = StubResolver(9)
    cache: dict[tuple[str, str], int | None] = {}

    assert resolve_token_decimals(mint, "solana", cache=cache, resolver=resolver) == 9
    assert cache[("solana", mint)] == 9


def test_unknown_chain_fails_before_resolution() -> None:
    resolver = StubResolver(18)

    with pytest.raises(TokenResolutionError, match="Unknown chain"):
        resolve_token_decimals("USDC", "not-a-chain", resolver=resolver)
    assert resolver.calls == []
