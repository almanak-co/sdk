"""Resolver-side tests for DexScreener ambiguity propagation (VIB-2983).

When the gateway surfaces an AMBIGUOUS_SYMBOL marker (dominance gate failure),
the client-side resolver must:

1. Raise ``AmbiguousTokenError`` with the candidate address list parsed from
   the marker, so strategy authors see exactly which contracts collide.
2. NOT poison the 5-minute negative cache — ambiguity is not a miss; a user
   adding the correct address during the TTL window must be able to retry.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

from almanak.framework.data.tokens.exceptions import AmbiguousTokenError, TokenNotFoundError
from almanak.framework.data.tokens.resolver import TokenResolver, _parse_ambiguous_candidates


class _FakeRpcError(grpc.RpcError):
    """Simulate the gRPC error raised by a NOT_FOUND with AMBIGUOUS_SYMBOL details."""

    def __init__(self, details: str) -> None:
        super().__init__(details)
        self._details = details

    def __str__(self) -> str:
        return self._details


@pytest.fixture()
def fresh_resolver(tmp_path: Path):
    counter = {"n": 0}

    def _make() -> TokenResolver:
        TokenResolver._instance = None
        counter["n"] += 1
        cache_file = tmp_path / f"cache_{counter['n']}.json"
        return TokenResolver(gateway_channel=MagicMock(name="grpc_channel"), cache_file=str(cache_file))

    return _make


class TestParseCandidates:
    def test_extracts_address_list(self) -> None:
        details = "AMBIGUOUS_SYMBOL|addresses=0xAAA,0xBBB,0xCCC|Cannot resolve token 'DUPE' on base"
        assert _parse_ambiguous_candidates(details) == ["0xAAA", "0xBBB", "0xCCC"]

    def test_returns_empty_list_when_missing(self) -> None:
        assert _parse_ambiguous_candidates("AMBIGUOUS_SYMBOL||oops") == []
        assert _parse_ambiguous_candidates("Just an error") == []


class TestAmbiguityPropagation:
    def test_ambiguous_symbol_raises_with_candidates(
        self,
        fresh_resolver,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        resolver = fresh_resolver()
        # Bypass the static registry / fast path by resolving a fake symbol
        # that isn't in the static data. Patch just the gateway-lookup call.
        candidates = ["0x" + "aa" * 20, "0x" + "bb" * 20]
        error_payload = (
            f"AMBIGUOUS_SYMBOL|addresses={','.join(candidates)}|"
            f"Cannot resolve token 'DUPE' on base"
        )

        # Patch the gateway stub so ResolveToken raises an error whose str()
        # matches what the real gRPC NOT_FOUND with set_details() produces.
        fake_stub = MagicMock()
        fake_stub.ResolveToken = MagicMock(side_effect=_FakeRpcError(error_payload))
        monkeypatch.setattr(resolver, "_get_gateway_stub", lambda: fake_stub)
        monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)

        with pytest.raises(AmbiguousTokenError) as exc_info:
            resolver._resolve_symbol_via_gateway("DUPE", "base")

        err = exc_info.value
        assert err.matching_addresses == candidates
        assert "DUPE" in str(err)

    def test_ambiguous_symbol_does_not_poison_negative_cache(
        self,
        fresh_resolver,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        resolver = fresh_resolver()
        candidates = ["0x" + "aa" * 20, "0x" + "bb" * 20]
        error_payload = f"AMBIGUOUS_SYMBOL|addresses={','.join(candidates)}|..."

        fake_stub = MagicMock()
        fake_stub.ResolveToken = MagicMock(side_effect=_FakeRpcError(error_payload))
        monkeypatch.setattr(resolver, "_get_gateway_stub", lambda: fake_stub)
        monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)

        # Trigger the ambiguous path once
        with pytest.raises(AmbiguousTokenError):
            resolver._resolve_symbol_via_gateway("DUPE", "base")

        # Definitive-miss flag must remain False so `resolve()` does not
        # write into the negative cache on this path.
        assert resolver._gateway_miss_state.definitive is False

    def test_non_ambiguous_gateway_error_returns_none(
        self,
        fresh_resolver,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        resolver = fresh_resolver()
        fake_stub = MagicMock()
        fake_stub.ResolveToken = MagicMock(side_effect=_FakeRpcError("Token 'MISSING' not found"))
        monkeypatch.setattr(resolver, "_get_gateway_stub", lambda: fake_stub)
        monkeypatch.setattr(resolver, "_check_gateway_available", lambda: True)

        result = resolver._resolve_symbol_via_gateway("MISSING", "base")
        assert result is None
