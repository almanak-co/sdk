"""VIB-5746: a gateway SYMBOL lookup that times out must be short-TTL
negative-cached so the identical lookup fails fast for a cool-down window instead
of re-burning the full ~15s deadline on every strategy iteration.

Root cause (morpho_looping on Robinhood 4663): the chain has no USDC (only USDe /
USDG), yet a swap-guard evaluation resolved the symbol "USDC" twice per check.
Each lookup hit the gateway's 15s ResolveToken deadline (DEADLINE_EXCEEDED), and
because a timeout is not a *definitive* miss it was never negative-cached — so
every guard evaluation re-burned ~30s. The registry already knows the chain's
tokens (it produced "did you mean USDE/USDG?" suggestions); the fix stops the
timeout being used as control flow.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.tokens.exceptions import TokenNotFoundError
from almanak.framework.data.tokens.resolver import TokenResolver


def _resolver_with_gateway() -> TokenResolver:
    TokenResolver.reset_instance()
    return TokenResolver(gateway_channel=MagicMock())


def teardown_function(_func) -> None:
    TokenResolver.reset_instance()


def _timeout_side_effect(resolver: TokenResolver):
    """Return a side effect that simulates the gateway symbol lookup timing out:
    it flags the per-call miss state as timed-out (as the real DEADLINE_EXCEEDED
    branch does) and returns None (no resolution)."""

    def _se(symbol: str, chain_lower: str):
        resolver._gateway_miss_state.timed_out = True
        return None

    return _se


def test_symbol_timeout_is_short_ttl_negative_cached() -> None:
    """After one timed-out lookup, an identical lookup within the cooldown window
    fails fast WITHOUT re-invoking the gateway (no re-burn of the 15s deadline)."""
    resolver = _resolver_with_gateway()

    with patch.object(
        resolver, "_resolve_symbol_via_gateway", side_effect=_timeout_side_effect(resolver)
    ) as mock_gw:
        # First lookup: gateway attempted, times out, raises TokenNotFound.
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC", "robinhood")
        # Second identical lookup: served from the short-TTL negative cache —
        # the gateway is NOT called again.
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC", "robinhood")

    assert mock_gw.call_count == 1


def test_timeout_cooldown_is_shorter_than_definitive_miss_ttl() -> None:
    """A timeout is not proof of absence — its cooldown must be much shorter than
    the definitive-miss TTL so a transient gateway blip re-probes and recovers."""
    resolver = _resolver_with_gateway()
    assert resolver._gateway_timeout_cooldown_seconds < resolver._negative_cache_ttl_seconds


def test_timeout_cache_expires_and_reprobes() -> None:
    """Once the short cooldown elapses, the resolver re-probes the gateway (the
    symbol might now be resolvable / the blip may have cleared)."""
    resolver = _resolver_with_gateway()

    with patch.object(
        resolver, "_resolve_symbol_via_gateway", side_effect=_timeout_side_effect(resolver)
    ) as mock_gw:
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC", "robinhood")
        # Expire the cooldown by rewinding the stored expiry into the past.
        with resolver._lock:
            resolver._negative_cache = {k: 0.0 for k in resolver._negative_cache}
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("USDC", "robinhood")

    assert mock_gw.call_count == 2


def test_definitive_miss_still_uses_long_ttl() -> None:
    """A definitive gateway miss (not a timeout) keeps the original long TTL —
    the timeout path is additive and must not shorten definitive-miss caching."""
    resolver = _resolver_with_gateway()

    def _definitive(symbol: str, chain_lower: str):
        resolver._gateway_miss_state.definitive = True
        return None

    with patch.object(resolver, "_resolve_symbol_via_gateway", side_effect=_definitive):
        with pytest.raises(TokenNotFoundError):
            resolver.resolve("NOPE", "robinhood")

    # The stored expiry should reflect the long TTL, not the short cooldown.
    import time

    with resolver._lock:
        (expiry,) = list(resolver._negative_cache.values())
    remaining = expiry - time.monotonic()
    assert remaining > resolver._gateway_timeout_cooldown_seconds
