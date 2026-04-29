"""In-process gateway-isolation smoke test for the polymarket V2 strategy.

CI's ``sidecar-regression`` workflow runs registered demos under
``scripts/ci/run_isolated_strategy.py`` (loopback-only socket monkeypatch)
to catch direct egress from strategy / framework code. The polymarket
demo isn't in that registry yet because none of the polymarket strategies
under ``strategies/incubating/`` have a sidecar-compatible ``run_anvil.py``
(no ``--skip-cli`` flag, no Anvil bring-up — they're mock-driven dry-runs).

This file is the in-process equivalent: same socket monkeypatch as the CI
harness, applied around the polymarket strategy's import + construct +
``decide()`` cycle. It catches the most common boundary-violation classes
(direct ``requests``/``aiohttp``/``httpx`` import-time or hot-path calls,
``Web3(HTTPProvider(...))`` outside the gateway provider) without needing
a full Anvil + gateway harness.

Doesn't catch order submission egress — that path goes through
``GatewayPolymarketClient`` (gateway-routed by construction). The remaining
hole is "what happens when a real intent compiles + executes through the
real gateway"; that's the work tracked in the follow-up ticket to register
polymarket in ``.github/sidecar-demos.yml``.
"""

from __future__ import annotations

import errno
import socket
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

# Mirror scripts/ci/run_isolated_strategy.py: allow only the gateway address
# (default loopback:50051), block everything else. Run the patch as a
# pytest fixture so we can scope it to this file and reliably restore.
_GATEWAY_HOST = "127.0.0.1"
_GATEWAY_PORT = 50051
_ALLOWED_ADDRESSES: set[tuple[str, int]] = {
    (_GATEWAY_HOST, _GATEWAY_PORT),
    ("::1", _GATEWAY_PORT),
    ("localhost", _GATEWAY_PORT),
}


def _is_allowed(sock: socket.socket, address) -> bool:  # type: ignore[no-untyped-def]
    if sock.family == socket.AF_UNIX:
        return True  # Python internals use AF_UNIX (NSS, D-Bus) — not egress.
    host = address[0] if address else ""
    port = address[1] if address and len(address) > 1 else None
    return (host, port) in _ALLOWED_ADDRESSES


@pytest.fixture
def isolated_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restrict ``socket.socket.connect`` / ``connect_ex`` to the gateway."""
    orig_connect = socket.socket.connect
    orig_connect_ex = socket.socket.connect_ex

    def restricted_connect(self: socket.socket, address):  # type: ignore[no-untyped-def]
        if not _is_allowed(self, address):
            raise OSError(
                f"GATEWAY_BOUNDARY_VIOLATION: strategy attempted egress to {address!r}; "
                f"only {_GATEWAY_HOST}:{_GATEWAY_PORT} is permitted in sidecar mode."
            )
        return orig_connect(self, address)

    def restricted_connect_ex(self: socket.socket, address):  # type: ignore[no-untyped-def]
        if not _is_allowed(self, address):
            return errno.EPERM
        return orig_connect_ex(self, address)

    monkeypatch.setattr(socket.socket, "connect", restricted_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", restricted_connect_ex)


# =============================================================================
# Smoke: the patch actually blocks
# =============================================================================


class TestIsolationFixture:
    """Sanity-check that the fixture does what we think it does."""

    def test_blocks_disallowed_egress(self, isolated_socket: None) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(OSError, match="GATEWAY_BOUNDARY_VIOLATION"):
                s.connect(("8.8.8.8", 53))
        finally:
            s.close()

    def test_allows_gateway_address(self, isolated_socket: None) -> None:
        """Connect attempt to the gateway port doesn't raise the boundary
        error (it raises ConnectionRefusedError because nothing is listening,
        which is a different beast — proves the host/port pair is allowed)."""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        try:
            with pytest.raises((ConnectionRefusedError, OSError)) as exc_info:
                s.connect((_GATEWAY_HOST, _GATEWAY_PORT))
            assert "GATEWAY_BOUNDARY_VIOLATION" not in str(exc_info.value), (
                "gateway address must be on the allow-list"
            )
        finally:
            s.close()


# =============================================================================
# Polymarket V2 strategy under isolation
# =============================================================================


class TestPolymarketStrategyImportUnderIsolation:
    """Importing the strategy module must not open any sockets.

    Catches: a future refactor that adds an import-time HTTP call (e.g.
    fetching a config from a CDN, warming a CoinGecko client, instantiating
    a global ``httpx.Client``) directly inside ``strategies/incubating/
    polymarket_signal_trader/strategy.py`` or any of its imports.
    """

    def test_strategy_module_imports_without_egress(self, isolated_socket: None) -> None:
        # If anything in the import chain opens a forbidden socket, this
        # raises GATEWAY_BOUNDARY_VIOLATION and the test fails.
        import strategies.incubating.polymarket_signal_trader.strategy as strat_mod

        assert hasattr(strat_mod, "PolymarketSignalTraderStrategy")


class TestPolymarketStrategyDecideUnderIsolation:
    """Running ``decide()`` under isolation must not open any sockets.

    The strategy fetches data via the ``MarketSnapshot`` (gateway-backed by
    construction) and emits Intents. Direct egress in ``decide()`` itself
    — or in a method it calls before returning the Intent — would fire the
    GATEWAY_BOUNDARY_VIOLATION here.
    """

    def _build_strategy(self) -> object:
        from strategies.incubating.polymarket_signal_trader.strategy import (
            PolymarketSignalTraderStrategy,
        )

        return PolymarketSignalTraderStrategy(
            config={
                "market_id": "test-market-slug",
                "trade_size_usd": "10",
                "min_confidence": "0.6",
                "min_edge": "0.05",
                "order_type": "limit",
                "stop_loss_pct": "0.20",
                "take_profit_pct": "0.30",
            },
            chain="polygon",
            wallet_address="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        )

    def _build_market_snapshot_with_yes_price(self, yes_price: Decimal) -> MagicMock:
        """Stub a MarketSnapshot that returns the given YES price without
        going to the gateway. The real MarketSnapshot routes through the
        gateway — which is also fine in isolation, but we don't need a
        running gateway just to exercise decide()'s control flow."""
        market = MagicMock()
        market.prediction_price.return_value = yes_price
        # Strategy may also touch market.balance(...) downstream of the
        # decision. Return a low USDC balance so it short-circuits cleanly.
        balance = MagicMock()
        balance.balance = Decimal("0")
        balance.balance_usd = Decimal("0")
        market.balance.return_value = balance
        return market

    def test_decide_with_bullish_signal_does_not_egress(self, isolated_socket: None) -> None:
        """Construct + decide() under isolation. Both module-load AND the
        decision hot path are exercised; any direct socket from either
        fails the test with GATEWAY_BOUNDARY_VIOLATION."""
        strategy = self._build_strategy()
        market = self._build_market_snapshot_with_yes_price(Decimal("0.30"))

        # decide() may return a PredictionBuyIntent or a HoldIntent — we
        # don't care which, only that the call completes without opening
        # a forbidden socket.
        result = strategy.decide(market)  # type: ignore[attr-defined]

        # Sanity: a real Intent (or None) was produced.
        assert result is None or hasattr(result, "intent_type") or hasattr(result, "outcome")

    def test_decide_when_no_market_id_returns_hold_without_egress(
        self, isolated_socket: None
    ) -> None:
        """Edge-path: empty market_id config returns HOLD immediately. Still
        exercises the import + construct path and proves the no-op decision
        loop doesn't open sockets either."""
        from strategies.incubating.polymarket_signal_trader.strategy import (
            PolymarketSignalTraderStrategy,
        )

        strategy = PolymarketSignalTraderStrategy(
            config={"market_id": "", "use_signals": ["mock"]},
            chain="polygon",
            wallet_address="0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        )
        market = self._build_market_snapshot_with_yes_price(Decimal("0.50"))

        result = strategy.decide(market)
        assert result is not None  # HOLD intent
