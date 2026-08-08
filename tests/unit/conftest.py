"""Hermeticity guards for the unit suite.

A developer running a standalone gateway (``almanak gateway --standalone``)
has a live gRPC endpoint on ``localhost:50051`` — exactly the address
``GatewayClientConfig.from_env()`` resolves when nothing is configured.
Several framework lanes fall back to that ambient default-port singleton
(``get_gateway_client()``) when no client was injected:

* the ``MarketSnapshot.lending_rate()`` / ``best_lending_rate()`` lazy
  ``RateMonitor`` lane (``_monitor_get_connected_gateway_client``);
* ``RateHistoryReader``'s gateway adapter
  (``_rate_history_get_connected_gateway_client``);
* the backtest Chainlink / TWAP price providers
  (``backtesting/pnl/providers/chainlink.py`` / ``twap.py``);
* the CoinGecko / Binance integration helpers
  (``almanak/framework/integrations/``).

A unit test asserting "no provider configured -> raises / placeholder /
unmeasured" would instead resolve REAL data through that ambient gateway and
flip nondeterministically depending on what happens to be listening on the
developer's machine (observed: 10 tests across
``tests/unit/strategies/test_market_snapshot_lending_rate.py``,
``tests/unit/data/rates/test_monitor.py``, and
``tests/unit/data/test_market_snapshot_strategy_api.py`` failed with a
rate-serving gateway on 50051 and passed with the port closed).

``_hermetic_ambient_gateway`` pins ``GatewayClientConfig.from_env()`` — the
seam every no-config ``GatewayClient()`` construction funnels through,
including the ``get_gateway_client()`` singleton — to an unroutable loopback
address for every unit test, and hides any process-wide singleton cached
before the test. It deliberately does NOT export ``ALMANAK_GATEWAY_HOST`` /
``ALMANAK_GATEWAY_PORT``: the mere PRESENCE of that env var is the product
signal ``gateway_backtest_configured()`` consumes, so an env-var pin would
flip every unit test into gateway mode (e.g. ``CoinGeckoDataProvider`` /
``SubgraphClient`` growing gateway transports instead of their default
direct/offline behaviour) and silently move tests onto fail-closed gateway
branches. Tests that exercise a gateway explicitly are unaffected: injected
clients, explicit ``GatewayClientConfig(...)`` constructions, and
monkeypatched seams carry their own config, and env-detection tests set the
env vars themselves. Inherited gateway host/port exports are scrubbed for
the same reason — a developer's shell must not flip presence detection for
the unit suite.

Regression pin: ``tests/unit/strategies/test_ambient_gateway_hermeticity.py``
binds a live rate-serving gRPC server on the default port and asserts the
no-injection paths still raise.
"""

import pytest

import almanak.framework.gateway_client as _gateway_client_module
from almanak.framework.gateway_client import GatewayClientConfig

# Port 1 (tcpmux) needs root to bind and is never in service on a dev or CI
# host, so a loopback connect gets an instant ECONNREFUSED and pinned lanes
# fail fast with the same UNAVAILABLE the "no gateway running" contract
# expects — no deadline stalls. GatewayClient.connect() itself does no I/O
# (it only builds the channel), so connect() still "succeeds" and the
# failure surfaces at RPC time, matching the closed-port behaviour exactly.
_HERMETIC_GATEWAY_HOST = "127.0.0.1"
_HERMETIC_GATEWAY_PORT = 1


def _hermetic_gateway_config() -> GatewayClientConfig:
    return GatewayClientConfig(
        host=_HERMETIC_GATEWAY_HOST,
        port=_HERMETIC_GATEWAY_PORT,
        timeout=5.0,
        auth_token=None,
    )


@pytest.fixture(autouse=True)
def _hermetic_ambient_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests from resolving data through an ambient local gateway."""
    monkeypatch.setattr(
        GatewayClientConfig,
        "from_env",
        classmethod(lambda cls: _hermetic_gateway_config()),
    )
    # Scrub inherited gateway host/port exports so a developer's shell cannot
    # flip gateway-presence detection (gateway_backtest_configured()) for the
    # unit suite. Env-detection tests opt back in with monkeypatch.setenv.
    for var in ("ALMANAK_GATEWAY_HOST", "GATEWAY_HOST", "ALMANAK_GATEWAY_PORT", "GATEWAY_PORT"):
        monkeypatch.delenv(var, raising=False)
    # The from_env pin only matters at construction time: get_gateway_client()
    # caches a singleton and never re-reads its config. Hide whatever an
    # earlier test (or another suite in the same run) left cached so the first
    # ambient dial inside this test constructs a client from the pinned
    # config; monkeypatch restores the previous singleton afterwards.
    monkeypatch.setattr(_gateway_client_module, "_default_client", None)
