"""Hermeticity guard: unit tests must never reach a LIVE local gateway.

A developer's `almanak gateway --standalone` listening on localhost:50051
leaked into this suite: the pool-history/lending ladders connected and served
REAL measured data, flipping tests that pin the no-gateway unavailability
path (test_mixed_category_routing accrued real pool fees;
test_decision_inputs_alm2951's lending_rate lane returned a real rate instead
of raising).

Every backtesting gateway lane funnels through
``almanak.framework.gateway_client.get_gateway_client`` and then runs the
same ``is_connected`` / ``connect()`` dance, so one stub here makes every
connect attempt fail exactly the way an absent gateway does (the callers wrap
it into their canonical "Gateway connect failed" ``DataSourceUnavailable``).
Tests that deliberately exercise the plumbing keep working: they monkeypatch
``get_gateway_client`` (or a higher seam like ``get_connected_gateway_client``)
inside the test, which overrides this autouse guard.
"""

import pytest


class _HermeticGatewayClient:
    """Never-connectable stand-in for the gateway client singleton."""

    is_connected = False

    def connect(self) -> None:
        raise ConnectionError(
            "hermetic unit tests: live gateway connections are disabled (tests/unit/backtesting/conftest.py)"
        )

    def disconnect(self) -> None:  # parity with reset_gateway_client()
        return None


@pytest.fixture(autouse=True)
def _no_live_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "almanak.framework.gateway_client.get_gateway_client",
        lambda: _HermeticGatewayClient(),
    )
