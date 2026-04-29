"""Regression tests for Polygon fixture setup helpers."""

from types import SimpleNamespace

from tests.intents.polygon import conftest as polygon_conftest


def test_polygon_anvil_rpc_url_fixture_is_side_effect_free() -> None:  # noqa: layers
    """Polygon module setup must not issue admin RPCs before recovery can run."""
    anvil_fixture = SimpleNamespace(port=43123)

    rpc_url = polygon_conftest.anvil_rpc_url.__wrapped__(anvil_fixture)

    assert rpc_url == "http://127.0.0.1:43123"
