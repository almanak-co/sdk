"""The intent-test gateway doubles must satisfy the venue verifier's client surface.

``GatewayClientVenueVerificationGateway`` drives an SDK ``GatewayClient``. The
intent lanes hand it two test doubles instead: ``AnvilEthCallAdapter`` (managed
fork) and ``OperatorGatewayClient`` (EOA lanes). Nothing bound those doubles to
the interface they stand in for, so when #3749 began verifying exact V3 pools
the lanes failed one missing attribute at a time -- ``block_number``, then
``rpc``, then ``config`` -- each discovered only by a full nine-chain CI run.

This test is the cheap version of that feedback loop: it reads the attributes the
verifier actually touches on its client, and asserts both doubles carry them with
call shapes that bind. It needs no fork and no network.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts.qa.operator_gateway import OperatorGatewayClient
from tests.intents.conftest import AnvilEthCallAdapter

GATEWAY_SOURCE = Path(__file__).resolve().parents[4] / "almanak" / "framework" / "venues" / "gateway.py"
#: ``integration`` is reached only by ``exact_pool_ohlcv``, the CoinGecko candle
#: path. A fork-backed double cannot serve candles, and inventing one would
#: manufacture a capability. Leaving it absent keeps that path failing loudly.
OHLCV_ONLY = {"integration"}


def _verifier_client_surface() -> set[str]:
    attributes = set(re.findall(r"self\._client\.(\w+)", GATEWAY_SOURCE.read_text(encoding="utf-8")))
    assert attributes, "found no client attribute references; the verifier was refactored"
    return attributes - OHLCV_ONLY


def _doubles() -> list[tuple[str, object]]:
    web3 = MagicMock()
    return [
        ("AnvilEthCallAdapter", AnvilEthCallAdapter(web3)),
        ("OperatorGatewayClient", OperatorGatewayClient(web3, "arbitrum")),
    ]


@pytest.mark.parametrize("name,double", _doubles(), ids=lambda value: value if isinstance(value, str) else "")
def test_double_carries_every_attribute_the_verifier_reads(name: str, double: object) -> None:
    missing = sorted(attribute for attribute in _verifier_client_surface() if not hasattr(double, attribute))
    assert not missing, f"{name} is missing gateway-client attributes the venue verifier reads: {missing}"


@pytest.mark.parametrize("name,double", _doubles(), ids=lambda value: value if isinstance(value, str) else "")
def test_double_accepts_the_verifier_call_shapes(name: str, double: object) -> None:
    """Presence is not enough; the verifier calls these with specific arguments."""
    # framework/venues/gateway.py: read() -> self._client.eth_call(chain=, to=, data=, block=, raise_on_error=)
    inspect.signature(double.eth_call).bind(
        chain="arbitrum", to="0x" + "1" * 40, data="0x", block=1, raise_on_error=True
    )
    # framework/venues/gateway.py: block_number() -> self._client.block_number(chain)  [POSITIONAL]
    inspect.signature(double.block_number).bind("arbitrum")
    # framework/venues/gateway.py: _rpc() -> self._client.rpc.Call(request, timeout=self._client.config.timeout)
    inspect.signature(double.rpc.Call).bind(MagicMock(), timeout=double.config.timeout)


def test_the_surface_probe_still_finds_the_verifier() -> None:
    """Negative control: a renamed client attribute must not silently empty this suite."""
    assert "block_number" in _verifier_client_surface()
    assert "eth_call" in _verifier_client_surface()


def test_operator_gateway_reports_the_underlying_connection_state() -> None:
    web3 = MagicMock()
    web3.is_connected.return_value = False

    assert OperatorGatewayClient(web3, "arbitrum").is_connected is False
