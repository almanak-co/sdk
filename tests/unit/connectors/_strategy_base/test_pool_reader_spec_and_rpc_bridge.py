"""Unit tests for PoolReaderSpec lazy-reference validation and reader_rpc_call."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.pool_reader import ImportRef, PoolReaderSpec
from almanak.connectors._strategy_base.pool_validation_base import reader_rpc_call

_READER = ImportRef(module="almanak.framework.data.pools.reader", attribute="UniswapV3PoolPriceReader")


@pytest.mark.parametrize("field_name", ["pair_resolver", "identity_probe"])
def test_spec_rejects_non_importref_lazy_references(field_name):
    with pytest.raises(TypeError, match=field_name):
        PoolReaderSpec(protocol="uniswap_v3", factory_addresses={}, reader=_READER, **{field_name: "not_a_ref"})


def test_spec_accepts_importref_lazy_references():
    spec = PoolReaderSpec(
        protocol="uniswap_v3", factory_addresses={}, reader=_READER, pair_resolver=_READER, identity_probe=_READER
    )
    assert spec.pair_resolver is _READER
    assert spec.identity_probe is _READER


def test_reader_rpc_call_forwards_arguments_and_returns_bytes():
    with patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", return_value=b"\x01" * 32) as mock:
        call = reader_rpc_call(gateway_client=None, rpc_url="http://rpc", timeout=5.0)
        assert call("base", "0xpool", "0xdata") == b"\x01" * 32
    mock.assert_called_once_with("http://rpc", "0xpool", "0xdata", timeout=5.0, chain="base", gateway_client=None)


def test_reader_rpc_call_raises_when_eth_call_returns_none():
    with patch("almanak.connectors._strategy_base.pool_validation_base.eth_call", return_value=None):
        call = reader_rpc_call(rpc_url="http://rpc")
        with pytest.raises(ValueError, match="no data"):
            call("base", "0xpool", "0xdata")
