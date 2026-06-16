"""VIB-5140: the typed-query wrappers forward an optional ``block`` ref.

QueryAllowance / QueryBalance / QueryPositionLiquidity / QueryPositionTokensOwed
gained an optional ``block`` parameter so post-transaction reads (the teardown
closure verifier) can pin to the confirmed receipt's block instead of an
unpinned "latest" that a trailing read replica can answer with PRE-tx state.

The wrapper encodes the block ref into the proto's ``string block`` field
exactly as ``eth_call`` does:

- ``None`` (default) → "" (proto default; the gateway handler maps "" →
  "latest", preserving the pre-VIB-5140 behaviour for legacy callers).
- ``int`` → ``hex(N)`` (JSON-RPC block-number form); ``bool`` / negative reject.
- ``str`` → passed through unchanged.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from almanak.framework.gateway_client import (
    GatewayClient,
    GatewayClientConfig,
    _encode_block_tag,
)


def _make_client() -> GatewayClient:
    config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
    client = GatewayClient(config)
    client._rpc_stub = MagicMock()
    return client


class TestEncodeBlockTag:
    def test_none_maps_to_empty_string(self):
        assert _encode_block_tag(None) == ""

    def test_int_maps_to_hex(self):
        assert _encode_block_tag(19_000_000) == hex(19_000_000)

    def test_str_passthrough(self):
        assert _encode_block_tag("latest") == "latest"
        assert _encode_block_tag("0xabc") == "0xabc"

    def test_bool_rejected(self):
        with pytest.raises(ValueError):
            _encode_block_tag(True)  # noqa: FBT003

    def test_negative_rejected(self):
        with pytest.raises(ValueError):
            _encode_block_tag(-1)

    def test_float_raises_type_error(self):
        with pytest.raises(TypeError):
            _encode_block_tag(19_000_000.0)  # type: ignore[arg-type]

    def test_other_type_raises_type_error(self):
        with pytest.raises(TypeError):
            _encode_block_tag([19_000_000])  # type: ignore[arg-type]


class TestQueryWrappersForwardBlock:
    def test_position_liquidity_omitted_block_is_empty(self):
        client = _make_client()
        resp = MagicMock(success=True, liquidity="0")
        client._rpc_stub.QueryPositionLiquidity.return_value = resp

        client.query_position_liquidity(chain="arbitrum", position_manager="0xNPM", token_id=1)

        req = client._rpc_stub.QueryPositionLiquidity.call_args.args[0]
        assert req.block == ""

    def test_position_liquidity_int_block_encoded_hex(self):
        client = _make_client()
        resp = MagicMock(success=True, liquidity="0")
        client._rpc_stub.QueryPositionLiquidity.return_value = resp

        client.query_position_liquidity(
            chain="arbitrum", position_manager="0xNPM", token_id=1, block=19_000_000
        )

        req = client._rpc_stub.QueryPositionLiquidity.call_args.args[0]
        assert req.block == hex(19_000_000)

    def test_position_tokens_owed_int_block_encoded_hex(self):
        client = _make_client()
        resp = MagicMock(success=True, tokens_owed0="0", tokens_owed1="0")
        client._rpc_stub.QueryPositionTokensOwed.return_value = resp

        client.query_position_tokens_owed(
            chain="arbitrum", position_manager="0xNPM", token_id=1, block=19_000_000
        )

        req = client._rpc_stub.QueryPositionTokensOwed.call_args.args[0]
        assert req.block == hex(19_000_000)

    def test_allowance_str_block_passthrough(self):
        client = _make_client()
        resp = MagicMock(success=True, allowance="0")
        client._rpc_stub.QueryAllowance.return_value = resp

        client.query_allowance(
            chain="arbitrum",
            token_address="0xtok",
            owner_address="0xowner",
            spender_address="0xspender",
            block="0xabc123",
        )

        req = client._rpc_stub.QueryAllowance.call_args.args[0]
        assert req.block == "0xabc123"

    def test_erc20_balance_omitted_block_is_empty(self):
        client = _make_client()
        resp = MagicMock(success=True, balance="0")
        client._rpc_stub.QueryBalance.return_value = resp

        client.query_erc20_balance(chain="arbitrum", token_address="0xtok", wallet_address="0xw")

        req = client._rpc_stub.QueryBalance.call_args.args[0]
        assert req.block == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
