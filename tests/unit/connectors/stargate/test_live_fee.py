"""Live messaging fees must match the send and never fall back to stale hints."""

from decimal import Decimal
from unittest.mock import Mock

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.stargate.adapter import StargateBridgeAdapter, StargateQuoteError
from tests.unit.connectors.stargate.test_adapter_build_deposit_tx import (
    POOL,
    RECIPIENT,
    _decode_calldata,
    _quote,
    _route_data,
)


@pytest.mark.parametrize("token", ["USDC", "ETH"])
def test_live_fee_quotes_exact_send_and_updates_value_without_mutating_discovery(token):
    adapter = StargateBridgeAdapter()
    quote = _quote(
        route_data=_route_data(token=token), gas_fee_amount=Decimal("0.003"), relayer_fee_amount=Decimal("0.6")
    )
    call = Mock(return_value="0x" + encode(["uint256", "uint256"], [3_100_000_000_000_000_001, 0]).hex())
    refreshed = adapter.refresh_quote_for_execution(quote, RECIPIENT, call)
    tx = adapter.build_deposit_tx(refreshed, RECIPIENT)
    send_param, fee, _ = _decode_calldata(tx)
    to, calldata = call.call_args.args
    assert to == POOL
    send_type = "(uint32,bytes32,uint256,uint256,bytes,bytes,bytes)"
    assert calldata[2:10] == keccak(text=f"quoteSend({send_type},bool)")[:4].hex()
    quoted_send, pay_in_lz = decode([send_type, "bool"], bytes.fromhex(calldata[10:]))
    assert quoted_send == send_param
    assert pay_in_lz is False
    assert fee == (3_720_000_000_000_000_002, 0)
    assert tx["value"] == fee[0] + (send_param[2] if token == "ETH" else 0)
    assert refreshed.gas_fee_amount == Decimal("3.720000000000000002")
    assert refreshed.fee_amount == refreshed.relayer_fee_amount == Decimal("0.6")
    assert quote.route_data["lz_fee_wei"] == "3000000000000000"


@pytest.mark.parametrize("raw", [None, "0x", "0x1234", "garbage", "0xzz", "0x" + "00" * 65])
def test_unmeasured_or_malformed_fee_fails_closed(raw):
    with pytest.raises(StargateQuoteError, match="Cannot measure Stargate native fee"):
        StargateBridgeAdapter().refresh_quote_for_execution(_quote(), RECIPIENT, Mock(return_value=raw))


def test_rpc_exception_fails_closed():
    with pytest.raises(StargateQuoteError, match="RPC unavailable"):
        StargateBridgeAdapter().refresh_quote_for_execution(
            _quote(), RECIPIENT, Mock(side_effect=TimeoutError("RPC unavailable"))
        )


def test_lz_token_payment_is_rejected():
    call = Mock(return_value="0x" + encode(["uint256", "uint256"], [100, 1]).hex())
    with pytest.raises(StargateQuoteError, match="unsupported LZ token"):
        StargateBridgeAdapter().refresh_quote_for_execution(_quote(), RECIPIENT, call)


def test_measured_zero_is_preserved():
    call = Mock(return_value="0x" + encode(["uint256", "uint256"], [0, 0]).hex())
    quote = StargateBridgeAdapter().refresh_quote_for_execution(_quote(), RECIPIENT, call)
    assert quote.route_data["lz_fee_wei"] == "0"
