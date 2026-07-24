"""Branch coverage for StargateBridgeAdapter.build_deposit_tx.

build_deposit_tx turns a BridgeQuote's route_data into Stargate V2 OFT
send() calldata. Covered here: expiry/route-data guards, missing pool and
destination-chain wrapping, recipient normalization, the ETH-vs-token
value split, and full ABI round-trip of the encoded SendParam/MessagingFee
structs. Pure encoding — no network.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from eth_abi import decode

from almanak.connectors._strategy_base.bridge_base import BridgeQuote
from almanak.connectors.stargate.adapter import (
    StargateBridgeAdapter,
    StargateTransactionError,
)

POOL = "0xe8CDF27AcD73a434D661C84887215F7598e7d0d3"
RECIPIENT = "0x" + "ab" * 20
SEND_SELECTOR = "c7c7f5b3"
SEND_ABI_TYPES = [
    "(uint32,bytes32,uint256,uint256,bytes,bytes,bytes)",  # SendParam
    "(uint256,uint256)",  # MessagingFee
    "address",  # refundAddress
]


def _route_data(**overrides):
    data = {
        "pool_address": POOL,
        "from_lz_chain_id": 30110,
        "to_lz_chain_id": 30111,
        "from_evm_chain_id": 42161,
        "to_evm_chain_id": 10,
        "amount_wei": "1000000000",
        "min_amount_wei": "995000000",
        "lz_fee_wei": "3000000000000000",
        "token": "USDC",
    }
    data.update(overrides)
    return data


def _quote(route_data=None, **overrides):
    fields = {
        "bridge_name": "Stargate",
        "token": "USDC",
        "input_amount": Decimal("1000"),
        "output_amount": Decimal("999.4"),
        "from_chain": "arbitrum",
        "to_chain": "optimism",
        "fee_amount": Decimal("0.603"),
        "route_data": _route_data() if route_data is None else route_data,
    }
    fields.update(overrides)
    return BridgeQuote(**fields)


def _decode_calldata(tx_data):
    raw = bytes.fromhex(tx_data["data"][2:])
    assert raw[:4].hex() == SEND_SELECTOR
    return decode(SEND_ABI_TYPES, raw[4:])


@pytest.fixture
def adapter():
    return StargateBridgeAdapter()


class TestGuards:
    def test_expired_quote_rejected(self, adapter):
        quote = _quote(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        with pytest.raises(StargateTransactionError, match="Quote has expired"):
            adapter.build_deposit_tx(quote, RECIPIENT)

    def test_missing_route_data_rejected(self, adapter):
        with pytest.raises(StargateTransactionError, match="Quote missing route data"):
            adapter.build_deposit_tx(_quote(route_data={}), RECIPIENT)

    def test_missing_pool_address_wrapped(self, adapter):
        route = _route_data()
        del route["pool_address"]
        with pytest.raises(
            StargateTransactionError,
            match="Failed to build deposit transaction: Missing pool address in quote",
        ):
            adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)

    @pytest.mark.parametrize("chain_id", [0, None])
    def test_missing_destination_chain_wrapped(self, adapter, chain_id):
        route = _route_data()
        if chain_id is None:
            del route["to_lz_chain_id"]
        else:
            route["to_lz_chain_id"] = chain_id
        with pytest.raises(
            StargateTransactionError,
            match="Failed to build deposit transaction: Missing destination chain ID in quote",
        ):
            adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)

    def test_invalid_recipient_hex_wrapped(self, adapter):
        with pytest.raises(StargateTransactionError, match="Failed to build deposit transaction"):
            adapter.build_deposit_tx(_quote(), "0x" + "zz" * 20)


class TestHappyPath:
    def test_token_bridge_tx_shape(self, adapter):
        tx_data = adapter.build_deposit_tx(_quote(), RECIPIENT)

        assert tx_data["to"] == POOL
        # Token bridges only send the LayerZero fee as native value.
        assert tx_data["value"] == 3_000_000_000_000_000

        send_param, messaging_fee, refund_address = _decode_calldata(tx_data)
        dst_eid, to_bytes32, amount_ld, min_amount_ld, extra_options, compose_msg, oft_cmd = send_param
        assert dst_eid == 30111
        # Recipient is left-padded into bytes32.
        assert to_bytes32 == bytes(12) + bytes.fromhex(RECIPIENT[2:])
        assert amount_ld == 1_000_000_000
        assert min_amount_ld == 995_000_000
        assert extra_options == b""
        assert compose_msg == b""
        assert oft_cmd == b""
        assert messaging_fee == (3_000_000_000_000_000, 0)
        assert refund_address == RECIPIENT

    def test_eth_bridge_value_includes_amount(self, adapter):
        route = _route_data(
            token="ETH",
            amount_wei="2000000000000000000",
            lz_fee_wei="3000000000000000",
        )
        tx_data = adapter.build_deposit_tx(_quote(route_data=route, token="ETH"), RECIPIENT)
        assert tx_data["value"] == 2_000_000_000_000_000_000 + 3_000_000_000_000_000

    def test_eth_token_casing_normalized(self, adapter):
        route = _route_data(token="eth", amount_wei="5", lz_fee_wei="7")
        tx_data = adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)
        assert tx_data["value"] == 12

    def test_unprefixed_recipient_normalized(self, adapter):
        tx_data = adapter.build_deposit_tx(_quote(), RECIPIENT[2:])
        send_param, _fee, refund_address = _decode_calldata(tx_data)
        assert send_param[1] == bytes(12) + bytes.fromhex(RECIPIENT[2:])
        assert refund_address == RECIPIENT

    def test_missing_optional_route_fields_default_to_zero(self, adapter):
        # Only pool + destination are required; amounts, fee and token
        # fall back to their .get() defaults.
        route = {"pool_address": POOL, "to_lz_chain_id": 30111}
        tx_data = adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)

        assert tx_data["to"] == POOL
        assert tx_data["value"] == 0
        send_param, messaging_fee, _refund = _decode_calldata(tx_data)
        assert send_param[2] == 0  # amountLD
        assert send_param[3] == 0  # minAmountLD
        assert messaging_fee == (0, 0)
