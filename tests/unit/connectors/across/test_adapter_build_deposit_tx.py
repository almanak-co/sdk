"""Branch coverage for AcrossBridgeAdapter.build_deposit_tx."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from eth_abi import decode

from almanak.connectors._strategy_base.bridge_base import BridgeQuote
from almanak.connectors.across.adapter import (
    DEPOSIT_V3_SELECTOR,
    AcrossBridgeAdapter,
    AcrossTransactionError,
)

SPOKE_POOL = "0x" + "10" * 20
INPUT_TOKEN = "0x" + "20" * 20
OUTPUT_TOKEN = "0x" + "30" * 20
RECIPIENT = "0x" + "40" * 20
EXCLUSIVE_RELAYER = "0x" + "50" * 20
ZERO_ADDRESS = "0x" + "00" * 20
DEPOSIT_V3_ABI_TYPES = [
    "address",
    "address",
    "address",
    "address",
    "uint256",
    "uint256",
    "uint256",
    "address",
    "uint32",
    "uint32",
    "uint32",
    "bytes",
]


def _route_data(**overrides):
    data = {
        "spoke_pool_address": SPOKE_POOL,
        "token_address": INPUT_TOKEN,
        "from_chain_id": 42161,
        "to_chain_id": 10,
        "amount_wei": "1000000000",
        "output_amount_wei": "995000000",
        "timestamp": "1700000000",
        "fill_deadline": "1700014400",
        "exclusivity_deadline": "1700000060",
        "exclusive_relayer": EXCLUSIVE_RELAYER,
    }
    data.update(overrides)
    return data


def _quote(route_data=None, **overrides):
    fields = {
        "bridge_name": "Across",
        "token": "USDC",
        "input_amount": Decimal("1000"),
        "output_amount": Decimal("995"),
        "from_chain": "arbitrum",
        "to_chain": "optimism",
        "fee_amount": Decimal("5"),
        "route_data": _route_data() if route_data is None else route_data,
    }
    fields.update(overrides)
    return BridgeQuote(**fields)


def _decode_calldata(tx_data):
    raw = bytes.fromhex(tx_data["data"].removeprefix("0x"))
    assert raw[:4] == DEPOSIT_V3_SELECTOR
    return decode(DEPOSIT_V3_ABI_TYPES, raw[4:])


@pytest.fixture
def adapter(monkeypatch):
    adapter = AcrossBridgeAdapter(token_resolver=MagicMock())
    monkeypatch.setattr(adapter, "_get_token_address", lambda token, chain_id: OUTPUT_TOKEN)
    return adapter


class TestGuards:
    def test_expired_quote_rejected(self, adapter):
        quote = _quote(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        with pytest.raises(AcrossTransactionError, match="Quote has expired"):
            adapter.build_deposit_tx(quote, RECIPIENT)

    def test_missing_route_data_rejected(self, adapter):
        with pytest.raises(AcrossTransactionError, match="Quote missing route data"):
            adapter.build_deposit_tx(_quote(route_data={}), RECIPIENT)

    def test_missing_spoke_pool_wrapped(self, adapter):
        route = _route_data()
        del route["spoke_pool_address"]
        with pytest.raises(
            AcrossTransactionError,
            match="Failed to build deposit transaction: Missing spoke pool address in quote",
        ):
            adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)

    def test_missing_token_address_wrapped(self, adapter):
        route = _route_data(token_address="")
        with pytest.raises(
            AcrossTransactionError,
            match="Failed to build deposit transaction: Missing token address in quote",
        ):
            adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("amount_wei", "invalid"),
            ("output_amount_wei", "invalid"),
            ("timestamp", ""),
            ("fill_deadline", ""),
            ("exclusivity_deadline", None),
        ],
    )
    def test_invalid_numeric_route_data_wrapped(self, adapter, field, value):
        with pytest.raises(AcrossTransactionError, match="Failed to build deposit transaction"):
            adapter.build_deposit_tx(_quote(route_data=_route_data(**{field: value})), RECIPIENT)

    def test_invalid_recipient_wrapped(self, adapter):
        with pytest.raises(AcrossTransactionError, match="Failed to build deposit transaction"):
            adapter.build_deposit_tx(_quote(), "0x" + "zz" * 20)


class TestHappyPath:
    def test_full_depositv3_calldata_round_trip(self, adapter):
        tx_data = adapter.build_deposit_tx(_quote(), RECIPIENT)

        assert tx_data["to"] == SPOKE_POOL
        assert tx_data["value"] == 0
        (
            depositor,
            recipient,
            input_token,
            output_token,
            input_amount,
            output_amount,
            destination_chain_id,
            exclusive_relayer,
            quote_timestamp,
            fill_deadline,
            exclusivity_deadline,
            message,
        ) = _decode_calldata(tx_data)
        assert depositor == RECIPIENT
        assert recipient == RECIPIENT
        assert input_token == INPUT_TOKEN
        assert output_token == OUTPUT_TOKEN
        assert input_amount == 1_000_000_000
        assert output_amount == 995_000_000
        assert destination_chain_id == 10
        assert exclusive_relayer == EXCLUSIVE_RELAYER
        assert quote_timestamp == 1_700_000_000
        assert fill_deadline == 1_700_014_400
        assert exclusivity_deadline == 1_700_000_060
        assert message == b""

    @pytest.mark.parametrize(
        ("token", "expected_value"),
        [
            ("ETH", 1_000_000_000),
            ("eth", 1_000_000_000),
            ("WETH", 0),
            ("weth", 0),
            ("USDC", 0),
        ],
    )
    def test_only_native_eth_sends_input_amount_as_value(self, adapter, token, expected_value):
        tx_data = adapter.build_deposit_tx(_quote(token=token), RECIPIENT)
        assert tx_data["value"] == expected_value

    def test_missing_numeric_and_time_fields_use_defaults(self, adapter, monkeypatch):
        monkeypatch.setattr("almanak.connectors.across.adapter.time.time", lambda: 1_800_000_000)
        route = {
            "spoke_pool_address": SPOKE_POOL,
            "token_address": INPUT_TOKEN,
        }

        tx_data = adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT)
        decoded = _decode_calldata(tx_data)

        assert decoded[4:7] == (0, 0, 0)
        assert decoded[7] == ZERO_ADDRESS
        assert decoded[8] == 1_800_000_000
        assert decoded[9] == 1_800_014_400
        assert decoded[10] == 0

    def test_zero_timestamps_and_0x_relayer_use_defaults(self, adapter, monkeypatch):
        monkeypatch.setattr("almanak.connectors.across.adapter.time.time", lambda: 1_800_000_000)
        route = _route_data(timestamp=0, fill_deadline=0, exclusive_relayer="0x")

        decoded = _decode_calldata(adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT))

        assert decoded[7] == ZERO_ADDRESS
        assert decoded[8] == 1_800_000_000
        assert decoded[9] == 1_800_014_400

    def test_destination_token_falls_back_to_input_token(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "_get_token_address", lambda token, chain_id: None)

        decoded = _decode_calldata(adapter.build_deposit_tx(_quote(), RECIPIENT))

        assert decoded[3] == INPUT_TOKEN

    def test_unprefixed_addresses_are_encoded(self, adapter, monkeypatch):
        monkeypatch.setattr(adapter, "_get_token_address", lambda token, chain_id: OUTPUT_TOKEN[2:])
        route = _route_data(
            token_address=INPUT_TOKEN[2:],
            exclusive_relayer=EXCLUSIVE_RELAYER[2:],
        )

        decoded = _decode_calldata(adapter.build_deposit_tx(_quote(route_data=route), RECIPIENT[2:]))

        assert decoded[:4] == (RECIPIENT, RECIPIENT, INPUT_TOKEN, OUTPUT_TOKEN)
        assert decoded[7] == EXCLUSIVE_RELAYER
