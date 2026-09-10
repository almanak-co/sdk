"""Native V4 receipt identity retains measured prices across the accounting seam."""

import json
from decimal import Decimal

import pytest
from eth_abi import encode

from almanak.connectors._strategy_base.v4_pool_abi import V4_ZERO_ADDRESS
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import SWAP_EVENT_TOPIC, UniswapV4ReceiptParser
from almanak.framework.accounting.category_handlers.swap_handler import handle_swap
from almanak.framework.data.tokens.defaults import NATIVE_SENTINEL
from tests.unit.framework.accounting.test_swap_accounting import _make_ledger_row, _make_outbox_row, _price_json

USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
KEY = PoolKey(V4_ZERO_ADDRESS, USDC, 3000, 60, V4_ZERO_ADDRESS)


@pytest.mark.parametrize("native_input", [True, False])
@pytest.mark.parametrize("price_key", ["ETH", NATIVE_SENTINEL, f"base:{NATIVE_SENTINEL}"])
def test_native_receipt_to_accounting_preserves_price_and_symbol(native_input, price_key):
    amount0, amount1 = (-(10**15), 2_000_000) if native_input else (10**15, -2_000_000)
    receipt = {
        "status": 1,
        "logs": [
            {
                "address": UNISWAP_V4["base"]["pool_manager"],
                "topics": [SWAP_EVENT_TOPIC, KEY.pool_id, "0x" + "a" * 64],
                "data": "0x"
                + encode(
                    ["int128", "int128", "uint160", "uint128", "int24", "uint24"],
                    [amount0, amount1, 2**96, 100000, 0, 3000],
                ).hex(),
            }
        ],
    }
    amounts = UniswapV4ReceiptParser(chain="base").extract_swap_amounts(receipt, swap_pool_key=KEY.to_wire())
    assert amounts is not None
    native_side, other_side = ("token_in", "token_out") if native_input else ("token_out", "token_in")
    assert getattr(amounts, native_side) == "ETH"
    assert getattr(amounts, f"{native_side}_address") == NATIVE_SENTINEL
    assert getattr(amounts, f"{other_side}_address").lower() == USDC
    assert KEY.currency0 == V4_ZERO_ADDRESS

    ledger = _make_ledger_row(
        token_in=amounts.token_in,
        token_out=amounts.token_out,
        amount_in=str(amounts.amount_in_decimal),
        amount_out=str(amounts.amount_out_decimal),
        protocol="uniswap_v4",
        chain="base",
        price_inputs_json=_price_json({price_key: "2000", "USDC": "1"}),
    )
    ledger["extracted_data_json"] = json.dumps({"swap_amounts": amounts.to_dict()})
    event = handle_swap(_make_outbox_row(), ledger)
    assert event.amount_in_usd == event.amount_out_usd == Decimal("2")
    assert event.price_source == event.price_out_source == "chainlink"
    assert getattr(event, native_side) == "ETH"
