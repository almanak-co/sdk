"""Replay the measured Base LP lifecycle through independent persistence surfaces."""

import copy
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import MODIFY_LIQUIDITY_TOPIC, UniswapV4ReceiptParser
from almanak.framework.intents import Intent
from almanak.framework.observability.position_events import build_position_event_from_intent
from almanak.framework.runner.strategy_runner import StrategyRunner

FIXTURE = json.loads((Path(__file__).parents[3] / "fixtures/execution/base_v4_lp_lifecycle.json").read_text())
KEY = PoolKey(**FIXTURE["pool_key"])


def lookup(pool_id, chain):
    assert pool_id == KEY.pool_id
    assert chain == "base"
    return KEY


def test_registry_parser_receives_gateway_lookup_and_retains_actual_close_legs():
    runner = SimpleNamespace(_extract_receipt_from_result=lambda result: result, _build_pool_key_lookup=lambda: lookup)
    receipt, parser = StrategyRunner._registry_resolve_receipt_and_parser(
        runner, result=FIXTURE["close_receipt"], chain="base", intent_type_str="LP_CLOSE", protocol="uniswap_v4"
    )
    opened = parser.extract_registry_payload_open(FIXTURE["open_receipt"], fee_tier=500)
    closed = parser.extract_registry_payload_close(receipt, open_payload=opened)
    assert closed is not None
    assert closed["token_id"] == "3026126"
    assert closed["amount0_close"] == "529769045987132"
    assert closed["amount1_close"] == "1476189"
    assert (closed["currency0"], closed["currency1"]) == (KEY.currency0.lower(), KEY.currency1.lower())
    open_data = parser.extract_lp_open_data(FIXTURE["open_receipt"])
    close_data = parser.extract_lp_close_data(receipt)
    assert close_data.position_hash == open_data.position_hash
    assert close_data.to_dict()["position_hash"] == open_data.position_hash
    assert close_data.position_id == "3026126"


@pytest.mark.parametrize("mutation", ["token_id", "pool_id", "position_manager", "tick_lower"])
def test_registry_close_rejects_unrelated_open_identity(mutation):
    parser = UniswapV4ReceiptParser(chain="base", pool_key_lookup=lookup)
    opened = parser.extract_registry_payload_open(FIXTURE["open_receipt"])
    opened[mutation] = {
        "token_id": "3026127",
        "pool_id": "0x" + "11" * 32,
        "position_manager": "0x" + "11" * 20,
        "tick_lower": 0,
    }[mutation]
    assert parser.extract_registry_payload_close(FIXTURE["close_receipt"], open_payload=opened) is None


def test_forged_modify_liquidity_emitter_cannot_supply_close_identity():
    parser = UniswapV4ReceiptParser(chain="base", pool_key_lookup=lookup)
    receipt = copy.deepcopy(FIXTURE["close_receipt"])
    for log in receipt["logs"]:
        if log["topics"][0].lower() == MODIFY_LIQUIDITY_TOPIC.lower():
            log["address"] = "0x" + "11" * 20
    assert parser.extract_lp_close_data(receipt) is None


def test_pool_id_only_close_event_prices_parser_currencies_without_open_cache():
    parser = UniswapV4ReceiptParser(chain="base", pool_key_lookup=lookup)
    data = parser.extract_lp_close_data(FIXTURE["close_receipt"])
    result = SimpleNamespace(
        success=True,
        extracted_data={"lp_close_data": data},
        transaction_results=[],
        tx_hash=FIXTURE["close_receipt"]["transactionHash"],
    )
    event = build_position_event_from_intent(
        deployment_id="replay",
        intent=Intent.lp_close(position_id="3026126", pool=KEY.pool_id, protocol="uniswap_v4"),
        result=result,
        chain="base",
        price_oracle={"WETH": Decimal(2500), "USDC": Decimal(1)},
    )
    assert event is not None
    assert (event.token0, event.token1) == ("WETH", "USDC")
    assert Decimal(event.value_usd) == Decimal("0.000529769045987132") * 2500 + Decimal("1.476189")


def test_measured_close_identity_survives_canonical_accounting_serialization():
    from almanak.framework.accounting.category_handlers.lp_handler import handle_lp
    from almanak.framework.observability.ledger import serialize_extracted_data

    parser = UniswapV4ReceiptParser(chain="base", pool_key_lookup=lookup)
    close_data = parser.extract_lp_close_data(FIXTURE["close_receipt"])
    wallet = FIXTURE["close_receipt"]["from"]
    identity = f"lp:uniswap_v4:base:{wallet}:{KEY.pool_id}"
    outbox = {
        "deployment_id": "replay",
        "cycle_id": "close",
        "position_key": identity,
        "event_type": "LP_CLOSE",
        "chain": "base",
        "protocol": "uniswap_v4",
        "wallet_address": wallet,
        "execution_mode": "paper",
    }
    ledger = {
        "id": "close",
        "deployment_id": "replay",
        "cycle_id": "close",
        "intent_type": "LP_CLOSE",
        "protocol": "uniswap_v4",
        "chain": "base",
        "execution_mode": "paper",
        "tx_hash": FIXTURE["close_receipt"]["transactionHash"],
        "token_in": "WETH",
        "token_out": "USDC",
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-09-08T17:16:37+00:00",
        "extracted_data_json": serialize_extracted_data({"lp_close_data": close_data}),
        "price_inputs_json": "{}",
    }
    event = handle_lp(outbox, ledger)
    assert event is not None
    assert event.position_hash == close_data.position_hash


def test_multiple_nft_burns_cannot_be_attributed_to_one_close():
    parser = UniswapV4ReceiptParser(chain="base", pool_key_lookup=lookup)
    receipt = copy.deepcopy(FIXTURE["close_receipt"])
    burn = next(log for log in receipt["logs"] if log["topics"][0].lower() == MODIFY_LIQUIDITY_TOPIC.lower())
    receipt["logs"].append(copy.deepcopy(burn))
    assert parser.extract_lp_close_data(receipt) is None
