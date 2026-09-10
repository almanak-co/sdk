"""Trader settlement excludes hook rebalances and measures output after hook fees."""

from copy import deepcopy
from decimal import Decimal

import pytest
from eth_abi import encode

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.receipt_parser import SWAP_EVENT_TOPIC, TRANSFER_EVENT_TOPIC, UniswapV4ReceiptParser
from tests.unit.connectors.uniswap_v4.test_doppler_dependencies import KEY, NESTED

WALLET = "0x" + "a" * 40
ROUTER = UNISWAP_V4["robinhood"]["universal_router"].lower()
MANAGER = UNISWAP_V4["robinhood"]["pool_manager"].lower()
META = {
    "token_in": {"address": KEY.currency1, "symbol": "USDG", "decimals": 6},
    "token_out": {"address": KEY.currency0, "symbol": "ROBINLY", "decimals": 18},
}
OPERATION = {
    "schema_version": 2,
    "operation": "swap_exact_in",
    "chain": "robinhood",
    "pool_key": KEY.to_wire(),
    "wallet": WALLET,
    "token_in": KEY.currency1,
    "token_out": KEY.currency0,
    "amount_in": "100",
    "minimum_out": "990",
    "hook_evidence": {
        "operation": "swap_exact_in",
        "route": "universal_router_eoa",
        "hook": KEY.hooks,
        "chain": "robinhood",
    },
}


def swap(sender, amount0, amount1, *, pool_id=KEY.pool_id, emitter=MANAGER):
    return {
        "address": emitter,
        "topics": [SWAP_EVENT_TOPIC, pool_id, "0x" + sender[2:].rjust(64, "0")],
        "data": "0x"
        + encode(
            ["int128", "int128", "uint160", "uint128", "int24", "uint24"], [amount0, amount1, 2**96, 100000, 0, 32100]
        ).hex(),
    }


def transfer(token, source, target, amount):
    return {
        "address": token,
        "topics": [TRANSFER_EVENT_TOPIC, "0x" + source[2:].rjust(64, "0"), "0x" + target[2:].rjust(64, "0")],
        "data": "0x" + encode(["uint256"], [amount]).hex(),
    }


@pytest.fixture
def receipt():
    return {
        "status": 1,
        "from": WALLET,
        "to": ROUTER,
        "logs": [
            swap(ROUTER, 1000, -100),
            transfer(KEY.currency0, MANAGER, NESTED, 3),
            swap(NESTED, -50, 5),
            transfer(KEY.currency0, NESTED, MANAGER, 50),
            transfer(KEY.currency1, MANAGER, NESTED, 5),
            transfer(KEY.currency1, WALLET, MANAGER, 100),
            transfer(KEY.currency0, MANAGER, WALLET, 997),
        ],
    }


def parse(receipt, operation=OPERATION):
    return UniswapV4ReceiptParser(chain="robinhood").parse_receipt(
        receipt, swap_pool_key=KEY.to_wire(), swap_operation=operation, swap_token_meta=META
    )


def test_nested_swap_and_output_fee_use_settled_wallet_amounts(receipt):
    result = parse(receipt)
    assert len(result.swap_events) == 2
    amounts = result.swap_result
    assert (amounts.amount_in, amounts.amount_out) == (100, 997)
    assert amounts.amount_in_decimal == Decimal("0.0001")
    assert amounts.amount_out_decimal == Decimal("0.000000000000000997")
    assert amounts.amount_in_decimal_resolved and amounts.amount_out_decimal_resolved
    assert amounts.token_in == KEY.currency1 and amounts.token_out == KEY.currency0


def test_reordering_nested_events_does_not_attribute_internal_swap_to_user(receipt):
    receipt["logs"].insert(0, receipt["logs"].pop(2))
    assert parse(receipt).swap_result.amount_out == 997


def test_self_transfers_and_hook_only_transfers_do_not_inflate_settlement(receipt):
    receipt["logs"] += [
        transfer(KEY.currency0, WALLET, WALLET, 1000000),
        transfer(KEY.currency0, NESTED, ROUTER, 50000),
    ]
    assert parse(receipt).swap_result.amount_out == 997


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r["logs"].pop(),
        lambda r: r["logs"].__setitem__(-1, transfer(KEY.currency0, MANAGER, WALLET, 989)),
        lambda r: r["logs"].__setitem__(-2, transfer(KEY.currency1, WALLET, MANAGER, 101)),
        lambda r: r["logs"].append(swap(ROUTER, 1000, -100)),
        lambda r: r["logs"].append(swap(NESTED, -10, 1, pool_id="0x" + "1" * 64)),
    ],
)
def test_missing_or_ambiguous_settlement_fails_closed(receipt, change):
    change(receipt)
    with pytest.raises(ValueError):
        parse(receipt)


@pytest.mark.parametrize(
    "field,value",
    [
        ("wallet", ""),
        ("wallet", "0x" + "b" * 40),
        ("chain", "base"),
        ("amount_in", "99"),
        ("minimum_out", 0),
        ("schema_version", True),
    ],
)
def test_operation_identity_and_executable_bounds_are_required(receipt, field, value):
    operation = deepcopy(OPERATION)
    operation[field] = value
    with pytest.raises(ValueError):
        parse(receipt, operation)


def test_gross_swap_cannot_replace_missing_operation_context(receipt):
    with pytest.raises(ValueError, match="bound operation metadata"):
        parse(receipt, None)


def test_spoofed_swap_emitter_cannot_supply_trader_event(receipt):
    receipt["logs"][0]["address"] = NESTED
    with pytest.raises(ValueError, match="ambiguous trader swap"):
        parse(receipt)


def test_parser_owned_enrichment_threads_operation_without_framework_changes(receipt):
    parser = UniswapV4ReceiptParser(chain="robinhood")
    metadata = {"pool_key": KEY.to_wire(), "pool_id": KEY.pool_id, "v4_operation": OPERATION}
    kwargs = parser.build_extract_kwargs(field="swap_amounts", bundle_metadata=metadata)
    assert kwargs["swap_operation"] is OPERATION
    amounts = parser.extract_swap_amounts(receipt, swap_token_meta=META, **kwargs)
    assert (amounts.amount_in, amounts.amount_out) == (100, 997)


@pytest.mark.parametrize("addresses", [None, {}])
def test_absent_router_configuration_is_a_typed_settlement_refusal(monkeypatch, addresses):
    from almanak.connectors.uniswap_v4.swap_settlement import active_hook_swap_settlement

    if addresses is None:
        monkeypatch.delitem(UNISWAP_V4, "robinhood")
    else:
        monkeypatch.setitem(UNISWAP_V4, "robinhood", addresses)
    with pytest.raises(ValueError, match="configured Universal Router"):
        active_hook_swap_settlement(chain="robinhood", key=KEY, operation=OPERATION, swaps=[], transfers=[])


@pytest.mark.parametrize(
    "field,value,message",
    [
        ("wallet", None, "canonical wallet identity"),
        ("wallet", "0x" + "0" * 40, "canonical wallet identity"),
        ("wallet", "0x1234", "canonical wallet identity"),
        ("token_in", None, "token identities are absent"),
        ("token_out", 1, "token identities are absent"),
        ("hook_evidence", None, "bind the selected hook route"),
        ("pool_key", {}, "bind the selected hook route"),
        ("operation", "swap_exact_out", "bind the selected hook route"),
        ("amount_in", True, "positive integral operation bounds"),
        ("minimum_out", "1.5", "positive integral operation bounds"),
    ],
)
def test_malformed_operation_refuses_before_receipt_attribution(receipt, field, value, message):
    from almanak.connectors.uniswap_v4.swap_settlement import active_hook_swap_settlement

    operation = deepcopy(OPERATION)
    operation[field] = value
    parsed = parse(receipt)
    with pytest.raises(ValueError, match=message):
        active_hook_swap_settlement(
            chain="robinhood",
            key=KEY,
            operation=operation,
            swaps=parsed.swap_events,
            transfers=parsed.transfer_events,
        )


def test_active_hook_native_currency_requires_balance_evidence():
    from dataclasses import replace

    from almanak.connectors.uniswap_v4.swap_settlement import active_hook_swap_settlement

    key = replace(KEY, currency0="0x" + "0" * 40)
    operation = deepcopy(OPERATION)
    operation.update(pool_key=key.to_wire(), token_out=key.currency0)
    with pytest.raises(ValueError, match="separate native balance evidence"):
        active_hook_swap_settlement(chain="robinhood", key=key, operation=operation, swaps=[], transfers=[])


def test_operation_refusal_precedes_missing_router_configuration(monkeypatch):
    from almanak.connectors.uniswap_v4.swap_settlement import active_hook_swap_settlement

    monkeypatch.delitem(UNISWAP_V4, "robinhood")
    with pytest.raises(ValueError, match="bound operation metadata"):
        active_hook_swap_settlement(chain="robinhood", key=KEY, operation=None, swaps=[], transfers=[])


def test_receipt_direction_cannot_override_bound_operation(receipt):
    receipt["logs"][0] = swap(ROUTER, -1000, 100)
    with pytest.raises(ValueError, match="direction conflicts with operation identity"):
        parse(receipt)
