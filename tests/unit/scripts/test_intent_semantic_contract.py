"""Scientific controls for independently derived Intent semantic contracts."""

from __future__ import annotations

from copy import deepcopy

import pytest
from eth_utils import keccak

from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES
from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS
from almanak.connectors.traderjoe_v2.addresses import TRADERJOE_V2, TRADERJOE_V2_LBPAIRS, TRADERJOE_V2_TOKENS
from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
from almanak.connectors.uniswap_v3.sdk import compute_pool_address
from qa_lab.intent_semantic_contract import validate_semantic_contract

ACCOUNT = "0x" + "11" * 20
ASSET = "0x" + "22" * 20
VAULT = "0x" + "33" * 20
AMOUNT = 1_000_000


def _topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def _address_topic(address: str) -> str:
    return "0x" + address.removeprefix("0x").rjust(64, "0")


def _word(value: int) -> str:
    return f"{value:064x}"


def _signed_word(value: int) -> str:
    return _word(value if value >= 0 else (1 << 256) + value)


def _supply_payload() -> dict:
    return {
        "chain": "ethereum",
        "protocol": "euler_v2",
        "intent": "SUPPLY",
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "SUPPLY",
            "asset_reference": ASSET,
            "amount": "1",
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "lending.v1",
            "intent": "SUPPLY",
            "account": ACCOUNT,
            "asset_address": ASSET,
            "asset_decimals": 6,
            "resource_address": VAULT,
            "requested_amount_raw": AMOUNT,
            "wallet_before_raw": 5_000_000,
            "wallet_after_raw": 4_000_000,
            "position_before": "0",
            "position_after": "999999",
            "parser_amount_raw": AMOUNT,
        },
        "raw_receipt": {
            "logs": [
                {
                    "address": VAULT,
                    "topics": [
                        _topic("Deposit(address,address,uint256,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(ACCOUNT),
                    ],
                    "data": "0x" + _word(AMOUNT) + _word(999999),
                },
                {
                    "address": ASSET,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(VAULT),
                    ],
                    "data": "0x" + _word(AMOUNT),
                },
            ]
        },
    }


def test_lending_contract_is_rederived_from_raw_value_flow() -> None:
    result = validate_semantic_contract(_supply_payload(), expected_profile="lending.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["wallet_delta_raw"] == str(-AMOUNT)
    assert all(result["checks"].values())


def test_compiler_observed_symbol_is_resolved_from_committed_chain_registry() -> None:
    payload = _supply_payload()
    base_usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    payload["chain"] = "base"
    payload["source_request"]["asset_reference"] = "USDC"
    payload["semantic_contract"]["asset_address"] = base_usdc
    payload["raw_receipt"]["logs"][1]["address"] = base_usdc

    assert validate_semantic_contract(payload, expected_profile="lending.v1")["status"] == "VERIFIED"


def test_compiler_observed_symbol_cannot_be_relabelled_as_another_asset() -> None:
    payload = _supply_payload()
    payload["chain"] = "base"
    payload["source_request"]["asset_reference"] = "WETH"
    payload["semantic_contract"]["asset_address"] = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"

    with pytest.raises(ValueError, match="asset does not match"):
        validate_semantic_contract(payload, expected_profile="lending.v1")


@pytest.mark.parametrize(
    ("intent", "signature", "amount_word", "wallet_direction", "position_after"),
    [
        ("SUPPLY", "Supply(address,address,address,uint256,uint16)", 1, "out", "2"),
        ("WITHDRAW", "Withdraw(address,address,address,uint256)", 0, "in", "0"),
        ("BORROW", "Borrow(address,address,address,uint256,uint8,uint256,uint16)", 1, "in", "2"),
        ("REPAY", "Repay(address,address,address,uint256,bool)", 0, "out", "0"),
    ],
)
def test_all_aave_lending_actions_have_independent_event_contracts(
    intent: str,
    signature: str,
    amount_word: int,
    wallet_direction: str,
    position_after: str,
) -> None:
    pool = AAVE_V3_POOL_ADDRESSES["base"]
    assert _topic(signature) == EVENT_TOPICS[intent.title()]
    wallet_before = 5_000_000
    wallet_after = wallet_before + (AMOUNT if wallet_direction == "in" else -AMOUNT)
    data_words = [0, 0, 0, 0, 0]
    data_words[amount_word] = AMOUNT
    payload = {
        "chain": "base",
        "protocol": "aave_v3",
        "intent": intent,
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": intent,
            "asset_reference": "USDC",
            "amount": "1",
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "lending.v1",
            "intent": intent,
            "account": ACCOUNT,
            "asset_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "asset_decimals": 6,
            "resource_address": pool,
            "requested_amount_raw": AMOUNT,
            "wallet_before_raw": wallet_before,
            "wallet_after_raw": wallet_after,
            "position_before": "1",
            "position_after": position_after,
            "parser_amount_raw": AMOUNT,
        },
        "raw_receipt": {
            "logs": [
                {
                    "address": pool,
                    "topics": [
                        _topic(signature),
                        _address_topic("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
                        _address_topic(ACCOUNT),
                        _address_topic(ACCOUNT),
                    ],
                    "data": "0x" + "".join(_word(value) for value in data_words),
                },
                {
                    "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT if wallet_direction == "out" else pool),
                        _address_topic(ACCOUNT if wallet_direction == "in" else pool),
                    ],
                    "data": "0x" + _word(AMOUNT),
                },
            ]
        },
    }

    result = validate_semantic_contract(payload, expected_profile="lending.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["event_signature"] == signature


def test_aave_event_for_a_different_reserve_cannot_prove_the_claim() -> None:
    payload = _supply_payload()
    payload["chain"] = "base"
    payload["protocol"] = "aave_v3"
    payload["source_request"]["asset_reference"] = "USDC"
    payload["semantic_contract"]["asset_address"] = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    payload["semantic_contract"]["resource_address"] = AAVE_V3_POOL_ADDRESSES["base"]
    payload["raw_receipt"]["logs"][0] = {
        "address": AAVE_V3_POOL_ADDRESSES["base"],
        "topics": [
            _topic("Supply(address,address,address,uint256,uint16)"),
            _address_topic("0x" + "77" * 20),
            _address_topic(ACCOUNT),
        ],
        "data": "0x" + _word(0) + _word(AMOUNT) + _word(0),
    }
    payload["raw_receipt"]["logs"][1]["address"] = payload["semantic_contract"]["asset_address"]

    with pytest.raises(ValueError, match="found 0"):
        validate_semantic_contract(payload, expected_profile="lending.v1")


def test_aave_lending_contract_rejects_noncanonical_pool_emitter() -> None:
    payload = _supply_payload()
    payload["chain"] = "base"
    payload["protocol"] = "aave_v3"
    payload["source_request"]["asset_reference"] = "USDC"
    payload["semantic_contract"]["asset_address"] = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    payload["raw_receipt"]["logs"][1]["address"] = payload["semantic_contract"]["asset_address"]

    with pytest.raises(ValueError, match="committed Aave V3 Pool"):
        validate_semantic_contract(payload, expected_profile="lending.v1")


def _swap_payload() -> dict:
    usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    weth = "0x4200000000000000000000000000000000000006"
    factory = UNISWAP_V3["base"]["factory"]
    pool = compute_pool_address(factory, usdc, weth, 500)
    output = 5_000_000_000_000_000
    return {
        "chain": "base",
        "protocol": "uniswap_v3",
        "intent": "SWAP",
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "SWAP",
            "asset_reference": usdc,
            "target_asset_reference": weth,
            "amount": "1",
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "swap.v1",
            "intent": "SWAP",
            "account": ACCOUNT,
            "asset_address": usdc,
            "asset_decimals": 6,
            "output_asset_address": weth,
            "output_asset_decimals": 18,
            "resource_address": pool,
            "factory_address": factory,
            "fee_tier": 500,
            "requested_amount_raw": AMOUNT,
            "wallet_before_raw": 5_000_000,
            "wallet_after_raw": 4_000_000,
            "output_wallet_before_raw": 0,
            "output_wallet_after_raw": output,
            "parser_amount_raw": AMOUNT,
            "parser_output_amount_raw": output,
        },
        "raw_receipt": {
            "logs": [
                {
                    "address": pool,
                    "topics": [
                        _topic("Swap(address,address,int256,int256,uint160,uint128,int24)"),
                        _address_topic(VAULT),
                        _address_topic(ACCOUNT),
                    ],
                    "data": "0x" + _signed_word(-output) + _signed_word(AMOUNT) + _word(1) + _word(1) + _word(0),
                },
                {
                    "address": usdc,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(pool),
                    ],
                    "data": "0x" + _word(AMOUNT),
                },
                {
                    "address": weth,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(pool),
                        _address_topic(ACCOUNT),
                    ],
                    "data": "0x" + _word(output),
                },
            ]
        },
    }


def test_uniswap_swap_contract_is_rederived_from_pool_and_bilateral_flow() -> None:
    result = validate_semantic_contract(_swap_payload(), expected_profile="swap.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["fee_tier"] == 500
    assert all(result["checks"].values())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["semantic_contract"].update(resource_address="0x" + "77" * 20), "deterministic"),
        (lambda p: p["semantic_contract"].update(factory_address="0x" + "77" * 20), "factory"),
        (lambda p: p["semantic_contract"].update(parser_output_amount_raw=1), "output flow"),
        (lambda p: p["source_request"].update(target_asset_reference=ASSET), "output asset"),
        (lambda p: p["raw_receipt"]["logs"].pop(0), "authoritative Swap"),
    ],
    ids=("pool", "factory", "parser-output", "target-asset", "event"),
)
def test_uniswap_swap_contract_mutations_cannot_inherit_green(mutation, message: str) -> None:
    payload = _swap_payload()
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="swap.v1")


def _traderjoe_swap_payload() -> dict:
    token_in = TRADERJOE_V2_TOKENS["avalanche"]["WAVAX"]
    token_out = TRADERJOE_V2_TOKENS["avalanche"]["USDT"]
    factory = TRADERJOE_V2["avalanche"]["factory"]
    router = TRADERJOE_V2["avalanche"]["router"]
    pair = next(
        str(row["address"])
        for row in TRADERJOE_V2_LBPAIRS["avalanche"]
        if row["tokenX"] == "WAVAX" and row["tokenY"] == "USDT" and row["bin_step"] == 20
    )
    amount_in, amount_out = 10**16, 250_000
    calldata = (
        "0x704037bd"
        + token_in.removeprefix("0x").lower().zfill(64)
        + token_out.removeprefix("0x").lower().zfill(64)
        + _word(20)
    )
    factory_result = "0x" + _word(20) + pair.removeprefix("0x").lower().zfill(64) + _word(0) + _word(0)
    return {
        "chain": "avalanche",
        "protocol": "traderjoe_v2",
        "intent": "SWAP",
        "tx": {"to": router},
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "SWAP",
            "asset_reference": token_in,
            "target_asset_reference": token_out,
            "amount": "0.01",
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "liquidity_book_swap.v1",
            "intent": "SWAP",
            "account": ACCOUNT,
            "asset_address": token_in,
            "asset_decimals": 18,
            "output_asset_address": token_out,
            "output_asset_decimals": 6,
            "resource_address": pair,
            "factory_address": factory,
            "router_address": router,
            "bin_step": 20,
            "requested_amount_raw": amount_in,
            "wallet_before_raw": amount_in,
            "wallet_after_raw": 0,
            "output_wallet_before_raw": 0,
            "output_wallet_after_raw": amount_out,
            "parser_amount_raw": amount_in,
            "parser_output_amount_raw": amount_out,
            "factory_witness": {
                "block_number": 101,
                "block_hash": "0x" + "aa" * 32,
                "to": factory,
                "calldata": calldata,
                "raw_result": factory_result,
            },
        },
        "raw_receipt": {
            "blockNumber": 100,
            "logs": [
                {
                    "address": token_in,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(pair),
                    ],
                    "data": "0x" + _word(amount_in),
                },
                {
                    "address": token_out,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(pair),
                        _address_topic(ACCOUNT),
                    ],
                    "data": "0x" + _word(amount_out),
                },
            ],
        },
    }


def test_traderjoe_swap_contract_rederives_factory_pair_and_bilateral_flow() -> None:
    result = validate_semantic_contract(_traderjoe_swap_payload(), expected_profile="liquidity_book_swap.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["bin_step"] == 20
    assert all(result["checks"].values())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["semantic_contract"].update(resource_address="0x" + "77" * 20), "factory response"),
        (lambda p: p["semantic_contract"].update(router_address="0x" + "77" * 20), "factory/router"),
        (lambda p: p["semantic_contract"]["factory_witness"].update(raw_result="0x"), "factory response"),
        (lambda p: p["semantic_contract"].update(parser_output_amount_raw=1), "bilateral"),
        (lambda p: p["raw_receipt"]["logs"].pop(), "bilateral ERC-20"),
        (
            lambda p: p["raw_receipt"]["logs"][0]["topics"].__setitem__(2, _address_topic("0x" + "77" * 20)),
            "bilateral ERC-20",
        ),
    ],
)
def test_traderjoe_swap_contract_mutations_fail_closed(mutation, message: str) -> None:
    payload = _traderjoe_swap_payload()
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="liquidity_book_swap.v1")


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["semantic_contract"].update(wallet_after_raw=4_000_001), "wallet delta"),
        (lambda p: p["semantic_contract"].update(parser_amount_raw=AMOUNT - 1), "parser amount"),
        (lambda p: p["source_request"].update(amount="2"), "raw amount"),
        (
            lambda p: p["source_request"].update(asset_reference="0x" + "77" * 20),
            "asset does not match",
        ),
        (lambda p: p["semantic_contract"].update(position_after="0"), "position did not increase"),
        (lambda p: p["semantic_contract"].update(resource_address="0x" + "44" * 20), "found 0"),
        (lambda p: p["semantic_contract"].update(account="0x" + "55" * 20), "account is absent"),
        (
            lambda p: p["semantic_contract"].update(asset_address="0x" + "66" * 20),
            "asset does not match",
        ),
        (lambda p: p["raw_receipt"]["logs"].pop(0), "found 0"),
    ],
    ids=(
        "wallet",
        "parser",
        "source-amount",
        "source-asset",
        "position",
        "resource",
        "account",
        "asset",
        "event-removed",
    ),
)
def test_lending_contract_mutations_cannot_inherit_green(mutation, message: str) -> None:
    payload = deepcopy(_supply_payload())
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="lending.v1")


def _v3_lp_open_payload() -> dict:
    token0 = "0x4200000000000000000000000000000000000006"
    token1 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    factory = UNISWAP_V3["base"]["factory"]
    npm = UNISWAP_V3["base"]["position_manager"]
    pool = compute_pool_address(factory, token0, token1, 500)
    token_id = 42
    liquidity = 987654321
    amount0 = 10**15
    amount1 = 10**6
    block_hash = "0x" + "ab" * 32
    position_words = [0, 0, int(token0, 16), int(token1, 16), 500, (1 << 256) - 100, 100, liquidity, 0, 0, 0, 0]
    return {
        "chain": "base",
        "protocol": "uniswap_v3",
        "intent": "LP_OPEN",
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "LP_OPEN",
            "pool_reference": pool,
            "amount0": "0.001",
            "amount1": "1",
            "range_lower": "1000",
            "range_upper": "3000",
            "fee_tier_units": 500,
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "v3_lp.v1",
            "intent": "LP_OPEN",
            "account": ACCOUNT,
            "pool_reference": pool,
            "amount0": "0.001",
            "amount1": "1",
            "range_lower": "1000",
            "range_upper": "3000",
            "resource_address": npm,
            "factory_address": factory,
            "pool_address": pool,
            "token0": token0,
            "token1": token1,
            "fee_tier": 500,
            "position_id": token_id,
            "tick_lower": -100,
            "tick_upper": 100,
            "liquidity": liquidity,
            "max_amount0_raw": amount0,
            "max_amount1_raw": amount1,
            "actual_amount0_raw": amount0,
            "actual_amount1_raw": amount1,
            "parser_position_id": token_id,
            "parser_liquidity": liquidity,
            "parser_amount0_raw": amount0,
            "parser_amount1_raw": amount1,
            "position_state_raw": "0x" + "".join(_word(value) for value in position_words),
            "owner_state_raw": _address_topic(ACCOUNT),
            "position_state_block": 123,
            "position_state_block_hash": block_hash,
        },
        "raw_receipt": {
            "blockNumber": 123,
            "blockHash": block_hash,
            "logs": [
                {
                    "address": npm,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic("0x" + "0" * 40),
                        _address_topic(ACCOUNT),
                        "0x" + _word(token_id),
                    ],
                    "data": "0x",
                },
                {
                    "address": npm,
                    "topics": [_topic("IncreaseLiquidity(uint256,uint128,uint256,uint256)"), "0x" + _word(token_id)],
                    "data": "0x" + _word(liquidity) + _word(amount0) + _word(amount1),
                },
                {
                    "address": token0,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(npm),
                    ],
                    "data": "0x" + _word(amount0),
                },
                {
                    "address": token1,
                    "topics": [
                        _topic("Transfer(address,address,uint256)"),
                        _address_topic(ACCOUNT),
                        _address_topic(npm),
                    ],
                    "data": "0x" + _word(amount1),
                },
            ],
        },
    }


def test_uniswap_v3_lp_open_contract_rederives_nft_pool_state_and_bilateral_flow() -> None:
    result = validate_semantic_contract(_v3_lp_open_payload(), expected_profile="v3_lp.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["position_id"] == "42"
    assert all(result["checks"].values())


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda p: p["source_request"].update(amount0="0.002"), "amount0"),
        (lambda p: p["semantic_contract"].update(pool_address="0x" + "77" * 20), "deterministic"),
        (lambda p: p["semantic_contract"].update(resource_address="0x" + "77" * 20), "committed registry"),
        (lambda p: p["semantic_contract"].update(position_id=43), "one mint"),
        (lambda p: p["semantic_contract"].update(parser_liquidity=1), "parser liquidity"),
        (lambda p: p["semantic_contract"].update(position_state_block=122), "receipt block"),
        (lambda p: p["raw_receipt"]["logs"].pop(), "bilateral token outflow"),
    ],
    ids=("source", "pool", "npm", "nft", "parser", "block", "transfer"),
)
def test_uniswap_v3_lp_open_mutations_cannot_inherit_green(mutation, message: str) -> None:
    payload = _v3_lp_open_payload()
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="v3_lp.v1")


def _v3_lp_close_payloads() -> dict[str, dict]:
    token0 = "0x4200000000000000000000000000000000000006"
    token1 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    factory = UNISWAP_V3["base"]["factory"]
    npm = UNISWAP_V3["base"]["position_manager"]
    pool = compute_pool_address(factory, token0, token1, 500)
    token_id = 42
    liquidity = 987654321
    amount0 = 10**15
    amount1 = 10**6
    receipt_set = {"decrease": "0x" + "11" * 32, "collect": "0x" + "22" * 32, "burn": "0x" + "33" * 32}
    position_words = [0, 0, int(token0, 16), int(token1, 16), 500, (1 << 256) - 100, 100, liquidity, 0, 0, 0, 0]
    compiled_calls = [
        {
            "to": npm,
            "data": "0x0c49ccbe" + _word(token_id) + _word(liquidity) + _word(0) + _word(0) + _word(9999999999),
            "value": 0,
            "tx_type": "lp_decrease_liquidity",
        },
        {
            "to": npm,
            "data": "0xfc6f7865" + _word(token_id) + _word(int(ACCOUNT, 16)) + _word(2**128 - 1) + _word(2**128 - 1),
            "value": 0,
            "tx_type": "lp_collect",
        },
        {"to": npm, "data": "0x42966c68" + _word(token_id), "value": 0, "tx_type": "lp_burn"},
    ]
    common = {
        "schema_version": 1,
        "profile": "v3_lp.v1",
        "intent": "LP_CLOSE",
        "account": ACCOUNT,
        "pool_reference": pool,
        "resource_address": npm,
        "factory_address": factory,
        "pool_address": pool,
        "token0": token0,
        "token1": token1,
        "fee_tier": 500,
        "position_id": token_id,
        "pre_liquidity": liquidity,
        "pre_position_state_raw": "0x" + "".join(_word(value) for value in position_words),
        "pre_owner_state_raw": _address_topic(ACCOUNT),
        "pre_state_block": 123,
        "pre_state_block_hash": "0x" + "aa" * 32,
        "compiled_calls": compiled_calls,
        "receipt_set": receipt_set,
        "parser_liquidity_removed": liquidity,
        "parser_amount0_raw": amount0,
        "parser_amount1_raw": amount1,
        "actual_amount0_raw": amount0,
        "actual_amount1_raw": amount1,
        "terminal_position_response": {"jsonrpc": "2.0", "id": 1, "error": {"code": 3, "message": "revert"}},
        "terminal_owner_response": {"jsonrpc": "2.0", "id": 2, "error": {"code": 3, "message": "revert"}},
        "terminal_state_block": 126,
        "terminal_state_block_hash": "0x" + "cc" * 32,
    }
    source = {
        "schema_version": 1,
        "captured_by": "compiler_observer",
        "intent": "LP_CLOSE",
        "pool_reference": pool,
        "position_id": str(token_id),
        "collect_fees": True,
    }
    logs = {
        "decrease": [
            {
                "address": npm,
                "topics": [_topic("DecreaseLiquidity(uint256,uint128,uint256,uint256)"), "0x" + _word(token_id)],
                "data": "0x" + _word(liquidity) + _word(amount0) + _word(amount1),
            }
        ],
        "collect": [
            {
                "address": pool,
                "topics": [
                    _topic("Collect(address,address,int24,int24,uint128,uint128)"),
                    _address_topic(npm),
                    "0x" + _word(0),
                    "0x" + _word(0),
                ],
                "data": "0x" + _word(int(ACCOUNT, 16)) + _word(amount0) + _word(amount1),
            },
            {
                "address": token0,
                "topics": [_topic("Transfer(address,address,uint256)"), _address_topic(pool), _address_topic(ACCOUNT)],
                "data": "0x" + _word(amount0),
            },
            {
                "address": token1,
                "topics": [_topic("Transfer(address,address,uint256)"), _address_topic(pool), _address_topic(ACCOUNT)],
                "data": "0x" + _word(amount1),
            },
        ],
        "burn": [
            {
                "address": npm,
                "topics": [
                    _topic("Transfer(address,address,uint256)"),
                    _address_topic(ACCOUNT),
                    _address_topic("0x" + "0" * 40),
                    "0x" + _word(token_id),
                ],
                "data": "0x",
            }
        ],
    }
    blocks = {"decrease": (124, "0x" + "ab" * 32), "collect": (125, "0x" + "bb" * 32), "burn": (126, "0x" + "cc" * 32)}
    return {
        role: {
            "chain": "base",
            "protocol": "uniswap_v3",
            "intent": "LP_CLOSE",
            "receipt_role": role,
            "source_request": deepcopy(source),
            "semantic_contract": {**deepcopy(common), "receipt_role_name": role},
            "raw_receipt": {
                "transactionHash": receipt_set[role],
                "blockNumber": blocks[role][0],
                "blockHash": blocks[role][1],
                "logs": logs[role],
            },
        }
        for role in ("decrease", "collect", "burn")
    }


@pytest.mark.parametrize("role", ["decrease", "collect", "burn"])
def test_uniswap_v3_lp_close_contract_rederives_every_target_receipt(role: str) -> None:
    result = validate_semantic_contract(_v3_lp_close_payloads()[role], expected_profile="v3_lp.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["receipt_role"] == role
    assert all(result["checks"].values())


@pytest.mark.parametrize(
    ("role", "mutation", "message"),
    [
        (
            "decrease",
            lambda p: p["semantic_contract"]["compiled_calls"][0].update(
                data="0x0c49ccbe" + _word(42) + _word(1) + _word(0) + _word(0) + _word(9)
            ),
            "pre-state liquidity",
        ),
        (
            "decrease",
            lambda p: p["semantic_contract"]["receipt_set"].update(decrease="0x" + "99" * 32),
            "raw transaction",
        ),
        ("collect", lambda p: p["raw_receipt"]["logs"].pop(), "wallet inflows"),
        (
            "burn",
            lambda p: p["semantic_contract"].update(terminal_owner_response={"result": "0x" + "00" * 32}),
            "absent position",
        ),
        ("burn", lambda p: p["semantic_contract"].update(pool_address="0x" + "77" * 20), "deterministic"),
    ],
    ids=("compiled-liquidity", "receipt-role", "bilateral-inflow", "terminal-state", "pool-identity"),
)
def test_uniswap_v3_lp_close_mutations_cannot_inherit_green(role, mutation, message: str) -> None:
    payload = _v3_lp_close_payloads()[role]
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="v3_lp.v1")


# ---------------------------------------------------------------------------
# Solidly (classic Aerodrome) fungible LP — solidly_lp.v1
# ---------------------------------------------------------------------------

_SOLIDLY_POOL = "0x" + "77" * 20
_SOLIDLY_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_SOLIDLY_WETH = "0x4200000000000000000000000000000000000006"
_SOLIDLY_USDC_RAW = 10_000_000
_SOLIDLY_WETH_RAW = 4_594_437_930_502_768
_SOLIDLY_LP_RAW = 209_795_168_350


def _solidly_payload() -> dict:
    from almanak.connectors.aerodrome.addresses import AERODROME

    router = AERODROME["base"]["router"]
    transfer = _topic("Transfer(address,address,uint256)")
    # Canonical pool order: WETH (0x4200…) sorts before USDC (0x8335…).
    token0, token1 = _SOLIDLY_WETH, _SOLIDLY_USDC
    amount0, amount1 = _SOLIDLY_WETH_RAW, _SOLIDLY_USDC_RAW
    return {
        "chain": "base",
        "protocol": "aerodrome",
        "intent": "LP_OPEN",
        "source_request": {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": "LP_OPEN",
            "pool_reference": "USDC/WETH/volatile",
            "amount0": "10",
            "amount1": "0.005",
        },
        "raw_receipt": {
            "block_number": 50190524,
            "logs": [
                {
                    "address": _SOLIDLY_POOL,
                    "topics": [_topic("Mint(address,uint256,uint256)"), _address_topic(router)],
                    "data": "0x" + _word(amount0) + _word(amount1),
                },
                {
                    "address": _SOLIDLY_POOL,
                    "topics": [transfer, _address_topic("0x" + "0" * 40), _address_topic(ACCOUNT)],
                    "data": "0x" + _word(_SOLIDLY_LP_RAW),
                },
                {
                    "address": token0,
                    "topics": [transfer, _address_topic(ACCOUNT), _address_topic(_SOLIDLY_POOL)],
                    "data": "0x" + _word(amount0),
                },
                {
                    "address": token1,
                    "topics": [transfer, _address_topic(ACCOUNT), _address_topic(_SOLIDLY_POOL)],
                    "data": "0x" + _word(amount1),
                },
            ],
        },
        "semantic_contract": {
            "schema_version": 1,
            "profile": "solidly_lp.v1",
            "intent": "LP_OPEN",
            "account": ACCOUNT,
            "pool_reference": "USDC/WETH/volatile",
            "pool_address": _SOLIDLY_POOL,
            "resource_address": router,
            "token0": token0,
            "token1": token1,
            "amount0": "10",
            "amount1": "0.005",
            "max_amount0_raw": 5_000_000_000_000_000,
            "max_amount1_raw": 10_000_000,
            "actual_amount0_raw": amount0,
            "actual_amount1_raw": amount1,
            "parser_amount0_raw": amount0,
            "parser_amount1_raw": amount1,
            "lp_tokens_minted": _SOLIDLY_LP_RAW,
        },
    }


def test_solidly_lp_contract_is_rederived_from_the_pool_mint_and_wallet_flow() -> None:
    result = validate_semantic_contract(_solidly_payload(), expected_profile="solidly_lp.v1")

    assert result["status"] == "VERIFIED"
    assert result["profile"] == "solidly_lp.v1"
    assert all(result["checks"].values())
    assert result["facts"]["event_signature"] == "Mint(address,uint256,uint256)"


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"actual_amount1_raw": _SOLIDLY_USDC_RAW + 1}, "exceeds the compiler-observed maximum"),
        ({"resource_address": "0x" + "11" * 20}, "router differs from the committed registry"),
        ({"pool_address": "0x" + "22" * 20}, "exactly one authoritative pool Mint"),
        ({"token0": _SOLIDLY_USDC, "token1": _SOLIDLY_WETH}, "token order is not canonical"),
        ({"parser_amount0_raw": 1}, "parser amounts differ from the authoritative Mint event"),
        ({"lp_tokens_minted": _SOLIDLY_LP_RAW + 1}, "exactly one LP balance to the wallet"),
        ({"pool_reference": "USDC/WETH/stable"}, "differs from the compiler-observed request"),
    ],
)
def test_solidly_lp_contract_refuses_every_tampered_claim(mutation: dict, expected: str) -> None:
    """Liveness: a validator that only ever verifies proves nothing."""
    payload = _solidly_payload()
    payload["semantic_contract"].update(mutation)

    with pytest.raises(ValueError, match=expected):
        validate_semantic_contract(payload, expected_profile="solidly_lp.v1")


def test_solidly_lp_profile_is_refused_for_a_protocol_it_cannot_rederive() -> None:
    payload = _solidly_payload()
    payload["protocol"] = "curve"

    with pytest.raises(ValueError, match="No authoritative Solidly LP event contract"):
        validate_semantic_contract(payload, expected_profile="solidly_lp.v1")


# ---------------------------------------------------------------------------
# Harness discrimination vectors (2026-08-28 adversarial-review round)
# ---------------------------------------------------------------------------


def test_a_zero_delta_wallet_cannot_inherit_green() -> None:
    """A run in which nothing moved must not verify.

    The classic vacuous green: every recorded balance identical before and
    after, a plausible receipt attached. The wallet-flow re-derivation must
    refuse on the delta, not average it away. (Distinct from the ``wallet``
    mutation above, which records a wrong-but-nonzero delta.)
    """
    payload = deepcopy(_supply_payload())
    payload["semantic_contract"]["wallet_after_raw"] = payload["semantic_contract"]["wallet_before_raw"]

    with pytest.raises(ValueError, match="wallet delta"):
        validate_semantic_contract(payload, expected_profile="lending.v1")


def test_an_approval_only_receipt_cannot_prove_a_supply() -> None:
    """A status=1 receipt full of plausible ERC-20 noise proves nothing.

    The dangerous shape is not an empty receipt (the ``event-removed`` mutation)
    but a SUCCESSFUL transaction whose logs are all approvals and transfers on
    the right token with no protocol event at the committed resource. The
    protocol parser itself returns success=True for such receipts
    (receipt_parser.py returns success on any log mix), so this validator is
    the only layer that can refuse it.
    """
    payload = deepcopy(_supply_payload())
    asset = payload["semantic_contract"]["asset_address"]
    payload["raw_receipt"]["logs"] = [
        {
            "address": asset,
            "topics": [
                _topic("Approval(address,address,uint256)"),
                _address_topic(ACCOUNT),
                _address_topic(payload["semantic_contract"]["resource_address"]),
            ],
            "data": "0x" + _word(AMOUNT),
        },
        {
            "address": asset,
            "topics": [
                _topic("Transfer(address,address,uint256)"),
                _address_topic(ACCOUNT),
                _address_topic(payload["semantic_contract"]["resource_address"]),
            ],
            "data": "0x" + _word(AMOUNT),
        },
    ]

    with pytest.raises(ValueError, match="found 0"):
        validate_semantic_contract(payload, expected_profile="lending.v1")


def test_a_crossed_parser_input_amount_cannot_inherit_green() -> None:
    """The parser's input amount carrying the OUTPUT's value must refuse on the input leg.

    Both numbers are genuine amounts from the same swap — only the attribution is
    crossed. Corrupting exactly one field isolates the input-flow check: the
    output leg stays valid, so nothing else can refuse on its behalf. (The mirror
    corruption is the existing ``parser-output`` mutation above.) A first draft of
    this test swapped BOTH fields and survived a neutered input check because the
    output leg refused instead — a control that a mutant can dodge pins nothing.
    """
    payload = _swap_payload()
    contract = payload["semantic_contract"]
    contract["parser_amount_raw"] = contract["parser_output_amount_raw"]

    with pytest.raises(ValueError, match="input flow"):
        validate_semantic_contract(payload, expected_profile="swap.v1")


# ---------------------------------------------------------------------------
# Aerodrome Slipstream rides the V3 LP profile on the open side; only venue
# identity differs (tick-spacing pools on reviewed factory generations).
# ---------------------------------------------------------------------------

_SLIPSTREAM_LEGACY_FACTORY = "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A"
_SLIPSTREAM_LEGACY_NPM = "0x827922686190790b37229fd06084350E74485b72"
_SLIPSTREAM_CURRENT_NPM = "0xe1f8cd9AC4e4A65F54f38a5CdAfCA44f6dD68b53"
_SLIPSTREAM_POOL = "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59"


def _slipstream_lp_open_payload() -> dict:
    payload = _v3_lp_open_payload()
    token0 = "0x4200000000000000000000000000000000000006"
    token1 = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
    tick_spacing = 100
    liquidity = 987654321
    position_words = [
        0,
        0,
        int(token0, 16),
        int(token1, 16),
        tick_spacing,
        (1 << 256) - 100,
        100,
        liquidity,
        0,
        0,
        0,
        0,
    ]
    payload["protocol"] = "aerodrome_slipstream"
    payload["source_request"]["pool_reference"] = "WETH/USDC/100"
    payload["source_request"].pop("fee_tier_units", None)
    payload["semantic_contract"].update(
        pool_reference="WETH/USDC/100",
        resource_address=_SLIPSTREAM_LEGACY_NPM,
        factory_address=_SLIPSTREAM_LEGACY_FACTORY,
        pool_address=_SLIPSTREAM_POOL,
        fee_tier=tick_spacing,
        pool_key_kind="tick_spacing",
        pool_lookup_raw=_address_topic(_SLIPSTREAM_POOL),
        position_state_raw="0x" + "".join(_word(value) for value in position_words),
    )
    for log in payload["raw_receipt"]["logs"]:
        if log["address"] == UNISWAP_V3["base"]["position_manager"]:
            log["address"] = _SLIPSTREAM_LEGACY_NPM
        for index, topic in enumerate(log["topics"]):
            if topic == _address_topic(UNISWAP_V3["base"]["position_manager"]):
                log["topics"][index] = _address_topic(_SLIPSTREAM_LEGACY_NPM)
    return payload


def test_slipstream_lp_open_is_rederived_from_reviewed_deployment_and_factory_lookup() -> None:
    result = validate_semantic_contract(_slipstream_lp_open_payload(), expected_profile="v3_lp.v1")

    assert result["status"] == "VERIFIED"
    assert result["facts"]["pool_key_kind"] == "tick_spacing"
    assert result["facts"]["fee_tier"] == 100
    assert result["facts"]["pool_address"] == _SLIPSTREAM_POOL.lower()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        # The legacy pool with the CURRENT generation's NPM is not a reviewed pair:
        # an NFT minted by one NPM cannot be closed through another.
        (lambda p: p["semantic_contract"].update(resource_address=_SLIPSTREAM_CURRENT_NPM), "reviewed Slipstream"),
        (lambda p: p["semantic_contract"].update(factory_address="0x" + "77" * 20), "reviewed Slipstream"),
        (
            lambda p: p["semantic_contract"].update(pool_lookup_raw=_address_topic("0x" + "77" * 20)),
            "tick-spacing pool",
        ),
        (
            lambda p: p["semantic_contract"].update(pool_lookup_raw=_address_topic("0x" + "00" * 20)),
            "tick-spacing pool",
        ),
        (lambda p: p["semantic_contract"].pop("pool_key_kind"), "tick_spacing pool key"),
        (lambda p: p["semantic_contract"].update(fee_tier=50), "positions\\(\\) witness"),
    ],
    ids=("npm-generation", "factory", "lookup-other-pool", "lookup-zero", "key-kind", "tick-spacing"),
)
def test_slipstream_lp_open_mutations_cannot_inherit_green(mutation, message: str) -> None:
    payload = _slipstream_lp_open_payload()
    mutation(payload)

    with pytest.raises(ValueError, match=message):
        validate_semantic_contract(payload, expected_profile="v3_lp.v1")


def test_slipstream_close_is_not_admitted_to_the_burning_v3_close_contract() -> None:
    payload = _slipstream_lp_open_payload()
    payload["intent"] = "LP_CLOSE"
    payload["semantic_contract"]["intent"] = "LP_CLOSE"

    with pytest.raises(ValueError, match="No authoritative V3 LP event contract for aerodrome_slipstream.LP_CLOSE"):
        validate_semantic_contract(payload, expected_profile="v3_lp.v1")
