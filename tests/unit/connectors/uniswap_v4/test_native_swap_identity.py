"""Native settlement identity survives compiler transport and missing Transfer logs."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from eth_abi import encode

from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config
from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import EVENT_TOPICS, UniswapV4ReceiptParser

ZERO = "0x" + "0" * 40
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH = "0x4200000000000000000000000000000000000006"
WALLET = "0x" + "7" * 40
NATIVE_AMOUNT = 1239031392772972


def receipt(key, native_output=True, chain="base"):
    amounts = (NATIVE_AMOUNT, -3100000) if native_output else (-NATIVE_AMOUNT, 3100000)
    return {
        "logs": [
            {
                "address": UNISWAP_V4[chain]["pool_manager"],
                "topics": [EVENT_TOPICS["Swap"], key.pool_id, "0x" + "0" * 24 + WALLET[2:]],
                "data": "0x"
                + encode(
                    ["int128", "int128", "uint160", "uint128", "int24", "uint24"],
                    [*amounts, 2**96, 10**18, 0, key.fee],
                ).hex(),
            }
        ],
    }


@pytest.mark.parametrize("native_output", [True, False])
def test_compiler_native_slot_survives_extraction_transport(native_output):
    adapter = UniswapV4Adapter(config=UniswapV4Config(chain="base", wallet_address=WALLET))
    intent = MagicMock()
    intent.from_token, intent.to_token = ("USDC", "ETH") if native_output else ("ETH", "USDC")
    intent.amount = Decimal("3.1") if native_output else Decimal("0.001")
    intent.amount_usd = None
    intent.max_slippage = Decimal("0.005")
    intent.intent_id = "native-identity"
    intent.swap_params = None
    bundle = adapter.compile_swap_intent(
        intent, {"ETH": Decimal("2500"), "USDC": Decimal(1)}, permission_discovery=True
    )
    slot = "token_out" if native_output else "token_in"
    assert bundle.metadata["swap_token_meta"][slot] == {"address": ZERO, "symbol": "ETH", "decimals": 18}
    kwargs = UniswapV4ReceiptParser(chain="base").build_extract_kwargs(
        field="swap_amounts", bundle_metadata=bundle.metadata
    )
    assert kwargs["swap_token_meta"][slot]["address"] == ZERO
    assert kwargs["swap_pool_key"] == bundle.metadata["pool_key"]


@pytest.mark.parametrize("native_output", [True, False])
def test_full_key_recovers_native_without_any_transfer_or_native_hint(native_output):
    key = PoolKey(ZERO, USDC, 3000, 60)
    parser = UniswapV4ReceiptParser(chain="base")
    kwargs = parser.build_extract_kwargs(
        field="swap_amounts", bundle_metadata={"pool_key": key.to_wire(), "pool_id": key.pool_id}
    )
    result = parser.extract_swap_amounts(receipt(key, native_output), **kwargs)
    assert result is not None
    native_side = "out" if native_output else "in"
    stable_side = "in" if native_output else "out"
    assert getattr(result, f"token_{native_side}") == "ETH"
    assert getattr(result, f"amount_{native_side}_decimal") == Decimal("0.001239031392772972")
    assert getattr(result, f"amount_{native_side}_decimal_resolved") is True
    assert getattr(result, f"token_{stable_side}") == "USDC"
    assert getattr(result, f"amount_{stable_side}_decimal") == Decimal("3.1")


def test_wrapped_native_remains_a_distinct_currency():
    key = PoolKey(WETH, USDC, 3000, 60)
    result = UniswapV4ReceiptParser(chain="base").extract_swap_amounts(receipt(key), swap_pool_key=key.to_wire())
    assert result.token_out == "WETH"


@pytest.mark.parametrize("chain,symbol", [("polygon", "POL"), ("avalanche", "AVAX"), ("bsc", "BNB")])
def test_native_symbol_comes_from_chain_registry(chain, symbol):
    key = PoolKey(ZERO, USDC, 3000, 60)
    result = UniswapV4ReceiptParser(chain=chain).extract_swap_amounts(
        receipt(key, chain=chain), swap_pool_key=key.to_wire()
    )
    assert result.token_out == symbol
    assert result.amount_out_decimal_resolved


def test_wrong_pool_and_multiple_swaps_are_not_assigned_intent_endpoints():
    key = PoolKey(ZERO, USDC, 3000, 60)
    parser = UniswapV4ReceiptParser(chain="base")
    wrong = receipt(PoolKey(ZERO, USDC, 500, 10))
    with pytest.raises(ValueError, match="selected single-pool"):
        parser.extract_swap_amounts(wrong, swap_pool_key=key.to_wire())
    multiple = receipt(key)
    multiple["logs"] *= 2
    with pytest.raises(ValueError, match="selected single-pool"):
        parser.extract_swap_amounts(multiple, swap_pool_key=key.to_wire())


def test_foreign_emitter_cannot_supply_swap_identity():
    key = PoolKey(ZERO, USDC, 3000, 60)
    forged = receipt(key)
    forged["logs"][0]["address"] = USDC
    assert UniswapV4ReceiptParser(chain="base").extract_swap_amounts(forged, swap_pool_key=key.to_wire()) is None


def test_missing_identity_preserves_unresolved_state():
    key = PoolKey(ZERO, USDC, 3000, 60)
    result = UniswapV4ReceiptParser(chain="base").extract_swap_amounts(receipt(key))
    assert result.token_out is None
    assert result.amount_out == NATIVE_AMOUNT
    assert result.amount_out_decimal == Decimal(0)
    assert result.amount_out_decimal_resolved is False


def test_metadata_pool_hash_mismatch_refuses():
    key = PoolKey(ZERO, USDC, 3000, 60)
    with pytest.raises(ValueError, match="pool ID"):
        UniswapV4ReceiptParser(chain="base").build_extract_kwargs(
            field="swap_amounts", bundle_metadata={"pool_key": key.to_wire(), "pool_id": "0x" + "a" * 64}
        )
