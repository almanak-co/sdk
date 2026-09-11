from almanak.services.backtest.services.backtest_runner import _mapping_token_refs


def test_nested_token_positions_preserve_contract_identity():
    address = "0x1234567890123456789012345678901234567890"
    config = {
        "variants": [{"params": {"tokens": [[" USDT ", {"symbol": "WRONG", "address": f" {address} "}]]}}],
        "universe": [{"symbol": "FALLBACK", "address": " "}],
        "reserve_token": {"symbol": "USDC"},
    }

    assert _mapping_token_refs(config) == ["FALLBACK", "USDT", address, "USDC"]


def test_empty_and_unsupported_values_do_not_become_token_references():
    config = {
        "tokens": [None, False, 0, b"USDC", bytearray(b"WETH"), " ", {}, {"address": 1, "symbol": None}],
        "universe": [{"pool": "0x1234567890123456789012345678901234567890"}],
        "wallet_address": "0x9876543210987654321098765432109876543210",
        "reserve_token": None,
    }

    assert _mapping_token_refs(config) == []
    assert _mapping_token_refs(None) == []
