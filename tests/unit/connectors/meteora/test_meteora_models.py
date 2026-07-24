"""Pin-down tests for MeteoraPool.from_api_response.

Characterizes the exact field-extraction semantics across the new
datapi.meteora.ag nested format and the legacy flat format BEFORE the
cc-reduction refactor, so the refactor is provably behavior-preserving:
key-presence vs truthiness fallbacks, None-vs-zero decimals, symbol
splitting from ``name``, pct->bps fee conversion, and reserve/vault
precedence chains.
"""

from almanak.connectors.meteora.models import MeteoraPool


def _new_format(**overrides):
    data = {
        "address": "PoolAddr111",
        "token_x": {"address": "MintX111", "symbol": "SOL", "decimals": 9},
        "token_y": {"address": "MintY111", "symbol": "USDC", "decimals": 6},
        "pool_config": {"bin_step": 25, "base_fee_pct": 0.25},
        "active_id": 8388608,
        "current_price": 150.5,
        "liquidity": 1234567.89,
        "reserve_x": "1000",
        "reserve_y": "2000",
        "reserve_x_address": "VaultX111",
        "reserve_y_address": "VaultY111",
        "oracle": "Oracle111",
    }
    data.update(overrides)
    return data


class TestNewApiFormat:
    def test_nested_token_fields(self):
        pool = MeteoraPool.from_api_response(_new_format())
        assert pool.address == "PoolAddr111"
        assert pool.mint_x == "MintX111"
        assert pool.mint_y == "MintY111"
        assert pool.symbol_x == "SOL"
        assert pool.symbol_y == "USDC"
        assert pool.decimals_x == 9
        assert pool.decimals_y == 6
        assert pool.bin_step == 25
        assert pool.active_bin_id == 8388608
        assert pool.current_price == 150.5
        assert pool.tvl == 1234567.89
        assert pool.reserve_x == "1000"
        assert pool.reserve_y == "2000"
        assert pool.vault_x == "VaultX111"
        assert pool.vault_y == "VaultY111"
        assert pool.oracle_address == "Oracle111"

    def test_raw_response_is_same_object(self):
        data = _new_format()
        pool = MeteoraPool.from_api_response(data)
        assert pool.raw_response is data

    def test_none_decimals_fall_back_to_legacy_then_default(self):
        data = _new_format(
            token_x={"address": "MintX111", "symbol": "SOL", "decimals": None},
            token_y={"address": "MintY111", "symbol": "USDC", "decimals": None},
        )
        data["mint_x_decimals"] = 5
        pool = MeteoraPool.from_api_response(data)
        assert pool.decimals_x == 5  # legacy key wins when nested is None
        assert pool.decimals_y == 6  # no legacy key -> default

    def test_zero_decimals_do_not_fall_through(self):
        # Explicit None check: nested decimals of 0 is a measured value.
        data = _new_format(token_x={"address": "MintX111", "symbol": "S", "decimals": 0})
        pool = MeteoraPool.from_api_response(data)
        assert pool.decimals_x == 0

    def test_empty_nested_address_falls_back_to_legacy_mint(self):
        data = _new_format(token_x={"address": "", "symbol": "SOL", "decimals": 9})
        data["mint_x"] = "LegacyMintX"
        pool = MeteoraPool.from_api_response(data)
        assert pool.mint_x == "LegacyMintX"

    def test_non_dict_token_objects_treated_as_absent(self):
        data = _new_format(token_x="not-a-dict", token_y=None)
        data["mint_x"] = "FlatMintX"
        data["mintY"] = "CamelMintY"
        data["name"] = "SOL-USDC"
        pool = MeteoraPool.from_api_response(data)
        assert pool.mint_x == "FlatMintX"
        assert pool.mint_y == "CamelMintY"
        # Symbols come from the name split when nested symbols are absent.
        assert pool.symbol_x == "SOL"
        assert pool.symbol_y == "USDC"

    def test_pct_fee_converted_to_bps_only_without_flat_fee(self):
        pool = MeteoraPool.from_api_response(_new_format())
        assert pool.fee_bps == 25  # 0.25% -> 25 bps

    def test_flat_fee_beats_pool_config_pct(self):
        data = _new_format(fee_bps=30)
        pool = MeteoraPool.from_api_response(data)
        assert pool.fee_bps == 30

    def test_fractional_base_fee_percentage_truncates_then_uses_pct_path(self):
        # int(float("0.3")) == 0 is falsy, so the pct->bps conversion runs.
        data = _new_format(base_fee_percentage="0.3")
        pool = MeteoraPool.from_api_response(data)
        assert pool.fee_bps == 25


class TestLegacyFlatFormat:
    def test_snake_case_fields(self):
        pool = MeteoraPool.from_api_response(
            {
                "pair_address": "PairAddr111",
                "mint_x": "MintX111",
                "mint_y": "MintY111",
                "mint_x_decimals": 9,
                "mint_y_decimals": 6,
                "bin_step": 10,
                "active_id": 100,
                "current_price": 1.5,
                "tvl": 999.0,
                "reserve_x_amount": "5",
                "reserve_y_amount": "6",
                "vault_x": "VX",
                "vault_y": "VY",
                "name": "SOL-USDC",
            }
        )
        assert pool.address == "PairAddr111"  # pair_address fallback
        assert pool.mint_x == "MintX111"
        assert pool.decimals_x == 9
        assert pool.decimals_y == 6
        assert pool.bin_step == 10
        assert pool.active_bin_id == 100
        assert pool.tvl == 999.0
        assert pool.reserve_x == "5"
        assert pool.reserve_y == "6"
        assert pool.vault_x == "VX"
        assert pool.vault_y == "VY"
        assert pool.symbol_x == "SOL"
        assert pool.symbol_y == "USDC"

    def test_camel_case_fallbacks(self):
        pool = MeteoraPool.from_api_response(
            {"mintX": "CamelX", "mintY": "CamelY", "activeId": 42}
        )
        assert pool.mint_x == "CamelX"
        assert pool.mint_y == "CamelY"
        assert pool.active_bin_id == 42

    def test_defaults_on_empty_payload(self):
        pool = MeteoraPool.from_api_response({})
        assert pool.address == ""
        assert pool.mint_x == ""
        assert pool.mint_y == ""
        assert pool.symbol_x == ""
        assert pool.symbol_y == ""
        assert pool.decimals_x == 9
        assert pool.decimals_y == 6
        assert pool.bin_step == 10
        assert pool.active_bin_id == 0
        assert pool.current_price == 0.0
        assert pool.tvl == 0.0
        assert pool.reserve_x == "0"
        assert pool.reserve_y == "0"
        assert pool.fee_bps == 0
        assert pool.vault_x == ""
        assert pool.vault_y == ""
        assert pool.oracle_address == ""

    def test_name_without_dash_leaves_symbol_y_empty(self):
        pool = MeteoraPool.from_api_response({"name": "WSOL"})
        assert pool.symbol_x == "WSOL"
        assert pool.symbol_y == ""

    def test_name_segments_are_stripped(self):
        pool = MeteoraPool.from_api_response({"name": " SOL - USDC "})
        assert pool.symbol_x == "SOL"
        assert pool.symbol_y == "USDC"

    def test_three_segment_name_takes_second(self):
        pool = MeteoraPool.from_api_response({"name": "A-B-C"})
        assert pool.symbol_x == "A"
        assert pool.symbol_y == "B"

    def test_reserve_precedence_chain(self):
        pool = MeteoraPool.from_api_response(
            {
                "reserve_x": "1",
                "reserve_x_amount": "2",
                "token_x_amount": "3",
                "reserve_y_amount": "4",
                "token_y_amount": "5",
            }
        )
        assert pool.reserve_x == "1"
        assert pool.reserve_y == "4"

    def test_token_amount_is_last_reserve_fallback(self):
        pool = MeteoraPool.from_api_response(
            {"token_x_amount": 7, "token_y_amount": 8}
        )
        assert pool.reserve_x == "7"  # str() coercion
        assert pool.reserve_y == "8"

    def test_liquidity_beats_tvl_key(self):
        pool = MeteoraPool.from_api_response({"liquidity": 10.0, "tvl": 20.0})
        assert pool.tvl == 10.0

    def test_flat_bin_step_when_no_pool_config(self):
        pool = MeteoraPool.from_api_response({"bin_step": 80})
        assert pool.bin_step == 80

    def test_vault_precedence_reserve_address_first(self):
        pool = MeteoraPool.from_api_response(
            {"reserve_x_address": "RA", "vault_x": "VX", "vault_y": "VY"}
        )
        assert pool.vault_x == "RA"
        assert pool.vault_y == "VY"
