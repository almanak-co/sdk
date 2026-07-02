"""Byte-exact unit tests for the Hyperliquid CoreWriter encoder.

These pin the wire format against the Hyperliquid spec so a regression in the
action encoding fails loudly. The definitive proof is a testnet fill; these
guard the encoding between here and there.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from eth_utils import keccak

from almanak.connectors.hyperliquid import sdk


class TestSelector:
    def test_send_raw_action_selector_matches_signature(self) -> None:
        assert sdk.SELECTOR_SEND_RAW_ACTION == keccak(b"sendRawAction(bytes)")[:4]


class TestTickRounding:
    @pytest.mark.parametrize(
        ("price", "sz_decimals", "expected"),
        [
            # BTC szDecimals=5 → max 1 decimal place, 5 sig figs.
            (Decimal("59897.234"), 5, Decimal("59897.0")),
            (Decimal("3.14159"), 5, Decimal("3.1")),
            # ETH szDecimals=4 → max 2 decimal places.
            (Decimal("2000.12345"), 4, Decimal("2000.1")),
            # Integer prices bypass the 5-sig-fig cap (Hyperliquid rule): a 6-figure
            # integer must be preserved, NOT distorted to 123460.
            (Decimal("123456"), 5, Decimal("123456")),
        ],
    )
    def test_round_perp_price(self, price: Decimal, sz_decimals: int, expected: Decimal) -> None:
        assert sdk.round_perp_price(price, sz_decimals) == expected

    def test_round_size_rounds_down_to_sz_decimals(self) -> None:
        assert sdk.round_size(Decimal("0.0012345"), 5) == Decimal("0.00123")

    def test_round_price_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError):
            sdk.round_perp_price(Decimal("0"), 5)

    def test_round_size_rejects_dust_that_quantises_to_zero(self) -> None:
        with pytest.raises(ValueError):
            sdk.round_size(Decimal("0.000001"), 2)


class TestWireScaling:
    def test_price_to_wire_is_1e8(self) -> None:
        assert sdk.price_to_wire(Decimal("59897"), 5) == 59897 * 10**8

    def test_size_to_wire_is_1e8(self) -> None:
        assert sdk.size_to_wire(Decimal("0.00123"), 5) == 123000


class TestMarketLimitPrice:
    def test_fail_closed_on_non_positive_reference(self) -> None:
        with pytest.raises(ValueError, match="fail-closed"):
            sdk.market_limit_price(Decimal("0"), 50, is_buy=True, sz_decimals=5)

    def test_buy_crosses_up_within_band(self) -> None:
        # 60000 * (1 + 50bps) = 60300.
        assert sdk.market_limit_price(Decimal("60000"), 50, is_buy=True, sz_decimals=5) == 60300 * 10**8

    def test_sell_crosses_down_within_band(self) -> None:
        assert sdk.market_limit_price(Decimal("60000"), 50, is_buy=False, sz_decimals=5) == 59700 * 10**8

    def test_rejects_out_of_range_slippage(self) -> None:
        with pytest.raises(ValueError):
            sdk.market_limit_price(Decimal("60000"), 20_000, is_buy=True, sz_decimals=5)


class TestLimitOrderActionEncoding:
    def _order(self, **kw: object) -> sdk.LimitOrderAction:
        base = {
            "asset": 0,
            "is_buy": True,
            "limit_px": sdk.price_to_wire(Decimal("60000"), 5),
            "sz": sdk.size_to_wire(Decimal("0.001"), 5),
            "reduce_only": False,
            "tif": sdk.TIF_IOC,
            "cloid": 0,
        }
        base.update(kw)
        return sdk.LimitOrderAction(**base)  # type: ignore[arg-type]

    def test_action_blob_is_228_bytes(self) -> None:
        blob = sdk.encode_limit_order_action(self._order())
        assert len(blob) == 228  # 1 version + 3 action-id + 224 ABI body

    def test_action_header_is_version1_actionid1(self) -> None:
        blob = sdk.encode_limit_order_action(self._order())
        assert blob[:4].hex() == "01000001"

    def test_calldata_has_send_raw_action_selector(self) -> None:
        blob = sdk.encode_limit_order_action(self._order())
        cd = sdk.encode_send_raw_action_calldata(blob)
        assert cd[:4] == sdk.SELECTOR_SEND_RAW_ACTION

    def test_rejects_invalid_tif(self) -> None:
        with pytest.raises(ValueError):
            sdk.encode_limit_order_action(self._order(tif=9))

    def test_rejects_zero_size(self) -> None:
        with pytest.raises(ValueError):
            sdk.encode_limit_order_action(self._order(sz=0))


class TestCancelEncoding:
    def test_cancel_by_oid_header(self) -> None:
        blob = sdk.encode_cancel_by_oid_action(0, 12345)
        assert blob[:4].hex() == "0100000a"  # action id 10

    def test_cancel_by_cloid_header(self) -> None:
        blob = sdk.encode_cancel_by_cloid_action(0, 999)
        assert blob[:4].hex() == "0100000b"  # action id 11


class TestUsdClassTransfer:
    def test_header_and_1e6_scaling(self) -> None:
        blob = sdk.encode_usd_class_transfer_action(Decimal("100"), to_perp=True)
        assert blob[:4].hex() == "01000007"  # action id 7


class TestPrecompileInputEncoders:
    def test_perp_query_no_selector_raw_abi(self) -> None:
        # 32-byte ABI word, no 4-byte selector prefix.
        assert sdk.encode_perp_query(0).hex() == "00" * 32

    def test_position_query_encodes_address_and_perp(self) -> None:
        data = sdk.encode_position_query("0x" + "11" * 20, 3)
        assert len(data) == 64  # address word + uint32 word

    def test_account_margin_query_encodes_perp_dex_index_first(self) -> None:
        # accountMarginSummary input is INVERTED vs position: (uint32 perpDexIndex,
        # address user). perpDexIndex default 0 → first word all-zero, then the
        # address word. This mirrors the live-confirmed encoding that returned a
        # summary (the (address, uint32) order reverts with PrecompileError).
        from eth_abi import encode as abi_encode

        wallet = "0x" + "11" * 20
        data = sdk.encode_account_margin_query(wallet)
        assert data == abi_encode(["uint32", "address"], [0, wallet])
        assert data[:32] == b"\x00" * 32  # perpDexIndex 0 is the FIRST word


class TestAccountMarginSummaryDecode:
    """0x080F accountMarginSummary decode — layout + scale CONFIRMED live
    (2026-07-02, two independent cross accounts; see perps_read.py docstring)."""

    def test_decodes_four_1e6_usd_fields_in_order(self) -> None:
        from eth_abi import encode as abi_encode

        # account_value $1.5, margin_used $0.5, ntl_pos $10, raw_usd $4 (all 1e6).
        blob = "0x" + abi_encode(
            ["int64", "uint64", "uint64", "int64"], [1_500_000, 500_000, 10_000_000, 4_000_000]
        ).hex()
        s = sdk.decode_account_margin_summary(blob)
        assert s is not None
        assert (s.account_value, s.margin_used, s.ntl_pos, s.raw_usd) == (1_500_000, 500_000, 10_000_000, 4_000_000)

    def test_signed_fields_decode_negative(self) -> None:
        from eth_abi import encode as abi_encode

        # accountValue and rawUsd are int64 — can go negative (underwater account).
        blob = "0x" + abi_encode(
            ["int64", "uint64", "uint64", "int64"], [-1_000_000, 0, 0, -2_000_000]
        ).hex()
        s = sdk.decode_account_margin_summary(blob)
        assert s is not None
        assert s.account_value == -1_000_000
        assert s.raw_usd == -2_000_000

    def test_empty_return_is_none_not_zero(self) -> None:
        # Empty≠Zero: an unmeasured account (no HyperCore cross account / revert)
        # decodes to None, NOT an all-zero summary.
        assert sdk.decode_account_margin_summary("0x") is None
        assert sdk.decode_account_margin_summary("") is None

    def test_layout_matches_live_confirmed_ordering_and_scale(self) -> None:
        # Regression on the CONFIRMED layout order + 1e6 scale, using the shape of a
        # real live read (2026-07-02 account 0x31ca8395…974b ≈ $3.00M equity,
        # rawUsd ≈ $4.33M): accountValue and rawUsd are the SIGNED int64 fields,
        # marginUsed and ntlPos the unsigned ones, in that exact order. A layout
        # regression (e.g. swapping ntlPos/marginUsed or mis-scaling) fails here.
        from eth_abi import encode as abi_encode

        account_value = 3_001_008_760_000  # $3,001,008.76 at 1e6
        margin_used = 198_982_610_000  # $198,982.61
        ntl_pos = 3_979_652_190_000  # $3,979,652.19
        raw_usd = 4_334_549_030_000  # $4,334,549.03
        blob = "0x" + abi_encode(
            ["int64", "uint64", "uint64", "int64"], [account_value, margin_used, ntl_pos, raw_usd]
        ).hex()
        s = sdk.decode_account_margin_summary(blob)
        assert s is not None
        assert round(s.account_value / 1e6, 2) == 3_001_008.76
        assert round(s.margin_used / 1e6, 2) == 198_982.61
        assert round(s.ntl_pos / 1e6, 2) == 3_979_652.19
        assert round(s.raw_usd / 1e6, 2) == 4_334_549.03
