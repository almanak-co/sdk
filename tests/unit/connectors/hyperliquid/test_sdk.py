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


class TestSpotSend:
    """Byte-exact tests for the spot-send (action 6) encoder (VIB-5615).

    The wire body is ABI ``(address destination, uint64 token, uint64 wei)`` and
    the ``wei`` field uses the token's OWN weiDecimals (USDC = 8) — NOT the 1e6
    ``ntl`` scale ``usdClassTransfer`` uses. These pin that convention.
    """

    _DEST = "0x" + "11" * 20

    def test_header_is_action_id_6(self) -> None:
        blob = sdk.encode_spot_send_action(self._DEST, 0, 699_000_000)
        assert blob[:4].hex() == "01000006"  # version 1 + action id 6

    def test_spot_wei_uses_token_wei_decimals_not_1e6(self) -> None:
        # 6.99 USDC at weiDecimals 8 → 699_000_000 (NOT 6_990_000 which the 1e6
        # perp-ntl scale would give — that is the exact trap this guards).
        assert sdk.spot_wei(Decimal("6.99"), 8) == 699_000_000
        assert sdk.spot_wei(Decimal("6.99"), 6) == 6_990_000  # a 6-decimal token

    def test_body_matches_independent_abi_encode(self) -> None:
        from eth_abi import encode as abi_encode

        wei = sdk.spot_wei(Decimal("6.99"), 8)
        blob = sdk.encode_spot_send_action(self._DEST, 0, wei)
        expected_body = abi_encode(["address", "uint64", "uint64"], [self._DEST, 0, 699_000_000])
        assert blob[4:] == expected_body

    def test_spot_wei_rounds_down_and_rejects_sub_wei(self) -> None:
        # ROUND_DOWN: 1.999999999 USDC @ 8dp → 199_999_999 (truncates the 10th dp).
        assert sdk.spot_wei(Decimal("1.999999999"), 8) == 199_999_999
        # An amount that rounds to zero wei is fail-closed (never a no-op send).
        with pytest.raises(ValueError):
            sdk.spot_wei(Decimal("0.000000009"), 8)

    def test_spot_wei_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError):
            sdk.spot_wei(Decimal("0"), 8)
        with pytest.raises(ValueError):
            sdk.spot_wei(Decimal("-1"), 8)

    def test_encode_rejects_bad_address(self) -> None:
        with pytest.raises(ValueError):
            sdk.encode_spot_send_action("0xnothex", 0, 1)

    def test_usdc_withdraw_calldata_targets_system_address(self) -> None:
        from eth_abi import decode as abi_decode

        from almanak.connectors.hyperliquid.addresses import USDC_SPOT_SYSTEM_ADDRESS

        cd = sdk.build_usdc_withdraw_calldata(Decimal("6.99"))
        assert cd[:4] == sdk.SELECTOR_SEND_RAW_ACTION
        (inner,) = abi_decode(["bytes"], cd[4:])
        assert inner[:4].hex() == "01000006"
        dest, token, wei = abi_decode(["address", "uint64", "uint64"], inner[4:])
        # USDC (token 0) to the USDC system address, 8-decimal wei — the bridge
        # request HyperCore credits back to the sender's HyperEVM wallet.
        assert dest.lower() == USDC_SPOT_SYSTEM_ADDRESS.lower()
        assert token == 0
        assert wei == 699_000_000


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


class TestFailClosedEncoderGuards:
    """Fail-closed guard branches on the CoreWriter encoders (VIB-5615).

    Every encoder rejects out-of-range / non-positive / malformed input with a
    ``ValueError`` rather than emitting calldata that would silently send the
    wrong amount on-chain. These pin the "never emit a bad action" contract that
    a real fill can only prove after money has already moved.
    """

    def test_round_perp_price_rounds_to_non_positive_raises(self) -> None:
        # A price so small it quantises to <= 0 at the decimal ceiling is rejected
        # (never round a positive intent down to a zero/negative wire price).
        with pytest.raises(ValueError, match="non-positive"):
            sdk.round_perp_price(Decimal("1e-30"), 8)

    def test_round_perp_price_negative_sz_decimals_clamps_max_decimals(self) -> None:
        # sz_decimals > PERP_PX_MAX_DECIMALS drives max_decimals negative; the
        # encoder clamps to 0 (integer-only ceiling) rather than crashing.
        px = sdk.round_perp_price(Decimal("123456"), 99)
        assert px == px.to_integral_value()

    def test_round_size_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            sdk.round_size(Decimal("0"), 5)

    def test_round_size_rejects_negative_sz_decimals(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            sdk.round_size(Decimal("1"), -1)

    def test_to_wire_rejects_non_integral_residual(self) -> None:
        # A human value that does NOT scale to a (near-)integer wire means the
        # caller skipped tick rounding — reject rather than truncate silently.
        # (Call _to_wire directly; price_to_wire tick-rounds first, erasing the
        # residual this branch guards.)
        with pytest.raises(ValueError, match="does not scale to an integer wire"):
            sdk._to_wire(Decimal("0.1234567895"))

    def test_to_wire_rejects_out_of_uint64_range(self) -> None:
        # A cleanly-integral value whose ×1e8 wire exceeds uint64 (2**64) is
        # rejected — never emit an out-of-range limit price / size.
        with pytest.raises(ValueError, match="out of uint64 range"):
            sdk._to_wire(Decimal("200000000000"))

    def test_action_header_rejects_out_of_3_byte_range(self) -> None:
        with pytest.raises(ValueError, match="out of 3-byte range"):
            sdk._action_header(2**24)
        with pytest.raises(ValueError, match="out of 3-byte range"):
            sdk._action_header(0)

    def test_usd_class_transfer_rejects_non_positive(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            sdk.encode_usd_class_transfer_action(Decimal("0"), to_perp=True)

    def test_spot_wei_rejects_negative_wei_decimals(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            sdk.spot_wei(Decimal("1"), -1)

    def test_spot_wei_rejects_out_of_uint64_range(self) -> None:
        # A large amount whose scaled wei exceeds uint64 (2**64 ≈ 1.8e19) is
        # rejected. 2e11 units @ 8dp → 2e19 wei, over the ceiling.
        with pytest.raises(ValueError, match="out of uint64 range"):
            sdk.spot_wei(Decimal("200000000000"), 8)

    def test_send_raw_action_calldata_rejects_short_blob(self) -> None:
        with pytest.raises(ValueError, match=">= 4 bytes"):
            sdk.encode_send_raw_action_calldata(b"\x01\x00")

    def test_send_raw_action_calldata_rejects_non_bytes(self) -> None:
        with pytest.raises(ValueError, match=">= 4 bytes"):
            sdk.encode_send_raw_action_calldata("0100000101")  # type: ignore[arg-type]


class TestDecodeGuards:
    """Guard branches on the CoreWriter decoders — malformed / empty input.

    The receipt parser feeds these raw log payloads; an undecodable blob must
    raise or return an explicit empty value, never a fabricated action / position.
    """

    def test_decode_limit_order_rejects_short_blob(self) -> None:
        with pytest.raises(ValueError, match="too short"):
            sdk.decode_limit_order_action(b"\x01\x00")

    def test_decode_limit_order_rejects_wrong_action_id(self) -> None:
        # A validly-shaped header but the wrong action id (e.g. a cancel blob)
        # is not a limit order — reject rather than mis-decode.
        cancel_blob = sdk.encode_cancel_by_oid_action(0, 123)
        with pytest.raises(ValueError, match="not a v"):
            sdk.decode_limit_order_action(cancel_blob)

    def test_decode_limit_order_round_trips_a_valid_blob(self) -> None:
        order = sdk.LimitOrderAction(
            asset=0,
            is_buy=True,
            limit_px=sdk.price_to_wire(Decimal("60000"), 5),
            sz=sdk.size_to_wire(Decimal("0.001"), 5),
            reduce_only=True,
            tif=sdk.TIF_IOC,
            cloid=42,
        )
        decoded = sdk.decode_limit_order_action(sdk.encode_limit_order_action(order))
        assert decoded == order

    def test_decode_raw_action_log_data_empty_is_empty_bytes(self) -> None:
        # Empty / "0x" log data → b"" (Empty≠Zero: an unparseable log is not a
        # fabricated action).
        assert sdk.decode_raw_action_log_data("0x") == b""
        assert sdk.decode_raw_action_log_data(b"") == b""

    def test_decode_position_empty_is_explicit_no_position(self) -> None:
        # An empty precompile return → explicit zero-size Position, distinct from
        # a decoded measured zero (callers treat szi==0 as "no position").
        pos = sdk.decode_position("0x")
        assert pos.szi == 0
        assert pos.entry_ntl == 0

    def test_to_bytes_rejects_non_str_non_bytes(self) -> None:
        with pytest.raises(ValueError, match="expected hex str or bytes"):
            sdk._to_bytes(12345)  # type: ignore[arg-type]

    def test_check_address_rejects_non_hex_body(self) -> None:
        # Right length + 0x prefix but a non-hex body must still be rejected
        # (exercises the int() parse guard, not just the shape check).
        bad = "0x" + "zz" * 20
        with pytest.raises(ValueError, match="Invalid EVM address"):
            sdk._check_address(bad)

    def test_check_uint_rejects_bool(self) -> None:
        # A bool is an int subclass but is NOT a valid wire uint — reject it so a
        # True/False can't masquerade as 1/0 in an encoded field.
        with pytest.raises(ValueError, match="must be an int"):
            sdk._check_uint(True, sdk._UINT64_MAX, "sz")
