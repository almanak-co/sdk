"""VIB-5141 — Aerodrome Slipstream (cl_nft) LP valuation path.

Slipstream is Aerodrome's (Base) concentrated-liquidity AMM — a Uniswap V3
fork at the LP layer with its OWN NonfungiblePositionManager (kind ``cl_nft``)
and a ``positions(uint256)`` return struct that diverges from V3 at exactly one
word: V3 word [4] is ``fee`` (uint24 bps), Slipstream word [4] is
``tickSpacing`` (int24). The CL NPM has no per-position ``fee`` field.

Two gaps stacked before this fix:
1. ``_resolve_position_manager`` only tried kinds ``("position_manager","nft")``
   and fell back to the Uniswap-V3 NPM, so the ``aerodrome_slipstream``
   pseudo-slug (which records its NPM under ``cl_nft`` on the ``aerodrome``
   table) never resolved its real CL NPM.
2. ``_parse_position_hex`` parsed the V3 12-word layout only, mis-reading
   ``tickSpacing`` as ``fee``.

These tests pin both halves AND assert the shared V3 parse still works
byte-for-byte. Struct layout source: the Aerodrome connector ABI
``almanak/connectors/aerodrome/abis/cl_nft.json`` (positions(uint256) outputs).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from eth_abi import encode

# Boot the contract-role registry so aerodrome_slipstream's CL_POSITION_MANAGER
# role (cl_nft kind on the aerodrome table) is registered.
import almanak.connectors._strategy_contract_role_registry  # noqa: F401
from almanak.framework.valuation.lp_position_reader import (
    LPPositionReader,
    _is_slipstream_protocol,
    _parse_position_hex,
)

# Aerodrome Slipstream NonfungiblePositionManager on Base (addresses.py:cl_nft).
SLIPSTREAM_NPM_BASE = "0x827922686190790b37229fd06084350E74485b72"
# Uniswap V3 NPM (the wrong address the legacy fallback used to return).
UNISWAP_V3_NPM = "0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1"

WETH_BASE = "0x4200000000000000000000000000000000000006"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
WETH_ARB = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC_ARB = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"


def _encode_v3_positions(*, fee: int, tick_lower: int, tick_upper: int, liquidity: int) -> str:
    """ABI-encode a Uniswap V3 positions() return struct (word [4] = fee)."""
    types = [
        "uint96", "address", "address", "address", "uint24",
        "int24", "int24", "uint128", "uint256", "uint256", "uint128", "uint128",
    ]
    vals = [1, "0x" + "00" * 20, WETH_ARB, USDC_ARB, fee, tick_lower, tick_upper, liquidity, 0, 0, 0, 0]
    return "0x" + encode(types, vals).hex()


def _encode_slipstream_positions(
    *, tick_spacing: int, tick_lower: int, tick_upper: int, liquidity: int, owed0: int = 0, owed1: int = 0
) -> str:
    """ABI-encode a Slipstream CL positions() return struct (word [4] = tickSpacing).

    Field order matches almanak/connectors/aerodrome/abis/cl_nft.json:
    (nonce, operator, token0, token1, tickSpacing, tickLower, tickUpper,
     liquidity, feeGrowthInside0LastX128, feeGrowthInside1LastX128,
     tokensOwed0, tokensOwed1).
    """
    types = [
        "uint96", "address", "address", "address", "int24",
        "int24", "int24", "uint128", "uint256", "uint256", "uint128", "uint128",
    ]
    vals = [
        7, "0x" + "00" * 20, WETH_BASE, USDC_BASE, tick_spacing,
        tick_lower, tick_upper, liquidity, 0, 0, owed0, owed1,
    ]
    return "0x" + encode(types, vals).hex()


class TestSlipstreamProtocolDetection:
    def test_aerodrome_slipstream_is_cl(self):
        assert _is_slipstream_protocol("aerodrome_slipstream") is True

    def test_uniswap_v3_is_not_cl(self):
        assert _is_slipstream_protocol("uniswap_v3") is False

    def test_unknown_protocol_is_not_cl(self):
        assert _is_slipstream_protocol("not_a_real_protocol") is False


class TestSlipstreamNpmResolution:
    """Gap #1: the resolver must find the Slipstream cl_nft NPM, not fall back
    to the Uniswap-V3 manager."""

    def test_resolves_cl_nft_npm_on_base(self):
        reader = LPPositionReader()
        addr = reader._resolve_position_manager("base", "aerodrome_slipstream")
        assert addr is not None
        assert addr.lower() == SLIPSTREAM_NPM_BASE.lower()
        # Critically: NOT the Uniswap-V3 fallback NPM.
        assert addr.lower() != UNISWAP_V3_NPM.lower()

    def test_v3_resolution_unchanged(self):
        """Regression: V3-family slugs still resolve their canonical NPM."""
        reader = LPPositionReader()
        addr = reader._resolve_position_manager("arbitrum", "uniswap_v3")
        assert addr is not None
        assert addr.lower() == "0xC36442b4a4522E871399CD717aBDD847Ab11FE88".lower()

    def test_miss_is_fail_closed(self):
        reader = LPPositionReader()
        assert reader._resolve_position_manager("base", "aerodrome_slipstream") is not None
        # An entirely unknown protocol on an unknown chain stays None (the
        # uniswap_v3 fallback has no table for "notachain").
        assert reader._resolve_position_manager("notachain", "uniswap_v3") is None


class TestSlipstreamStructParse:
    """Gap #2: parse the Slipstream CL positions() layout (word [4] =
    tickSpacing), and DON'T regress the shared V3 12-word parse."""

    def test_slipstream_parse_reads_tick_spacing_not_fee(self):
        hex_data = _encode_slipstream_positions(
            tick_spacing=100, tick_lower=-200, tick_upper=200, liquidity=5_000_000_000, owed0=123, owed1=456
        )
        pos = _parse_position_hex(hex_data, 7, slipstream=True)
        assert pos is not None
        # fee is UNMEASURED for Slipstream (Empty != Zero), NOT 0 or the spacing.
        assert pos.fee is None
        assert pos.fee_tier_percent is None
        assert pos.tick_spacing == 100
        assert pos.tick_lower == -200
        assert pos.tick_upper == 200
        assert pos.liquidity == 5_000_000_000
        assert pos.token0.lower() == WETH_BASE.lower()
        assert pos.token1.lower() == USDC_BASE.lower()
        assert pos.tokens_owed0 == 123
        assert pos.tokens_owed1 == 456

    def test_v3_parse_unchanged(self):
        """Regression: the V3 path still reads word [4] as fee and never sets
        tick_spacing."""
        hex_data = _encode_v3_positions(fee=500, tick_lower=-100, tick_upper=100, liquidity=10_000_000_000)
        pos = _parse_position_hex(hex_data, 12345)  # slipstream defaults False
        assert pos is not None
        assert pos.fee == 500
        assert pos.fee_tier_percent == Decimal("0.05")
        assert pos.tick_spacing is None
        assert pos.tick_lower == -100
        assert pos.tick_upper == 100
        assert pos.liquidity == 10_000_000_000

    def test_short_response_returns_none(self):
        """A truncated read is UNMEASURED (None), never a fabricated zero."""
        assert _parse_position_hex("0x" + "00" * 100, 7, slipstream=True) is None


class TestSlipstreamReadPositionRoutesLayout:
    """read_position must pick the Slipstream layout for a CL protocol and the
    V3 layout otherwise — both driven off the same registry signal used to
    resolve the NPM address."""

    def _reader_with_eth_call(self, return_hex: str) -> LPPositionReader:
        reader = LPPositionReader(gateway_client=MagicMock())
        reader._eth_call = MagicMock(return_value=return_hex)  # type: ignore[method-assign]
        return reader

    def test_slipstream_protocol_decodes_cl_layout(self):
        hex_data = _encode_slipstream_positions(
            tick_spacing=100, tick_lower=-50, tick_upper=50, liquidity=999
        )
        reader = self._reader_with_eth_call(hex_data)
        pos = reader.read_position(chain="base", token_id=7, protocol="aerodrome_slipstream")
        assert pos is not None
        assert pos.fee is None
        assert pos.tick_spacing == 100
        assert pos.liquidity == 999

    def test_v3_protocol_decodes_v3_layout(self):
        hex_data = _encode_v3_positions(fee=3000, tick_lower=-50, tick_upper=50, liquidity=777)
        reader = self._reader_with_eth_call(hex_data)
        pos = reader.read_position(chain="arbitrum", token_id=1, protocol="uniswap_v3")
        assert pos is not None
        assert pos.fee == 3000
        assert pos.tick_spacing is None
        assert pos.liquidity == 777


class TestSlipstreamEndToEndValuation:
    """The acceptance criterion: a Slipstream LP position resolves its NPM,
    parses its struct, and yields a real value_usd (repriced=True), not the
    UNAVAILABLE no_path outcome."""

    def test_slipstream_position_reprices_to_real_value(self):
        from almanak.framework.teardown.models import PositionInfo, PositionType
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        valuer = PortfolioValuer()

        # Real cl_nft layout, in-range, with active liquidity.
        hex_data = _encode_slipstream_positions(
            tick_spacing=100, tick_lower=-887200, tick_upper=887200, liquidity=10_000_000_000
        )
        valuer._lp_reader = LPPositionReader(gateway_client=MagicMock())
        valuer._lp_reader._eth_call = MagicMock(return_value=hex_data)  # type: ignore[method-assign]
        # No pool slot0 — the price-ratio fallback fills the tick.
        valuer._lp_reader.read_pool_slot0 = MagicMock(return_value=None)  # type: ignore[method-assign]

        position = PositionInfo(
            position_type=PositionType.LP,
            position_id="7",
            chain="base",
            protocol="aerodrome_slipstream",
            value_usd=Decimal("0"),
            details={"token0": "WETH", "token1": "USDC"},
        )

        market = MagicMock()
        market.price = MagicMock(side_effect=lambda sym: {"WETH": 3500.0, "USDC": 1.0}[sym])

        value_usd, enriched, repriced = valuer._reprice_position_enriched(position, "base", market)

        assert repriced is True
        assert value_usd > Decimal("0")
        assert enriched.get("valuation_source") == "on_chain"
        # Did NOT degrade to the no_path / UNAVAILABLE outcome.
        assert enriched.get("valuation_status") != "no_path"
