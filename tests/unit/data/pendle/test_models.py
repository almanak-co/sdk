"""Unit tests for Pendle data models."""

from decimal import Decimal

from almanak.framework.data.pendle.models import (
    PendleAsset,
    PendleMarketData,
    PendleSwapQuote,
)


class TestPendleAsset:
    """Test PendleAsset dataclass."""

    def test_basic_construction(self):
        asset = PendleAsset(
            address="0xabc",
            symbol="PT-sUSDe",
            decimals=18,
            chain_id=1,
            asset_type="PT",
        )
        assert asset.address == "0xabc"
        assert asset.symbol == "PT-sUSDe"
        assert asset.decimals == 18
        assert asset.chain_id == 1
        assert asset.asset_type == "PT"

    def test_default_values(self):
        asset = PendleAsset(address="0x1", symbol="X", decimals=6, chain_id=42161)
        assert asset.asset_type == ""
        assert asset.underlying_address == ""
        assert asset.expiry == 0

    def test_is_expired_true(self):
        asset = PendleAsset(address="0x1", symbol="X", decimals=18, chain_id=1, expiry=1000)
        assert asset.is_expired(current_timestamp=2000) is True

    def test_is_expired_false(self):
        asset = PendleAsset(address="0x1", symbol="X", decimals=18, chain_id=1, expiry=3000)
        assert asset.is_expired(current_timestamp=2000) is False

    def test_is_expired_zero_expiry(self):
        asset = PendleAsset(address="0x1", symbol="X", decimals=18, chain_id=1, expiry=0)
        assert asset.is_expired(current_timestamp=2000) is False

    def test_to_dict(self):
        asset = PendleAsset(
            address="0xabc",
            symbol="PT-sUSDe",
            decimals=18,
            chain_id=1,
            asset_type="PT",
            underlying_address="0xdef",
            expiry=1700000000,
        )
        d = asset.to_dict()
        assert d["address"] == "0xabc"
        assert d["symbol"] == "PT-sUSDe"
        assert d["decimals"] == 18
        assert d["chain_id"] == 1
        assert d["asset_type"] == "PT"
        assert d["underlying_address"] == "0xdef"
        assert d["expiry"] == 1700000000


class TestPendleMarketData:
    """Test PendleMarketData dataclass."""

    def test_basic_construction(self):
        market = PendleMarketData(
            market_address="0xmarket",
            chain_id=1,
            implied_apy=Decimal("0.05"),
            pt_price_in_asset=Decimal("0.97"),
            liquidity_usd=Decimal("5000000"),
        )
        assert market.market_address == "0xmarket"
        assert market.chain_id == 1
        assert market.implied_apy == Decimal("0.05")
        assert market.pt_price_in_asset == Decimal("0.97")

    def test_default_values(self):
        market = PendleMarketData(market_address="0x1", chain_id=1)
        assert market.pt_address == ""
        assert market.yt_address == ""
        assert market.sy_address == ""
        assert market.implied_apy == Decimal("0")
        assert market.is_expired is False

    def test_to_dict_decimal_serialization(self):
        market = PendleMarketData(
            market_address="0xmarket",
            chain_id=1,
            implied_apy=Decimal("0.15"),
            liquidity_usd=Decimal("10000000"),
        )
        d = market.to_dict()
        assert d["implied_apy"] == "0.15"
        assert d["liquidity_usd"] == "10000000"
        assert d["is_expired"] is False


class TestPendleSwapQuote:
    """Test PendleSwapQuote dataclass."""

    def test_basic_construction(self):
        quote = PendleSwapQuote(
            market_address="0xmarket",
            token_in="0xusdc",
            token_out="0xpt",
            amount_in=1000000,
            amount_out=1020000,
            price_impact_bps=5,
            exchange_rate=Decimal("1.02"),
            source="api",
        )
        assert quote.amount_in == 1000000
        assert quote.amount_out == 1020000
        assert quote.price_impact_bps == 5
        assert quote.source == "api"

    def test_default_values(self):
        quote = PendleSwapQuote(
            market_address="0x1",
            token_in="0xa",
            token_out="0xb",
            amount_in=100,
            amount_out=90,
        )
        assert quote.price_impact_bps == 0
        assert quote.exchange_rate == Decimal("0")
        assert quote.source == "estimate"
        assert quote.warnings == []

    def test_warnings_mutable_default(self):
        """Verify warnings list is not shared between instances."""
        q1 = PendleSwapQuote(market_address="0x1", token_in="a", token_out="b", amount_in=1, amount_out=1)
        q2 = PendleSwapQuote(market_address="0x2", token_in="c", token_out="d", amount_in=2, amount_out=2)
        q1.warnings.append("test warning")
        assert len(q2.warnings) == 0

    def test_to_dict(self):
        quote = PendleSwapQuote(
            market_address="0xmarket",
            token_in="0xusdc",
            token_out="0xpt",
            amount_in=1000000,
            amount_out=1020000,
            price_impact_bps=5,
            exchange_rate=Decimal("1.02"),
            source="api",
            warnings=["high impact"],
        )
        d = quote.to_dict()
        assert d["amount_in"] == "1000000"
        assert d["amount_out"] == "1020000"
        assert d["exchange_rate"] == "1.02"
        assert d["source"] == "api"
        assert d["warnings"] == ["high impact"]
