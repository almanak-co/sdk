"""Unit tests for BENQI adapter."""

from decimal import Decimal

import pytest

from almanak.framework.connectors.benqi.adapter import (
    BENQI_BORROW_SELECTOR,
    BENQI_COMPTROLLER_ADDRESS,
    BENQI_ENTER_MARKETS_SELECTOR,
    BENQI_MINT_NATIVE_SELECTOR,
    BENQI_MINT_SELECTOR,
    BENQI_QI_TOKENS,
    BENQI_REDEEM_SELECTOR,
    BENQI_REDEEM_UNDERLYING_SELECTOR,
    BENQI_REPAY_BORROW_NATIVE_SELECTOR,
    BENQI_REPAY_BORROW_SELECTOR,
    MAX_UINT256,
    BenqiAdapter,
    BenqiConfig,
)


@pytest.fixture
def adapter():
    """Create a BENQI adapter with default config."""
    config = BenqiConfig(
        chain="avalanche",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return BenqiAdapter(config)


class TestBenqiConfig:
    """Test BenqiConfig initialization."""

    def test_default_chain(self):
        config = BenqiConfig()
        assert config.chain == "avalanche"

    def test_invalid_chain_raises(self):
        with pytest.raises(ValueError, match="only available on Avalanche"):
            BenqiConfig(chain="ethereum")


class TestBenqiAdapterMarketInfo:
    """Test market info methods."""

    def test_get_market_info_usdc(self, adapter):
        info = adapter.get_market_info("USDC")
        assert info is not None
        assert info.asset == "USDC"
        assert info.decimals == 6
        assert info.is_native is False
        assert info.qi_token_address == BENQI_QI_TOKENS["USDC"]["qi_token"]

    def test_get_market_info_avax(self, adapter):
        info = adapter.get_market_info("AVAX")
        assert info is not None
        assert info.asset == "AVAX"
        assert info.decimals == 18
        assert info.is_native is True
        assert info.underlying_address is None

    def test_get_market_info_unknown(self, adapter):
        info = adapter.get_market_info("UNKNOWN_TOKEN")
        assert info is None

    def test_get_market_info_case_insensitive(self, adapter):
        info = adapter.get_market_info("usdc")
        assert info is not None
        assert info.asset == "USDC"

    def test_get_supported_assets(self, adapter):
        assets = adapter.get_supported_assets()
        assert "AVAX" in assets
        assert "USDC" in assets
        assert "USDT" in assets
        assert "WETH.e" in assets
        assert "BTC.b" in assets
        assert "sAVAX" in assets

    def test_get_qi_token_address(self, adapter):
        addr = adapter.get_qi_token_address("USDC")
        assert addr is not None
        assert addr.startswith("0x")

    def test_get_qi_token_address_unknown(self, adapter):
        addr = adapter.get_qi_token_address("UNKNOWN")
        assert addr is None


class TestBenqiSupply:
    """Test supply (mint) operations."""

    def test_supply_erc20(self, adapter):
        result = adapter.supply(asset="USDC", amount=Decimal("1000"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
        assert result.tx_data["value"] == 0
        # mint(uint256) selector
        assert result.tx_data["data"].startswith(BENQI_MINT_SELECTOR)
        # 1000 USDC = 1000 * 10^6 = 1000000000 = 0x3B9ACA00
        assert result.gas_estimate > 0

    def test_supply_native_avax(self, adapter):
        result = adapter.supply(asset="AVAX", amount=Decimal("10"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_QI_TOKENS["AVAX"]["qi_token"]
        # Native AVAX: value should be 10 * 10^18
        assert result.tx_data["value"] == 10 * 10**18
        # mint() payable selector (no args)
        assert result.tx_data["data"] == BENQI_MINT_NATIVE_SELECTOR

    def test_supply_unsupported_asset(self, adapter):
        result = adapter.supply(asset="UNKNOWN", amount=Decimal("100"))
        assert result.success is False
        assert "Unsupported asset" in result.error


class TestBenqiWithdraw:
    """Test withdraw (redeem) operations."""

    def test_withdraw_erc20(self, adapter):
        result = adapter.withdraw(asset="USDC", amount=Decimal("500"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
        # redeemUnderlying(uint256) selector
        assert result.tx_data["data"].startswith(BENQI_REDEEM_UNDERLYING_SELECTOR)

    def test_withdraw_all_unsupported(self, adapter):
        """withdraw_all is unsupported because Compound V2 redeem() needs exact qiToken balance."""
        result = adapter.withdraw(asset="USDC", amount=Decimal("0"), withdraw_all=True)
        assert result.success is False
        assert "withdraw_all is not supported" in result.error

    def test_withdraw_unsupported_asset(self, adapter):
        result = adapter.withdraw(asset="UNKNOWN", amount=Decimal("100"))
        assert result.success is False


class TestBenqiBorrow:
    """Test borrow operations."""

    def test_borrow_usdc(self, adapter):
        result = adapter.borrow(asset="USDC", amount=Decimal("500"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
        # borrow(uint256) selector
        assert result.tx_data["data"].startswith(BENQI_BORROW_SELECTOR)
        assert result.tx_data["value"] == 0

    def test_borrow_avax(self, adapter):
        result = adapter.borrow(asset="AVAX", amount=Decimal("5"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(BENQI_BORROW_SELECTOR)

    def test_borrow_unsupported(self, adapter):
        result = adapter.borrow(asset="UNKNOWN", amount=Decimal("100"))
        assert result.success is False


class TestBenqiRepay:
    """Test repay operations."""

    def test_repay_erc20(self, adapter):
        result = adapter.repay(asset="USDC", amount=Decimal("500"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
        # repayBorrow(uint256) selector
        assert result.tx_data["data"].startswith(BENQI_REPAY_BORROW_SELECTOR)
        assert result.tx_data["value"] == 0

    def test_repay_all_erc20(self, adapter):
        result = adapter.repay(asset="USDC", amount=Decimal("500"), repay_all=True)
        assert result.success is True
        assert result.tx_data is not None
        # repayBorrow(uint256.max) for full repay
        max_hex = f"{MAX_UINT256:064x}"
        assert max_hex in result.tx_data["data"]

    def test_repay_native_avax(self, adapter):
        result = adapter.repay(asset="AVAX", amount=Decimal("5"))
        assert result.success is True
        assert result.tx_data is not None
        # repayBorrow() payable for native
        assert result.tx_data["data"] == BENQI_REPAY_BORROW_NATIVE_SELECTOR
        assert result.tx_data["value"] == 5 * 10**18

    def test_repay_unsupported(self, adapter):
        result = adapter.repay(asset="UNKNOWN", amount=Decimal("100"))
        assert result.success is False


class TestBenqiEnterMarkets:
    """Test Comptroller enterMarkets."""

    def test_enter_single_market(self, adapter):
        result = adapter.enter_markets(["USDC"])
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == BENQI_COMPTROLLER_ADDRESS
        # enterMarkets(address[]) selector
        assert result.tx_data["data"].startswith(BENQI_ENTER_MARKETS_SELECTOR)
        assert result.tx_data["value"] == 0

    def test_enter_multiple_markets(self, adapter):
        result = adapter.enter_markets(["USDC", "AVAX"])
        assert result.success is True
        assert result.tx_data is not None
        # Gas estimate should scale with number of markets
        assert result.gas_estimate > adapter.enter_markets(["USDC"]).gas_estimate

    def test_enter_unsupported_market(self, adapter):
        result = adapter.enter_markets(["UNKNOWN"])
        assert result.success is False


class TestBenqiEncoding:
    """Test ABI encoding helpers."""

    def test_encode_uint256(self):
        assert BenqiAdapter._encode_uint256(0) == "0" * 64
        assert BenqiAdapter._encode_uint256(1) == "0" * 63 + "1"
        assert BenqiAdapter._encode_uint256(255) == "0" * 62 + "ff"

    def test_encode_address(self):
        result = BenqiAdapter._encode_address("0x1234567890123456789012345678901234567890")
        assert len(result) == 64
        assert result.endswith("1234567890123456789012345678901234567890")

    def test_encode_address_array(self):
        addrs = ["0x1111111111111111111111111111111111111111"]
        result = BenqiAdapter._encode_address_array(addrs)
        # offset (32 bytes) + length (32 bytes) + 1 address (32 bytes) = 192 hex chars
        assert len(result) == 192
