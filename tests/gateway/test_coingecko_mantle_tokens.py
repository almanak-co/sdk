"""Tests for Mantle token CoinGecko ID mappings.

Validates that WMNT, MNT, and other Mantle tokens are correctly
mapped in GLOBAL_TOKEN_IDS for price resolution.
"""

from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS, MANTLE_TOKEN_IDS


class TestMantleTokenIds:
    """Tests for Mantle chain token CoinGecko mappings."""

    def test_wmnt_mapped(self):
        """WMNT (Wrapped Mantle) has a CoinGecko ID."""
        assert "WMNT" in GLOBAL_TOKEN_IDS
        assert GLOBAL_TOKEN_IDS["WMNT"] == "mantle"

    def test_mnt_mapped(self):
        """MNT (native Mantle) has a CoinGecko ID."""
        assert "MNT" in GLOBAL_TOKEN_IDS
        assert GLOBAL_TOKEN_IDS["MNT"] == "mantle"

    def test_wmnt_same_as_mnt(self):
        """WMNT uses the same CoinGecko ID as MNT (wrapped = same price)."""
        assert GLOBAL_TOKEN_IDS["WMNT"] == GLOBAL_TOKEN_IDS["MNT"]

    def test_mantle_usdc_mapped(self):
        """USDC on Mantle resolves via MANTLE_TOKEN_IDS."""
        assert "USDC" in MANTLE_TOKEN_IDS
        assert MANTLE_TOKEN_IDS["USDC"] == "usd-coin"

    def test_mantle_weth_mapped(self):
        """WETH on Mantle resolves via MANTLE_TOKEN_IDS."""
        assert "WETH" in MANTLE_TOKEN_IDS
        assert MANTLE_TOKEN_IDS["WETH"] == "weth"

    def test_mantle_ids_included_in_global(self):
        """MANTLE_TOKEN_IDS are merged into GLOBAL_TOKEN_IDS."""
        for symbol, cg_id in MANTLE_TOKEN_IDS.items():
            assert symbol in GLOBAL_TOKEN_IDS, f"{symbol} missing from GLOBAL_TOKEN_IDS"
