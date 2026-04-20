"""Tests for Ethereum token CoinGecko ID mappings.

Validates that CVX, CRV, COMP, MKR, and other Ethereum-native tokens
are correctly mapped in GLOBAL_TOKEN_IDS for price resolution.
"""

from almanak.gateway.data.price.coingecko import ETHEREUM_TOKEN_IDS, GLOBAL_TOKEN_IDS


class TestEthereumTokenIds:
    """Tests for Ethereum chain token CoinGecko mappings."""

    def test_cvx_mapped(self):
        """CVX (Convex Finance) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["CVX"] == "convex-finance"

    def test_crv_mapped(self):
        """CRV (Curve DAO) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["CRV"] == "curve-dao-token"

    def test_comp_mapped(self):
        """COMP (Compound) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["COMP"] == "compound-governance-token"

    def test_mkr_mapped(self):
        """MKR (Maker) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["MKR"] == "maker"

    def test_snx_mapped(self):
        """SNX (Synthetix) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["SNX"] == "havven"

    def test_ldo_mapped(self):
        """LDO (Lido DAO) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["LDO"] == "lido-dao"

    def test_pendle_mapped(self):
        """PENDLE has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["PENDLE"] == "pendle"

    def test_steth_not_in_ethereum_ids(self):
        """STETH is excluded from ETHEREUM_TOKEN_IDS (chain-specific semantics differ)."""
        assert "STETH" not in ETHEREUM_TOKEN_IDS

    def test_wsteth_mapped(self):
        """WSTETH has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["WSTETH"] == "wrapped-steth"

    def test_reth_mapped(self):
        """RETH (Rocket Pool ETH) has a CoinGecko ID."""
        assert GLOBAL_TOKEN_IDS["RETH"] == "rocket-pool-eth"

    def test_ethereum_ids_included_in_global(self):
        """ETHEREUM_TOKEN_IDS are merged into GLOBAL_TOKEN_IDS."""
        for symbol, cg_id in ETHEREUM_TOKEN_IDS.items():
            assert symbol in GLOBAL_TOKEN_IDS, f"{symbol} missing from GLOBAL_TOKEN_IDS"

    def test_ethereum_ids_take_precedence(self):
        """Ethereum IDs override other chain IDs in GLOBAL_TOKEN_IDS (merged last)."""
        for symbol, cg_id in ETHEREUM_TOKEN_IDS.items():
            assert GLOBAL_TOKEN_IDS[symbol] == cg_id, (
                f"{symbol}: expected {cg_id}, got {GLOBAL_TOKEN_IDS[symbol]}"
            )
