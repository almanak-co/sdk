"""Unit tests for case-insensitive price oracle lookup in IntentCompiler.

VIB-1650: Token.__post_init__ uppercases symbols (e.g., "cbETH" -> "CBETH"),
but the price oracle may store prices under the original mixed-case key.
The compiler must resolve prices case-insensitively so CryptoSwap pools
with mixed-case tokens (cbETH, wstETH, crvUSD, sUSDe, USDe) compile correctly.
"""

from decimal import Decimal

import pytest

from almanak import IntentCompilerConfig
from almanak.framework.intents.compiler import IntentCompiler


@pytest.fixture()
def config():
    """Config with placeholder prices disabled (production mode)."""
    return IntentCompilerConfig(allow_placeholder_prices=False)


class TestCaseInsensitivePriceLookup:
    """Verify that _require_token_price resolves mixed-case keys."""

    def test_exact_uppercase_match(self, config):
        """Standard uppercase key matches directly."""
        compiler = IntentCompiler(
            chain="base",
            price_oracle={"CBETH": Decimal("3200")},
            config=config,
        )
        assert compiler._require_token_price("CBETH") == Decimal("3200")

    def test_mixed_case_key_resolved_via_fallback(self, config):
        """Oracle key 'cbETH' should match lookup for 'CBETH'."""
        compiler = IntentCompiler(
            chain="base",
            price_oracle={"cbETH": Decimal("3200"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler._require_token_price("CBETH") == Decimal("3200")

    def test_wsteth_case_insensitive(self, config):
        """Oracle key 'wstETH' should match lookup for 'WSTETH'."""
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"wstETH": Decimal("4100"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler._require_token_price("WSTETH") == Decimal("4100")

    def test_crvusd_case_insensitive(self, config):
        """Oracle key 'crvUSD' should match lookup for 'CRVUSD'."""
        compiler = IntentCompiler(
            chain="optimism",
            price_oracle={"crvUSD": Decimal("1.001"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler._require_token_price("CRVUSD") == Decimal("1.001")

    def test_susde_case_insensitive(self, config):
        """Oracle key 'sUSDe' should match lookup for 'SUSDE'."""
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"sUSDe": Decimal("1.05"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler._require_token_price("SUSDE") == Decimal("1.05")

    def test_usde_case_insensitive(self, config):
        """Oracle key 'USDe' should match lookup for 'USDE'."""
        compiler = IntentCompiler(
            chain="ethereum",
            price_oracle={"USDe": Decimal("0.999"), "USDC": Decimal("1")},
            config=config,
        )
        assert compiler._require_token_price("USDE") == Decimal("0.999")

    def test_lowercase_lookup_against_uppercase_key(self, config):
        """Lowercase lookup 'eth' should match oracle key 'ETH'."""
        compiler = IntentCompiler(
            chain="arbitrum",
            price_oracle={"ETH": Decimal("3400")},
            config=config,
        )
        assert compiler._require_token_price("eth") == Decimal("3400")

    def test_zero_price_not_matched(self, config):
        """Zero prices should not be returned by case-insensitive match."""
        compiler = IntentCompiler(
            chain="base",
            price_oracle={"cbETH": Decimal("0"), "USDC": Decimal("1")},
            config=config,
        )
        with pytest.raises(ValueError, match=r"missing|zero"):
            compiler._require_token_price("CBETH")

    def test_exact_match_takes_priority(self, config):
        """When both exact and case-insensitive matches exist, exact wins."""
        compiler = IntentCompiler(
            chain="base",
            price_oracle={
                "CBETH": Decimal("3300"),
                "cbETH": Decimal("3200"),
            },
            config=config,
        )
        # Exact match "CBETH" should win
        assert compiler._require_token_price("CBETH") == Decimal("3300")


class TestMarketSnapshotPriceOracleNormalization:
    """Verify that get_price_oracle_dict normalizes keys to uppercase."""

    def test_mixed_case_keys_normalized(self):
        """Pre-populated mixed-case keys should be uppercased."""
        from almanak.framework.strategies.intent_strategy import MarketSnapshot

        snapshot = MarketSnapshot(chain="base", wallet_address="0x1234")
        snapshot.set_price("cbETH", Decimal("3200"))
        snapshot.set_price("wstETH", Decimal("4100"))
        snapshot.set_price("ETH", Decimal("3400"))

        oracle = snapshot.get_price_oracle_dict()
        assert "CBETH" in oracle
        assert "WSTETH" in oracle
        assert "ETH" in oracle
        assert oracle["CBETH"] == Decimal("3200")
        assert oracle["WSTETH"] == Decimal("4100")

    def test_original_case_keys_not_present(self):
        """Original mixed-case keys should not appear if they differ from uppercase."""
        from almanak.framework.strategies.intent_strategy import MarketSnapshot

        snapshot = MarketSnapshot(chain="base", wallet_address="0x1234")
        snapshot.set_price("cbETH", Decimal("3200"))

        oracle = snapshot.get_price_oracle_dict()
        # Only uppercase key should exist
        assert "CBETH" in oracle
        assert "cbETH" not in oracle
