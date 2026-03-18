"""Multi-chain unit tests for unwrap native compilation.

Validates the _compile_unwrap_native path on Arbitrum, Base, Ethereum,
Optimism, Polygon, Avalanche, and BSC to catch chain-specific configuration gaps.

VIB-1448: Fix ax unwrap action across chains.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.vocabulary import UnwrapNativeIntent

# ============================================================================
# Chain-specific wrapped native tokens
# ============================================================================
CHAIN_WRAPPED_TOKENS = {
    "arbitrum": "WETH",
    "base": "WETH",
    "ethereum": "WETH",
    "optimism": "WETH",
    "polygon": "WMATIC",
    "avalanche": "WAVAX",
    "bsc": "WBNB",
}


@pytest.fixture(params=list(CHAIN_WRAPPED_TOKENS.keys()))
def chain_and_token(request):
    """Parametrized fixture yielding (chain, wrapped_token) tuples."""
    chain = request.param
    return chain, CHAIN_WRAPPED_TOKENS[chain]


@pytest.fixture()
def make_compiler():
    """Factory for creating chain-specific compilers."""

    def _make(chain: str) -> IntentCompiler:
        return IntentCompiler(
            chain=chain,
            wallet_address="0x" + "a" * 40,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

    return _make


class TestUnwrapMultiChain:
    """Validate unwrap compilation succeeds on every supported chain."""

    def test_unwrap_compiles_on_chain(self, chain_and_token, make_compiler):
        """Unwrap intent compiles successfully on each chain with sufficient balance."""
        chain, token = chain_and_token
        compiler = make_compiler(chain)
        intent = UnwrapNativeIntent(token=token, amount=Decimal("0.001"), chain=chain)

        with patch.object(compiler, "_query_erc20_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", (
            f"Unwrap compilation failed on {chain}/{token}: {result.error}"
        )
        assert result.action_bundle is not None
        txs = result.action_bundle.transactions
        assert len(txs) == 1
        # Verify calldata starts with withdraw(uint256) selector
        assert txs[0]["data"].startswith("0x2e1a7d4d")

    def test_unwrap_all_compiles_on_chain(self, chain_and_token, make_compiler):
        """Unwrap amount='all' compiles successfully on each chain."""
        chain, token = chain_and_token
        compiler = make_compiler(chain)
        intent = UnwrapNativeIntent(token=token, amount="all", chain=chain)

        with patch.object(compiler, "_query_erc20_balance", return_value=5 * 10**17):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS", (
            f"Unwrap all failed on {chain}/{token}: {result.error}"
        )

    def test_unwrap_zero_balance_fails(self, chain_and_token, make_compiler):
        """Unwrap with amount='all' fails when balance is zero."""
        chain, token = chain_and_token
        compiler = make_compiler(chain)
        intent = UnwrapNativeIntent(token=token, amount="all", chain=chain)

        with patch.object(compiler, "_query_erc20_balance", return_value=0):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "No" in result.error and "balance" in result.error


class TestUnwrapEdgeCases:
    """Test edge cases for unwrap compilation."""

    def test_wrong_token_symbol_fails(self, make_compiler):
        """Unwrap with non-wrapped-native token (e.g. USDC) should fail."""
        compiler = make_compiler("arbitrum")
        intent = UnwrapNativeIntent(token="USDC", amount=Decimal("100"), chain="arbitrum")

        with patch.object(compiler, "_query_erc20_balance", return_value=10**20):
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "not the wrapped native token" in result.error

    def test_balance_query_failure_still_compiles(self, make_compiler):
        """When balance query returns None, compilation proceeds (no pre-flight)."""
        compiler = make_compiler("arbitrum")
        intent = UnwrapNativeIntent(token="WETH", amount=Decimal("0.001"), chain="arbitrum")

        with patch.object(compiler, "_query_erc20_balance", return_value=None):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"

    def test_insufficient_balance_gives_clear_error(self, make_compiler):
        """When wallet has less than requested, error states both amounts."""
        compiler = make_compiler("base")
        intent = UnwrapNativeIntent(token="WETH", amount=Decimal("1.0"), chain="base")

        with patch.object(compiler, "_query_erc20_balance", return_value=10**17):  # 0.1 WETH
            result = compiler.compile(intent)

        assert result.status.value == "FAILED"
        assert "Insufficient WETH" in result.error
        assert "0.1" in result.error
        assert "1.0" in result.error

    def test_calldata_encodes_correct_amount(self, make_compiler):
        """Verify withdraw calldata encodes the correct wei amount."""
        compiler = make_compiler("arbitrum")
        intent = UnwrapNativeIntent(token="WETH", amount=Decimal("0.001"), chain="arbitrum")

        with patch.object(compiler, "_query_erc20_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        calldata = result.action_bundle.transactions[0]["data"]
        # 0.001 ETH = 10^15 wei = 0x38D7EA4C68000
        amount_hex = calldata[10:]  # Skip selector (0x2e1a7d4d)
        amount_int = int(amount_hex, 16)
        assert amount_int == 10**15

    def test_metadata_includes_token_and_chain(self, make_compiler):
        """Action bundle metadata should include token and chain info."""
        compiler = make_compiler("polygon")
        intent = UnwrapNativeIntent(token="WMATIC", amount=Decimal("1.0"), chain="polygon")

        with patch.object(compiler, "_query_erc20_balance", return_value=10**18):
            result = compiler.compile(intent)

        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["token"] == "WMATIC"
        assert metadata["chain"] == "polygon"

    def test_wrapped_native_address_resolution(self, make_compiler):
        """Verify the compiler resolves the correct wrapped native address per chain."""
        for chain, expected_token in CHAIN_WRAPPED_TOKENS.items():
            compiler = make_compiler(chain)
            address = compiler._get_wrapped_native_address()
            assert address is not None, f"No wrapped native address for {chain}"
            assert address.startswith("0x"), f"Invalid address for {chain}: {address}"
            assert len(address) == 42, f"Wrong address length for {chain}: {address}"
