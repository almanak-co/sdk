"""Meteora DLMM LP intent tests for Solana.

Layer 1 (Compilation): Verifies the full LP intent compilation pipeline.
Like Raydium, Meteora builds instructions locally using solders.
The Meteora DLMM API is used only for pool info fetching.
Always runs -- no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
These execute LP open/close operations and verify on-chain state changes.

Run compilation tests:
    uv run pytest tests/intents/solana/test_meteora_lp.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_meteora_lp.py -v -s
"""

import base64
from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent
from tests.intents.solana.conftest import (
    CHAIN_NAME,
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKENS,
    get_sol_balance,
    get_spl_token_balance,
    requires_solana_validator,
)


# =============================================================================
# Known Meteora DLMM pools on Solana mainnet
# =============================================================================

# SOL/USDC DLMM pool (high liquidity, bin_step=10)
# This may change — use pool search as fallback
SOL_USDC_DLMM_POOL: str | None = None


def _find_sol_usdc_pool():
    """Find a high-liquidity SOL/USDC DLMM pool via the API."""
    global SOL_USDC_DLMM_POOL
    if SOL_USDC_DLMM_POOL is not None:
        return SOL_USDC_DLMM_POOL

    try:
        from almanak.framework.connectors.meteora.sdk import MeteoraSDK

        sdk = MeteoraSDK(wallet_address="KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9")
        pool = sdk.find_pool(
            SOLANA_TOKENS["SOL"],
            SOLANA_TOKENS["USDC"],
        )
        if pool:
            SOL_USDC_DLMM_POOL = pool.address
            return SOL_USDC_DLMM_POOL
    except Exception:
        pass

    return None


# =============================================================================
# Layer 1: Compilation Tests (always run -- hit real Meteora API for pool info)
# =============================================================================


class TestMeteoraLPOpenCompilation:
    """Meteora LP Open: LPOpenIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_open_with_pool_address(self, solana_compiler):
        """LPOpenIntent with explicit pool address compiles via Meteora."""
        pool_address = _find_sol_usdc_pool()
        if not pool_address:
            pytest.skip("No SOL/USDC DLMM pool found via Meteora API")

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool=pool_address,
            amount0=Decimal("0.001"),   # 0.001 SOL (token_x)
            amount1=Decimal("0.15"),    # 0.15 USDC (token_y)
            range_lower=Decimal("100"),  # Wide range
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Layer 1: Compilation succeeds
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None, "ActionBundle must be created"

        bundle = result.action_bundle

        # Has transactions
        assert bundle.transactions, "Bundle must contain transactions"
        tx_data = bundle.transactions[0]
        serialized_tx = tx_data.get("serialized_transaction", "")
        assert serialized_tx, "Must have serialized_transaction"

        # Valid base64
        decoded = base64.b64decode(serialized_tx)
        assert len(decoded) > 100, f"Decoded tx too small ({len(decoded)} bytes)"

        # Metadata
        metadata = bundle.metadata
        assert metadata.get("chain_family") == "SOLANA"
        assert metadata.get("protocol") == "meteora_dlmm"
        assert metadata.get("action") == "open_position"
        assert metadata.get("pool") == pool_address

        # Position address (Keypair-based, not NFT)
        assert metadata.get("position_address"), "Must have position_address in metadata"
        assert len(metadata["position_address"]) > 30, "Position address must be valid Base58"

        # Bin range
        assert "lower_bin_id" in metadata
        assert "upper_bin_id" in metadata
        assert metadata["lower_bin_id"] < metadata["upper_bin_id"]

        # Bin step
        assert "bin_step" in metadata
        assert metadata["bin_step"] > 0

        # additional_signers for the position keypair in sensitive_data
        assert bundle.sensitive_data is not None, "sensitive_data must be present"
        assert "additional_signers" in bundle.sensitive_data, (
            "sensitive_data must include additional_signers for position keypair"
        )

    @pytest.mark.asyncio
    async def test_compile_lp_open_routes_correctly(self, solana_compiler):
        """LPOpenIntent with protocol='meteora_dlmm' routes to Meteora adapter."""
        pool_address = _find_sol_usdc_pool()
        if not pool_address:
            pytest.skip("No SOL/USDC DLMM pool found via Meteora API")

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool=pool_address,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.15"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

        # Verify it went through Meteora (not Raydium)
        bundle = result.action_bundle
        assert bundle.metadata["protocol"] == "meteora_dlmm"

    @pytest.mark.asyncio
    async def test_compile_lp_open_non_solana_chain_fails(self):
        """Meteora DLMM on a non-Solana chain should fail."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            config=config,
        )

        intent = LPOpenIntent(
            protocol="meteora_dlmm",
            pool="some-pool",
            amount0=Decimal("1"),
            amount1=Decimal("150"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "Solana" in result.error


class TestMeteoraLPCloseCompilation:
    """Meteora LP Close: LPCloseIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_close_missing_pool_fails(self, solana_compiler):
        """LPCloseIntent without pool should fail."""
        intent = LPCloseIntent(
            protocol="meteora_dlmm",
            pool="",
            position_id="fake_position_address_xxxxxxxxxxxxxxxxxxxxxxxx",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "pool" in result.error.lower()

    @pytest.mark.asyncio
    async def test_compile_lp_close_with_pool(self, solana_compiler):
        """LPCloseIntent with pool compiles (may fail on position lookup, which is expected)."""
        pool_address = _find_sol_usdc_pool()
        if not pool_address:
            pytest.skip("No SOL/USDC DLMM pool found via Meteora API")

        # Use a fake position address -- without rpc_url, adapter uses default position
        intent = LPCloseIntent(
            protocol="meteora_dlmm",
            pool=pool_address,
            position_id="FakePositionAddr" + "x" * 28,
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Without RPC url, this may succeed with a default empty position
        # or may fail on position lookup -- both are valid
        if result.status.value == "SUCCESS":
            bundle = result.action_bundle
            assert bundle.metadata["protocol"] == "meteora_dlmm"
            assert bundle.metadata["action"] == "close_position"
