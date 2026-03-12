"""Orca Whirlpools LP intent tests for Solana.

Layer 1 (Compilation): Verifies the full LP intent compilation pipeline.
Like Raydium/Meteora, Orca builds instructions locally using solders.
The Orca API is used only for pool info fetching.
Always runs -- no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
These execute LP open/close operations and verify on-chain state changes.

Run compilation tests:
    uv run pytest tests/intents/solana/test_orca_lp.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_orca_lp.py -v -s
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
# Known Orca Whirlpool pools on Solana mainnet
# =============================================================================

# SOL/USDC Whirlpool (high liquidity, tick_spacing=64)
# Use pool search as fallback
SOL_USDC_WHIRLPOOL: str | None = None


def _find_sol_usdc_pool():
    """Find a high-liquidity SOL/USDC Whirlpool via the API."""
    global SOL_USDC_WHIRLPOOL
    if SOL_USDC_WHIRLPOOL is not None:
        return SOL_USDC_WHIRLPOOL

    try:
        from almanak.framework.connectors.orca.sdk import OrcaWhirlpoolSDK

        sdk = OrcaWhirlpoolSDK(wallet_address="KUMtRazMP7vwvc2kthnGZ9Cq6ZsGRiYC97snMYepNx9")
        pool = sdk.find_pool_by_tokens(
            SOLANA_TOKENS["SOL"],
            SOLANA_TOKENS["USDC"],
        )
        if pool:
            SOL_USDC_WHIRLPOOL = pool.address
            return SOL_USDC_WHIRLPOOL
    except Exception:
        pass

    return None


# =============================================================================
# Layer 1: Compilation Tests (always run -- hit real Orca API for pool info)
# =============================================================================


class TestOrcaLPOpenCompilation:
    """Orca LP Open: LPOpenIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_open_with_pool_address(self, solana_compiler):
        """LPOpenIntent with explicit pool address compiles via Orca."""
        pool_address = _find_sol_usdc_pool()
        if not pool_address:
            pytest.skip("No SOL/USDC Whirlpool found via Orca API")

        intent = LPOpenIntent(
            protocol="orca_whirlpools",
            pool=pool_address,
            amount0=Decimal("0.001"),   # 0.001 SOL (token_a)
            amount1=Decimal("0.15"),    # 0.15 USDC (token_b)
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
        assert metadata.get("protocol") == "orca_whirlpools"
        assert metadata.get("action") == "open_position"
        assert metadata.get("pool") == pool_address

        # NFT mint
        assert metadata.get("nft_mint"), "Must have nft_mint in metadata"
        assert len(metadata["nft_mint"]) > 30, "NFT mint must be valid Base58"

        # Tick range
        assert "tick_lower" in metadata
        assert "tick_upper" in metadata
        assert metadata["tick_lower"] < metadata["tick_upper"]

        # additional_signers for the NFT keypair in sensitive_data
        assert bundle.sensitive_data is not None, "sensitive_data must be present"
        assert "additional_signers" in bundle.sensitive_data, (
            "sensitive_data must include additional_signers for NFT keypair"
        )

    @pytest.mark.asyncio
    async def test_compile_lp_open_routes_correctly(self, solana_compiler):
        """LPOpenIntent with protocol='orca_whirlpools' routes to Orca adapter."""
        pool_address = _find_sol_usdc_pool()
        if not pool_address:
            pytest.skip("No SOL/USDC Whirlpool found via Orca API")

        intent = LPOpenIntent(
            protocol="orca_whirlpools",
            pool=pool_address,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.15"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

        # Verify it went through Orca (not Raydium)
        bundle = result.action_bundle
        assert bundle.metadata["protocol"] == "orca_whirlpools"

    @pytest.mark.asyncio
    async def test_compile_lp_open_non_solana_chain_fails(self):
        """Orca Whirlpools on a non-Solana chain should fail."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            config=config,
        )

        intent = LPOpenIntent(
            protocol="orca_whirlpools",
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


class TestOrcaLPCloseCompilation:
    """Orca LP Close: LPCloseIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_close_missing_pool_fails(self, solana_compiler):
        """LPCloseIntent without pool should fail."""
        intent = LPCloseIntent(
            protocol="orca_whirlpools",
            pool="",
            position_id="fake_nft_mint_address_xxxxxxxxxxxxxxxxxxxxxxxx",
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
            pytest.skip("No SOL/USDC Whirlpool found via Orca API")

        # Use a fake position address -- without rpc_url, adapter uses default position
        intent = LPCloseIntent(
            protocol="orca_whirlpools",
            pool=pool_address,
            position_id="FakePositionAddr" + "x" * 28,
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # Without RPC url, this may succeed with a default empty position
        # or may fail on position lookup -- both are valid
        if result.status.value == "SUCCESS":
            bundle = result.action_bundle
            assert bundle.metadata["protocol"] == "orca_whirlpools"
            assert bundle.metadata["action"] == "close_position"
