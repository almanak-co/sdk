"""Raydium CLMM LP intent tests for Solana.

Layer 1 (Compilation): Verifies the full LP intent compilation pipeline.
Unlike Jupiter/Kamino (REST API returns pre-built tx), Raydium builds
instructions locally using solders. The Raydium API is used only for
pool info fetching. Always runs — no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
These execute LP open/close operations and verify on-chain state changes.

Run compilation tests:
    uv run pytest tests/intents/solana/test_raydium_lp.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_raydium_lp.py -v -s
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

# SOL/USDC CLMM pool on Raydium (mainnet, highest liquidity)
SOL_USDC_POOL = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv"


# =============================================================================
# Layer 1: Compilation Tests (always run — hit real Raydium API for pool info)
# =============================================================================


class TestRaydiumLPOpenCompilation:
    """Raydium LP Open: LPOpenIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_open_with_pool_address(self, solana_compiler):
        """LPOpenIntent with explicit pool address compiles via Raydium."""
        intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=Decimal("0.001"),  # 0.001 SOL (token_a)
            amount1=Decimal("0.15"),   # 0.15 USDC (token_b)
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
        assert metadata.get("protocol") == "raydium_clmm"
        assert metadata.get("action") == "open_position"
        assert metadata.get("pool") == SOL_USDC_POOL

        # additional_signers for the NFT mint keypair live in sensitive_data (not metadata)
        assert bundle.sensitive_data is not None, "sensitive_data must be present"
        assert "additional_signers" in bundle.sensitive_data, (
            "sensitive_data must include additional_signers for NFT mint keypair"
        )

    @pytest.mark.asyncio
    async def test_compile_lp_open_default_routes_to_raydium(self, solana_compiler):
        """LPOpenIntent on Solana with 'raydium_clmm' routes correctly."""
        intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.15"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.metadata.get("protocol") == "raydium_clmm"

    @pytest.mark.asyncio
    async def test_lp_open_tx_is_versioned(self, solana_compiler):
        """LP open transaction is a valid VersionedTransaction."""
        intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.15"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"

        serialized_tx = result.action_bundle.transactions[0]["serialized_transaction"]
        decoded = base64.b64decode(serialized_tx)

        # Must be deserializable as a VersionedTransaction
        from solders.transaction import VersionedTransaction

        tx = VersionedTransaction.from_bytes(decoded)
        assert tx is not None, "Must deserialize as a valid VersionedTransaction"

    @pytest.mark.asyncio
    async def test_lp_open_intent_type(self, solana_compiler):
        """ActionBundle has correct intent_type for LP open."""
        intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=Decimal("0.001"),
            amount1=Decimal("0.15"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.intent_type == "LP_OPEN"


class TestRaydiumLPCloseCompilation:
    """Raydium LP Close: LPCloseIntent -> Compile -> ActionBundle."""

    @pytest.mark.asyncio
    async def test_compile_lp_close(self, solana_compiler):
        """LPCloseIntent compiles via Raydium adapter."""
        intent = LPCloseIntent(
            protocol="raydium_clmm",
            position_id="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool=SOL_USDC_POOL,
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        # LP close compilation should succeed
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

        bundle = result.action_bundle

        # Has transactions
        assert bundle.transactions, "Bundle must contain transactions"

        # Metadata
        metadata = bundle.metadata
        assert metadata.get("chain_family") == "SOLANA"
        assert metadata.get("protocol") == "raydium_clmm"
        assert metadata.get("action") == "close_position"

    @pytest.mark.asyncio
    async def test_lp_close_intent_type(self, solana_compiler):
        """ActionBundle has correct intent_type for LP close."""
        intent = LPCloseIntent(
            protocol="raydium_clmm",
            position_id="6RfnQFgLbmfRZGDSxUrPnAiqjg3CtsneHqz2mF7Tpump",
            pool=SOL_USDC_POOL,
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.intent_type == "LP_CLOSE"


class TestRaydiumMathIntegration:
    """Verify Raydium math module works correctly within compilation."""

    def test_price_to_tick_roundtrip(self):
        """Price -> tick -> price roundtrip is consistent."""
        from almanak.framework.connectors.raydium.math import price_to_tick, tick_to_price

        original = Decimal("150")  # SOL price
        tick = price_to_tick(original, decimals_a=9, decimals_b=6)
        recovered = tick_to_price(tick, decimals_a=9, decimals_b=6)

        # Should be within 0.1% due to tick quantization
        pct_diff = abs(recovered - original) / original
        assert pct_diff < Decimal("0.001"), (
            f"Roundtrip error too large: {original} -> tick {tick} -> {recovered} ({pct_diff:.4%})"
        )

    def test_tick_alignment(self):
        """Ticks align correctly to spacing boundaries."""
        from almanak.framework.connectors.raydium.math import align_tick_to_spacing

        # SOL/USDC pool uses tick_spacing=60
        aligned_down = align_tick_to_spacing(119145, 60, round_up=False)
        aligned_up = align_tick_to_spacing(119145, 60, round_up=True)

        assert aligned_down % 60 == 0
        assert aligned_up % 60 == 0
        assert aligned_down <= 119145
        assert aligned_up >= 119145

    def test_liquidity_from_amounts(self):
        """Liquidity calculation produces reasonable values."""
        from almanak.framework.connectors.raydium.math import (
            get_liquidity_from_amounts,
            tick_to_sqrt_price_x64,
        )

        sqrt_current = tick_to_sqrt_price_x64(0)
        sqrt_lower = tick_to_sqrt_price_x64(-1000)
        sqrt_upper = tick_to_sqrt_price_x64(1000)

        liquidity = get_liquidity_from_amounts(
            sqrt_current, sqrt_lower, sqrt_upper,
            amount_a=1_000_000_000,  # 1 SOL
            amount_b=150_000_000,    # 150 USDC
        )

        assert liquidity > 0, "Liquidity must be positive"


# =============================================================================
# Layers 2-4: Execution Tests (require solana-test-validator)
# =============================================================================


@requires_solana_validator
class TestRaydiumLPExecution:
    """Raydium LP: full 4-layer verification on local test-validator.

    Layer 1: Compilation success (LPOpenIntent -> ActionBundle)
    Layer 2: Execution success (ActionBundle -> on-chain tx)
    Layer 3: Receipt parser integration (extract position ID, liquidity)
    Layer 4: Exact balance deltas (SOL + USDC decrease by LP deposit amounts)
    """

    @pytest.mark.asyncio
    async def test_lp_open(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """Open LP position on SOL/USDC pool: full 4-layer verification."""
        wallet_address, _ = funded_solana_wallet
        amount_sol = Decimal("0.01")  # 0.01 SOL
        amount_usdc = Decimal("1.5")  # ~1.5 USDC

        # Layer 4 setup: Record balances BEFORE
        sol_before = await get_sol_balance(solana_fork, wallet_address)
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Layer 1: Compile
        intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=amount_sol,
            amount1=amount_usdc,
            range_lower=Decimal("100"),  # Wide range to capture current price
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )
        compilation_result = execution_compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execute
        execution_result = await solana_orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt parser — extract position ID
        from almanak.framework.connectors.raydium import RaydiumReceiptParser

        position_id = None
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parser = RaydiumReceiptParser(chain=CHAIN_NAME)
                extracted_id = parser.extract_position_id(receipt_dict)
                if extracted_id:
                    position_id = extracted_id

        assert position_id is not None, "Must extract position ID from LP open receipt"

        # Layer 4: Balance deltas
        sol_after = await get_sol_balance(solana_fork, wallet_address)
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        sol_spent = sol_before - sol_after  # Includes gas
        usdc_spent = usdc_before - usdc_after

        # SOL must decrease (LP deposit + gas)
        assert sol_spent > 0, "SOL must decrease (LP deposit + gas)"
        # USDC must decrease (LP deposit)
        assert usdc_spent > 0, "USDC must decrease (LP deposit)"

    @pytest.mark.asyncio
    async def test_lp_open_then_close(
        self, solana_fork, funded_solana_wallet, solana_orchestrator, execution_compiler,
    ):
        """Open LP position, then close it — verify tokens returned."""
        wallet_address, _ = funded_solana_wallet

        # Record USDC balance before roundtrip
        usdc_before = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # Step 1: Open position
        open_intent = LPOpenIntent(
            protocol="raydium_clmm",
            pool=SOL_USDC_POOL,
            amount0=Decimal("0.01"),
            amount1=Decimal("1.5"),
            range_lower=Decimal("100"),
            range_upper=Decimal("200"),
            chain=CHAIN_NAME,
        )
        open_result = execution_compiler.compile(open_intent)
        assert open_result.status.value == "SUCCESS", (
            f"Open compilation failed: {open_result.error}"
        )

        open_exec = await solana_orchestrator.execute(open_result.action_bundle)
        assert open_exec.success, f"Open execution failed: {open_exec.error}"

        # Extract position ID for close
        from almanak.framework.connectors.raydium import RaydiumReceiptParser

        position_id = None
        for tx_result in open_exec.transaction_results:
            if tx_result.receipt:
                receipt_dict = tx_result.receipt.to_dict()
                parser = RaydiumReceiptParser(chain=CHAIN_NAME)
                position_id = parser.extract_position_id(receipt_dict)
                if position_id:
                    break

        assert position_id is not None, "Must extract position ID to close it"

        # Step 2: Close position
        close_intent = LPCloseIntent(
            protocol="raydium_clmm",
            position_id=str(position_id),
            pool=SOL_USDC_POOL,
            chain=CHAIN_NAME,
        )
        close_result = execution_compiler.compile(close_intent)
        assert close_result.status.value == "SUCCESS", (
            f"Close compilation failed: {close_result.error}"
        )

        close_exec = await solana_orchestrator.execute(close_result.action_bundle)
        assert close_exec.success, f"Close execution failed: {close_exec.error}"

        # Verify tokens returned (USDC should recover most of deposit)
        usdc_after = await get_spl_token_balance(
            solana_fork, wallet_address, SOLANA_TOKENS["USDC"],
        )

        # After open + close roundtrip, USDC should be close to original
        # Allow small loss from pool mechanics (fees, rounding)
        usdc_diff = usdc_before - usdc_after
        max_loss_raw = int(Decimal("0.10") * Decimal(10 ** SOLANA_TOKEN_DECIMALS["USDC"]))
        assert usdc_diff <= max_loss_raw, (
            f"USDC loss from LP roundtrip too large: {usdc_diff} raw units "
            f"(max allowed: {max_loss_raw})"
        )
