"""Drift perpetual futures intent tests for Solana.

Layer 1 (Compilation): Verifies the full perp intent compilation pipeline.
Drift adapter calls the Drift Data API (https://data.api.drift.trade) for
oracle prices to calculate base_asset_amount from size_usd.
Always runs -- no solana-test-validator required.

Layers 2-4 (Execution + Receipt + Balance Deltas): Require solana-test-validator.
Not implemented yet -- Drift perps require deposited collateral + account init,
which need on-chain infrastructure beyond the current test harness.

Run compilation tests:
    uv run pytest tests/intents/solana/test_drift_perp.py -v -s -k Compilation

Run all tests (needs solana-test-validator):
    uv run pytest tests/intents/solana/test_drift_perp.py -v -s
"""

import base64
from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import PerpCloseIntent, PerpOpenIntent
from tests.intents.solana.conftest import CHAIN_NAME


# =============================================================================
# Helper: Check if Drift Data API is reachable
# =============================================================================

_drift_api_ok: bool | None = None


def _check_drift_api() -> bool:
    """Check if Drift Data API is reachable (cached for session)."""
    global _drift_api_ok
    if _drift_api_ok is not None:
        return _drift_api_ok
    try:
        import requests

        # Use the same endpoint the DriftDataClient uses for oracle prices
        resp = requests.get("https://data.api.drift.trade/stats/markets", timeout=10)
        _drift_api_ok = resp.status_code == 200
    except Exception:
        _drift_api_ok = False
    return _drift_api_ok


requires_drift_api = pytest.mark.skipif(
    not _check_drift_api(),
    reason="Drift Data API not reachable",
)


# =============================================================================
# Layer 1: Compilation Tests — Perp Open
# =============================================================================


class TestDriftPerpOpenCompilation:
    """Drift Perp Open: PerpOpenIntent -> Compile -> ActionBundle."""

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_open_sol_long(self, solana_compiler):
        """PerpOpenIntent for SOL-PERP long compiles to a valid ActionBundle."""
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            max_slippage=Decimal("0.01"),
            protocol="drift",
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

        # Valid base64 that decodes to a Solana VersionedTransaction
        decoded = base64.b64decode(serialized_tx)
        assert len(decoded) > 100, f"Decoded tx too small ({len(decoded)} bytes)"

        from solders.transaction import VersionedTransaction

        tx = VersionedTransaction.from_bytes(decoded)
        assert tx is not None, "Must deserialize as a valid VersionedTransaction"

        # Metadata assertions
        metadata = bundle.metadata
        assert metadata.get("protocol") == "drift"
        assert metadata.get("chain") == "solana"
        assert metadata.get("chain_family") == "SOLANA"
        assert metadata.get("action") == "perp_open"
        assert metadata.get("direction") == "long"
        assert metadata.get("market") == "SOL-PERP"
        assert metadata.get("market_index") == 0
        assert metadata.get("order_type") == "market"
        assert metadata.get("size_usd") == "500"
        assert metadata.get("collateral_token") == "USDC"
        assert metadata.get("collateral_amount") == "100"
        assert metadata.get("leverage") == "5"

        # Oracle price must have been fetched
        oracle_price_str = metadata.get("oracle_price", "unknown")
        assert oracle_price_str != "unknown", "Oracle price must be resolved"
        oracle_price = Decimal(oracle_price_str)
        assert oracle_price > 0, "Oracle price must be positive"

        # Base asset amount must be calculated
        base_amount = int(metadata.get("base_asset_amount", "0"))
        assert base_amount > 0, "base_asset_amount must be positive"

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_open_short(self, solana_compiler):
        """PerpOpenIntent for SOL-PERP short compiles with direction='short'."""
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("50"),
            size_usd=Decimal("250"),
            is_long=False,
            leverage=Decimal("5"),
            protocol="drift",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

        bundle = result.action_bundle
        assert bundle.metadata.get("protocol") == "drift"
        assert bundle.metadata.get("direction") == "short"
        assert bundle.metadata.get("market_index") == 0

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_open_routes_correctly(self, solana_compiler):
        """PerpOpenIntent with protocol='drift' routes through Drift adapter."""
        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol="drift",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)
        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

        # Verify it went through Drift (not GMX)
        bundle = result.action_bundle
        assert bundle.metadata["protocol"] == "drift"
        assert bundle.metadata["chain_family"] == "SOLANA"

    @pytest.mark.asyncio
    async def test_compile_perp_open_non_solana_chain_fails(self):
        """Drift on a non-Solana chain should fail with clear error."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            config=config,
        )

        intent = PerpOpenIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol="drift",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "Solana" in result.error

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_open_market_alias(self, solana_compiler):
        """PerpOpenIntent with bare 'SOL' resolves to SOL-PERP (market_index=0)."""
        intent = PerpOpenIntent(
            market="SOL",  # Alias — adapter resolves to "SOL-PERP"
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            size_usd=Decimal("500"),
            is_long=True,
            leverage=Decimal("5"),
            protocol="drift",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

        metadata = result.action_bundle.metadata
        assert metadata.get("market") == "SOL-PERP"
        assert metadata.get("market_index") == 0


# =============================================================================
# Layer 1: Compilation Tests — Perp Close
# =============================================================================


class TestDriftPerpCloseCompilation:
    """Drift Perp Close: PerpCloseIntent -> Compile -> ActionBundle."""

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_close_partial(self, solana_compiler):
        """PerpCloseIntent with size_usd compiles to a partial close."""
        intent = PerpCloseIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal("250"),
            protocol="drift",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

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
        assert metadata.get("protocol") == "drift"
        assert metadata.get("action") == "perp_close"
        assert metadata.get("direction") == "long"
        assert metadata.get("market") == "SOL-PERP"
        assert metadata.get("market_index") == 0
        assert metadata.get("size_usd") == "250"
        assert metadata.get("reduce_only") is True

        # Base asset amount calculated from size_usd
        base_amount = int(metadata.get("base_asset_amount", "0"))
        assert base_amount > 0, "base_asset_amount must be positive for partial close"

    @requires_drift_api
    @pytest.mark.asyncio
    async def test_compile_perp_close_short_direction(self, solana_compiler):
        """PerpCloseIntent for short position compiles with correct direction."""
        intent = PerpCloseIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            is_long=False,
            size_usd=Decimal("100"),
            protocol="drift",
            chain=CHAIN_NAME,
        )

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None

        metadata = result.action_bundle.metadata
        assert metadata.get("direction") == "short"
        assert metadata.get("reduce_only") is True

    @pytest.mark.asyncio
    async def test_compile_perp_close_non_solana_chain_fails(self):
        """Drift perp close on a non-Solana chain should fail."""
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig

        config = IntentCompilerConfig(allow_placeholder_prices=True)
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            price_oracle={"ETH": Decimal("3000"), "USDC": Decimal("1")},
            config=config,
        )

        intent = PerpCloseIntent(
            market="SOL-PERP",
            collateral_token="USDC",
            is_long=True,
            size_usd=Decimal("500"),
            protocol="drift",
            chain="arbitrum",
        )

        result = compiler.compile(intent)
        assert result.status.value == "FAILED"
        assert "Solana" in result.error
