"""Solana demo strategy end-to-end compilation tests.

Exercises the full pipeline for each Solana demo strategy:
1. Load strategy class + config.json
2. Call decide() with a mock MarketSnapshot
3. Compile the resulting intent via solana_compiler
4. Verify the ActionBundle has valid serialized VersionedTransactions

These tests prove: strategy.decide() -> Intent -> compiler.compile() -> ActionBundle
-> valid serialized VersionedTransaction (via real APIs).
"""

import base64
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.intents.solana.conftest import CHAIN_NAME

# Strategy root
DEMO_DIR = Path(__file__).resolve().parents[3] / "strategies" / "demo"


def _make_mock_market() -> MagicMock:
    """Create a minimal mock MarketSnapshot for demo strategy decide() calls."""
    market = MagicMock()
    market.price.return_value = Decimal("87")
    market.balance.return_value = MagicMock(balance=Decimal("100"), balance_usd=Decimal("100"))
    return market


def _load_config(strategy_dir: str) -> dict:
    """Load config.json for a demo strategy."""
    config_path = DEMO_DIR / strategy_dir / "config.json"
    with open(config_path) as f:
        return json.load(f)


def _assert_valid_solana_tx(bundle) -> None:
    """Assert the ActionBundle contains a valid deserializable VersionedTransaction."""
    assert bundle.transactions, "Bundle must contain transactions"

    tx_data = bundle.transactions[0]
    serialized_tx = tx_data.get("serialized_transaction", "")
    assert serialized_tx, "Transaction must have serialized_transaction"

    # Verify it's valid base64
    decoded = base64.b64decode(serialized_tx)
    assert len(decoded) > 50, f"Decoded tx too small ({len(decoded)} bytes)"

    # Verify deserialization with solders
    from solders.transaction import VersionedTransaction

    tx = VersionedTransaction.from_bytes(decoded)
    assert tx is not None, "Must deserialize as a valid VersionedTransaction"


class TestSolanaSwapDemoStrategy:
    """End-to-end: SolanaSwapStrategy.decide() -> compile -> valid ActionBundle."""

    @pytest.mark.asyncio
    async def test_decide_returns_swap_intent(self):
        """SolanaSwapStrategy.decide() returns a SwapIntent from config."""
        from strategies.demo.solana_swap.strategy import SolanaSwapStrategy

        config = _load_config("solana_swap")
        strategy = SolanaSwapStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)

        from almanak.framework.intents.vocabulary import SwapIntent

        assert isinstance(intent, SwapIntent), f"Expected SwapIntent, got {type(intent).__name__}"
        assert intent.from_token == config["from_token"]
        assert intent.to_token == config["to_token"]

    @pytest.mark.asyncio
    async def test_compile_swap_intent(self, solana_compiler):
        """SwapIntent from demo strategy compiles to a valid ActionBundle."""
        from strategies.demo.solana_swap.strategy import SolanaSwapStrategy

        config = _load_config("solana_swap")
        strategy = SolanaSwapStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)
        # Compiler uses its own chain="solana" when intent.chain is None

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "SWAP"
        assert result.action_bundle.metadata.get("protocol") == "jupiter"

        _assert_valid_solana_tx(result.action_bundle)

    @pytest.mark.asyncio
    async def test_strategy_metadata(self):
        """SolanaSwapStrategy has correct decorator metadata."""
        from strategies.demo.solana_swap.strategy import SolanaSwapStrategy

        assert SolanaSwapStrategy.STRATEGY_NAME == "solana_swap"
        metadata = SolanaSwapStrategy.STRATEGY_METADATA
        assert metadata is not None
        assert "solana" in metadata.supported_chains
        assert "jupiter" in metadata.supported_protocols
        assert "SWAP" in metadata.intent_types


class TestSolanaLendDemoStrategy:
    """End-to-end: SolanaLendStrategy.decide() -> compile -> valid ActionBundle."""

    @pytest.mark.asyncio
    async def test_decide_returns_supply_intent(self):
        """SolanaLendStrategy.decide() returns a SupplyIntent from config."""
        from strategies.demo.solana_lend.strategy import SolanaLendStrategy

        config = _load_config("solana_lend")
        strategy = SolanaLendStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)

        from almanak.framework.intents.vocabulary import SupplyIntent

        assert isinstance(intent, SupplyIntent), f"Expected SupplyIntent, got {type(intent).__name__}"
        assert intent.token == config["token"]
        assert intent.amount == Decimal(config["amount"])

    @pytest.mark.asyncio
    async def test_compile_supply_intent(self, solana_compiler):
        """SupplyIntent from demo strategy compiles to a valid ActionBundle."""
        from strategies.demo.solana_lend.strategy import SolanaLendStrategy

        config = _load_config("solana_lend")
        strategy = SolanaLendStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)
        # Compiler uses its own chain="solana" when intent.chain is None

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "SUPPLY"
        assert result.action_bundle.metadata.get("protocol") == "kamino"

        _assert_valid_solana_tx(result.action_bundle)

    @pytest.mark.asyncio
    async def test_strategy_metadata(self):
        """SolanaLendStrategy has correct decorator metadata."""
        from strategies.demo.solana_lend.strategy import SolanaLendStrategy

        assert SolanaLendStrategy.STRATEGY_NAME == "solana_lend"
        metadata = SolanaLendStrategy.STRATEGY_METADATA
        assert metadata is not None
        assert "solana" in metadata.supported_chains
        assert "kamino" in metadata.supported_protocols
        assert "SUPPLY" in metadata.intent_types


class TestSolanaLPDemoStrategy:
    """End-to-end: SolanaLPStrategy.decide() -> compile -> valid ActionBundle."""

    @pytest.mark.asyncio
    async def test_decide_returns_lp_open_intent(self):
        """SolanaLPStrategy.decide() returns an LPOpenIntent from config."""
        from strategies.demo.solana_lp.strategy import SolanaLPStrategy

        config = _load_config("solana_lp")
        strategy = SolanaLPStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)

        from almanak.framework.intents.vocabulary import LPOpenIntent

        assert isinstance(intent, LPOpenIntent), f"Expected LPOpenIntent, got {type(intent).__name__}"
        assert intent.pool == config["pool"]
        assert intent.range_lower == Decimal(config["range_lower"])
        assert intent.range_upper == Decimal(config["range_upper"])

    @pytest.mark.asyncio
    async def test_compile_lp_open_intent(self, solana_compiler):
        """LPOpenIntent from demo strategy compiles to a valid ActionBundle."""
        from strategies.demo.solana_lp.strategy import SolanaLPStrategy

        config = _load_config("solana_lp")
        strategy = SolanaLPStrategy(config=config, chain="solana", wallet_address="test_wallet")
        market = _make_mock_market()

        intent = strategy.decide(market)
        # Compiler uses its own chain="solana" when intent.chain is None

        result = solana_compiler.compile(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "LP_OPEN"
        assert result.action_bundle.metadata.get("protocol") == "raydium_clmm"
        assert result.action_bundle.metadata.get("pool") == config["pool"]

        _assert_valid_solana_tx(result.action_bundle)

    @pytest.mark.asyncio
    async def test_lp_range_captures_current_price(self):
        """LP config range [50, 150] captures the current SOL price (~$87)."""
        config = _load_config("solana_lp")
        range_lower = Decimal(config["range_lower"])
        range_upper = Decimal(config["range_upper"])

        # SOL is ~$87 as of 2026-03-01
        current_sol_price = Decimal("87")
        assert range_lower < current_sol_price < range_upper, (
            f"LP range [{range_lower}, {range_upper}] must capture SOL price ~${current_sol_price}"
        )

    @pytest.mark.asyncio
    async def test_strategy_metadata(self):
        """SolanaLPStrategy has correct decorator metadata."""
        from strategies.demo.solana_lp.strategy import SolanaLPStrategy

        assert SolanaLPStrategy.STRATEGY_NAME == "solana_lp"
        metadata = SolanaLPStrategy.STRATEGY_METADATA
        assert metadata is not None
        assert "solana" in metadata.supported_chains
        assert "raydium_clmm" in metadata.supported_protocols
        assert "LP_OPEN" in metadata.intent_types
