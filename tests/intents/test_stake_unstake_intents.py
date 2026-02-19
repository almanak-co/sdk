"""Tests for StakeIntent and UnstakeIntent compilation in IntentCompiler.

Tests verify that IntentCompiler correctly routes stake/unstake intents
to the appropriate protocol adapters (Lido, Ethena).

To run:
    uv run pytest tests/intents/test_stake_unstake_intents.py -v
"""

from decimal import Decimal

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import StakeIntent, UnstakeIntent

# =============================================================================
# Test Configuration
# =============================================================================

TEST_WALLET = "0x1234567890123456789012345678901234567890"


# =============================================================================
# StakeIntent Compilation Tests
# =============================================================================


class TestCompileStakeIntentLido:
    """Test StakeIntent compilation for Lido protocol."""

    def test_compile_stake_intent_lido_success(self):
        """Test successful compilation of Lido stake intent."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=True,  # Get wstETH
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "STAKE"
        # Wrapped (wstETH) requires 3 transactions: stake, approve, wrap
        assert len(result.transactions) == 3
        assert result.total_gas_estimate > 0

    def test_compile_stake_intent_lido_unwrapped(self):
        """Test Lido stake intent for unwrapped stETH."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=False,  # Get stETH
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        # Unwrapped (stETH) requires 1 transaction: stake only
        assert len(result.transactions) == 1

    def test_compile_stake_intent_lido_wrong_chain(self):
        """Test that Lido stake fails on non-Ethereum chains."""
        compiler = IntentCompiler(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Ethereum mainnet" in result.error

    def test_compile_stake_intent_lido_chained_amount_fails(self):
        """Test that chained amount='all' fails with clear error."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount="all",  # type: ignore[arg-type]
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "resolved" in result.error.lower()


class TestCompileStakeIntentEthena:
    """Test StakeIntent compilation for Ethena protocol."""

    def test_compile_stake_intent_ethena_success(self):
        """Test successful compilation of Ethena stake intent."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "STAKE"
        # Ethena stake requires 2 transactions: approve + stake
        assert len(result.transactions) == 2
        assert result.total_gas_estimate > 0

    def test_compile_stake_intent_ethena_wrong_chain(self):
        """Test that Ethena stake fails on non-Ethereum chains."""
        compiler = IntentCompiler(
            chain="base",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Ethereum mainnet" in result.error


class TestCompileStakeIntentUnsupported:
    """Test StakeIntent compilation for unsupported protocols."""

    def test_compile_stake_intent_unsupported_protocol(self):
        """Test that unsupported protocol fails with clear error."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="rocketpool",  # Not implemented
            token_in="ETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unsupported staking protocol" in result.error
        assert "lido" in result.error.lower()
        assert "ethena" in result.error.lower()


# =============================================================================
# UnstakeIntent Compilation Tests
# =============================================================================


class TestCompileUnstakeIntentLido:
    """Test UnstakeIntent compilation for Lido protocol."""

    def test_compile_unstake_intent_lido_wsteth(self):
        """Test Lido unstake from wstETH (requires unwrap first)."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "UNSTAKE"
        # wstETH unstake: unwrap + request withdrawal = 2 txs
        assert len(result.transactions) == 2
        assert result.total_gas_estimate > 0

    def test_compile_unstake_intent_lido_steth(self):
        """Test Lido unstake from stETH directly."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="lido",
            token_in="stETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        # stETH unstake: request withdrawal only = 1 tx
        assert len(result.transactions) == 1

    def test_compile_unstake_intent_lido_wrong_chain(self):
        """Test that Lido unstake fails on non-Ethereum chains."""
        compiler = IntentCompiler(
            chain="polygon",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Ethereum mainnet" in result.error


class TestCompileUnstakeIntentEthena:
    """Test UnstakeIntent compilation for Ethena protocol."""

    def test_compile_unstake_intent_ethena_success(self):
        """Test successful compilation of Ethena unstake intent."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle is not None
        assert result.action_bundle.intent_type == "UNSTAKE"
        # Ethena unstake initiates cooldown - 1 transaction
        assert len(result.transactions) == 1
        assert result.total_gas_estimate > 0
        # Metadata should indicate cooldown is required
        assert result.action_bundle.metadata.get("cooldown_required") is True

    def test_compile_unstake_intent_ethena_wrong_chain(self):
        """Test that Ethena unstake fails on non-Ethereum chains."""
        compiler = IntentCompiler(
            chain="optimism",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Ethereum mainnet" in result.error

    def test_compile_unstake_intent_ethena_chained_amount_fails(self):
        """Test that chained amount='all' fails with clear error."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount="all",  # type: ignore[arg-type]
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "resolved" in result.error.lower()


class TestCompileUnstakeIntentUnsupported:
    """Test UnstakeIntent compilation for unsupported protocols."""

    def test_compile_unstake_intent_unsupported_protocol(self):
        """Test that unsupported protocol fails with clear error."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = UnstakeIntent(
            protocol="rocketpool",  # Not implemented
            token_in="rETH",
            amount=Decimal("1.0"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Unsupported unstaking protocol" in result.error
        assert "lido" in result.error.lower()
        assert "ethena" in result.error.lower()


# =============================================================================
# Transaction Data Verification Tests
# =============================================================================


class TestTransactionDataConversion:
    """Test that transaction data is correctly converted."""

    def test_transaction_data_fields_present(self):
        """Test that all TransactionData fields are populated."""
        compiler = IntentCompiler(
            chain="ethereum",
            wallet_address=TEST_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=False,
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS
        assert len(result.transactions) == 1

        tx = result.transactions[0]
        assert tx.to != ""
        assert tx.data.startswith("0x")
        assert tx.gas_estimate > 0
        assert tx.description != ""
        # Value should be 1 ETH in wei for stake
        assert tx.value > 0
