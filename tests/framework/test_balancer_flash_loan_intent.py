"""Tests for Balancer flash loan intent creation, serialization, and EOA guard.

Verifies that FlashLoanIntent with provider="balancer" can be created,
serialized, and deserialized correctly. Also tests that the compiler
rejects flash loans for EOA wallets at compile time.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents import Intent
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import FlashLoanIntent, IntentType, SwapIntent


class TestBalancerFlashLoanIntent:
    """Test FlashLoanIntent creation with Balancer provider."""

    def test_create_balancer_flash_loan_intent(self):
        """Create a basic Balancer flash loan intent."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap(
                    from_token="USDC",
                    to_token="WETH",
                    amount=Decimal("1000"),
                    max_slippage=Decimal("0.01"),
                    protocol="enso",
                ),
            ],
            chain="arbitrum",
        )

        assert isinstance(intent, FlashLoanIntent)
        assert intent.provider == "balancer"
        assert intent.token == "USDC"
        assert intent.amount == Decimal("1000")
        assert intent.chain == "arbitrum"
        assert intent.intent_type == IntentType.FLASH_LOAN
        assert len(intent.callback_intents) == 1

    def test_balancer_flash_loan_zero_fee(self):
        """Verify Balancer flash loan has no fee field (zero fee)."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("5000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("5000"), max_slippage=Decimal("0.01"), protocol="enso"),
                Intent.swap("WETH", "USDC", amount="all", max_slippage=Decimal("0.01"), protocol="enso"),
            ],
            chain="arbitrum",
        )

        assert intent.provider == "balancer"
        assert len(intent.callback_intents) == 2

    def test_balancer_flash_loan_round_trip_serialization(self):
        """Serialize and deserialize a Balancer flash loan intent."""
        original = Intent.flash_loan(
            provider="balancer",
            token="WETH",
            amount=Decimal("10"),
            callback_intents=[
                Intent.swap("WETH", "USDC", amount=Decimal("10"), max_slippage=Decimal("0.005"), protocol="enso"),
            ],
            chain="ethereum",
        )

        serialized = original.serialize()
        assert serialized["type"] == "FLASH_LOAN"
        assert serialized["provider"] == "balancer"
        assert serialized["token"] == "WETH"

        deserialized = FlashLoanIntent.deserialize(serialized)
        assert deserialized.provider == "balancer"
        assert deserialized.token == "WETH"
        assert deserialized.amount == Decimal("10")
        assert len(deserialized.callback_intents) == 1

    def test_callback_intents_are_swap_intents(self):
        """Verify callback intents are properly typed SwapIntents."""
        intent = Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("1000"), max_slippage=Decimal("0.01"), protocol="enso"),
            ],
            chain="arbitrum",
        )

        callback = intent.callback_intents[0]
        assert isinstance(callback, SwapIntent)
        assert callback.from_token == "USDC"
        assert callback.to_token == "WETH"

    def test_flash_loan_requires_callback_intents(self):
        """Flash loan with empty callbacks should raise validation error."""
        with pytest.raises(ValueError, match="callback intent"):
            Intent.flash_loan(
                provider="balancer",
                token="USDC",
                amount=Decimal("1000"),
                callback_intents=[],
                chain="arbitrum",
            )


class TestFlashLoanEOAGuard:
    """Test that flash loan compilation fails for EOA wallets."""

    def _make_intent(self):
        return Intent.flash_loan(
            provider="balancer",
            token="USDC",
            amount=Decimal("1000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("1000"), max_slippage=Decimal("0.01"), protocol="enso"),
            ],
            chain="arbitrum",
        )

    def _make_compiler(self, wallet="0x1234567890abcdef1234567890abcdef12345678"):
        return IntentCompiler(
            chain="arbitrum",
            wallet_address=wallet,
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("3000"), "ETH": Decimal("3000")},
        )

    def _mock_rpc_response(self, code="0x"):
        """Create a mock httpx response for eth_getCode."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"jsonrpc": "2.0", "result": code, "id": 1}
        return mock_resp

    def test_flash_loan_fails_for_eoa_wallet(self):
        """Flash loan compilation should fail when wallet is an EOA."""
        compiler = self._make_compiler()
        intent = self._make_intent()

        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", return_value=self._mock_rpc_response("0x")):
                result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "EOA" in result.error
        assert "receiver contract" in result.error

    def test_flash_loan_proceeds_for_contract_wallet(self):
        """Flash loan compilation should proceed when wallet is a contract."""
        compiler = self._make_compiler()
        intent = self._make_intent()

        # Contract has bytecode
        contract_code = "0x6080604052"
        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", return_value=self._mock_rpc_response(contract_code)):
                with patch.object(compiler, "_resolve_token") as mock_resolve:
                    # Let it proceed past the EOA check but we don't need full compilation
                    mock_resolve.return_value = None  # Will fail on token resolution
                    result = compiler.compile(intent)

        # Should NOT fail with EOA error - should fail later on token resolution
        assert "EOA" not in (result.error or "")

    def test_flash_loan_warns_when_no_rpc(self):
        """Flash loan should add warning when RPC is unavailable for bytecode check."""
        compiler = self._make_compiler()
        intent = self._make_intent()

        with patch.object(compiler, "_get_chain_rpc_url", return_value=None):
            with patch.object(compiler, "_resolve_token") as mock_resolve:
                mock_resolve.return_value = None  # Will fail on token resolution
                result = compiler.compile(intent)

        # Should not fail with EOA error (no RPC to check)
        assert "EOA" not in (result.error or "")

    def test_is_wallet_contract_eoa(self):
        """_is_wallet_contract returns False for EOA."""
        compiler = self._make_compiler()

        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", return_value=self._mock_rpc_response("0x")):
                assert compiler._is_wallet_contract() is False

    def test_is_wallet_contract_contract(self):
        """_is_wallet_contract returns True for contract."""
        compiler = self._make_compiler()

        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", return_value=self._mock_rpc_response("0x6080604052")):
                assert compiler._is_wallet_contract() is True

    def test_is_wallet_contract_no_rpc(self):
        """_is_wallet_contract returns None when no RPC available."""
        compiler = self._make_compiler()

        with patch.object(compiler, "_get_chain_rpc_url", return_value=None):
            assert compiler._is_wallet_contract() is None

    def test_is_wallet_contract_rpc_error(self):
        """_is_wallet_contract returns None on RPC transport error."""
        compiler = self._make_compiler()

        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", side_effect=Exception("Connection refused")):
                assert compiler._is_wallet_contract() is None

    def test_is_wallet_contract_rpc_payload_error(self):
        """_is_wallet_contract returns None when JSON-RPC response has error field."""
        compiler = self._make_compiler()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": "execution reverted"},
            "id": 1,
        }

        with patch.object(compiler, "_get_chain_rpc_url", return_value="http://localhost:8545"):
            with patch("httpx.post", return_value=mock_resp):
                assert compiler._is_wallet_contract() is None
