"""Tests for LidoAdapter compile_stake_intent method.

VIB-132: Validates that compile_stake_intent() produces correct ActionBundles
for both direct stETH and wrapped wstETH paths, including the critical
stETH approve gas estimate (80K, not the previous 50K that caused reverts).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.lido.adapter import (
    DEFAULT_GAS_ESTIMATES,
    LIDO_ADDRESSES,
    LIDO_STAKE_SELECTOR,
    LIDO_WRAP_SELECTOR,
    LidoAdapter,
    LidoConfig,
)

TEST_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def adapter():
    """Create a LidoAdapter for Ethereum."""
    config = LidoConfig(chain="ethereum", wallet_address=TEST_WALLET)
    return LidoAdapter(config, token_resolver=MagicMock())


def _make_stake_intent(amount: Decimal | str, receive_wrapped: bool = False):
    """Create a mock StakeIntent."""
    intent = MagicMock()
    intent.intent_id = "test-stake-001"
    intent.intent_type.value = "STAKE"
    intent.protocol = "lido"
    intent.token_in = "ETH"
    intent.amount = amount
    intent.receive_wrapped = receive_wrapped
    return intent


class TestCompileStakeIntentDirect:
    """Tests for compile_stake_intent with receive_wrapped=False (stETH only)."""

    def test_direct_stake_produces_one_transaction(self, adapter):
        """Direct stake (no wrap) should produce exactly 1 transaction."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert len(bundle.transactions) == 1

    def test_direct_stake_action_type(self, adapter):
        """First transaction should be a stake action."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[0]["action_type"] == "stake"

    def test_direct_stake_value_is_amount_in_wei(self, adapter):
        """Stake TX value should be the ETH amount in wei."""
        intent = _make_stake_intent(Decimal("0.5"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        expected_wei = int(Decimal("0.5") * Decimal(10**18))
        assert bundle.transactions[0]["value"] == expected_wei

    def test_direct_stake_sends_to_steth_contract(self, adapter):
        """Stake TX should target the stETH contract."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[0]["to"] == LIDO_ADDRESSES["ethereum"]["steth"]

    def test_direct_stake_uses_submit_selector(self, adapter):
        """Stake TX calldata should start with submit() selector."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[0]["data"].startswith(LIDO_STAKE_SELECTOR)

    def test_direct_stake_gas_estimate(self, adapter):
        """Stake TX gas should match DEFAULT_GAS_ESTIMATES['stake']."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[0]["gas_estimate"] == DEFAULT_GAS_ESTIMATES["stake"]

    def test_direct_stake_metadata_output_token(self, adapter):
        """Direct stake metadata should report stETH as output."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.metadata["output_token"] == "stETH"
        assert bundle.metadata["receive_wrapped"] is False


class TestCompileStakeIntentWrapped:
    """Tests for compile_stake_intent with receive_wrapped=True (wstETH path).

    This is the path that was broken in VIB-132 due to insufficient gas
    for the stETH approve transaction.
    """

    def test_wrapped_stake_produces_three_transactions(self, adapter):
        """Wrapped stake should produce 3 TXs: stake + approve + wrap."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert len(bundle.transactions) == 3

    def test_wrapped_stake_transaction_order(self, adapter):
        """TXs must be: stake, approve, wrap (in that order)."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[0]["action_type"] == "stake"
        assert bundle.transactions[1]["action_type"] == "approve"
        assert bundle.transactions[2]["action_type"] == "wrap"

    def test_approve_gas_is_80k(self, adapter):
        """CRITICAL (VIB-132 fix): stETH approve gas must be 80K, not 50K.

        stETH is behind a proxy contract with rebasing share-based logic
        that requires more gas than standard ERC-20 approve (~46K).
        The previous 50K estimate (buffered to 55K by simulator) was
        insufficient, causing reverts on Anvil fork.
        """
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        approve_tx = bundle.transactions[1]
        assert approve_tx["gas_estimate"] == 80000, (
            f"stETH approve gas must be 80K (was {approve_tx['gas_estimate']}). "
            "stETH proxy contract needs more gas than standard ERC-20 approve."
        )

    def test_approve_targets_steth_contract(self, adapter):
        """Approve TX should target the stETH contract."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[1]["to"] == LIDO_ADDRESSES["ethereum"]["steth"]

    def test_approve_uses_approve_selector(self, adapter):
        """Approve TX calldata should start with approve(address,uint256) selector."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        approve_selector = "0x095ea7b3"
        assert bundle.transactions[1]["data"].startswith(approve_selector)

    def test_approve_spender_is_wsteth(self, adapter):
        """Approve TX should authorize wstETH contract as spender."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        # Extract spender from calldata (after 4-byte selector, next 32 bytes)
        calldata = bundle.transactions[1]["data"]
        # Remove '0x095ea7b3' prefix (10 chars)
        spender_hex = calldata[10:74]  # 64 hex chars = 32 bytes
        spender_addr = "0x" + spender_hex[-40:]  # Last 20 bytes
        expected = LIDO_ADDRESSES["ethereum"]["wsteth"].lower()
        assert spender_addr == expected

    def test_wrap_uses_wrap_selector(self, adapter):
        """Wrap TX calldata should start with wrap(uint256) selector."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[2]["data"].startswith(LIDO_WRAP_SELECTOR)

    def test_wrap_targets_wsteth_contract(self, adapter):
        """Wrap TX should target the wstETH contract."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.transactions[2]["to"] == LIDO_ADDRESSES["ethereum"]["wsteth"]

    def test_wrapped_total_gas_includes_approve(self, adapter):
        """Total gas should include stake + 80K approve + wrap."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        expected_total = (
            DEFAULT_GAS_ESTIMATES["stake"]
            + 80000  # stETH approve
            + DEFAULT_GAS_ESTIMATES["wrap"]
        )
        assert bundle.metadata["total_gas_estimate"] == expected_total

    def test_wrapped_metadata_output_token(self, adapter):
        """Wrapped stake metadata should report wstETH as output."""
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=True)
        bundle = adapter.compile_stake_intent(intent)
        assert bundle.metadata["output_token"] == "wstETH"
        assert bundle.metadata["receive_wrapped"] is True


class TestCompileStakeIntentEdgeCases:
    """Edge case tests for compile_stake_intent."""

    def test_amount_all_raises_value_error(self, adapter):
        """amount='all' must be resolved before compilation."""
        intent = _make_stake_intent(amount="all")
        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_stake_intent(intent)

    def test_non_ethereum_chain_fails_gracefully(self):
        """Staking is only available on Ethereum."""
        config = LidoConfig(chain="arbitrum", wallet_address=TEST_WALLET)
        adapter = LidoAdapter(config, token_resolver=MagicMock())
        intent = _make_stake_intent(Decimal("1.0"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        # Should return empty bundle with error (no stETH on Arbitrum)
        assert len(bundle.transactions) == 0
        assert "error" in bundle.metadata

    def test_small_amount_compiles(self, adapter):
        """Very small stake amounts should compile successfully."""
        intent = _make_stake_intent(Decimal("0.001"), receive_wrapped=False)
        bundle = adapter.compile_stake_intent(intent)
        assert len(bundle.transactions) == 1
        expected_wei = int(Decimal("0.001") * Decimal(10**18))
        assert bundle.transactions[0]["value"] == expected_wei
