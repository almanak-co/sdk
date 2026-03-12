"""Tests for SolanaExecutionPlanner execute_actions flow."""

import base64
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from solders.hash import Hash as SolHash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.signature import Signature
from solders.transaction import VersionedTransaction

import solders.system_program as sp

from almanak.framework.execution.solana.planner import SolanaExecutionPlanner
from almanak.framework.execution.solana.rpc import TransactionReceipt
from almanak.framework.models.reproduction_bundle import ActionBundle


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_unsigned_tx_b64(keypair: Keypair) -> str:
    """Create a simple unsigned VersionedTransaction as base64."""
    ix = sp.transfer(
        sp.TransferParams(
            from_pubkey=keypair.pubkey(),
            to_pubkey=keypair.pubkey(),
            lamports=1000,
        )
    )
    msg = MessageV0.try_compile(keypair.pubkey(), [ix], [], SolHash.default())
    unsigned_tx = VersionedTransaction.populate(msg, [Signature.default()])
    return base64.b64encode(bytes(unsigned_tx)).decode()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def keypair():
    return Keypair()


@pytest.fixture
def planner(keypair):
    return SolanaExecutionPlanner(
        wallet_address=str(keypair.pubkey()),
        rpc_url="https://api.devnet.solana.com",
        private_key=str(keypair),
    )


@pytest.fixture
def action_bundle(keypair):
    tx_b64 = _make_unsigned_tx_b64(keypair)
    return ActionBundle(
        intent_type="SWAP",
        transactions=[
            {
                "serialized_transaction": tx_b64,
                "chain_family": "SOLANA",
                "tx_type": "swap",
            }
        ],
        metadata={
            "protocol": "jupiter",
            "chain": "solana",
            "chain_family": "SOLANA",
        },
    )


@pytest.fixture
def deferred_bundle(keypair):
    tx_b64 = _make_unsigned_tx_b64(keypair)
    return ActionBundle(
        intent_type="SWAP",
        transactions=[
            {
                "serialized_transaction": tx_b64,
                "chain_family": "SOLANA",
                "tx_type": "swap",
            }
        ],
        metadata={
            "protocol": "jupiter",
            "chain": "solana",
            "chain_family": "SOLANA",
            "deferred_swap": True,
            "route_params": {
                "input_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "output_mint": "So11111111111111111111111111111111111111112",
                "amount": 100_000_000,
                "slippage_bps": 50,
            },
        },
    )


@pytest.fixture
def mock_receipt():
    return TransactionReceipt(
        signature="MockSigABC123",
        slot=280000000,
        block_time=1700000000,
        fee_lamports=5000,
        success=True,
        logs=["Program log: swap ok"],
        pre_token_balances=[],
        post_token_balances=[],
    )


# ---------------------------------------------------------------------------
# No-config error tests
# ---------------------------------------------------------------------------


class TestPlannerNoConfig:
    @pytest.mark.asyncio
    async def test_no_rpc_returns_error(self):
        planner = SolanaExecutionPlanner(wallet_address="abc")
        outcome = await planner.execute_actions([])
        assert outcome.success is False
        assert "no RPC URL" in outcome.error

    @pytest.mark.asyncio
    async def test_no_private_key_returns_error(self):
        planner = SolanaExecutionPlanner(
            wallet_address="abc",
            rpc_url="https://api.devnet.solana.com",
        )
        outcome = await planner.execute_actions([MagicMock()])
        assert outcome.success is False
        assert "no private key" in outcome.error


# ---------------------------------------------------------------------------
# execute_actions tests
# ---------------------------------------------------------------------------


class TestExecuteActions:
    @pytest.mark.asyncio
    async def test_empty_actions(self, planner):
        """Empty action list should succeed with no tx_ids."""
        outcome = await planner.execute_actions([])
        assert outcome.success is True
        assert outcome.tx_ids == []

    @pytest.mark.asyncio
    async def test_empty_bundle(self, planner):
        """Bundle with no transactions should be skipped."""
        empty_bundle = ActionBundle(intent_type="SWAP", transactions=[], metadata={})
        outcome = await planner.execute_actions([empty_bundle])
        assert outcome.success is True
        assert outcome.tx_ids == []

    @pytest.mark.asyncio
    async def test_successful_execution(self, planner, action_bundle, mock_receipt):
        """Full flow: sign -> send -> confirm -> receipt."""
        with (
            patch.object(planner._rpc, "send_transaction", new_callable=AsyncMock) as mock_send,
            patch.object(planner._rpc, "confirm_and_get_receipt", new_callable=AsyncMock) as mock_confirm,
        ):
            mock_send.return_value = "MockSigABC123"
            mock_confirm.return_value = mock_receipt

            outcome = await planner.execute_actions([action_bundle])

        assert outcome.success is True
        assert outcome.chain_family == "SOLANA"
        assert outcome.tx_ids == ["MockSigABC123"]
        assert len(outcome.receipts) == 1
        assert outcome.receipts[0]["signature"] == "MockSigABC123"
        assert outcome.total_fee_native == Decimal("5000") / Decimal("1000000000")

    @pytest.mark.asyncio
    async def test_dry_run(self, planner, action_bundle):
        """Dry run should sign but not submit."""
        outcome = await planner.execute_actions(
            [action_bundle],
            context={"dry_run": True},
        )
        assert outcome.success is True
        assert outcome.tx_ids == ["dry-run-signature"]

    @pytest.mark.asyncio
    async def test_send_failure(self, planner, action_bundle):
        """RPC send error should return failed outcome."""
        from almanak.framework.execution.solana.rpc import SolanaRpcError

        with patch.object(
            planner._rpc,
            "send_transaction",
            new_callable=AsyncMock,
            side_effect=SolanaRpcError("sendTransaction", {"code": -1}),
        ):
            outcome = await planner.execute_actions([action_bundle])

        assert outcome.success is False
        assert "submission failed" in outcome.error

    @pytest.mark.asyncio
    async def test_confirmation_timeout(self, planner, action_bundle):
        """Confirmation timeout should return failed outcome."""
        with (
            patch.object(planner._rpc, "send_transaction", new_callable=AsyncMock) as mock_send,
            patch.object(
                planner._rpc,
                "confirm_and_get_receipt",
                new_callable=AsyncMock,
                side_effect=TimeoutError("timed out"),
            ),
        ):
            mock_send.return_value = "sig123"
            outcome = await planner.execute_actions([action_bundle])

        assert outcome.success is False
        assert "timeout" in outcome.error.lower()

    @pytest.mark.asyncio
    async def test_on_chain_failure(self, planner, action_bundle):
        """On-chain transaction failure should return failed outcome."""
        failed_receipt = TransactionReceipt(
            signature="sig_fail",
            slot=100,
            success=False,
            err={"InstructionError": [0, "Custom"]},
        )
        with (
            patch.object(planner._rpc, "send_transaction", new_callable=AsyncMock) as mock_send,
            patch.object(planner._rpc, "confirm_and_get_receipt", new_callable=AsyncMock) as mock_confirm,
        ):
            mock_send.return_value = "sig_fail"
            mock_confirm.return_value = failed_receipt

            outcome = await planner.execute_actions([action_bundle])

        assert outcome.success is False
        assert "failed on-chain" in outcome.error


# ---------------------------------------------------------------------------
# Multi-signer (additional_signers passthrough) tests
# ---------------------------------------------------------------------------


class TestMultiSignerPassthrough:
    @pytest.mark.asyncio
    async def test_additional_signers_passed_to_signer(self, planner, keypair, mock_receipt):
        """Planner should pass metadata.additional_signers to SolanaSigner."""
        extra_kp = Keypair()

        # Build 2-signer tx (wallet + additional)
        ix1 = sp.transfer(
            sp.TransferParams(from_pubkey=keypair.pubkey(), to_pubkey=extra_kp.pubkey(), lamports=1000)
        )
        ix2 = sp.transfer(
            sp.TransferParams(from_pubkey=extra_kp.pubkey(), to_pubkey=keypair.pubkey(), lamports=500)
        )
        msg = MessageV0.try_compile(keypair.pubkey(), [ix1, ix2], [], SolHash.default())
        num_signers = msg.header.num_required_signatures
        unsigned_tx = VersionedTransaction.populate(msg, [Signature.default()] * num_signers)
        tx_b64 = base64.b64encode(bytes(unsigned_tx)).decode()

        bundle = ActionBundle(
            intent_type="LP_OPEN",
            transactions=[{"serialized_transaction": tx_b64, "tx_type": "lp_open"}],
            metadata={
                "protocol": "raydium_clmm",
                "chain_family": "SOLANA",
                "additional_signers": [base64.b64encode(bytes(extra_kp)).decode()],
            },
        )

        with (
            patch.object(planner._rpc, "send_transaction", new_callable=AsyncMock) as mock_send,
            patch.object(planner._rpc, "confirm_and_get_receipt", new_callable=AsyncMock) as mock_confirm,
        ):
            mock_send.return_value = "sig_multi"
            mock_confirm.return_value = mock_receipt

            outcome = await planner.execute_actions([bundle])

        assert outcome.success is True
        assert "sig_multi" in outcome.tx_ids


# ---------------------------------------------------------------------------
# Deferred swap (Jupiter route refresh) tests
# ---------------------------------------------------------------------------


class TestDeferredSwap:
    @pytest.mark.asyncio
    async def test_deferred_swap_refreshes_route(self, planner, deferred_bundle, mock_receipt, keypair):
        """Deferred swap should call Jupiter to refresh the route."""
        fresh_tx_b64 = _make_unsigned_tx_b64(keypair)

        with (
            patch.object(
                planner,
                "_refresh_jupiter_route",
                return_value={"serialized_transaction": fresh_tx_b64},
            ) as mock_refresh,
            patch.object(planner._rpc, "send_transaction", new_callable=AsyncMock) as mock_send,
            patch.object(planner._rpc, "confirm_and_get_receipt", new_callable=AsyncMock) as mock_confirm,
        ):
            mock_send.return_value = "sig_fresh"
            mock_confirm.return_value = mock_receipt

            outcome = await planner.execute_actions([deferred_bundle])

        mock_refresh.assert_called_once()
        assert outcome.success is True

    @pytest.mark.asyncio
    async def test_deferred_swap_fails_hard_on_refresh_error(
        self, planner, deferred_bundle, mock_receipt
    ):
        """If Jupiter refresh fails, should fail hard (not submit stale tx)."""
        with patch.object(
            planner,
            "_refresh_jupiter_route",
            side_effect=Exception("Jupiter API error"),
        ):
            outcome = await planner.execute_actions([deferred_bundle])

        # Should fail — no stale transaction fallback
        assert outcome.success is False


# ---------------------------------------------------------------------------
# check_connection tests
# ---------------------------------------------------------------------------


class TestCheckConnection:
    @pytest.mark.asyncio
    async def test_check_connection_healthy(self, planner):
        with patch.object(planner._rpc, "get_health", new_callable=AsyncMock, return_value=True):
            assert await planner.check_connection() is True

    @pytest.mark.asyncio
    async def test_check_connection_unhealthy(self, planner):
        with patch.object(planner._rpc, "get_health", new_callable=AsyncMock, return_value=False):
            assert await planner.check_connection() is False
