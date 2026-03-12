"""Integration tests for Solana local testing via solana-test-validator.

These tests require `solana-test-validator` to be installed:
    sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"

They also require network access to clone accounts from mainnet-beta.

Run with:
    pytest tests/integration/chains/test_solana_fork.py -v --timeout=120
"""

from __future__ import annotations

import asyncio
import os
import shutil
from decimal import Decimal

import pytest
import pytest_asyncio

from almanak.framework.anvil.solana_fork_manager import (
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKEN_MINTS,
    SolanaForkManager,
)

# Skip entire module if solana-test-validator not installed.
# xdist_group ensures all tests run in the same worker (avoids port conflicts).
pytestmark = [
    pytest.mark.skipif(
        shutil.which("solana-test-validator") is None,
        reason="solana-test-validator not installed",
    ),
    pytest.mark.timeout(180),
    pytest.mark.xdist_group("solana_validator"),
]


@pytest_asyncio.fixture(scope="module")
async def solana_fork():
    """Start a single SolanaForkManager shared across all tests in this module."""
    rpc = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    mgr = SolanaForkManager(
        rpc_url=rpc,
        validator_port=18899,  # Non-default port to avoid conflicts
        faucet_port=19900,
    )
    started = await mgr.start()
    if not started:
        pytest.skip("solana-test-validator failed to start (network issue?)")
    yield mgr
    await mgr.stop()


@pytest.fixture()
def test_wallet():
    """Generate a fresh test wallet keypair per test."""
    from solders.keypair import Keypair

    return Keypair()


# =============================================================================
# Validator Lifecycle
# =============================================================================


class TestValidatorLifecycle:
    """Test that solana-test-validator starts and stops correctly."""

    @pytest.mark.asyncio
    async def test_start_and_health(self, solana_fork: SolanaForkManager):
        assert solana_fork.is_running
        assert solana_fork.current_slot is not None
        assert solana_fork.current_slot >= 0

    @pytest.mark.asyncio
    async def test_rpc_url(self, solana_fork: SolanaForkManager):
        url = solana_fork.get_rpc_url()
        assert url.startswith("http://127.0.0.1:")
        assert "18899" in url


# =============================================================================
# SOL Funding
# =============================================================================


class TestSOLFunding:
    """Test native SOL airdrop on local validator."""

    @pytest.mark.asyncio
    async def test_fund_sol(self, solana_fork: SolanaForkManager, test_wallet):
        addr = str(test_wallet.pubkey())

        ok = await solana_fork.fund_wallet(addr, Decimal("50"))
        assert ok is True

        balance = await solana_fork._get_sol_balance(addr)
        assert balance == 50_000_000_000  # 50 SOL in lamports

    @pytest.mark.asyncio
    async def test_fund_sol_large_amount(self, solana_fork: SolanaForkManager, test_wallet):
        addr = str(test_wallet.pubkey())

        ok = await solana_fork.fund_wallet(addr, Decimal("500"))
        assert ok is True

        balance = await solana_fork._get_sol_balance(addr)
        assert balance == 500_000_000_000


# =============================================================================
# SPL Token Funding
# =============================================================================


class TestTokenFunding:
    """Test SPL token minting via mint authority trick."""

    @pytest.mark.asyncio
    async def test_fund_usdc(self, solana_fork: SolanaForkManager, test_wallet):
        addr = str(test_wallet.pubkey())

        # Need SOL for the authority to pay tx fees (done automatically in fund_tokens)
        ok = await solana_fork.fund_tokens(addr, {"USDC": Decimal("5000")})
        assert ok is True

        # Verify via ATA
        from solders.pubkey import Pubkey

        mint_pk = Pubkey.from_string(SOLANA_TOKEN_MINTS["USDC"])
        owner_pk = Pubkey.from_string(addr)
        ata = solana_fork._derive_ata(owner_pk, mint_pk)
        balance = await solana_fork._get_token_balance(str(ata))
        assert balance == "5000"

    @pytest.mark.asyncio
    async def test_fund_usdt(self, solana_fork: SolanaForkManager, test_wallet):
        addr = str(test_wallet.pubkey())

        ok = await solana_fork.fund_tokens(addr, {"USDT": Decimal("10000")})
        assert ok is True

        from solders.pubkey import Pubkey

        mint_pk = Pubkey.from_string(SOLANA_TOKEN_MINTS["USDT"])
        owner_pk = Pubkey.from_string(addr)
        ata = solana_fork._derive_ata(owner_pk, mint_pk)
        balance = await solana_fork._get_token_balance(str(ata))
        assert balance == "10000"

    @pytest.mark.asyncio
    async def test_fund_multiple_tokens(self, solana_fork: SolanaForkManager, test_wallet):
        addr = str(test_wallet.pubkey())

        ok = await solana_fork.fund_tokens(
            addr, {"USDC": Decimal("1000"), "USDT": Decimal("2000")}
        )
        assert ok is True

        from solders.pubkey import Pubkey

        for symbol, expected in [("USDC", "1000"), ("USDT", "2000")]:
            mint_pk = Pubkey.from_string(SOLANA_TOKEN_MINTS[symbol])
            owner_pk = Pubkey.from_string(addr)
            ata = solana_fork._derive_ata(owner_pk, mint_pk)
            balance = await solana_fork._get_token_balance(str(ata))
            assert balance == expected, f"{symbol} balance mismatch: {balance} != {expected}"


# =============================================================================
# Full Wallet Setup (mirrors CLI --network anvil for Solana)
# =============================================================================


class TestFullWalletSetup:
    """Test the complete wallet setup that CLI performs."""

    @pytest.mark.asyncio
    async def test_cli_equivalent_funding(self, solana_fork: SolanaForkManager, test_wallet):
        """Replicate what `almanak strat run --network anvil` does for Solana."""
        addr = str(test_wallet.pubkey())

        # Step 1: Fund SOL (100 SOL, same as CLI)
        ok = await solana_fork.fund_wallet(addr, Decimal("100"))
        assert ok is True

        # Step 2: Fund tokens (10K USDC + 10K USDT, same as CLI)
        ok = await solana_fork.fund_tokens(
            addr, {"USDC": Decimal("10000"), "USDT": Decimal("10000")}
        )
        assert ok is True

        # Verify all balances
        sol_balance = await solana_fork._get_sol_balance(addr)
        assert sol_balance == 100_000_000_000  # 100 SOL

        from solders.pubkey import Pubkey

        for symbol, expected in [("USDC", "10000"), ("USDT", "10000")]:
            mint_pk = Pubkey.from_string(SOLANA_TOKEN_MINTS[symbol])
            owner_pk = Pubkey.from_string(addr)
            ata = solana_fork._derive_ata(owner_pk, mint_pk)
            balance = await solana_fork._get_token_balance(str(ata))
            assert balance == expected


# =============================================================================
# Native Transaction Sending (proves we can sign + send on local validator)
# =============================================================================


class TestTransactionExecution:
    """Test that we can build, sign, and execute Solana transactions."""

    @pytest.mark.asyncio
    async def test_sol_transfer(self, solana_fork: SolanaForkManager, test_wallet):
        """Send SOL from test wallet to a recipient — real on-chain tx."""
        import base64

        from solders.hash import Hash
        from solders.keypair import Keypair
        from solders.message import Message as LegacyMessage
        from solders.system_program import TransferParams, transfer
        from solders.transaction import Transaction as LegacyTransaction

        sender = test_wallet
        recipient = Keypair()
        sender_addr = str(sender.pubkey())
        recipient_addr = str(recipient.pubkey())

        # Fund sender
        await solana_fork.fund_wallet(sender_addr, Decimal("10"))

        # Build SOL transfer using solders system_program helper
        transfer_ix = transfer(TransferParams(
            from_pubkey=sender.pubkey(),
            to_pubkey=recipient.pubkey(),
            lamports=1_000_000_000,  # 1 SOL
        ))

        # Get blockhash
        bh_resp = await solana_fork._rpc_call(
            "getLatestBlockhash", [{"commitment": "confirmed"}]
        )
        blockhash = Hash.from_string(bh_resp["value"]["blockhash"])

        # Build, sign, send (legacy transaction — most reliable on test-validator)
        msg = LegacyMessage.new_with_blockhash([transfer_ix], sender.pubkey(), blockhash)
        tx = LegacyTransaction.new_unsigned(msg)
        tx.sign([sender], blockhash)
        tx_b64 = base64.b64encode(bytes(tx)).decode("ascii")

        sig = await solana_fork._rpc_call(
            "sendTransaction",
            [tx_b64, {"encoding": "base64", "preflightCommitment": "confirmed"}],
        )
        assert sig is not None

        # Poll until recipient balance reflects the transfer
        recipient_balance = 0
        for _ in range(30):
            recipient_balance = await solana_fork._get_sol_balance(recipient_addr)
            if recipient_balance > 0:
                break
            await asyncio.sleep(0.5)

        assert recipient_balance == 1_000_000_000  # 1 SOL


# =============================================================================
# SolanaExecutionPlanner — Full Execution Engine (sign + submit + confirm)
# =============================================================================


class TestSolanaRpcClient:
    """Test the SolanaRpcClient against a real local validator.

    Validates send_transaction + confirm_and_get_receipt — the core execution
    engine used by SolanaExecutionPlanner. Uses legacy transactions because
    solana-test-validator v3.x doesn't reliably process V0 VersionedTransactions
    (V0 simulates OK but never lands). Mainnet V0 support is proven via VIB-77.
    """

    @pytest.mark.asyncio
    async def test_rpc_client_send_and_confirm(self, solana_fork: SolanaForkManager, test_wallet):
        """Full SolanaRpcClient pipeline: send, confirm, get receipt."""
        import base64

        from solders.hash import Hash
        from solders.keypair import Keypair
        from solders.message import Message as LegacyMessage
        from solders.system_program import TransferParams, transfer
        from solders.transaction import Transaction as LegacyTransaction

        from almanak.framework.execution.solana.rpc import SolanaRpcClient, SolanaRpcConfig

        sender = test_wallet
        recipient = Keypair()
        sender_addr = str(sender.pubkey())
        recipient_addr = str(recipient.pubkey())

        await solana_fork.fund_wallet(sender_addr, Decimal("10"))

        # Build and sign a legacy SOL transfer
        transfer_ix = transfer(TransferParams(
            from_pubkey=sender.pubkey(),
            to_pubkey=recipient.pubkey(),
            lamports=1_000_000_000,
        ))

        bh_resp = await solana_fork._rpc_call(
            "getLatestBlockhash", [{"commitment": "confirmed"}]
        )
        blockhash = Hash.from_string(bh_resp["value"]["blockhash"])
        msg = LegacyMessage.new_with_blockhash([transfer_ix], sender.pubkey(), blockhash)
        tx = LegacyTransaction.new_unsigned(msg)
        tx.sign([sender], blockhash)
        tx_b64 = base64.b64encode(bytes(tx)).decode("ascii")

        # Use SolanaRpcClient to send + confirm + get receipt
        rpc = SolanaRpcClient(SolanaRpcConfig(
            rpc_url=solana_fork.get_rpc_url(),
            commitment="confirmed",
        ))

        # Health check
        healthy = await rpc.get_health()
        assert healthy is True

        # Send
        signature = await rpc.send_transaction(tx_b64, skip_preflight=False)
        assert signature is not None
        assert len(signature) > 40  # Base58 signature

        # Confirm and get receipt
        receipt = await rpc.confirm_and_get_receipt(signature, commitment="confirmed")
        assert receipt.success is True
        assert receipt.slot > 0
        assert receipt.fee_lamports > 0
        assert receipt.signature == signature

        # Verify on-chain
        recipient_balance = await solana_fork._get_sol_balance(recipient_addr)
        assert recipient_balance == 1_000_000_000

    @pytest.mark.asyncio
    async def test_rpc_client_get_blockhash(self, solana_fork: SolanaForkManager):
        """SolanaRpcClient can fetch a recent blockhash."""
        from almanak.framework.execution.solana.rpc import SolanaRpcClient, SolanaRpcConfig

        rpc = SolanaRpcClient(SolanaRpcConfig(
            rpc_url=solana_fork.get_rpc_url(),
            commitment="confirmed",
        ))

        blockhash, last_valid_block_height = await rpc.get_latest_blockhash("confirmed")
        assert isinstance(blockhash, str)
        assert len(blockhash) > 30
        assert last_valid_block_height > 0

    @pytest.mark.asyncio
    async def test_planner_dry_run(self, solana_fork: SolanaForkManager, test_wallet):
        """SolanaExecutionPlanner dry_run mode: sign but don't submit."""
        import base64

        from solders.hash import Hash
        from solders.keypair import Keypair
        from solders.message import MessageV0
        from solders.signature import Signature
        from solders.system_program import TransferParams, transfer
        from solders.transaction import VersionedTransaction

        from almanak.framework.execution.solana.planner import SolanaExecutionPlanner

        sender = test_wallet
        recipient = Keypair()
        sender_addr = str(sender.pubkey())

        await solana_fork.fund_wallet(sender_addr, Decimal("5"))

        transfer_ix = transfer(TransferParams(
            from_pubkey=sender.pubkey(),
            to_pubkey=recipient.pubkey(),
            lamports=500_000_000,
        ))

        bh_resp = await solana_fork._rpc_call(
            "getLatestBlockhash", [{"commitment": "confirmed"}]
        )
        blockhash = Hash.from_string(bh_resp["value"]["blockhash"])
        msg = MessageV0.try_compile(sender.pubkey(), [transfer_ix], [], blockhash)
        unsigned_tx = VersionedTransaction.populate(msg, [Signature.default()])
        tx_b64 = base64.b64encode(bytes(unsigned_tx)).decode("ascii")

        action_bundle = {
            "metadata": {"deferred_swap": False},
            "transactions": [{"serialized_transaction": tx_b64}],
        }

        planner = SolanaExecutionPlanner(
            wallet_address=sender_addr,
            rpc_url=solana_fork.get_rpc_url(),
            private_key=str(sender),
        )

        outcome = await planner.execute_actions(
            [action_bundle],
            context={"dry_run": True},
        )

        assert outcome.success is True
        assert outcome.tx_ids == ["dry-run-signature"]
        assert len(outcome.receipts) == 0

        # Sender balance unchanged (no real transfer)
        sender_balance = await solana_fork._get_sol_balance(sender_addr)
        assert sender_balance == 5_000_000_000
