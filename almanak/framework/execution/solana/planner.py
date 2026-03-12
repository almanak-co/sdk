"""SolanaExecutionPlanner — ChainExecutionStrategy for Solana.

Handles the full Solana transaction lifecycle:
1. Refresh deferred swap routes (Jupiter) for fresh blockhash
2. Sign with Ed25519 keypair via SolanaSigner
3. Submit via JSON-RPC
4. Confirm and parse receipts
5. Return chain-agnostic ExecutionOutcome
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from almanak.framework.execution.chain_strategy import ChainExecutionStrategy
from almanak.framework.execution.outcome import ExecutionOutcome
from almanak.framework.execution.solana.rpc import (
    SolanaRpcClient,
    SolanaRpcConfig,
    SolanaRpcError,
    SolanaTransactionError,
)
from almanak.framework.execution.solana.signer import SolanaSigner, SolanaSignerError
from almanak.framework.execution.solana.types import (
    SignedSolanaTransaction,
    SolanaTransaction,
)

logger = logging.getLogger(__name__)

# Default priority fee ceiling (10M lamports ~ 0.01 SOL)
DEFAULT_PRIORITY_FEE_CEILING_LAMPORTS = 10_000_000

# Default CU simulation buffer multiplier
DEFAULT_CU_BUFFER_MULTIPLIER = 1.2

# Lamports per SOL
LAMPORTS_PER_SOL = 1_000_000_000


class SolanaExecutionPlanner(ChainExecutionStrategy):
    """Solana execution strategy.

    Implements ChainExecutionStrategy for signing and submitting Solana
    transactions. Designed for Jupiter swap transactions that arrive as
    base64-encoded VersionedTransactions in ActionBundles.

    Args:
        wallet_address: Base58-encoded Solana wallet public key.
        rpc_url: Solana RPC endpoint URL.
        private_key: Base58-encoded Ed25519 private key for signing.
        commitment: Transaction commitment level (default: "confirmed").
        priority_fee_ceiling_lamports: Max priority fee in lamports (default: 10M).
        cu_buffer_multiplier: Safety buffer for CU estimates (default: 1.2x).
    """

    chain_family: str = "SOLANA"

    def __init__(
        self,
        wallet_address: str,
        rpc_url: str = "",
        private_key: str = "",
        commitment: str = "confirmed",
        priority_fee_ceiling_lamports: int = DEFAULT_PRIORITY_FEE_CEILING_LAMPORTS,
        cu_buffer_multiplier: float = DEFAULT_CU_BUFFER_MULTIPLIER,
    ):
        self.wallet_address = wallet_address
        self.rpc_url = rpc_url
        self.commitment = commitment
        self.priority_fee_ceiling_lamports = priority_fee_ceiling_lamports
        self.cu_buffer_multiplier = cu_buffer_multiplier

        # Initialize RPC client
        self._rpc = (
            SolanaRpcClient(
                SolanaRpcConfig(
                    rpc_url=rpc_url,
                    commitment=commitment,
                )
            )
            if rpc_url
            else None
        )

        # Initialize signer
        self._signer = SolanaSigner.from_base58(private_key) if private_key else None

    # =========================================================================
    # ChainExecutionStrategy interface
    # =========================================================================

    async def execute_actions(
        self,
        actions: list[Any],
        context: dict[str, Any] | None = None,
    ) -> ExecutionOutcome:
        """Execute Solana actions from compiled ActionBundles.

        Handles Jupiter swap bundles with the deferred_swap pattern:
        1. For each ActionBundle, refresh the route if deferred_swap=True
        2. Sign each transaction with Ed25519
        3. Submit to Solana RPC
        4. Confirm and parse receipts

        Args:
            actions: List of ActionBundle objects (or dicts).
            context: Optional execution context.

        Returns:
            ExecutionOutcome with success status, signatures, and receipts.
        """
        if not self._rpc:
            return ExecutionOutcome(
                success=False,
                chain_family="SOLANA",
                error="SolanaExecutionPlanner: no RPC URL configured",
            )
        if not self._signer:
            return ExecutionOutcome(
                success=False,
                chain_family="SOLANA",
                error="SolanaExecutionPlanner: no private key configured",
            )

        context = context or {}
        dry_run = context.get("dry_run", False)

        all_signatures: list[str] = []
        all_receipts: list[dict[str, Any]] = []
        total_fee_lamports = 0

        for action_bundle in actions:
            # Extract metadata and transactions from the ActionBundle
            metadata = _get_metadata(action_bundle)
            transactions = _get_transactions(action_bundle)

            if not transactions:
                logger.warning("ActionBundle has no transactions, skipping")
                continue

            for tx_data in transactions:
                serialized_tx = tx_data.get("serialized_transaction", "")
                if not serialized_tx:
                    logger.warning("Transaction missing serialized_transaction field, skipping")
                    continue

                # Step 1: Refresh deferred swap route for fresh blockhash + quote
                blockhash_refreshed = False
                if metadata.get("deferred_swap"):
                    try:
                        fresh_tx = self._refresh_jupiter_route(metadata)
                        fresh_serialized = fresh_tx.get("serialized_transaction")
                        if not fresh_serialized:
                            return ExecutionOutcome(
                                success=False,
                                chain_family="SOLANA",
                                tx_ids=all_signatures,
                                error="Jupiter route refresh returned no serialized_transaction",
                            )

                        # Validate slippage: reject if fresh out_amount degraded
                        # beyond the original slippage tolerance
                        original_out = metadata.get("amount_out")
                        fresh_out = fresh_tx.get("amount_out")
                        route_params = metadata.get("route_params", {})
                        slippage_bps = route_params.get("slippage_bps", 50)
                        if original_out and fresh_out:
                            try:
                                original_out_int = int(original_out)
                                fresh_out_int = int(fresh_out)
                                if original_out_int > 0:
                                    slippage_ratio = (original_out_int - fresh_out_int) / original_out_int
                                    slippage_tolerance = slippage_bps / 10_000
                                    if slippage_ratio > slippage_tolerance:
                                        return ExecutionOutcome(
                                            success=False,
                                            chain_family="SOLANA",
                                            tx_ids=all_signatures,
                                            error=(
                                                f"Jupiter route refresh slippage too high: "
                                                f"original_out={original_out_int}, fresh_out={fresh_out_int}, "
                                                f"degradation={slippage_ratio:.4%} exceeds tolerance={slippage_tolerance:.4%}"
                                            ),
                                        )
                            except (ValueError, TypeError) as conv_err:
                                logger.warning(f"Could not compare out_amounts for slippage check: {conv_err}")

                        serialized_tx = fresh_serialized
                        blockhash_refreshed = True
                        logger.info(
                            f"Refreshed Jupiter route for fresh blockhash "
                            f"(original_out={original_out}, fresh_out={fresh_out})"
                        )
                    except Exception as e:
                        return ExecutionOutcome(
                            success=False,
                            chain_family="SOLANA",
                            tx_ids=all_signatures,
                            error=f"Jupiter route refresh failed: {e}",
                        )

                # Step 1b: For non-deferred transactions (e.g. Raydium LP), replace
                # the stale compile-time blockhash with a fresh one so it doesn't
                # expire before submission.
                if not blockhash_refreshed:
                    try:
                        serialized_tx = await self._replace_blockhash(serialized_tx)
                        logger.info("Replaced stale blockhash with fresh one")
                    except Exception as e:
                        logger.error(f"Failed to replace blockhash: {e}")
                        return ExecutionOutcome(
                            success=False,
                            error=f"Blockhash replacement failed: {e}",
                            chain_family="SOLANA",
                        )

                # Step 2: Sign the transaction (with additional signers if present)
                # Check sensitive_data first (preferred), fall back to metadata for compat
                sensitive = _get_sensitive_data(action_bundle)
                additional_signers = sensitive.get("additional_signers") or metadata.get("additional_signers")
                try:
                    signed_tx_b64 = self._signer.sign_serialized_transaction(
                        serialized_tx,
                        additional_signers=additional_signers,
                    )
                except SolanaSignerError as e:
                    return ExecutionOutcome(
                        success=False,
                        chain_family="SOLANA",
                        tx_ids=all_signatures,
                        error=f"Signing failed: {e}",
                    )

                if dry_run:
                    logger.info("DRY RUN: Would submit signed transaction to Solana RPC")
                    all_signatures.append("dry-run-signature")
                    continue

                # Step 3: Submit to Solana RPC
                try:
                    signature = await self._rpc.send_transaction(
                        signed_tx_base64=signed_tx_b64,
                        skip_preflight=False,
                    )
                except SolanaRpcError as e:
                    return ExecutionOutcome(
                        success=False,
                        chain_family="SOLANA",
                        tx_ids=all_signatures,
                        error=f"Transaction submission failed: {e}",
                    )

                all_signatures.append(signature)

                # Step 4: Confirm and get receipt
                try:
                    receipt = await self._rpc.confirm_and_get_receipt(
                        signature=signature,
                        commitment=self.commitment,
                    )

                    if not receipt.success:
                        return ExecutionOutcome(
                            success=False,
                            chain_family="SOLANA",
                            tx_ids=all_signatures,
                            receipts=[receipt.to_dict()],
                            error=f"Transaction failed on-chain: {receipt.err}",
                        )

                    all_receipts.append(receipt.to_dict())
                    total_fee_lamports += receipt.fee_lamports

                    logger.info(
                        f"Transaction confirmed: {signature}, slot={receipt.slot}, fee={receipt.fee_lamports} lamports"
                    )

                except SolanaTransactionError as e:
                    return ExecutionOutcome(
                        success=False,
                        chain_family="SOLANA",
                        tx_ids=all_signatures,
                        error=f"Transaction failed: {e}",
                    )
                except TimeoutError as e:
                    return ExecutionOutcome(
                        success=False,
                        chain_family="SOLANA",
                        tx_ids=all_signatures,
                        error=f"Confirmation timeout: {e}",
                    )

        return ExecutionOutcome(
            success=True,
            chain_family="SOLANA",
            tx_ids=all_signatures,
            receipts=all_receipts,
            total_fee_native=Decimal(total_fee_lamports) / Decimal(LAMPORTS_PER_SOL),
        )

    async def check_connection(self) -> bool:
        """Check Solana RPC connectivity.

        Returns:
            True if the RPC endpoint is healthy.
        """
        if not self._rpc:
            return False
        return await self._rpc.get_health()

    # =========================================================================
    # Solana-specific methods
    # =========================================================================

    async def ensure_atas(
        self,
        mints: list[str],
        owner: str | None = None,
        policy: str = "create_if_missing",
    ) -> list[str]:
        """ATA pre-flight — ensure Associated Token Accounts exist.

        Args:
            mints: List of SPL token mint addresses.
            owner: Owner public key (defaults to self.wallet_address).
            policy: "create_if_missing" or "error_if_missing".

        Returns:
            List of ATA addresses.
        """
        raise NotImplementedError(
            "ensure_atas() is not yet implemented. Jupiter API handles ATAs internally for swap transactions."
        )

    async def simulate_cu(self, tx: SolanaTransaction) -> int:
        """Simulate transaction to estimate compute units.

        Args:
            tx: The unsigned Solana transaction to simulate.

        Returns:
            Estimated compute units with buffer applied.
        """
        raise NotImplementedError(
            "simulate_cu() is not yet implemented for SolanaTransaction objects. "
            "Use simulate_serialized_tx() for base64 serialized transactions."
        )

    async def simulate_serialized_tx(self, tx_base64: str) -> dict[str, Any]:
        """Simulate a base64-encoded transaction.

        Args:
            tx_base64: Base64-encoded transaction.

        Returns:
            Simulation result with logs, unitsConsumed, etc.
        """
        if not self._rpc:
            raise RuntimeError("No RPC client configured")
        return await self._rpc.simulate_transaction(tx_base64)

    async def resolve_luts(self, addresses: list[str]) -> list[dict[str, Any]]:
        """Resolve Address Lookup Tables.

        Args:
            addresses: List of ALT addresses.

        Returns:
            List of resolved ALT data dicts.
        """
        raise NotImplementedError("resolve_luts() is not yet implemented.")

    async def fetch_blockhash(self) -> str:
        """Fetch a recent blockhash for transaction construction.

        Returns:
            Base58-encoded recent blockhash.
        """
        if not self._rpc:
            raise RuntimeError("No RPC client configured")
        blockhash, _ = await self._rpc.get_latest_blockhash(self.commitment)
        return blockhash

    async def sign_transaction(
        self,
        tx: SolanaTransaction,
        blockhash: str | None = None,
    ) -> SignedSolanaTransaction:
        """Sign a SolanaTransaction with Ed25519.

        Args:
            tx: Unsigned transaction to sign.
            blockhash: Optional blockhash (fetched JIT if None).

        Returns:
            SignedSolanaTransaction ready for submission.
        """
        raise NotImplementedError(
            "sign_transaction() for SolanaTransaction objects is not yet implemented. "
            "Use execute_actions() with ActionBundles containing serialized transactions."
        )

    async def submit_and_confirm(
        self,
        signed_tx: SignedSolanaTransaction,
        commitment: str | None = None,
    ) -> dict[str, Any]:
        """Submit a signed transaction and wait for confirmation.

        Args:
            signed_tx: Signed transaction to submit.
            commitment: Override commitment level.

        Returns:
            Transaction receipt dict.
        """
        raise NotImplementedError(
            "submit_and_confirm() for SignedSolanaTransaction is not yet implemented. "
            "Use execute_actions() with ActionBundles containing serialized transactions."
        )

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _refresh_jupiter_route(self, metadata: dict[str, Any]) -> dict[str, Any]:
        """Refresh a deferred Jupiter swap route for a fresh transaction.

        Calls Jupiter API to get a fresh quote + serialized transaction
        with a fresh blockhash. This is critical because Jupiter routes
        expire within ~60 seconds.

        Args:
            metadata: ActionBundle metadata with route_params.

        Returns:
            Fresh transaction data dict with serialized_transaction.
        """
        from almanak.framework.connectors.jupiter.adapter import JupiterAdapter
        from almanak.framework.connectors.jupiter.client import JupiterConfig

        config = JupiterConfig(wallet_address=self.wallet_address)
        adapter = JupiterAdapter(
            config=config,
            allow_placeholder_prices=True,
        )
        return adapter.get_fresh_swap_transaction(metadata)

    async def _replace_blockhash(self, serialized_tx_base64: str) -> str:
        """Replace the blockhash in a serialized transaction with a fresh one.

        For transactions built at compile time (e.g. Raydium LP), the blockhash
        may expire before execution. This method deserializes the transaction
        message, replaces the blockhash, and re-serializes.

        Args:
            serialized_tx_base64: Base64-encoded unsigned VersionedTransaction.

        Returns:
            Base64-encoded transaction with fresh blockhash.
        """
        import base64 as b64

        from solders.hash import Hash
        from solders.message import MessageV0
        from solders.signature import Signature
        from solders.transaction import VersionedTransaction

        fresh_blockhash = await self.fetch_blockhash()

        tx_bytes = b64.b64decode(serialized_tx_base64)
        tx = VersionedTransaction.from_bytes(tx_bytes)

        # Reconstruct message with fresh blockhash
        old_msg = tx.message
        lookups = list(old_msg.address_table_lookups) if hasattr(old_msg, "address_table_lookups") else []
        new_msg = MessageV0(
            header=old_msg.header,
            account_keys=list(old_msg.account_keys),
            recent_blockhash=Hash.from_string(fresh_blockhash),
            instructions=list(old_msg.instructions),
            address_table_lookups=lookups,
        )

        # Create unsigned transaction with placeholder signatures
        # (real signatures will be added during the signing step)
        num_signers = old_msg.header.num_required_signatures
        placeholder_sigs = [Signature.default()] * num_signers
        new_tx = VersionedTransaction.populate(new_msg, placeholder_sigs)
        return b64.b64encode(bytes(new_tx)).decode("ascii")


def _get_metadata(action_bundle: Any) -> dict[str, Any]:
    """Extract metadata from an ActionBundle (object or dict)."""
    if hasattr(action_bundle, "metadata"):
        return action_bundle.metadata or {}
    if isinstance(action_bundle, dict):
        return action_bundle.get("metadata", {})
    return {}


def _get_sensitive_data(action_bundle: Any) -> dict[str, Any]:
    """Extract sensitive_data from an ActionBundle (not serialized, not logged)."""
    if hasattr(action_bundle, "sensitive_data"):
        return action_bundle.sensitive_data or {}
    if isinstance(action_bundle, dict):
        return action_bundle.get("sensitive_data", {}) or {}
    return {}


def _get_transactions(action_bundle: Any) -> list[dict[str, Any]]:
    """Extract transactions from an ActionBundle (object or dict)."""
    if hasattr(action_bundle, "transactions"):
        return action_bundle.transactions or []
    if isinstance(action_bundle, dict):
        return action_bundle.get("transactions", [])
    return []
