"""Lightweight Solana JSON-RPC client.

Uses requests (already in deps) for synchronous HTTP calls to Solana RPC endpoints.
This avoids pulling in the heavy solana-py dependency — we only need a handful of
RPC methods for transaction signing and submission.

Async wrappers run the sync calls in a thread pool to avoid blocking the event loop.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Default timeout for RPC calls
DEFAULT_RPC_TIMEOUT = 30

# Confirmation polling interval
POLL_INTERVAL_SECONDS = 0.5

# Maximum confirmation wait time (60s, ~100 slots)
MAX_CONFIRM_WAIT_SECONDS = 60


@dataclass
class SolanaRpcConfig:
    """Configuration for the Solana RPC client.

    Attributes:
        rpc_url: Solana RPC endpoint URL.
        commitment: Default commitment level for queries.
        timeout: HTTP timeout in seconds.
    """

    rpc_url: str
    commitment: str = "confirmed"
    timeout: int = DEFAULT_RPC_TIMEOUT


class SolanaRpcError(Exception):
    """Error from a Solana RPC call."""

    def __init__(self, method: str, error: dict[str, Any] | str, code: int = -1) -> None:
        self.method = method
        self.error = error
        self.code = code
        msg = f"Solana RPC error in {method}: {error}"
        if code != -1:
            msg += f" (code={code})"
        super().__init__(msg)


class SolanaTransactionError(Exception):
    """Error during transaction execution (on-chain failure)."""

    def __init__(self, signature: str, error: Any, logs: list[str] | None = None) -> None:
        self.signature = signature
        self.error = error
        self.logs = logs or []
        super().__init__(f"Transaction {signature} failed: {error}")


@dataclass
class ConfirmationResult:
    """Result of transaction confirmation polling.

    Attributes:
        signature: Transaction signature (base58).
        confirmed: Whether the transaction reached the requested commitment level.
        slot: Slot number of confirmation.
        err: Error from on-chain execution (None if success).
    """

    signature: str
    confirmed: bool
    slot: int = 0
    err: dict[str, Any] | None = None


@dataclass
class TransactionReceipt:
    """Parsed transaction receipt from getTransaction.

    Attributes:
        signature: Transaction signature (base58).
        slot: Slot number.
        block_time: Unix timestamp of the block.
        fee_lamports: Transaction fee in lamports.
        success: Whether the transaction succeeded.
        err: Error dict (None if success).
        logs: Program log messages.
        pre_token_balances: Token balances before execution.
        post_token_balances: Token balances after execution.
    """

    signature: str
    slot: int = 0
    block_time: int | None = None
    fee_lamports: int = 0
    success: bool = True
    err: dict[str, Any] | None = None
    logs: list[str] = field(default_factory=list)
    pre_token_balances: list[dict[str, Any]] = field(default_factory=list)
    post_token_balances: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for receipt parsing."""
        return {
            "signature": self.signature,
            "slot": self.slot,
            "block_time": self.block_time,
            "fee_lamports": self.fee_lamports,
            "success": self.success,
            "err": self.err,
            "logs": self.logs,
            "pre_token_balances": self.pre_token_balances,
            "post_token_balances": self.post_token_balances,
        }


class SolanaRpcClient:
    """Lightweight Solana JSON-RPC client.

    Uses requests for HTTP calls. Provides both sync methods (prefixed with _sync)
    and async wrappers that run sync calls in a thread pool.

    Example:
        client = SolanaRpcClient(SolanaRpcConfig(rpc_url="https://api.mainnet-beta.solana.com"))
        blockhash = await client.get_latest_blockhash()
        sig = await client.send_transaction(signed_tx_base64)
        receipt = await client.confirm_and_get_receipt(sig)
    """

    def __init__(self, config: SolanaRpcConfig) -> None:
        self._config = config
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._request_id_counter = itertools.count(1)

    @property
    def rpc_url(self) -> str:
        return self._config.rpc_url

    def _next_id(self) -> int:
        return next(self._request_id_counter)

    def _rpc_call(self, method: str, params: list[Any] | None = None) -> Any:
        """Make a synchronous JSON-RPC call.

        Args:
            method: RPC method name.
            params: RPC parameters.

        Returns:
            The "result" field from the RPC response.

        Raises:
            SolanaRpcError: If the RPC returns an error.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params or [],
        }
        response = self._session.post(
            self._config.rpc_url,
            json=payload,
            timeout=self._config.timeout,
        )
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            err = data["error"]
            code = err.get("code", -1) if isinstance(err, dict) else -1
            raise SolanaRpcError(method, err, code)

        return data.get("result")

    async def _async_rpc_call(self, method: str, params: list[Any] | None = None) -> Any:
        """Async wrapper around _rpc_call using thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._rpc_call, method, params)

    # =========================================================================
    # RPC Methods
    # =========================================================================

    async def get_health(self) -> bool:
        """Check if the Solana node is healthy.

        Returns:
            True if healthy, False otherwise.
        """
        try:
            result = await self._async_rpc_call("getHealth")
            return result == "ok"
        except Exception:
            return False

    async def get_latest_blockhash(self, commitment: str | None = None) -> tuple[str, int]:
        """Get the latest blockhash for transaction construction.

        Args:
            commitment: Commitment level override.

        Returns:
            Tuple of (blockhash, last_valid_block_height).
        """
        params: list[Any] = [{"commitment": commitment or self._config.commitment}]
        result = await self._async_rpc_call("getLatestBlockhash", params)
        value = result["value"]
        return value["blockhash"], value["lastValidBlockHeight"]

    async def send_transaction(
        self,
        signed_tx_base64: str,
        skip_preflight: bool = False,
        max_retries: int = 3,
    ) -> str:
        """Send a signed transaction to the network.

        Args:
            signed_tx_base64: Base64-encoded signed transaction bytes.
            skip_preflight: Skip preflight simulation.
            max_retries: Maximum send retries.

        Returns:
            Transaction signature (base58).

        Raises:
            SolanaRpcError: If the RPC rejects the transaction.
        """
        params: list[Any] = [
            signed_tx_base64,
            {
                "encoding": "base64",
                "skipPreflight": skip_preflight,
                "preflightCommitment": self._config.commitment,
                "maxRetries": max_retries,
            },
        ]
        signature = await self._async_rpc_call("sendTransaction", params)
        logger.info(f"Transaction sent: {signature}")
        return signature

    async def get_signature_statuses(
        self,
        signatures: list[str],
        search_transaction_history: bool = False,
    ) -> list[dict[str, Any] | None]:
        """Get the status of one or more transaction signatures.

        Args:
            signatures: List of base58 transaction signatures.
            search_transaction_history: Search beyond recent slots.

        Returns:
            List of status dicts (or None for unknown signatures).
        """
        params: list[Any] = [
            signatures,
            {"searchTransactionHistory": search_transaction_history},
        ]
        result = await self._async_rpc_call("getSignatureStatuses", params)
        return result["value"]

    async def get_transaction(
        self,
        signature: str,
        commitment: str | None = None,
    ) -> dict[str, Any] | None:
        """Get full transaction details by signature.

        Args:
            signature: Base58 transaction signature.
            commitment: Commitment level override.

        Returns:
            Transaction details dict, or None if not found.
        """
        params: list[Any] = [
            signature,
            {
                "encoding": "json",
                "commitment": commitment or self._config.commitment,
                "maxSupportedTransactionVersion": 0,
            },
        ]
        return await self._async_rpc_call("getTransaction", params)

    async def confirm_transaction(
        self,
        signature: str,
        commitment: str | None = None,
        timeout_seconds: float = MAX_CONFIRM_WAIT_SECONDS,
    ) -> ConfirmationResult:
        """Poll until a transaction is confirmed or times out.

        Args:
            signature: Transaction signature to confirm.
            commitment: Commitment level override.
            timeout_seconds: Maximum wait time.

        Returns:
            ConfirmationResult with confirmation status.
        """
        target_commitment = commitment or self._config.commitment
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            statuses = await self.get_signature_statuses([signature], search_transaction_history=True)
            status = statuses[0] if statuses else None

            if status is not None:
                confirmation_status = status.get("confirmationStatus", "")
                err = status.get("err")

                # Check if we've reached the desired commitment level
                if err is not None:
                    # Transaction failed on-chain
                    return ConfirmationResult(
                        signature=signature,
                        confirmed=True,
                        slot=status.get("slot", 0),
                        err=err,
                    )

                commitment_reached = _commitment_met(confirmation_status, target_commitment)
                if commitment_reached:
                    return ConfirmationResult(
                        signature=signature,
                        confirmed=True,
                        slot=status.get("slot", 0),
                    )

            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        logger.warning(f"Transaction {signature} confirmation timed out after {timeout_seconds}s")
        return ConfirmationResult(signature=signature, confirmed=False)

    async def confirm_and_get_receipt(
        self,
        signature: str,
        commitment: str | None = None,
        timeout_seconds: float = MAX_CONFIRM_WAIT_SECONDS,
    ) -> TransactionReceipt:
        """Confirm a transaction and fetch its full receipt.

        Args:
            signature: Transaction signature.
            commitment: Commitment level.
            timeout_seconds: Maximum wait time for confirmation.

        Returns:
            TransactionReceipt with full details.

        Raises:
            SolanaTransactionError: If the transaction fails on-chain.
            TimeoutError: If confirmation times out.
        """
        # Wait for confirmation
        confirmation = await self.confirm_transaction(signature, commitment, timeout_seconds)

        if not confirmation.confirmed:
            raise TimeoutError(f"Transaction {signature} was not confirmed within {timeout_seconds}s")

        if confirmation.err:
            raise SolanaTransactionError(
                signature=signature,
                error=confirmation.err,
            )

        # Fetch full receipt
        tx_data = await self.get_transaction(signature, commitment)
        if tx_data is None:
            logger.warning(f"Transaction {signature} confirmed but getTransaction returned None")
            return TransactionReceipt(
                signature=signature,
                slot=confirmation.slot,
                success=True,
            )

        return _parse_transaction_response(signature, tx_data)

    async def get_token_accounts_by_owner(
        self,
        wallet_address: str,
        mint_address: str,
        commitment: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get SPL token accounts for a wallet filtered by mint.

        Uses the getTokenAccountsByOwner RPC method with jsonParsed encoding
        to get human-readable token balances.

        Args:
            wallet_address: Wallet public key (base58).
            mint_address: SPL token mint address (base58).
            commitment: Commitment level override.

        Returns:
            List of token account dicts with parsed balance info.
            Each dict has: pubkey, account.data.parsed.info.tokenAmount
            (amount, decimals, uiAmount, uiAmountString).
        """
        params: list[Any] = [
            wallet_address,
            {"mint": mint_address},
            {"encoding": "jsonParsed", "commitment": commitment or self._config.commitment},
        ]
        result = await self._async_rpc_call("getTokenAccountsByOwner", params)
        return result.get("value", [])

    async def get_spl_token_balance(
        self,
        wallet_address: str,
        mint_address: str,
    ) -> tuple[int, int]:
        """Get the total SPL token balance for a wallet.

        Queries all token accounts for the given mint and sums the balances.

        Args:
            wallet_address: Wallet public key (base58).
            mint_address: SPL token mint address (base58).

        Returns:
            Tuple of (raw_amount, decimals). raw_amount is the total balance
            in smallest units. Returns (0, 0) if no token accounts found.
        """
        accounts = await self.get_token_accounts_by_owner(wallet_address, mint_address)
        if not accounts:
            return 0, 0

        total_raw = 0
        decimals = 0
        for account in accounts:
            try:
                parsed = account["account"]["data"]["parsed"]["info"]["tokenAmount"]
                total_raw += int(parsed["amount"])
                decimals = int(parsed["decimals"])
            except (KeyError, ValueError, TypeError):
                continue

        return total_raw, decimals

    async def simulate_transaction(
        self,
        tx_base64: str,
        commitment: str | None = None,
    ) -> dict[str, Any]:
        """Simulate a transaction without sending it.

        Args:
            tx_base64: Base64-encoded transaction bytes.
            commitment: Commitment level.

        Returns:
            Simulation result dict with logs, unitsConsumed, etc.
        """
        params: list[Any] = [
            tx_base64,
            {
                "encoding": "base64",
                "commitment": commitment or self._config.commitment,
                "replaceRecentBlockhash": True,
            },
        ]
        result = await self._async_rpc_call("simulateTransaction", params)
        return result.get("value", {})


def _commitment_met(actual: str, target: str) -> bool:
    """Check if the actual commitment level meets or exceeds the target.

    Commitment hierarchy: processed < confirmed < finalized.
    """
    levels = {"processed": 0, "confirmed": 1, "finalized": 2}
    actual_level = levels.get(actual, -1)
    target_level = levels.get(target, 1)
    return actual_level >= target_level


def _parse_transaction_response(signature: str, tx_data: dict[str, Any]) -> TransactionReceipt:
    """Parse a getTransaction RPC response into a TransactionReceipt."""
    meta = tx_data.get("meta", {}) or {}
    return TransactionReceipt(
        signature=signature,
        slot=tx_data.get("slot", 0),
        block_time=tx_data.get("blockTime"),
        fee_lamports=meta.get("fee", 0),
        success=meta.get("err") is None,
        err=meta.get("err"),
        logs=meta.get("logMessages", []),
        pre_token_balances=meta.get("preTokenBalances", []),
        post_token_balances=meta.get("postTokenBalances", []),
    )


__all__ = [
    "ConfirmationResult",
    "SolanaRpcClient",
    "SolanaRpcConfig",
    "SolanaRpcError",
    "SolanaTransactionError",
    "TransactionReceipt",
]
