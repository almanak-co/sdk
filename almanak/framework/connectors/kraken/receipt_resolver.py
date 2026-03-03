"""Receipt resolver for Kraken CEX operations.

Handles status polling with exponential backoff and converts
CEX operation results into standardized ExecutionDetails format.

Key features:
- Exponential backoff polling for async operations
- Timeout detection and stuck operation handling
- Standardized result format matching on-chain receipts
"""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime

import structlog

from .exceptions import KrakenOrderNotFoundError, KrakenTimeoutError
from .models import CEXIdempotencyKey, CEXOperationType, KrakenConfig
from .sdk import KrakenSDK

logger = structlog.get_logger(__name__)


# =============================================================================
# Execution Details (Standardized Result)
# =============================================================================


@dataclass
class TokenAmount:
    """Amount of a specific token."""

    token: str
    amount: int  # In wei units
    decimals: int = 18


@dataclass
class ExecutionDetails:
    """Standardized execution result for CEX and on-chain operations.

    This provides a common interface for strategies to process
    results regardless of whether execution happened on-chain or CEX.
    """

    success: bool
    venue: str  # "kraken", "uniswap_v3", etc.
    operation_type: str  # "swap", "withdraw", "deposit"

    # Amounts
    amounts_in: list[TokenAmount] = field(default_factory=list)
    amounts_out: list[TokenAmount] = field(default_factory=list)
    fees: list[TokenAmount] = field(default_factory=list)

    # Identifiers
    source_id: str = ""  # tx_hash for on-chain, order_id/refid for CEX

    # Timing
    timestamp: datetime | None = None

    # CEX-specific metadata
    cex_metadata: dict | None = None

    def to_dict(self) -> dict:
        """Serialize to dict for state persistence."""
        return {
            "success": self.success,
            "venue": self.venue,
            "operation_type": self.operation_type,
            "amounts_in": [{"token": a.token, "amount": a.amount, "decimals": a.decimals} for a in self.amounts_in],
            "amounts_out": [{"token": a.token, "amount": a.amount, "decimals": a.decimals} for a in self.amounts_out],
            "fees": [{"token": a.token, "amount": a.amount, "decimals": a.decimals} for a in self.fees],
            "source_id": self.source_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "cex_metadata": self.cex_metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ExecutionDetails":
        """Deserialize from dict."""
        return cls(
            success=data["success"],
            venue=data["venue"],
            operation_type=data["operation_type"],
            amounts_in=[
                TokenAmount(a["token"], a["amount"], a.get("decimals", 18)) for a in data.get("amounts_in", [])
            ],
            amounts_out=[
                TokenAmount(a["token"], a["amount"], a.get("decimals", 18)) for a in data.get("amounts_out", [])
            ],
            fees=[TokenAmount(a["token"], a["amount"], a.get("decimals", 18)) for a in data.get("fees", [])],
            source_id=data.get("source_id", ""),
            timestamp=(datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None),
            cex_metadata=data.get("cex_metadata"),
        )


# =============================================================================
# Receipt Resolver
# =============================================================================


class KrakenReceiptResolver:
    """Resolves CEX operation results with polling and retry logic.

    This class handles:
    - Polling for operation completion with exponential backoff
    - Converting raw API responses to ExecutionDetails
    - Detecting stuck operations and generating alerts

    Example:
        resolver = KrakenReceiptResolver(sdk, config)

        # Poll for swap completion
        details = await resolver.resolve_swap(
            txid="OXXXXX-XXXXX",
            userref=12345,
            asset_in="USDC",
            asset_out="ETH",
            decimals_in=6,
            decimals_out=18,
        )

        # Poll for withdrawal completion
        details = await resolver.resolve_withdrawal(
            refid="FXXXXX-XXXXX",
            asset="ETH",
            chain="arbitrum",
            decimals=18,
        )
    """

    def __init__(
        self,
        sdk: KrakenSDK,
        config: KrakenConfig | None = None,
    ) -> None:
        """Initialize receipt resolver.

        Args:
            sdk: KrakenSDK instance
            config: Optional configuration for timeouts and polling
        """
        self.sdk = sdk
        self.config = config or KrakenConfig()

    # =========================================================================
    # Swap Resolution
    # =========================================================================

    async def resolve_swap(
        self,
        txid: str,
        userref: int,
        asset_in: str,
        asset_out: str,
        decimals_in: int,
        decimals_out: int,
        chain: str = "ethereum",
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> ExecutionDetails:
        """Poll for swap completion and return execution details.

        Args:
            txid: Order transaction ID
            userref: Order reference for idempotency
            asset_in: Input asset symbol
            asset_out: Output asset symbol
            decimals_in: Input asset decimals
            decimals_out: Output asset decimals
            chain: Chain for token resolution
            idempotency_key: Optional key for tracking last poll time

        Returns:
            ExecutionDetails with swap result

        Raises:
            KrakenTimeoutError: If operation times out
        """
        status = await self._poll_swap_status(
            txid=txid,
            userref=userref,
            timeout=self.config.order_timeout_seconds,
            idempotency_key=idempotency_key,
        )

        if status in ("pending", "unknown"):
            raise KrakenTimeoutError(
                "swap",
                self.config.order_timeout_seconds,
                txid,
            )

        # Get detailed result
        if status in ("success", "partial"):
            result = self.sdk.get_swap_result(
                txid=txid,
                userref=userref,
                asset_in=asset_in,
                asset_out=asset_out,
                decimals_in=decimals_in,
                decimals_out=decimals_out,
                chain=chain,
            )

            return ExecutionDetails(
                success=(status == "success"),
                venue="kraken",
                operation_type="swap",
                amounts_in=[TokenAmount(asset_in, result["amount_in"], decimals_in)],
                amounts_out=[TokenAmount(asset_out, result["amount_out"], decimals_out)],
                fees=[
                    TokenAmount(
                        result["fee_asset"],
                        result["fee"],
                        decimals_in if result["fee_asset"] == asset_in else decimals_out,
                    )
                ],
                source_id=txid,
                timestamp=(datetime.fromtimestamp(result["timestamp"], tz=UTC) if result["timestamp"] else None),
                cex_metadata={
                    "userref": userref,
                    "average_price": str(result["average_price"]),
                    "status": status,
                },
            )

        # Failed or cancelled
        return ExecutionDetails(
            success=False,
            venue="kraken",
            operation_type="swap",
            source_id=txid,
            cex_metadata={
                "userref": userref,
                "status": status,
            },
        )

    async def _poll_swap_status(
        self,
        txid: str,
        userref: int,
        timeout: int,
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> str:
        """Poll swap status with exponential backoff.

        Args:
            txid: Order ID
            userref: Order reference
            timeout: Maximum wait time in seconds
            idempotency_key: Optional key for tracking

        Returns:
            Final status string
        """
        start_time = asyncio.get_event_loop().time()
        delay = self.config.poll_interval_seconds

        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                status = self.sdk.get_swap_status(txid, userref)

                # Update idempotency key if provided
                if idempotency_key:
                    idempotency_key.last_poll = datetime.now(UTC)

                logger.debug(
                    "Polled swap status",
                    txid=txid,
                    userref=userref,
                    status=status,
                )

                if status not in ("pending",):
                    return status

            except KrakenOrderNotFoundError:
                logger.warning("Order not found, may not have been submitted yet")

            await asyncio.sleep(delay)
            delay = min(
                delay * self.config.poll_backoff_factor,
                self.config.poll_max_interval_seconds,
            )

        return "pending"  # Timeout - still pending

    # =========================================================================
    # Withdrawal Resolution
    # =========================================================================

    async def resolve_withdrawal(
        self,
        refid: str,
        asset: str,
        chain: str,
        decimals: int,
        to_address: str,
        amount: int,
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> ExecutionDetails:
        """Poll for withdrawal completion and return execution details.

        Args:
            refid: Kraken withdrawal reference ID
            asset: Asset symbol
            chain: Target chain
            decimals: Asset decimals
            to_address: Destination address
            amount: Withdrawal amount in wei
            idempotency_key: Optional key for tracking

        Returns:
            ExecutionDetails with withdrawal result

        Raises:
            KrakenTimeoutError: If operation times out
        """
        status, tx_hash = await self._poll_withdrawal_status(
            refid=refid,
            asset=asset,
            chain=chain,
            timeout=self.config.withdrawal_timeout_seconds,
            idempotency_key=idempotency_key,
        )

        if status in ("pending", "unknown", None):
            raise KrakenTimeoutError(
                "withdrawal",
                self.config.withdrawal_timeout_seconds,
                refid,
            )

        return ExecutionDetails(
            success=(status == "success"),
            venue="kraken",
            operation_type="withdraw",
            amounts_out=[TokenAmount(asset, amount, decimals)],
            source_id=tx_hash or refid,
            timestamp=datetime.now(UTC),
            cex_metadata={
                "refid": refid,
                "chain": chain,
                "to_address": to_address,
                "tx_hash": tx_hash,
                "status": status,
            },
        )

    async def _poll_withdrawal_status(
        self,
        refid: str,
        asset: str,
        chain: str,
        timeout: int,
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> tuple[str, str | None]:
        """Poll withdrawal status with exponential backoff.

        Args:
            refid: Withdrawal reference ID
            asset: Asset symbol
            chain: Target chain
            timeout: Maximum wait time
            idempotency_key: Optional key for tracking

        Returns:
            Tuple of (status, tx_hash if available)
        """
        start_time = asyncio.get_event_loop().time()
        delay = self.config.poll_interval_seconds

        while asyncio.get_event_loop().time() - start_time < timeout:
            status = self.sdk.get_withdrawal_status(asset, chain, refid=refid)

            # Update idempotency key if provided
            if idempotency_key:
                idempotency_key.last_poll = datetime.now(UTC)

            logger.debug(
                "Polled withdrawal status",
                refid=refid,
                status=status,
            )

            if status and status != "pending":
                tx_hash = self.sdk.get_withdrawal_tx_hash(asset, chain, refid)
                return status, tx_hash

            await asyncio.sleep(delay)
            delay = min(
                delay * self.config.poll_backoff_factor,
                self.config.poll_max_interval_seconds,
            )

        return "pending", None  # Timeout

    # =========================================================================
    # Deposit Resolution
    # =========================================================================

    async def resolve_deposit(
        self,
        tx_hash: str,
        asset: str,
        chain: str,
        decimals: int,
        amount: int,
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> ExecutionDetails:
        """Poll for deposit confirmation on Kraken.

        Args:
            tx_hash: On-chain transaction hash of deposit
            asset: Asset symbol
            chain: Source chain
            decimals: Asset decimals
            amount: Deposit amount in wei
            idempotency_key: Optional key for tracking

        Returns:
            ExecutionDetails with deposit result

        Raises:
            KrakenTimeoutError: If operation times out
        """
        status = await self._poll_deposit_status(
            tx_hash=tx_hash,
            asset=asset,
            chain=chain,
            timeout=self.config.deposit_timeout_seconds,
            idempotency_key=idempotency_key,
        )

        if status in ("pending", "unknown", None):
            raise KrakenTimeoutError(
                "deposit",
                self.config.deposit_timeout_seconds,
                tx_hash,
            )

        return ExecutionDetails(
            success=(status == "success"),
            venue="kraken",
            operation_type="deposit",
            amounts_in=[TokenAmount(asset, amount, decimals)],
            source_id=tx_hash,
            timestamp=datetime.now(UTC),
            cex_metadata={
                "tx_hash": tx_hash,
                "chain": chain,
                "status": status,
            },
        )

    async def _poll_deposit_status(
        self,
        tx_hash: str,
        asset: str,
        chain: str,
        timeout: int,
        idempotency_key: CEXIdempotencyKey | None = None,
    ) -> str:
        """Poll deposit status with exponential backoff.

        Args:
            tx_hash: On-chain transaction hash
            asset: Asset symbol
            chain: Source chain
            timeout: Maximum wait time
            idempotency_key: Optional key for tracking

        Returns:
            Status string
        """
        start_time = asyncio.get_event_loop().time()
        delay = self.config.poll_interval_seconds

        while asyncio.get_event_loop().time() - start_time < timeout:
            status = self.sdk.get_deposit_status(tx_hash, asset, chain)

            # Update idempotency key if provided
            if idempotency_key:
                idempotency_key.last_poll = datetime.now(UTC)

            logger.debug(
                "Polled deposit status",
                tx_hash=tx_hash,
                status=status,
            )

            if status and status != "pending":
                return status

            await asyncio.sleep(delay)
            delay = min(
                delay * self.config.poll_backoff_factor,
                self.config.poll_max_interval_seconds,
            )

        return "pending"  # Timeout

    # =========================================================================
    # Crash Recovery
    # =========================================================================

    async def resume_operation(
        self,
        key: CEXIdempotencyKey,
        **context,
    ) -> ExecutionDetails | None:
        """Resume a pending operation after restart.

        Uses the idempotency key to check operation status and
        either return the result or resume polling.

        Args:
            key: Idempotency key from persisted state
            **context (Any): Additional context (asset names, decimals, etc.)

        Returns:
            ExecutionDetails if operation completed, None if still pending
        """
        if key.operation_type == CEXOperationType.SWAP:
            if not key.order_id or not key.userref:
                logger.warning("Cannot resume swap without order_id and userref")
                return None

            status = self.sdk.get_swap_status(key.order_id, key.userref)

            if status == "pending":
                return None  # Still pending

            if status in ("success", "partial"):
                return await self.resolve_swap(
                    txid=key.order_id,
                    userref=key.userref,
                    asset_in=context.get("asset_in", ""),
                    asset_out=context.get("asset_out", ""),
                    decimals_in=context.get("decimals_in", 18),
                    decimals_out=context.get("decimals_out", 18),
                    chain=context.get("chain", "ethereum"),
                    idempotency_key=key,
                )

            # Failed or cancelled
            return ExecutionDetails(
                success=False,
                venue="kraken",
                operation_type="swap",
                source_id=key.order_id,
                cex_metadata={"userref": key.userref, "status": status},
            )

        elif key.operation_type == CEXOperationType.WITHDRAW:
            if not key.refid:
                logger.warning("Cannot resume withdrawal without refid")
                return None

            withdraw_status = self.sdk.get_withdrawal_status(
                context.get("asset", ""),
                context.get("chain", ""),
                refid=key.refid,
            )

            if withdraw_status is None or withdraw_status == "pending":
                return None  # Still pending

            tx_hash = self.sdk.get_withdrawal_tx_hash(
                context.get("asset", ""),
                context.get("chain", ""),
                key.refid,
            )

            return ExecutionDetails(
                success=(withdraw_status == "success"),
                venue="kraken",
                operation_type="withdraw",
                source_id=tx_hash or key.refid,
                cex_metadata={"refid": key.refid, "status": withdraw_status, "tx_hash": tx_hash},
            )

        elif key.operation_type == CEXOperationType.DEPOSIT:
            if not key.order_id:  # order_id used for tx_hash in deposits
                logger.warning("Cannot resume deposit without tx_hash")
                return None

            deposit_status = self.sdk.get_deposit_status(
                key.order_id,
                context.get("asset"),
                context.get("chain"),
            )

            if deposit_status is None or deposit_status == "pending":
                return None

            return ExecutionDetails(
                success=(deposit_status == "success"),
                venue="kraken",
                operation_type="deposit",
                source_id=key.order_id,
                cex_metadata={"tx_hash": key.order_id, "status": deposit_status},
            )

        return None


__all__ = [
    "TokenAmount",
    "ExecutionDetails",
    "KrakenReceiptResolver",
]
