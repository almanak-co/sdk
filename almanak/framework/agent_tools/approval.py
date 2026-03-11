"""Human-approval actuator for high-value agent trades.

When a trade exceeds the configured ``require_human_approval_above_usd``
threshold, the actuator creates an ``ApprovalRequest``, notifies the
operator via the configured channel, and waits for a response with a
configurable timeout.

Supported notification channels:
- **file**: Writes a JSON request to a directory and watches for a
  response file. Ideal for local development and testing.
- **webhook**: POSTs the request to a URL and polls for the decision.
- **console**: Prints to stdout and reads from stdin (interactive TTY).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums & Models
# ---------------------------------------------------------------------------


class ApprovalStatus(StrEnum):
    """Status of a human-approval request."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class ApprovalChannel(StrEnum):
    """Notification channel used to deliver approval requests."""

    FILE = "file"
    WEBHOOK = "webhook"
    CONSOLE = "console"


@dataclass
class ApprovalRequest:
    """A pending human-approval request for a high-value trade.

    Attributes:
        request_id: Unique identifier for this approval request.
        tool_name: Name of the agent tool that triggered the request.
        args: Original arguments passed to the tool.
        estimated_value_usd: Estimated USD value of the trade.
        threshold_usd: The approval threshold that was exceeded.
        timestamp: When the request was created (UTC).
        expires_at: When the request expires if no response is received.
        status: Current status of the request.
        reason: Optional reason provided by the reviewer on reject.
    """

    request_id: str
    tool_name: str
    args: dict
    estimated_value_usd: Decimal
    threshold_usd: Decimal
    timestamp: datetime
    expires_at: datetime
    status: ApprovalStatus = ApprovalStatus.PENDING
    reason: str = ""

    def is_expired(self) -> bool:
        """Check whether this request has passed its expiry time."""
        return datetime.now(UTC) >= self.expires_at

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dictionary."""
        return {
            "request_id": self.request_id,
            "tool_name": self.tool_name,
            "args": self.args,
            "estimated_value_usd": str(self.estimated_value_usd),
            "threshold_usd": str(self.threshold_usd),
            "timestamp": self.timestamp.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "status": self.status.value,
            "reason": self.reason,
        }


@dataclass
class ApprovalDecision:
    """The outcome of an approval request.

    Attributes:
        request_id: The request this decision applies to.
        status: Final status (APPROVED, REJECTED, or EXPIRED).
        decided_at: When the decision was made.
        decided_by: Identifier of the reviewer (optional).
        reason: Free-text reason for rejection (optional).
    """

    request_id: str
    status: ApprovalStatus
    decided_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    decided_by: str = ""
    reason: str = ""


@dataclass
class ApprovalConfig:
    """Configuration for the human-approval actuator.

    Attributes:
        channel: Notification channel to use (file, webhook, console).
        timeout_seconds: How long to wait for a response before expiring.
        webhook_url: URL to POST approval requests to (webhook channel).
        webhook_poll_url: URL to poll for decisions (webhook channel).
            If not set, defaults to ``{webhook_url}/status``.
        webhook_poll_interval_seconds: Seconds between polls (webhook channel).
        queue_dir: Directory for file-based approval queue (file channel).
    """

    channel: ApprovalChannel = ApprovalChannel.FILE
    timeout_seconds: int = 300
    webhook_url: str | None = None
    webhook_poll_url: str | None = None
    webhook_poll_interval_seconds: int = 5
    queue_dir: str | None = None


# ---------------------------------------------------------------------------
# Notification channel interface
# ---------------------------------------------------------------------------


class ApprovalNotifier:
    """Base class for approval notification channels."""

    def notify(self, request: ApprovalRequest) -> None:
        """Send the approval request to the operator."""
        raise NotImplementedError

    def poll(self, request: ApprovalRequest) -> ApprovalDecision | None:
        """Check whether the operator has responded.

        Returns ``None`` if no decision has been made yet.
        """
        raise NotImplementedError


class FileApprovalNotifier(ApprovalNotifier):
    """File-based approval channel for local development and testing.

    Writes a JSON request file to ``queue_dir`` and watches for a
    corresponding ``.approved`` or ``.rejected`` response file.

    Request file: ``{queue_dir}/{request_id}.request.json``
    Approve: create ``{queue_dir}/{request_id}.approved``
    Reject: create ``{queue_dir}/{request_id}.rejected`` (content = reason)
    """

    def __init__(self, queue_dir: str) -> None:
        self._queue_dir = Path(queue_dir)
        self._queue_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def notify(self, request: ApprovalRequest) -> None:
        request_path = self._queue_dir / f"{request.request_id}.request.json"
        request_path.write_text(json.dumps(request.to_dict(), indent=2))
        logger.info(
            "Approval request written to %s  --  approve: touch %s/%s.approved  |  reject: touch %s/%s.rejected",
            request_path,
            self._queue_dir,
            request.request_id,
            self._queue_dir,
            request.request_id,
        )

    def poll(self, request: ApprovalRequest) -> ApprovalDecision | None:
        approved_path = self._queue_dir / f"{request.request_id}.approved"
        rejected_path = self._queue_dir / f"{request.request_id}.rejected"

        if approved_path.exists():
            return ApprovalDecision(
                request_id=request.request_id,
                status=ApprovalStatus.APPROVED,
                decided_by="file",
            )

        if rejected_path.exists():
            reason = ""
            try:
                content = rejected_path.read_text().strip()
                if content:
                    reason = content
            except OSError as exc:
                logger.warning("Failed to read rejection reason from %s: %s", rejected_path, exc)
            return ApprovalDecision(
                request_id=request.request_id,
                status=ApprovalStatus.REJECTED,
                decided_by="file",
                reason=reason,
            )

        return None


class WebhookApprovalNotifier(ApprovalNotifier):
    """Webhook-based approval channel for production use.

    POSTs the approval request as JSON to a configurable URL and polls
    a status URL for the operator's decision.
    """

    def __init__(
        self,
        webhook_url: str,
        poll_url: str | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._poll_url = poll_url or f"{webhook_url}/status"

    def notify(self, request: ApprovalRequest) -> None:
        try:
            resp = httpx.post(
                self._webhook_url,
                json=request.to_dict(),
                timeout=10.0,
            )
            resp.raise_for_status()
            logger.info(
                "Approval request %s sent to webhook %s (status=%d)",
                request.request_id,
                self._webhook_url,
                resp.status_code,
            )
        except httpx.HTTPError as exc:
            logger.error("Failed to send approval request %s to webhook: %s", request.request_id, exc)

    def poll(self, request: ApprovalRequest) -> ApprovalDecision | None:
        try:
            resp = httpx.get(
                self._poll_url,
                params={"request_id": request.request_id},
                timeout=10.0,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                logger.warning("Unexpected poll response type for %s: %s", request.request_id, type(data).__name__)
                return None
            status_str = data.get("status", "pending")

            if status_str == "approved":
                return ApprovalDecision(
                    request_id=request.request_id,
                    status=ApprovalStatus.APPROVED,
                    decided_by=data.get("decided_by", "webhook"),
                    reason=data.get("reason", ""),
                )
            elif status_str == "rejected":
                return ApprovalDecision(
                    request_id=request.request_id,
                    status=ApprovalStatus.REJECTED,
                    decided_by=data.get("decided_by", "webhook"),
                    reason=data.get("reason", ""),
                )
            # Still pending
            return None
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            logger.warning("Failed to poll approval status for %s: %s", request.request_id, exc)
            return None


class ConsoleApprovalNotifier(ApprovalNotifier):
    """Interactive console-based approval channel.

    Prints the approval request to stdout and reads y/n from stdin.
    Only works when stdin is a TTY.
    """

    def __init__(self) -> None:
        self._decisions: dict[str, ApprovalDecision] = {}

    def notify(self, request: ApprovalRequest) -> None:
        print(  # noqa: T201
            f"\n{'=' * 60}\n"
            f"HUMAN APPROVAL REQUIRED\n"
            f"{'=' * 60}\n"
            f"Request ID:  {request.request_id}\n"
            f"Tool:        {request.tool_name}\n"
            f"Value (USD): ${request.estimated_value_usd:,.2f}\n"
            f"Threshold:   ${request.threshold_usd:,.2f}\n"
            f"Expires at:  {request.expires_at.isoformat()}\n"
            f"Arguments:   {json.dumps(request.args, indent=2)}\n"
            f"{'=' * 60}",
            flush=True,
        )

        if not sys.stdin.isatty():
            logger.warning(
                "Console approval requested but stdin is not a TTY. Request %s will expire after timeout.",
                request.request_id,
            )
            return

        try:
            response = input("Approve this trade? [y/N]: ").strip().lower()
            if response in ("y", "yes"):
                self._decisions[request.request_id] = ApprovalDecision(
                    request_id=request.request_id,
                    status=ApprovalStatus.APPROVED,
                    decided_by="console",
                )
            else:
                reason = ""
                if response not in ("n", "no", ""):
                    reason = response
                self._decisions[request.request_id] = ApprovalDecision(
                    request_id=request.request_id,
                    status=ApprovalStatus.REJECTED,
                    decided_by="console",
                    reason=reason,
                )
        except (EOFError, KeyboardInterrupt):
            self._decisions[request.request_id] = ApprovalDecision(
                request_id=request.request_id,
                status=ApprovalStatus.REJECTED,
                decided_by="console",
                reason="Input interrupted",
            )

    def poll(self, request: ApprovalRequest) -> ApprovalDecision | None:
        return self._decisions.get(request.request_id)


# ---------------------------------------------------------------------------
# Main actuator
# ---------------------------------------------------------------------------


class HumanApprovalActuator:
    """Orchestrates the human-approval flow for high-value agent trades.

    This is the main entry point. It:
    1. Creates an ``ApprovalRequest`` from a tool call.
    2. Notifies the operator via the configured channel.
    3. Polls for a response with a configurable timeout.
    4. Returns the final ``ApprovalDecision``.

    All requests and decisions are logged for audit trail.

    Args:
        config: Approval configuration (channel, timeout, etc.).
        notifier: Optional explicit notifier override (mainly for testing).
    """

    def __init__(
        self,
        config: ApprovalConfig,
        *,
        notifier: ApprovalNotifier | None = None,
    ) -> None:
        self._config = config
        self._notifier = notifier or self._create_notifier(config)

        # In-memory log of all requests for this session (audit trail).
        # Capped at 1000 entries to prevent unbounded memory growth.
        self._max_history = 1000
        self._requests: dict[str, ApprovalRequest] = {}
        self._decisions: dict[str, ApprovalDecision] = {}

    @staticmethod
    def _create_notifier(config: ApprovalConfig) -> ApprovalNotifier:
        """Create the appropriate notifier from config."""
        if config.channel == ApprovalChannel.FILE:
            queue_dir = config.queue_dir or os.path.join(os.getcwd(), ".almanak", "approval_queue")
            return FileApprovalNotifier(queue_dir)
        elif config.channel == ApprovalChannel.WEBHOOK:
            if not config.webhook_url:
                raise ValueError("approval_webhook_url is required when channel is 'webhook'")
            return WebhookApprovalNotifier(
                webhook_url=config.webhook_url,
                poll_url=config.webhook_poll_url,
            )
        elif config.channel == ApprovalChannel.CONSOLE:
            return ConsoleApprovalNotifier()
        else:
            raise ValueError(f"Unknown approval channel: {config.channel}")

    def request_approval(
        self,
        tool_name: str,
        args: dict,
        estimated_value_usd: Decimal,
        threshold_usd: Decimal,
    ) -> ApprovalRequest:
        """Create and send an approval request.

        Args:
            tool_name: Name of the tool being executed.
            args: Arguments to the tool call.
            estimated_value_usd: Estimated USD value of the trade.
            threshold_usd: The threshold that was exceeded.

        Returns:
            The created ``ApprovalRequest`` (status=PENDING).
        """
        now = datetime.now(UTC)
        request = ApprovalRequest(
            request_id=str(uuid.uuid4()),
            tool_name=tool_name,
            args=args,
            estimated_value_usd=estimated_value_usd,
            threshold_usd=threshold_usd,
            timestamp=now,
            expires_at=now + timedelta(seconds=self._config.timeout_seconds),
        )

        self._requests[request.request_id] = request

        logger.info(
            "Approval required: tool=%s value=$%s threshold=$%s request_id=%s",
            tool_name,
            estimated_value_usd,
            threshold_usd,
            request.request_id,
        )

        self._notifier.notify(request)
        return request

    def poll_decision(self, request: ApprovalRequest) -> ApprovalDecision | None:
        """Poll for a decision on a pending request.

        Returns the decision if available, or ``None`` if still pending.
        Automatically marks the request as EXPIRED if the timeout has elapsed.
        """
        # Already decided?
        if request.request_id in self._decisions:
            return self._decisions[request.request_id]

        # Check timeout first
        if request.is_expired():
            decision = ApprovalDecision(
                request_id=request.request_id,
                status=ApprovalStatus.EXPIRED,
                reason="Approval timeout exceeded",
            )
            self._record_decision(request, decision)
            return decision

        # Poll the notifier
        polled = self._notifier.poll(request)
        if polled is not None:
            self._record_decision(request, polled)
            return polled

        return None

    async def wait_for_decision(
        self,
        request: ApprovalRequest,
        *,
        poll_interval: float = 1.0,
    ) -> ApprovalDecision:
        """Wait until a decision is made or the request expires.

        Uses ``asyncio.sleep`` so the event loop is not blocked while
        waiting for human approval (which may take minutes).

        Args:
            request: The pending approval request.
            poll_interval: Seconds between poll attempts.

        Returns:
            The final ``ApprovalDecision``.
        """
        while True:
            decision = self.poll_decision(request)
            if decision is not None:
                return decision
            await asyncio.sleep(poll_interval)

    def get_request(self, request_id: str) -> ApprovalRequest | None:
        """Look up a request by ID."""
        return self._requests.get(request_id)

    def get_decision(self, request_id: str) -> ApprovalDecision | None:
        """Look up a decision by request ID."""
        return self._decisions.get(request_id)

    def _record_decision(self, request: ApprovalRequest, decision: ApprovalDecision) -> None:
        """Record the decision and update the request status."""
        request.status = decision.status
        request.reason = decision.reason
        self._decisions[request.request_id] = decision

        # Evict oldest entries when history exceeds cap
        if len(self._requests) > self._max_history:
            oldest_id = next(iter(self._requests))
            self._requests.pop(oldest_id, None)
            self._decisions.pop(oldest_id, None)

        logger.info(
            "Approval decision: request_id=%s status=%s decided_by=%s reason=%s",
            decision.request_id,
            decision.status,
            decision.decided_by,
            decision.reason or "(none)",
        )
