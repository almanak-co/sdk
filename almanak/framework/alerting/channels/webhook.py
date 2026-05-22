"""Webhook notification channel for sending alerts to HTTP endpoints.

Sends structured JSON payloads to registered webhook URLs. Supports
retry with exponential backoff and rate limiting.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

from almanak.framework.services.operator_card_generator import OperatorCard

logger = logging.getLogger(__name__)


@dataclass
class WebhookSendResult:
    """Result of a webhook send attempt."""

    success: bool
    status_code: int = 0
    error: str = ""
    response_body: str = ""


class WebhookChannel:
    """Webhook notification channel for sending alerts to HTTP endpoints.

    Posts JSON payloads to a configured URL. Compatible with Slack
    incoming webhooks, Discord webhooks, custom endpoints, etc.

    Attributes:
        url: The webhook URL to POST to.
        headers: Optional custom headers (e.g., auth tokens).
        max_retries: Maximum number of retries for failed sends.
        base_delay: Base delay in seconds for exponential backoff.
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        base_delay: float = 1.0,
        timeout: float = 10.0,
    ) -> None:
        if not url:
            raise ValueError("url is required")

        self.url = url
        self.headers = headers or {}
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.timeout = timeout

        self._last_send_time: float = 0.0
        self._min_interval: float = 1.0  # 1s minimum between sends

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def _format_payload(self, card: OperatorCard) -> dict[str, Any]:
        """Format an OperatorCard as a JSON-serializable webhook payload."""
        return {
            "deployment_id": card.deployment_id,
            "event_type": card.event_type.value if hasattr(card.event_type, "value") else str(card.event_type),
            "severity": card.severity.value if hasattr(card.severity, "value") else str(card.severity),
            "reason": card.reason.value if hasattr(card.reason, "value") else str(card.reason),
            "risk_description": card.risk_description,
            "suggested_actions": [
                {"action": str(a.action), "description": a.description} for a in (card.suggested_actions or [])
            ],
            "timestamp": card.timestamp.isoformat() if card.timestamp else "",
        }

    async def send_alert(self, card: OperatorCard) -> WebhookSendResult:
        """Send an alert to the webhook URL.

        Args:
            card: The OperatorCard to send.

        Returns:
            WebhookSendResult with success status.
        """
        # Rate limiting
        now = time.monotonic()
        elapsed = now - self._last_send_time
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)

        payload = self._format_payload(card)
        headers = {"Content-Type": "application/json", **self.headers}

        for attempt in range(self.max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.url,
                        json=payload,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as resp:
                        self._last_send_time = time.monotonic()
                        body = await resp.text()

                        if resp.status < 300:
                            return WebhookSendResult(
                                success=True,
                                status_code=resp.status,
                                response_body=body,
                            )
                        elif resp.status == 429 and attempt < self.max_retries:
                            delay = self.base_delay * (2**attempt)
                            logger.warning(f"Webhook rate limited, retrying in {delay}s")
                            import asyncio

                            await asyncio.sleep(delay)
                            continue
                        else:
                            return WebhookSendResult(
                                success=False,
                                status_code=resp.status,
                                error=f"HTTP {resp.status}: {body[:200]}",
                            )
            except Exception as e:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    logger.warning(f"Webhook send failed ({e}), retrying in {delay}s")
                    import asyncio

                    await asyncio.sleep(delay)
                else:
                    return WebhookSendResult(success=False, error=str(e))

        return WebhookSendResult(success=False, error="Max retries exceeded")

    def send_alert_sync(self, card: OperatorCard) -> WebhookSendResult:
        """Synchronous version of send_alert using requests."""
        import requests

        # Rate limiting
        now = time.monotonic()
        elapsed = now - self._last_send_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

        payload = self._format_payload(card)
        headers = {"Content-Type": "application/json", **self.headers}

        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.post(
                    self.url,
                    data=json.dumps(payload, default=str),
                    headers=headers,
                    timeout=self.timeout,
                )
                self._last_send_time = time.monotonic()

                if resp.status_code < 300:
                    return WebhookSendResult(
                        success=True,
                        status_code=resp.status_code,
                        response_body=resp.text,
                    )
                elif resp.status_code == 429 and attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    time.sleep(delay)
                    continue
                else:
                    return WebhookSendResult(
                        success=False,
                        status_code=resp.status_code,
                        error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    )
            except Exception as e:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    time.sleep(delay)
                else:
                    return WebhookSendResult(success=False, error=str(e))

        return WebhookSendResult(success=False, error="Max retries exceeded")
