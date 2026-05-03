"""Enso SDK Exception Classes.

This module defines custom exceptions for the Enso SDK, providing
detailed error information for debugging and error handling.
"""

import logging
import re
from types import MappingProxyType
from typing import Any

logger = logging.getLogger(__name__)

# 4-byte selector regex used by ``check_known_router_revert`` to extract a
# Solidity custom-error selector from raw Enso error strings.
#
# - Leading ``\b`` requires a word boundary so we skip selectors embedded
#   mid-token (e.g. inside a tx hash like ``0x1234ef3dcb2f...``). The literal
#   ``0x`` is preceded by punctuation/whitespace in every realistic emitter
#   (``"reverted: 0x..."``, ``"selector=0x..."``, ``"data: 0x..."``).
# - ``0[xX]`` accepts both lowercase and uppercase hex prefixes — Enso has
#   historically used lowercase but Solidity tooling sometimes upper-cases.
# - ``[0-9a-fA-F]{8}`` consumes the 4-byte selector. Critically there is
#   NO trailing word boundary: real Solidity custom errors with arguments
#   surface as ``0x<selector><32-byte-args...>`` (ABI-encoded) and we MUST
#   match the leading 4 bytes there. The leading ``\b`` is sufficient to
#   prevent false positives inside arbitrary hex substrings.
_SELECTOR_RE = re.compile(r"\b0[xX][0-9a-fA-F]{8}")


class EnsoError(Exception):
    """Base exception class for all Enso SDK errors."""

    pass


class EnsoAPIError(EnsoError):
    """Exception raised for errors in the API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
        error_type: Classified error type (e.g., SERVER_ERROR, RATE_LIMIT)
        api_error_message: The specific error message from the API response
        error_data: Parsed error data from the response, if available
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        endpoint: str | None = None,
        error_data: dict | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        self.error_data = error_data
        self.error_type = self._classify_error()
        self.api_error_message = self._extract_api_message()

        super().__init__(self.message)

    def _classify_error(self) -> str:
        """Classify the error based on status code."""
        if self.status_code == 429:
            return "RATE_LIMIT"
        elif 500 <= self.status_code < 600:
            return "SERVER_ERROR"
        elif self.status_code == 400:
            return "VALIDATION_ERROR"
        elif self.status_code == 401:
            return "AUTHENTICATION_ERROR"
        elif self.status_code == 403:
            return "AUTHORIZATION_ERROR"
        elif 400 <= self.status_code < 500:
            return "CLIENT_ERROR"
        else:
            return "UNKNOWN_ERROR"

    def _extract_api_message(self) -> str | None:
        """Extract error message from API response."""
        if not self.error_data or not isinstance(self.error_data, dict):
            return None

        # Handle different error formats from the API
        if "error" in self.error_data:
            error = self.error_data["error"]
            if isinstance(error, str):
                return error
            elif isinstance(error, dict) and "message" in error:
                return error["message"]
        elif "message" in self.error_data:
            return self.error_data["message"]
        elif "errorMessage" in self.error_data:
            return self.error_data["errorMessage"]

        return None

    def __str__(self) -> str:
        error_msg = f"API Error ({self.status_code}): {self.message}"
        if self.api_error_message:
            error_msg += f"\nAPI Message: {self.api_error_message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_type:
            error_msg += f"\nError Type: {self.error_type}"
        if self.error_data and self.api_error_message is None:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class EnsoValidationError(EnsoError):
    """Exception raised for validation errors.

    Attributes:
        message: Error message
        field: Name of the field that failed validation
        value: The invalid value
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any | None = None,
    ):
        self.message = message
        self.field = field
        self.value = value
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.field and self.value:
            return f"Validation Error: {self.message} (Field: {self.field}, Value: {self.value})"
        elif self.field:
            return f"Validation Error: {self.message} (Field: {self.field})"
        return f"Validation Error: {self.message}"


class EnsoConfigError(EnsoError):
    """Exception raised for SDK configuration errors.

    Attributes:
        message: Error message
        parameter: Name of the configuration parameter that caused the error
    """

    def __init__(self, message: str, parameter: str | None = None):
        self.message = message
        self.parameter = parameter
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.parameter:
            return f"Configuration Error: {self.message} (Parameter: {self.parameter})"
        return f"Configuration Error: {self.message}"


class EnsoTokenError(EnsoError):
    """Exception raised for token-related errors.

    Attributes:
        message: Error message
        token_address: Address of the token that caused the error
        chain_id: Chain ID where the error occurred
    """

    def __init__(
        self,
        message: str,
        token_address: str | None = None,
        chain_id: int | None = None,
    ):
        self.message = message
        self.token_address = token_address
        self.chain_id = chain_id
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Token Error: {self.message}"
        if self.token_address:
            error_msg += f"\nToken Address: {self.token_address}"
        if self.chain_id:
            error_msg += f"\nChain ID: {self.chain_id}"
        return error_msg


class PriceImpactExceedsThresholdError(EnsoError):
    """Raised when route price impact exceeds maximum threshold.

    Attributes:
        message: Error message
        price_impact_bps: Actual price impact in basis points
        threshold_bps: Maximum allowed price impact in basis points
    """

    def __init__(
        self,
        message: str,
        price_impact_bps: float | None = None,
        threshold_bps: int | None = None,
    ):
        self.message = message
        self.price_impact_bps = price_impact_bps
        self.threshold_bps = threshold_bps
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Price Impact Error: {self.message}"
        if self.price_impact_bps is not None:
            error_msg += f"\nActual Price Impact: {self.price_impact_bps}bp ({self.price_impact_bps / 100:.2f}%)"
        if self.threshold_bps is not None:
            error_msg += f"\nThreshold: {self.threshold_bps}bp ({self.threshold_bps / 100:.2f}%)"
        return error_msg


class EnsoTransactionError(EnsoError):
    """Exception raised for blockchain transaction errors.

    Attributes:
        message: Error message
        tx_hash: Transaction hash if available
        error_data: Additional error data
    """

    def __init__(
        self,
        message: str,
        tx_hash: str | None = None,
        error_data: dict | None = None,
    ):
        self.message = message
        self.tx_hash = tx_hash
        self.error_data = error_data
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Transaction Error: {self.message}"
        if self.tx_hash:
            error_msg += f"\nTransaction Hash: {self.tx_hash}"
        if self.error_data:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class EnsoRouterRevertError(EnsoError):
    """Raised when the Enso router reverts with a known custom-error selector.

    The Enso router emits chain-and-route-specific custom errors. Two were
    observed by the QA April-31 harness:

    * ``0xef3dcb2f`` — VIB-3828 (BUG-43). Surfaces from the
      ``leverage_loop_cross_chain`` strategy on Base. Same selector previously
      seen in BUG-55 (Enso "amount inflation") was diagnosed as a logging bug
      (closed by VIB-3747); the on-chain reverts here are real.

    Without the live ABI, the four-byte selector is the only stable handle —
    so we log it verbatim and let strategies match on the
    ``KNOWN_REVERT_SELECTORS`` table for fail-fast classification.
    Decoding (signature recovery) lives outside this class — see
    ``almanak/framework/connectors/enso/`` README + the upstream Enso router
    source ``contracts/EnsoShortcuts.sol``.

    Attributes:
        selector: The 4-byte selector observed (``"0xef3dcb2f"`` etc.).
        chain: The chain the revert was observed on.
        route_summary: Human-facing description of the failing leg
            (token in/out, route hops, etc.).
        diagnosis_hint: Best-effort interpretation of what the selector
            means (per QA April-31 investigation), or ``None`` if unknown.

    Strategies can match on the stable error-message prefix
    (the ``ERROR_PREFIX`` class attribute below) returned in the connector's
    error path to emit a clean ``Intent.hold(...)``. The prefix intentionally
    avoids the substring ``"revert"`` — the state machine classifies any
    error containing ``"revert"`` as transient ``REVERT`` before consulting
    the ``COMPILATION_PERMANENT`` keyword table.
    """

    # NOTE: avoid the literal "revert" in this prefix — the state machine
    # classifies any error containing "revert" as transient REVERT before
    # consulting the COMPILATION_PERMANENT keyword table. Use "rejected".
    ERROR_PREFIX = "Enso router rejected route with selector"

    # Selector → diagnosis hint table. Append new entries here as the router
    # surfaces them in QA. ``None`` means "selector observed but root cause
    # still under investigation" — strategy still benefits from the typed
    # error + permanent-keyword classification.
    # NOTE: diagnosis hints are concatenated into the final exception
    # message, which is fed to ``IntentStateMachine._categorize_error``.
    # That classifier matches a hard-coded keyword set BEFORE the
    # ``COMPILATION_PERMANENT`` keyword block (see state_machine.py
    # ``_categorize_error`` lines 983-999). Any hint containing one of
    # ``insufficient``/``funds``/``balance``/``gas``/``limit``/``price``/
    # ``nonce``/``timeout``/``revert``/``slippage``/``rate limit``/
    # ``connection``/``network`` short-circuits to a transient/retryable
    # class — defeating the typed exception's purpose. A regression guard
    # in ``test_router_revert_error_vib_3828.py`` asserts every entry in
    # this table classifies as ``COMPILATION_PERMANENT`` end-to-end.
    #
    # Wrapped in ``MappingProxyType`` so accidental writes from downstream
    # callers (or parallel xdist workers) cannot mutate the global
    # classification table at runtime. Only the source declaration below
    # can change the entries.
    KNOWN_REVERT_SELECTORS: MappingProxyType[str, str | None] = MappingProxyType(
        {
            # VIB-3828 / BUG-43 — leverage_loop_cross_chain on Base
            "0xef3dcb2f": (
                "Likely a router-side route-validation custom error (under "
                "investigation; see VIB-3828). May be triggered by a token "
                "address not recognized by Enso's chain-specific token map "
                "(sister of BUG-55), an empty / shallow route, or a tolerance "
                "setting the route's natural fee tier cannot satisfy."
            ),
        }
    )

    def __init__(
        self,
        *,
        selector: str,
        chain: str,
        route_summary: str = "",
        diagnosis_hint: str | None = None,
        original_error: str | None = None,
    ) -> None:
        # Canonicalize to ``0x`` + 8 lowercase hex chars so callers passing
        # raw revert data (e.g. ``"ef3dcb2f<padding...>"``), uppercase, or
        # already-prefixed input all collapse to the same key — otherwise the
        # ``KNOWN_REVERT_SELECTORS`` lookup silently misses the diagnosis hint.
        raw = selector.lower().removeprefix("0x")
        self.selector = f"0x{raw[:8]}"
        self.chain = chain
        self.route_summary = route_summary
        # Preserve the raw upstream error string for log inspection / tooling
        # WITHOUT leaking it into ``str(self)`` — that would re-introduce
        # state-machine pre-classification keywords (``revert``/``slippage``/
        # ...) that the structural guardrail goes to lengths to keep out.
        # See ``check_known_router_revert`` for the canonical access pattern.
        self.original_error = original_error
        self.diagnosis_hint = (
            diagnosis_hint if diagnosis_hint is not None else self.KNOWN_REVERT_SELECTORS.get(self.selector)
        )
        msg = f"{self.ERROR_PREFIX} {self.selector} on {chain}"
        if route_summary:
            msg += f" (route: {route_summary})"
        if self.diagnosis_hint:
            msg += f". Diagnosis hint: {self.diagnosis_hint}"
        super().__init__(msg)


def check_known_router_revert(
    error_str: str,
    *,
    chain: str,
    route_summary: str = "",
) -> None:
    """Inspect an Enso route error string for a known router-revert selector.

    Conservative classifier (VIB-3828): only raises when a selector explicitly
    listed in :attr:`EnsoRouterRevertError.KNOWN_REVERT_SELECTORS` appears in
    the message. Unknown selectors and selector-free strings return ``None``,
    leaving the original error path intact so callers can raise their existing
    exception type with the unchanged message.

    Operator visibility for the raw upstream error is preserved two ways:
    (1) a WARNING log line emitted before raising, and (2) the
    ``original_error`` attribute on :class:`EnsoRouterRevertError`. The raw
    error string is intentionally NOT placed into ``str(err)`` — doing so
    would smuggle ``IntentStateMachine._categorize_error`` pre-classification
    keywords (``revert``/``slippage``/``timeout``/...) into the message and
    defeat the typed ``COMPILATION_PERMANENT`` classification.

    Args:
        error_str: The raw error message returned by the gateway or direct
            Enso API call (e.g. ``"Gateway Enso GetRoute failed: HTTP 400:
            execution reverted: 0xef3dcb2f"``).
        chain: The chain the route was attempted on. Used to construct the
            typed exception so strategy authors get chain context in logs.
        route_summary: Optional human-facing route description (e.g.
            ``"USDC -> WETH"`` or token addresses). MUST be free of the
            state-machine pre-classification keywords listed above. When in
            doubt, leave empty — the raw error is preserved separately via
            ``original_error`` and the warning log.

    Raises:
        EnsoRouterRevertError: when the message contains a 4-byte selector
            present in :attr:`EnsoRouterRevertError.KNOWN_REVERT_SELECTORS`.
    """
    if not error_str:
        return
    for match in _SELECTOR_RE.finditer(error_str):
        selector = match.group(0).lower()
        if selector in EnsoRouterRevertError.KNOWN_REVERT_SELECTORS:
            logger.warning(
                "Enso router emitted known custom-error selector %s on %s "
                "(route=%s); raising typed EnsoRouterRevertError for "
                "fail-fast classification. Raw upstream error: %s",
                selector,
                chain,
                route_summary or "<unspecified>",
                error_str,
            )
            raise EnsoRouterRevertError(
                selector=selector,
                chain=chain,
                route_summary=route_summary,
                original_error=error_str,
            )
