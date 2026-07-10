"""Shared connector RPC helpers.

Protocol connectors own ABI selectors, calldata encoding, contract addresses,
and result decoding. Transport belongs here so swap quoting, pool validation,
and future connector reads all share the same gateway-first boundary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

from almanak.connectors._strategy_base.pool_validation_base import eth_call as _eth_call

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

__all__ = [
    "StaticCallProbe",
    "decode_uint256",
    "eth_call",
    "eth_call_hex",
    "eth_call_static_probe",
    "eth_call_uint256",
    "eth_estimate_gas",
]


_SUPPORTED_DIRECT_RPC_SCHEMES: frozenset[str] = frozenset({"http", "https"})


def _gateway_connected(gateway_client: GatewayClient | None, chain: str | None) -> bool:
    return bool(gateway_client is not None and getattr(gateway_client, "is_connected", False) and chain)


def _validate_direct_rpc_url(rpc_url: str | None) -> None:
    if not rpc_url:
        return
    scheme = urlparse(rpc_url).scheme.lower()
    if scheme not in _SUPPORTED_DIRECT_RPC_SCHEMES:
        raise ValueError(f"Unsupported RPC URL scheme {scheme!r}; direct connector RPC calls require http or https")


def eth_call(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> bytes | None:
    """Perform a gateway-first eth_call and return raw bytes."""
    if not _gateway_connected(gateway_client, chain):
        _validate_direct_rpc_url(rpc_url)
    return _eth_call(
        rpc_url or "",
        to,
        data,
        timeout=timeout,
        chain=chain,
        gateway_client=gateway_client,
        raise_errors=True,
    )


def eth_call_hex(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> str | None:
    """Perform eth_call and return a 0x-prefixed hex string."""
    raw = eth_call(
        chain=chain,
        to=to,
        data=data,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
        timeout=timeout,
    )
    if raw is None:
        return None
    return "0x" + raw.hex()


def decode_uint256(data: bytes) -> int:
    """Decode a single uint256 word."""
    if len(data) < 32:
        raise ValueError(f"uint256 response must be at least 32 bytes, got {len(data)}")
    return int.from_bytes(data[:32], "big")


def eth_call_uint256(
    *,
    chain: str | None,
    to: str,
    data: str,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 5.0,
) -> int | None:
    """Perform eth_call and decode a single uint256 word."""
    raw = eth_call(
        chain=chain,
        to=to,
        data=data,
        rpc_url=rpc_url,
        gateway_client=gateway_client,
        timeout=timeout,
    )
    if raw is None:
        return None
    return decode_uint256(raw)


# ---------------------------------------------------------------------------
# Caller-aware static-call probe (VIB-5716)
# ---------------------------------------------------------------------------
#
# A probe eth_call differs from a plain read in two ways: it carries a ``from``
# (the outcome depends on caller state — allowances, whitelists), and the
# CALLER needs to know *why* it reverted, not just that it did. Both transports
# already deliver the upstream node's JSON-RPC error (the gateway RpcService
# forwards it verbatim in ``RpcResponse.error``); this seam surfaces it as a
# typed three-way outcome instead of the plain-read ``bytes | None`` collapse.

# Solidity ``Error(string)`` / ``Panic(uint256)`` revert-data selectors.
_ERROR_STRING_SELECTOR = "08c379a0"
_PANIC_SELECTOR = "4e487b71"

# The revert ``data`` field inside a stringified JSON-RPC error object, e.g.
# ``'data': '0x08c379a0…'`` (repr) or ``"data": "0x…"`` (json). Anchored to the
# ``data`` key so an address or calldata hex elsewhere in the message can never
# be misread as revert data.
_REVERT_DATA_FIELD_RE = re.compile(r"""['"]data['"]\s*:\s*['"](0x[0-9a-fA-F]*)['"]""")
# Reason text a node inlines into the error message ("execution reverted: !wl").
_REVERT_MESSAGE_RE = re.compile(r"execution reverted:?\s*([^'\"}\\]*)", re.IGNORECASE)


@dataclass(frozen=True)
class StaticCallProbe:
    """Typed outcome of a caller-aware static ``eth_call`` probe.

    ``outcome`` is the three-way split every probe consumer needs:

    - ``"success"`` — the call executed without reverting; ``data`` holds the
      return bytes (possibly empty).
    - ``"revert"`` — the node executed the call and it reverted.
      ``revert_reason`` is the best-effort decoded reason (``Error(string)``
      payload, panic code, ``custom error 0x…`` selector, or the node's inline
      message text) — ``None`` when the revert carried no decodable reason.
    - ``"transport"`` — the call never got a definitive answer (no transport,
      dropped channel, timeout, rate limit). A transport outcome says NOTHING
      about the target contract; consumers must treat it as inconclusive,
      never as a revert.
    """

    outcome: Literal["success", "revert", "transport"]
    data: bytes | None = None
    revert_reason: str | None = None
    error: str | None = None


def _decode_revert_data(hex_blob: str) -> str | None:
    """Decode a revert-data hex blob to a human-readable reason, best-effort."""
    blob = hex_blob.lower().removeprefix("0x")
    if blob.startswith(_ERROR_STRING_SELECTOR):
        try:
            payload = bytes.fromhex(blob[8:])
            length = int.from_bytes(payload[32:64], "big")
            text = payload[64 : 64 + length].decode("utf-8", errors="replace").strip()
            return text or None
        except (ValueError, IndexError):
            return None
    if blob.startswith(_PANIC_SELECTOR):
        try:
            code = int.from_bytes(bytes.fromhex(blob[8:72]), "big")
            return f"panic 0x{code:02x}"
        except ValueError:
            return None
    if len(blob) >= 8:
        # A bare 4-byte-selector-prefixed blob is a Solidity custom error; the
        # selector alone is still classifiable (a known-benign selector list
        # lives with the consumer, not here).
        return f"custom error 0x{blob[:8]}"
    return None


def _extract_revert_reason(error_text: str) -> str | None:
    """Best-effort revert reason from a stringified JSON-RPC error.

    Prefers decoding the error object's ``data`` field (exact — the ABI-encoded
    revert payload) over the node's inline message text (lossy but common).
    Returns ``None`` when neither yields anything — an empty-reason revert.
    """
    data_match = _REVERT_DATA_FIELD_RE.search(error_text)
    if data_match:
        decoded = _decode_revert_data(data_match.group(1))
        if decoded:
            return decoded
    message_match = _REVERT_MESSAGE_RE.search(error_text)
    if message_match:
        reason = message_match.group(1).strip()
        if reason:
            return reason
    return None


def _looks_like_revert(error_text: str) -> bool:
    """Whether a failed-call error text is an EXECUTED-and-reverted answer.

    Everything that doesn't positively look like a revert is classified as
    transport — the safe default, because a transport outcome is treated as
    inconclusive by consumers while a revert verdict may disqualify a target.
    """
    lowered = error_text.lower()
    if "revert" in lowered:
        return True
    # JSON-RPC "execution error" codes nodes use for reverts (3 = the standard
    # eth_call revert code; -32015 = legacy Parity/OpenEthereum "VM execution
    # error"). Matched in both repr and json stringifications.
    if re.search(r"['\"]code['\"]\s*:\s*(3|-32015)\b", error_text):
        return True
    return bool(_REVERT_DATA_FIELD_RE.search(error_text))


def eth_call_static_probe(
    *,
    chain: str | None,
    to: str,
    data: str,
    from_address: str,
    value: int = 0,
    rpc_url: str | None = None,
    gateway_client: GatewayClient | None = None,
    timeout: float = 10.0,
) -> StaticCallProbe:
    """Simulate a state-changing call from ``from_address`` and classify the outcome.

    Gateway-first like :func:`eth_call` (same boundary, no new egress — the
    direct-RPC branch reuses the existing ``vib-2986-exempt`` fallback). Never
    raises: every failure collapses into a typed ``revert`` or ``transport``
    outcome so probe consumers hold the full decision locally.
    """
    if _gateway_connected(gateway_client, chain):
        try:
            result = gateway_client.eth_call(  # type: ignore[union-attr]
                chain=chain,  # type: ignore[arg-type]
                to=to,
                data=data,
                from_address=from_address,
                value=value,
                raise_on_error=True,
            )
        except Exception as exc:  # noqa: BLE001 — classified below, never propagated
            text = str(exc)
            if _looks_like_revert(text):
                return StaticCallProbe(outcome="revert", revert_reason=_extract_revert_reason(text), error=text)
            return StaticCallProbe(outcome="transport", error=text)
        raw = bytes.fromhex(result.removeprefix("0x")) if result and result != "0x" else b""
        return StaticCallProbe(outcome="success", data=raw)

    if not rpc_url:
        return StaticCallProbe(outcome="transport", error="no read transport")
    try:
        _validate_direct_rpc_url(rpc_url)
        raw_result = _eth_call(
            rpc_url,
            to,
            data,
            timeout=timeout,
            raise_errors=True,
            from_address=from_address,
            value=value,
        )
    except Exception as exc:  # noqa: BLE001 — classified below, never propagated
        text = str(exc)
        if _looks_like_revert(text):
            return StaticCallProbe(outcome="revert", revert_reason=_extract_revert_reason(text), error=text)
        return StaticCallProbe(outcome="transport", error=text)
    return StaticCallProbe(outcome="success", data=raw_result if raw_result is not None else b"")


def eth_estimate_gas(
    *,
    chain: str | None,
    to: str,
    data: str,
    from_address: str | None = None,
    value: int = 0,
    gateway_client: GatewayClient | None = None,
) -> int | None:
    """Gateway-only ``eth_estimateGas`` returning the raw (un-buffered) estimate.

    ``eth_estimateGas`` is on the gateway's RPC allowlist, so a connected
    ``gateway_client`` serves the estimate over the gateway channel (no
    strategy-container egress). There is deliberately **no direct-RPC
    fallback**: unlike :func:`eth_call` (whose result a connector cannot
    proceed without), this estimate is OPTIONAL — callers (e.g. the Curve
    adapter's ``_resolve_gas``) fall back to a conservative static gas floor
    when it is ``None``. So a bare no-gateway context simply returns ``None``
    rather than opening a socket, and no new ``vib-2986-exempt`` bypass is
    introduced.

    Returns ``None`` — never ``0`` — when the estimate is unavailable (no
    connected gateway, RPC error, or the op reverts under the CURRENT state,
    which is common for a not-yet-approved spend). ``None`` means "unmeasured":
    callers apply their own safety buffer and clamp to a conservative static
    floor (Empty≠Zero).
    """
    if not _gateway_connected(gateway_client, chain):
        return None
    try:
        # ``chain`` is truthy here (checked by ``_gateway_connected``).
        return gateway_client.estimate_gas(  # type: ignore[union-attr]
            chain,  # type: ignore[arg-type]
            to,
            data,
            from_address=from_address,
            value=value,
        )
    except Exception:
        return None
