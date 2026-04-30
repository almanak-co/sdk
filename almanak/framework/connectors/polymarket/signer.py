"""EIP-712 signing helpers for Polymarket — local + remote.

Two signing paths are supported:

- **Local**: ``sign_typed_data_local`` — uses ``eth_account`` to sign with a
  private key held in process. This is the EOA / development path.
- **Remote**: ``sign_typed_data_remote`` — POSTs the 32-byte EIP-712 digest to
  the Almanak Signer Service's ``/sign/hash`` endpoint with a JWT, then
  reassembles the response into the canonical 65-byte ``r||s||v`` hex.
  This is the platform path: the trading EOA's private key never leaves the
  Signer Service GCS bucket.

The remote path mirrors ``platform-plugins/almanak_platform/signer.py``'s
``PlatformZodiacSigner._sign_wrapper_tx`` (which uses ``/sign/transaction`` for
full transactions). Same JWT auth shape, same error envelope, but ``/sign/hash``
takes a 32-byte digest hex and returns ethers v6 ``Signature.toJSON()`` shape
``{ _type, r, s, v, networkV }``.

Also exposes ``build_clob_auth_typed_data`` so the L1 ClobAuth typed-data dict
can be constructed in exactly one place — historically this was duplicated
between ``clob_client.py`` and ``polymarket_service.py`` and the duplication
caused at least one prod bug (``0x``-prefix drift between the two).
"""

from __future__ import annotations

import logging

import httpx
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

from almanak.framework.connectors.polymarket.exceptions import PolymarketSignatureError
from almanak.framework.connectors.polymarket.models import CLOB_AUTH_DOMAIN, CLOB_AUTH_MESSAGE, CLOB_AUTH_TYPES

logger = logging.getLogger(__name__)


SIGN_HASH_PATH = "/sign/hash"
SIGNING_TYPE_EVM = "EVM"
DEFAULT_SIGNER_TIMEOUT_SECONDS = 30.0


def build_clob_auth_typed_data(wallet_address: str, timestamp: str, nonce: int) -> dict:
    """Build the EIP-712 typed-data dict for Polymarket's L1 ClobAuth.

    Used by both the connector (``ClobClient._build_l1_headers``) and the
    gateway service (``PolymarketServiceServicer._build_l1_headers``). Keep
    the shape identical between callers — Polymarket's ``/auth/api-key``
    rejects mismatched signatures with HTTP 401.
    """
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            **CLOB_AUTH_TYPES,
        },
        "primaryType": "ClobAuth",
        "domain": CLOB_AUTH_DOMAIN,
        "message": {
            "address": wallet_address,
            "timestamp": timestamp,
            "nonce": nonce,
            "message": CLOB_AUTH_MESSAGE,
        },
    }


def sign_typed_data_local(typed_data: dict, private_key: str) -> str:
    """Sign EIP-712 typed data with a local private key.

    Returns ``0x``-prefixed 65-byte hex (``r||s||v``).
    """
    signable = encode_typed_data(full_message=typed_data)
    signed = Account.sign_message(signable, private_key)
    sig_hex = signed.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex
    return sig_hex


def sign_typed_data_remote(
    typed_data: dict,
    eoa_address: str,
    signer_service_url: str,
    signer_service_jwt: str,
    *,
    http_client: httpx.Client | None = None,
    timeout: float = DEFAULT_SIGNER_TIMEOUT_SECONDS,
) -> str:
    """Sign EIP-712 typed data via the Almanak Signer Service.

    Computes the 32-byte EIP-712 digest, POSTs it to ``{signer_service_url}/sign/hash``
    with the JWT bearer token, parses the ethers-v6 ``Signature.toJSON()`` response,
    and reassembles ``0x<r><s><v>`` (65 bytes).

    Args:
        typed_data: EIP-712 typed-data dict (same shape ``Account.sign_typed_data`` accepts).
        eoa_address: The trading EOA address whose key the Signer Service holds.
            Must be in the JWT's ``agent_eoa_address[]`` claim.
        signer_service_url: Base URL (no trailing slash) of the Signer Service.
        signer_service_jwt: PS256 JWT issued by the platform.
        http_client: Optional pre-built ``httpx.Client``; one is created per call otherwise.
            Reuse a client across calls in hot paths to amortize TLS handshake cost.
        timeout: Request timeout in seconds.

    Returns:
        ``0x``-prefixed 65-byte signature hex.

    Raises:
        PolymarketSignatureError: On non-2xx, missing fields, or transport failure.
            The 401 case is wrapped with a hint that the JWT may be expired.
    """
    # The remote ``/sign/hash`` endpoint signs the 32-byte hash it receives
    # verbatim (ECDSA over the bytes), so we have to send the FULL EIP-712
    # digest — the same value ``Account.sign_message(signable)`` signs in the
    # local path — not just ``signable.body`` (which is only the struct hash).
    # ``Account.sign_message`` internally computes
    # ``keccak(b"\x19" + version + header + body)`` per EIP-712; sending only
    # ``.body`` would silently produce a different signature for the same
    # typed data on the remote path, and Polymarket / EIP-712 verifiers would
    # recover a different address.
    signable = encode_typed_data(full_message=typed_data)
    digest = keccak(b"\x19" + signable.version + signable.header + signable.body)
    if len(digest) != 32:
        raise PolymarketSignatureError(f"EIP-712 digest must be 32 bytes, got {len(digest)}")
    digest_hex = "0x" + digest.hex()

    payload = {
        "eoa_address": eoa_address,
        "transaction_payload": [digest_hex],
        "signing_type": SIGNING_TYPE_EVM,
    }
    headers = {
        "Authorization": f"Bearer {signer_service_jwt}",
        "Content-Type": "application/json",
    }
    url = f"{signer_service_url.rstrip('/')}{SIGN_HASH_PATH}"

    owns_client = http_client is None
    client = http_client or httpx.Client(timeout=timeout)
    try:
        try:
            response = client.post(url, json=payload, headers=headers, timeout=timeout)
        except httpx.HTTPError as e:
            logger.warning("Signer service transport failure: %s: %s", type(e).__name__, e)
            raise PolymarketSignatureError(f"Failed to reach signer service: {type(e).__name__}") from e

        if response.status_code == 401:
            logger.warning("Signer service returned HTTP 401; JWT likely expired or wrong scope")
            raise PolymarketSignatureError(
                "Signer service authentication failed (HTTP 401); JWT may be expired or "
                f"missing eoa_address={eoa_address} in agent_eoa_address[] claim"
            )
        if not 200 <= response.status_code < 300:
            body_preview = (response.text or "")[:200]
            logger.warning("Signer service returned HTTP %s: %s", response.status_code, body_preview)
            raise PolymarketSignatureError(f"Signer service error (HTTP {response.status_code}): {body_preview}")

        try:
            data = response.json()
        except ValueError as e:
            raise PolymarketSignatureError(f"Signer service returned non-JSON response: {e}") from e
        # ``response.json()`` can legally return a list/string/null; a downstream
        # ``data.get(...)`` would then raise ``AttributeError``, which leaks as
        # an opaque server-side bug instead of a typed signing error. Validate
        # the shape explicitly so a misbehaving signer-service build fails
        # loud at the framework boundary.
        if not isinstance(data, dict):
            raise PolymarketSignatureError(f"Signer service returned unexpected JSON shape: {type(data).__name__}")

        signed = data.get("signed_transactions") or []
        if not signed:
            raise PolymarketSignatureError("Signer service response missing signed_transactions[]")

        first = signed[0]
        if isinstance(first, str):
            # Defensive: some Signer Service builds may flatten to a hex string.
            # Validate length + hex shape so a malformed flatten can't escape
            # as opaque downstream auth/order failures.
            sig_hex = first if first.startswith("0x") else "0x" + first
            raw = sig_hex.removeprefix("0x")
            if len(raw) != 130:  # 65 bytes = r(32) || s(32) || v(1)
                raise PolymarketSignatureError(f"Signer service flat signature has wrong length: {len(raw)}")
            try:
                bytes.fromhex(raw)
            except ValueError as e:
                raise PolymarketSignatureError("Signer service flat signature is not valid hex") from e
            return sig_hex
        if not isinstance(first, dict):
            raise PolymarketSignatureError(f"Unexpected signed_transactions[0] shape: {type(first).__name__}")

        return _reassemble_signature_hex(first)
    finally:
        if owns_client:
            client.close()


def _reassemble_signature_hex(sig_obj: dict) -> str:
    """Reassemble ethers-v6 ``Signature.toJSON()`` shape into ``0x<r><s><v>``.

    Expected shape: ``{ _type: "signature", r: "0x...", s: "0x...", v: 27|28, networkV: null }``.
    Tolerates ``r``/``s`` without the ``0x`` prefix and ``v`` as int, decimal
    string, or hex string. ``v`` is canonicalised to ``27`` / ``28``: a value
    of ``0`` / ``1`` is mapped to ``27`` / ``28`` respectively, anything else
    raises so a signer-service regression fails fast instead of producing a
    silently malformed signature (Polymarket's verifier rejects, but the
    error is opaque — far better to fail at parse time).
    """
    try:
        r = sig_obj["r"]
        s = sig_obj["s"]
        v = sig_obj["v"]
    except KeyError as e:
        raise PolymarketSignatureError(f"Signer service signature missing field: {e}") from e

    # Some signer-service / JSON-RPC libraries ship ``r`` / ``s`` as Python
    # ints (or hex strings without the ``0x`` prefix). ``str(int)`` produces
    # the *decimal* form, which would silently corrupt the reassembled
    # signature; ``hex(int)`` gives the right shape for the int branch.
    def _to_hex(component: object) -> str:
        if isinstance(component, int):
            return format(component, "x")
        return str(component).removeprefix("0x")

    r_hex = _to_hex(r).rjust(64, "0")
    s_hex = _to_hex(s).rjust(64, "0")

    # Validate r/s really are hex — a string component that contains
    # non-hex characters (e.g. a stray decimal-encoded value) would
    # silently produce a malformed signature otherwise. ``int(_, 16)``
    # gives a clean ValueError that we map to PolymarketSignatureError.
    try:
        int(r_hex, 16)
        int(s_hex, 16)
    except ValueError as e:
        raise PolymarketSignatureError(f"Signer service signature has non-hex r/s: r={r!r}, s={s!r}") from e

    # Parse v as int. Strings are decimal unless they're hex-prefixed or
    # contain hex-only digits (a-f); never silently re-parse plain decimal
    # as hex (the previous code did, so "27" parsed to 0x27 = 39).
    if isinstance(v, int):
        v_int = v
    else:
        v_str = str(v)
        try:
            if v_str.lower().startswith("0x") or any(c in "abcdefABCDEF" for c in v_str):
                v_int = int(v_str.removeprefix("0x"), 16)
            else:
                v_int = int(v_str, 10)
        except ValueError as e:
            raise PolymarketSignatureError(f"Signer service signature has invalid v field: {v!r}") from e

    # ethers v6 emits canonical 27/28 by default but some signer-service
    # builds (or older ethers configs) emit the EIP-2098 0/1 variant.
    # Normalise both to the legacy 27/28 that Polymarket's verifier expects.
    if v_int in (0, 1):
        v_int = v_int + 27
    if v_int not in (27, 28):
        raise PolymarketSignatureError(f"Signer service signature has invalid v field: {v!r}")
    v_hex = format(v_int, "02x")

    if len(r_hex) != 64 or len(s_hex) != 64:
        raise PolymarketSignatureError(f"Signer service signature has wrong r/s length: r={len(r_hex)}, s={len(s_hex)}")
    return "0x" + r_hex + s_hex + v_hex


__all__ = [
    "DEFAULT_SIGNER_TIMEOUT_SECONDS",
    "SIGN_HASH_PATH",
    "SIGNING_TYPE_EVM",
    "build_clob_auth_typed_data",
    "sign_typed_data_local",
    "sign_typed_data_remote",
]
