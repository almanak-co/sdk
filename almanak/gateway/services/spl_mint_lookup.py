"""Direct SPL mint account RPC lookup for the Gateway.

This module provides on-chain metadata fetching for Solana SPL token mints by
reading the mint account directly via ``getAccountInfo``. It is the Solana
counterpart to ``OnChainLookup`` (ERC-20), and serves as the fallback when the
Jupiter token list (curated subset) doesn't cover a mint — e.g. newly-listed
tokens, meme coins, long-tail assets.

The SPL token mint account has a fixed 82-byte layout. The authoritative
``decimals`` byte lives at offset 44 and is the only field the resolver
strictly needs. Token-2022 shares the same base layout (extensions are appended
after byte 82), so the same reader handles both programs.

Safety:
    - Owner must be the SPL Token Program or Token-2022 Program (defends against
      attacker-crafted accounts returning bogus decimals).
    - Account data length must be >= 82 bytes.
    - ``is_initialized`` byte (offset 45) must equal 1.
    - Decimals must be a valid u8 (0-77, matching the resolver's range guard).

The resolver layers additional integrity checks on top (decimals range, static
registry cross-check, address mismatch reject) — this module deliberately stays
narrow: fetch, validate, return. No caching here; the resolver owns that.

Usage:
    from almanak.gateway.services.spl_mint_lookup import SplMintLookup
    from almanak.gateway.utils import get_rpc_url

    lookup = SplMintLookup(get_rpc_url("solana"))
    info = await lookup.lookup("GWrbDx2K7vngKTcwipwEh99ia11DymNgERDAE7nCjNjc")
    if info:
        print(f"decimals={info.decimals} owner={info.owner_program}")
"""

from __future__ import annotations

import asyncio
import base64
import logging
from dataclasses import dataclass
from typing import Any

from almanak.framework.execution.solana.rpc import (
    SolanaRpcClient,
    SolanaRpcConfig,
)

logger = logging.getLogger(__name__)

# SPL Token Program (classic)
SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

# Token-2022 Program (Token Extensions) — shares the base 82-byte mint layout
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

SPL_MINT_ACCOUNT_MIN_SIZE = 82
DECIMALS_OFFSET = 44
IS_INITIALIZED_OFFSET = 45

# Decimals is a u8, but the resolver rejects >77 as an integrity failure
# (no real token has decimals that high). Match its range here so rejections
# happen close to the data source.
MAX_VALID_DECIMALS = 77

DEFAULT_TIMEOUT = 10.0


@dataclass
class SplMintInfo:
    """Metadata parsed from an SPL mint account.

    Attributes:
        address: Mint address (base58, case-preserved).
        decimals: Number of decimal places (authoritative from the mint account).
        owner_program: Which token program owns the mint (SPL Token or Token-2022).
        is_initialized: Whether the mint is initialized (always True here —
            uninitialized mints are rejected at lookup time).
    """

    address: str
    decimals: int
    owner_program: str
    is_initialized: bool = True


class SplMintLookup:
    """Fetches SPL mint metadata via ``getAccountInfo``.

    Uses the lightweight ``SolanaRpcClient`` already used by the gateway's
    execution service — no new dependency.

    Error semantics (important for cache correctness):
        * Returns ``None`` only for **definitive misses** — the account does
          not exist, the owner program is not SPL Token / Token-2022, the
          account data is malformed or uninitialized, or decimals are out of
          range. These are safe to negative-cache.
        * **Re-raises** ``TimeoutError`` / ``SolanaRpcError`` / other
          exceptions for **transient failures** (RPC unreachable, network
          blip). These must NOT be negative-cached by the caller: a single
          flaky request would otherwise poison resolution of a valid mint
          until the cache TTL expires.
    """

    def __init__(self, rpc_url: str, timeout: float = DEFAULT_TIMEOUT) -> None:
        # ``SolanaRpcConfig.timeout`` is typed as int. Clamp to >=1 so a
        # sub-second ``timeout`` doesn't truncate to 0 (which would raise
        # ``ValueError`` inside ``requests``).
        self._client = SolanaRpcClient(SolanaRpcConfig(rpc_url=rpc_url, timeout=max(1, int(timeout))))
        self._timeout = timeout

    async def lookup(self, mint: str) -> SplMintInfo | None:
        """Look up SPL mint metadata by address.

        Args:
            mint: Base58 SPL mint address.

        Returns:
            ``SplMintInfo`` on success, ``None`` on a definitive miss
            (missing account, wrong owner, malformed data, uninitialized
            mint, decimals out of range).

        Raises:
            TimeoutError: RPC call exceeded the configured timeout.
            SolanaRpcError: Solana RPC returned an error response.
            Exception: Any other transport / network / encoding failure.

        Transient errors intentionally propagate so the caller can
        distinguish them from "genuinely not a mint" and avoid negative-
        caching temporarily unreachable mints.
        """
        response = await asyncio.wait_for(
            self._client._async_rpc_call(
                "getAccountInfo",
                [mint, {"encoding": "base64", "commitment": "confirmed"}],
            ),
            timeout=self._timeout,
        )
        return self._parse_response(mint, response)

    async def close(self) -> None:
        """Release underlying RPC client resources.

        ``SolanaRpcClient`` holds a ``requests.Session`` that owns a
        connection pool. Closing on gateway shutdown prevents a file-
        descriptor leak across restarts.
        """
        session = getattr(self._client, "_session", None)
        if session is not None:
            try:
                session.close()
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("spl_mint_lookup_close_session_error error=%s", exc)

    def _parse_response(self, mint: str, response: Any) -> SplMintInfo | None:
        """Validate and parse a ``getAccountInfo`` response."""
        if not isinstance(response, dict):
            logger.debug("spl_mint_lookup_malformed mint=%s response_type=%s", mint, type(response))
            return None

        value = response.get("value")
        if value is None:
            # Account does not exist on-chain.
            logger.info("spl_mint_lookup_missing mint=%s", mint)
            return None
        if not isinstance(value, dict):
            logger.debug("spl_mint_lookup_malformed mint=%s value_type=%s", mint, type(value))
            return None

        owner = value.get("owner")
        if owner not in (SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
            # Could be any arbitrary Solana account — refuse to interpret its
            # bytes as a mint. This is the critical safety check.
            logger.warning(
                "spl_mint_lookup_wrong_owner mint=%s owner=%s",
                mint,
                owner,
            )
            return None

        data_field = value.get("data")
        raw = _decode_account_data(data_field)
        if raw is None:
            logger.warning("spl_mint_lookup_bad_encoding mint=%s data=%r", mint, data_field)
            return None

        if len(raw) < SPL_MINT_ACCOUNT_MIN_SIZE:
            logger.warning(
                "spl_mint_lookup_truncated mint=%s owner=%s length=%d",
                mint,
                owner,
                len(raw),
            )
            return None

        is_initialized = raw[IS_INITIALIZED_OFFSET]
        if is_initialized != 1:
            logger.warning(
                "spl_mint_lookup_uninitialized mint=%s owner=%s is_initialized=%d",
                mint,
                owner,
                is_initialized,
            )
            return None

        decimals = raw[DECIMALS_OFFSET]
        if decimals > MAX_VALID_DECIMALS:
            logger.warning(
                "spl_mint_lookup_decimals_out_of_range mint=%s owner=%s decimals=%d",
                mint,
                owner,
                decimals,
            )
            return None

        logger.info(
            "spl_mint_lookup_success mint=%s owner=%s decimals=%d",
            mint,
            owner,
            decimals,
        )
        return SplMintInfo(
            address=mint,
            decimals=decimals,
            owner_program=owner,
        )


def _decode_account_data(data_field: Any) -> bytes | None:
    """Decode the ``data`` field from a ``getAccountInfo`` response.

    Solana returns ``[<base64>, "base64"]`` for base64 encoding. Defensive
    against the (rarer) jsonParsed or base58 shapes — those aren't what we
    requested, so treat them as errors.
    """
    if not isinstance(data_field, list) or len(data_field) < 2:
        return None
    payload, encoding = data_field[0], data_field[1]
    if encoding != "base64" or not isinstance(payload, str):
        return None
    try:
        return base64.b64decode(payload, validate=True)
    except (ValueError, TypeError):
        return None


__all__ = [
    "SPL_TOKEN_PROGRAM",
    "TOKEN_2022_PROGRAM",
    "SplMintInfo",
    "SplMintLookup",
]
