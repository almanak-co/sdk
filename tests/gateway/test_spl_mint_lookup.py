"""Tests for SplMintLookup gateway service."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.solana.rpc import SolanaRpcError
from almanak.gateway.services.spl_mint_lookup import (
    SPL_MINT_ACCOUNT_MIN_SIZE,
    SPL_TOKEN_PROGRAM,
    TOKEN_2022_PROGRAM,
    SplMintInfo,
    SplMintLookup,
    _decode_account_data,
)


def _make_mint_data(decimals: int, is_initialized: int = 1, extra_bytes: int = 0) -> str:
    """Build a valid (or deliberately malformed) SPL mint account payload.

    The canonical SPL mint account is 82 bytes. ``extra_bytes`` lets us simulate
    Token-2022 mints that carry extensions after the base layout (still
    decodable because the base fields stay at fixed offsets).
    """
    buf = bytearray(SPL_MINT_ACCOUNT_MIN_SIZE + extra_bytes)
    buf[44] = decimals
    buf[45] = is_initialized
    return base64.b64encode(bytes(buf)).decode()


def _account_info(owner: str, data_b64: str) -> dict:
    return {
        "value": {
            "data": [data_b64, "base64"],
            "executable": False,
            "lamports": 1461600,
            "owner": owner,
            "rentEpoch": 0,
            "space": SPL_MINT_ACCOUNT_MIN_SIZE,
        }
    }


@pytest.fixture
def lookup() -> SplMintLookup:
    return SplMintLookup(rpc_url="https://test.rpc.example.com", timeout=5.0)


class TestSplMintLookup:
    """Happy-path tests covering both token programs."""

    @pytest.mark.asyncio
    async def test_spl_token_program_success(self, lookup: SplMintLookup) -> None:
        mint = "GWrbDx2K7vngKTcwipwEh99ia11DymNgERDAE7nCjNjc"
        response = _account_info(SPL_TOKEN_PROGRAM, _make_mint_data(decimals=6))

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup(mint)

        assert info == SplMintInfo(
            address=mint,
            decimals=6,
            owner_program=SPL_TOKEN_PROGRAM,
            is_initialized=True,
        )

    @pytest.mark.asyncio
    async def test_token_2022_program_success(self, lookup: SplMintLookup) -> None:
        """Token-2022 base layout is identical; extension bytes beyond 82 are ignored."""
        mint = "TokenExt1111111111111111111111111111111111"
        response = _account_info(
            TOKEN_2022_PROGRAM,
            _make_mint_data(decimals=9, extra_bytes=128),  # extensions appended
        )

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup(mint)

        assert info is not None
        assert info.decimals == 9
        assert info.owner_program == TOKEN_2022_PROGRAM

    @pytest.mark.asyncio
    async def test_mint_with_zero_decimals(self, lookup: SplMintLookup) -> None:
        """NFT-style mints legitimately have 0 decimals — must not be rejected."""
        mint = "NftMint1111111111111111111111111111111111"
        response = _account_info(SPL_TOKEN_PROGRAM, _make_mint_data(decimals=0))

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup(mint)

        assert info is not None
        assert info.decimals == 0


class TestDefinitiveMissReturnsNone:
    """Definitive-miss rejection paths return ``None`` so the caller can cache
    the negative result and move on. Any failure *here* means the account
    genuinely cannot be a valid SPL mint."""

    @pytest.mark.asyncio
    async def test_wrong_owner_rejected(self, lookup: SplMintLookup) -> None:
        """A random Solana program masquerading as a mint must not be decoded."""
        attacker_program = "11111111111111111111111111111111"  # system program
        response = _account_info(attacker_program, _make_mint_data(decimals=6))

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup("AttackerMint11111111111111111111111111111")

        assert info is None

    @pytest.mark.asyncio
    async def test_truncated_data_rejected(self, lookup: SplMintLookup) -> None:
        """Data shorter than 82 bytes isn't a valid mint — offset 44 might not exist."""
        short = base64.b64encode(b"\x00" * 40).decode()
        response = _account_info(SPL_TOKEN_PROGRAM, short)

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup("ShortMint11111111111111111111111111111111")

        assert info is None

    @pytest.mark.asyncio
    async def test_uninitialized_mint_rejected(self, lookup: SplMintLookup) -> None:
        """An allocated but never-initialised account would return garbage decimals."""
        response = _account_info(
            SPL_TOKEN_PROGRAM,
            _make_mint_data(decimals=6, is_initialized=0),
        )

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup("Uninit111111111111111111111111111111111111")

        assert info is None

    @pytest.mark.asyncio
    async def test_decimals_out_of_range_rejected(self, lookup: SplMintLookup) -> None:
        """Match the resolver's >77 integrity guard at the data source."""
        response = _account_info(SPL_TOKEN_PROGRAM, _make_mint_data(decimals=200))

        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value=response)):
            info = await lookup.lookup("BadDecimals111111111111111111111111111111")

        assert info is None

    @pytest.mark.asyncio
    async def test_nonexistent_account_returns_none(self, lookup: SplMintLookup) -> None:
        """getAccountInfo returns value=null for unknown addresses."""
        with patch.object(
            lookup._client, "_async_rpc_call", new=AsyncMock(return_value={"value": None})
        ):
            info = await lookup.lookup("DoesNotExist1111111111111111111111111111")

        assert info is None

    @pytest.mark.asyncio
    async def test_malformed_response_rejected(self, lookup: SplMintLookup) -> None:
        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(return_value="not a dict")):
            info = await lookup.lookup("Malformed111111111111111111111111111111111")

        assert info is None


class TestTransientErrorsPropagate:
    """Transient RPC failures MUST NOT be silently swallowed to ``None`` — that
    would cause the caller (gateway TokenService) to emit a NOT_FOUND that the
    resolver then negative-caches, poisoning resolution for a valid mint until
    the TTL expires. Instead, the lookup must re-raise so the caller can map
    the failure to a gRPC code that keeps the negative cache clean
    (UNAVAILABLE / DEADLINE_EXCEEDED)."""

    @pytest.mark.asyncio
    async def test_rpc_timeout_raises(self, lookup: SplMintLookup) -> None:
        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(side_effect=TimeoutError())):
            with pytest.raises(TimeoutError):
                await lookup.lookup("Timeout11111111111111111111111111111111111")

    @pytest.mark.asyncio
    async def test_rpc_error_raises(self, lookup: SplMintLookup) -> None:
        with patch.object(
            lookup._client,
            "_async_rpc_call",
            new=AsyncMock(side_effect=SolanaRpcError("getAccountInfo", "boom", code=-32000)),
        ):
            with pytest.raises(SolanaRpcError):
                await lookup.lookup("RpcError11111111111111111111111111111111")

    @pytest.mark.asyncio
    async def test_unexpected_exception_raises(self, lookup: SplMintLookup) -> None:
        """Network / DNS / library bugs propagate so the caller can distinguish
        them from definitive misses."""
        with patch.object(lookup._client, "_async_rpc_call", new=AsyncMock(side_effect=RuntimeError("boom"))):
            with pytest.raises(RuntimeError):
                await lookup.lookup("Unexpected11111111111111111111111111111111")


class TestClose:
    @pytest.mark.asyncio
    async def test_close_releases_session(self, lookup: SplMintLookup) -> None:
        """close() must release the underlying requests.Session connection
        pool so the gateway can shut down without leaking file descriptors."""
        session = MagicMock()
        lookup._client._session = session

        await lookup.close()

        session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_swallows_session_error(self, lookup: SplMintLookup) -> None:
        """A broken session must not prevent gateway shutdown."""
        session = MagicMock()
        session.close.side_effect = RuntimeError("pool already closed")
        lookup._client._session = session

        # Must not raise.
        await lookup.close()


class TestDecodeAccountData:
    """Direct tests for the encoding decoder — small surface, high leverage."""

    def test_valid_base64(self) -> None:
        payload = base64.b64encode(b"hello").decode()
        assert _decode_account_data([payload, "base64"]) == b"hello"

    def test_rejects_non_base64_encoding(self) -> None:
        payload = base64.b64encode(b"hello").decode()
        assert _decode_account_data([payload, "jsonParsed"]) is None
        assert _decode_account_data([payload, "base58"]) is None

    def test_rejects_non_list_shape(self) -> None:
        assert _decode_account_data({"data": "whatever"}) is None
        assert _decode_account_data(None) is None
        assert _decode_account_data("string") is None

    def test_rejects_short_list(self) -> None:
        assert _decode_account_data([base64.b64encode(b"hello").decode()]) is None

    def test_rejects_invalid_base64(self) -> None:
        assert _decode_account_data(["not valid base64!!!", "base64"]) is None
