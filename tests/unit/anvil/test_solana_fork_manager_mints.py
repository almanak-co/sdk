"""Branch-coverage tests for SolanaForkManager mint preparation and token funding.

Complements test_solana_fork_manager.py with focused coverage of:
- ``SolanaForkManager._prepare_modified_mints`` (mint authority rewriting)
- ``SolanaForkManager._prepare_clone_account_files`` (clone-account pre-fetch)
- ``SolanaForkManager._fund_single_token`` (ATA create/mint + balance polling)

All RPC, filesystem-listing, and solders seams are mocked or pointed at
tmp_path — no subprocesses are spawned and no sockets are opened.
"""

from __future__ import annotations

import base64
import json
import struct
import sys
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.anvil.solana_fork_manager import (
    MINT_FREEZE_AUTHORITY_OFFSET,
    MINT_LAYOUT_SIZE,
    SOLANA_TOKEN_MINTS,
    TOKEN_PROGRAM,
    WSOL_MINT,
    SolanaForkManager,
)

MINTS_PATH = "almanak.framework.anvil.solana_fork_manager.SOLANA_TOKEN_MINTS"

USDC_MINT = SOLANA_TOKEN_MINTS["USDC"]
USDT_MINT = SOLANA_TOKEN_MINTS["USDT"]

OUR_AUTHORITY = b"\xbb" * 32
ORIGINAL_AUTHORITY = b"\xaa" * 32
ORIGINAL_FREEZE_AUTHORITY = b"\xdd" * 32
OWNER_ADDRESS = "Hs5wSP3ancpUapqK5Q8R9YpFheRELSFHZwsWeofMVSpJ"


class _FakePubkey:
    """Minimal stand-in for solders Pubkey — only bytes()/str() are used."""

    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def __bytes__(self) -> bytes:
        return self._raw

    def __str__(self) -> str:
        return "FakeAuthority11111111111111111111111111111"


class _FakeKeypair:
    """Minimal stand-in for solders Keypair — only .pubkey() is used."""

    def __init__(self, raw: bytes = OUR_AUTHORITY) -> None:
        self._pubkey = _FakePubkey(raw)

    def pubkey(self) -> _FakePubkey:
        return self._pubkey


def _make_manager(**kwargs) -> SolanaForkManager:
    return SolanaForkManager(rpc_url="https://api.mainnet-beta.solana.com", **kwargs)


def _mint_account_bytes(
    *,
    has_authority: bool = True,
    has_freeze_authority: bool = True,
    size: int = MINT_LAYOUT_SIZE,
) -> bytes:
    """Build an SPL Token Mint account byte layout (82 bytes by default)."""
    data = bytearray(size)
    struct.pack_into("<I", data, 0, 1 if has_authority else 0)
    if has_authority:
        data[4:36] = ORIGINAL_AUTHORITY
    struct.pack_into("<Q", data, 36, 10**12)  # supply
    data[44] = 6  # decimals
    data[45] = 1  # is_initialized
    struct.pack_into("<I", data, MINT_FREEZE_AUTHORITY_OFFSET, 1 if has_freeze_authority else 0)
    if has_freeze_authority:
        data[MINT_FREEZE_AUTHORITY_OFFSET + 4 : MINT_FREEZE_AUTHORITY_OFFSET + 36] = ORIGINAL_FREEZE_AUTHORITY
    return bytes(data)


def _rpc_value(data: bytes, **fields) -> dict:
    """Wrap raw mint bytes in a getAccountInfo-shaped response."""
    value = {"data": [base64.b64encode(data).decode("ascii"), "base64"], **fields}
    return {"value": value}


def _read_written_mint(mint_dir, mint_address: str) -> tuple[dict, bytes]:
    with open(mint_dir / f"{mint_address}.json") as f:
        account_json = json.load(f)
    raw = base64.b64decode(account_json["account"]["data"][0])
    return account_json, raw


# =============================================================================
# _prepare_modified_mints
# =============================================================================


class TestPrepareModifiedMints:
    """Branch coverage for SolanaForkManager._prepare_modified_mints."""

    @pytest.mark.asyncio
    async def test_returns_early_without_mint_authority(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = None
        mgr._modified_mint_dir = str(tmp_path)

        with patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock) as rpc:
            await mgr._prepare_modified_mints()

        rpc.assert_not_awaited()
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_skips_native_sol_symbols(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        with (
            patch.dict(MINTS_PATH, {"SOL": WSOL_MINT, "WSOL": WSOL_MINT}, clear=True),
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock) as rpc,
        ):
            await mgr._prepare_modified_mints()

        rpc.assert_not_awaited()
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_replaces_existing_mint_and_freeze_authority(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        response = _rpc_value(
            _mint_account_bytes(has_authority=True, has_freeze_authority=True),
            owner=TOKEN_PROGRAM,
            lamports=7_777,
            space=MINT_LAYOUT_SIZE,
            executable=False,
            rentEpoch=361,
        )

        with (
            patch.dict(MINTS_PATH, {"USDC": USDC_MINT}, clear=True),
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=response) as rpc,
        ):
            await mgr._prepare_modified_mints()

        rpc.assert_awaited_once_with(
            mgr.rpc_url,
            "getAccountInfo",
            [USDC_MINT, {"encoding": "base64"}],
        )

        account_json, raw = _read_written_mint(tmp_path, USDC_MINT)
        assert account_json["pubkey"] == USDC_MINT
        assert account_json["account"]["owner"] == TOKEN_PROGRAM
        assert account_json["account"]["lamports"] == 7_777
        assert account_json["account"]["space"] == MINT_LAYOUT_SIZE
        assert account_json["account"]["executable"] is False
        assert account_json["account"]["rentEpoch"] == 361
        assert account_json["account"]["data"][1] == "base64"

        # COption::Some discriminator preserved, authority replaced with ours
        assert struct.unpack_from("<I", raw, 0)[0] == 1
        assert raw[4:36] == OUR_AUTHORITY
        # Supply and decimals untouched
        assert struct.unpack_from("<Q", raw, 36)[0] == 10**12
        assert raw[44] == 6
        # Freeze authority (discriminator == 1) also replaced with ours
        assert struct.unpack_from("<I", raw, MINT_FREEZE_AUTHORITY_OFFSET)[0] == 1
        assert raw[MINT_FREEZE_AUTHORITY_OFFSET + 4 : MINT_FREEZE_AUTHORITY_OFFSET + 36] == OUR_AUTHORITY

    @pytest.mark.asyncio
    async def test_sets_authority_when_absent_and_applies_defaults(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        # Value carries only "data" — every other field falls back to defaults
        response = _rpc_value(_mint_account_bytes(has_authority=False, has_freeze_authority=False))

        with (
            patch.dict(MINTS_PATH, {"USDT": USDT_MINT}, clear=True),
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=response),
        ):
            await mgr._prepare_modified_mints()

        account_json, raw = _read_written_mint(tmp_path, USDT_MINT)
        # Missing fields take documented defaults
        assert account_json["account"]["owner"] == TOKEN_PROGRAM
        assert account_json["account"]["lamports"] == 1461600
        assert account_json["account"]["space"] == MINT_LAYOUT_SIZE
        assert account_json["account"]["executable"] is False
        assert account_json["account"]["rentEpoch"] == 0

        # COption::None was upgraded to COption::Some with our authority
        assert struct.unpack_from("<I", raw, 0)[0] == 1
        assert raw[4:36] == OUR_AUTHORITY
        # Freeze authority discriminator == 0 — freeze region untouched
        assert struct.unpack_from("<I", raw, MINT_FREEZE_AUTHORITY_OFFSET)[0] == 0
        assert raw[MINT_FREEZE_AUTHORITY_OFFSET + 4 : MINT_FREEZE_AUTHORITY_OFFSET + 36] == b"\x00" * 32

    @pytest.mark.asyncio
    @pytest.mark.parametrize("response", [None, {"value": None}])
    async def test_skips_unfetchable_mint(self, tmp_path, response):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        with (
            patch.dict(MINTS_PATH, {"USDC": USDC_MINT}, clear=True),
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=response),
        ):
            await mgr._prepare_modified_mints()

        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_skips_mint_with_truncated_data(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        response = _rpc_value(b"\x00" * (MINT_LAYOUT_SIZE - 42))

        with (
            patch.dict(MINTS_PATH, {"USDC": USDC_MINT}, clear=True),
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=response),
        ):
            await mgr._prepare_modified_mints()

        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_rpc_failure_on_one_mint_continues_to_next(self, tmp_path):
        mgr = _make_manager()
        mgr._mint_authority_keypair = _FakeKeypair()
        mgr._modified_mint_dir = str(tmp_path)

        good_response = _rpc_value(_mint_account_bytes())

        with (
            patch.dict(MINTS_PATH, {"BAD": USDT_MINT, "USDC": USDC_MINT}, clear=True),
            patch.object(
                mgr,
                "_rpc_call_to_url",
                new_callable=AsyncMock,
                side_effect=[RuntimeError("rpc down"), good_response],
            ) as rpc,
        ):
            await mgr._prepare_modified_mints()

        assert rpc.await_count == 2
        # The failing mint produced no file; the loop still processed USDC
        assert not (tmp_path / f"{USDT_MINT}.json").exists()
        assert (tmp_path / f"{USDC_MINT}.json").exists()


# =============================================================================
# _prepare_clone_account_files
# =============================================================================


CLONE_ADDR_A = "CLoneAccountAAAA1111111111111111111111111111"
CLONE_ADDR_B = "CLoneAccountBBBB2222222222222222222222222222"


def _clone_value(**overrides) -> dict:
    """A getAccountInfo value shaped like a pre-fetchable clone account."""
    value = {
        "lamports": 123_456,
        "data": ["QUJDREVG", "base64"],
        "owner": TOKEN_PROGRAM,
        "executable": False,
        "rentEpoch": 361,
        "space": 82,
    }
    value.update(overrides)
    return value


class TestPrepareCloneAccountFiles:
    """Branch coverage for SolanaForkManager._prepare_clone_account_files."""

    @pytest.mark.asyncio
    async def test_no_clone_accounts_returns_early(self, tmp_path):
        mgr = _make_manager()
        mgr._modified_mint_dir = str(tmp_path)

        with patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock) as rpc:
            await mgr._prepare_clone_account_files()

        rpc.assert_not_awaited()
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.asyncio
    async def test_missing_mint_dir_returns_early(self):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A])
        mgr._modified_mint_dir = None

        with patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock) as rpc:
            await mgr._prepare_clone_account_files()

        rpc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prefetch_writes_account_json_passthrough(self, tmp_path):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A])
        mgr._modified_mint_dir = str(tmp_path)

        with patch.object(
            mgr,
            "_rpc_call_to_url",
            new_callable=AsyncMock,
            return_value={"value": _clone_value()},
        ) as rpc:
            await mgr._prepare_clone_account_files()

        rpc.assert_awaited_once_with(
            mgr.rpc_url,
            "getAccountInfo",
            [CLONE_ADDR_A, {"encoding": "base64"}],
        )
        with open(tmp_path / f"{CLONE_ADDR_A}.json") as f:
            account_json = json.load(f)
        assert account_json == {
            "pubkey": CLONE_ADDR_A,
            "account": {
                "lamports": 123_456,
                "data": ["QUJDREVG", "base64"],
                "owner": TOKEN_PROGRAM,
                "executable": False,
                "rentEpoch": 361,
                "space": 82,
            },
        }

    @pytest.mark.asyncio
    async def test_missing_optional_fields_default_to_zero(self, tmp_path):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A])
        mgr._modified_mint_dir = str(tmp_path)

        value = _clone_value()
        del value["rentEpoch"]
        del value["space"]

        with patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value={"value": value}):
            await mgr._prepare_clone_account_files()

        with open(tmp_path / f"{CLONE_ADDR_A}.json") as f:
            account_json = json.load(f)
        assert account_json["account"]["rentEpoch"] == 0
        assert account_json["account"]["space"] == 0

    @pytest.mark.asyncio
    async def test_skips_addresses_already_prepared_by_mint_pass(self, tmp_path):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A, CLONE_ADDR_B])
        mgr._modified_mint_dir = str(tmp_path)

        # ADDR_A was already written by _prepare_modified_mints; the stray
        # non-JSON file exercises the endswith(".json") filter.
        (tmp_path / f"{CLONE_ADDR_A}.json").write_text("{}")
        (tmp_path / "notes.txt").write_text("not an account file")

        with patch.object(
            mgr,
            "_rpc_call_to_url",
            new_callable=AsyncMock,
            return_value={"value": _clone_value()},
        ) as rpc:
            await mgr._prepare_clone_account_files()

        rpc.assert_awaited_once_with(
            mgr.rpc_url,
            "getAccountInfo",
            [CLONE_ADDR_B, {"encoding": "base64"}],
        )
        # ADDR_A untouched, ADDR_B freshly written
        assert (tmp_path / f"{CLONE_ADDR_A}.json").read_text() == "{}"
        assert (tmp_path / f"{CLONE_ADDR_B}.json").exists()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("response", [None, {"value": None}])
    async def test_unfetchable_account_falls_back_to_clone(self, tmp_path, response, caplog):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A])
        mgr._modified_mint_dir = str(tmp_path)

        with (
            patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=response),
            caplog.at_level("WARNING", logger="almanak.framework.anvil.solana_fork_manager"),
        ):
            await mgr._prepare_clone_account_files()

        assert list(tmp_path.iterdir()) == []
        assert "will fall back to --clone" in caplog.text

    @pytest.mark.asyncio
    async def test_nonexistent_dir_skips_existing_file_scan(self, tmp_path, caplog):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A])
        missing_dir = tmp_path / "does-not-exist"
        mgr._modified_mint_dir = str(missing_dir)

        # RPC returns nothing so the loop takes the warning-continue branch
        # instead of attempting to write into the missing directory.
        with patch.object(mgr, "_rpc_call_to_url", new_callable=AsyncMock, return_value=None) as rpc:
            await mgr._prepare_clone_account_files()

        rpc.assert_awaited_once()
        # The missing dir was neither scanned (os.listdir would raise) nor
        # created, and no account file was written anywhere under tmp_path.
        assert not missing_dir.exists()
        assert list(tmp_path.iterdir()) == []
        assert "will fall back to --clone" in caplog.text

    @pytest.mark.asyncio
    async def test_rpc_failure_on_one_account_continues_to_next(self, tmp_path, caplog):
        mgr = _make_manager(clone_accounts=[CLONE_ADDR_A, CLONE_ADDR_B])
        mgr._modified_mint_dir = str(tmp_path)

        with (
            patch.object(
                mgr,
                "_rpc_call_to_url",
                new_callable=AsyncMock,
                side_effect=[RuntimeError("rpc down"), {"value": _clone_value()}],
            ) as rpc,
            caplog.at_level("WARNING", logger="almanak.framework.anvil.solana_fork_manager"),
        ):
            await mgr._prepare_clone_account_files()

        assert rpc.await_count == 2
        assert not (tmp_path / f"{CLONE_ADDR_A}.json").exists()
        assert (tmp_path / f"{CLONE_ADDR_B}.json").exists()
        assert "Failed to pre-fetch clone account" in caplog.text


# =============================================================================
# _fund_single_token
# =============================================================================


class TestFundSingleToken:
    """Branch coverage for SolanaForkManager._fund_single_token."""

    @pytest.mark.asyncio
    async def test_unknown_symbol_short_circuits_before_rpc(self):
        mgr = _make_manager()

        with patch.object(mgr, "_rpc_call", new_callable=AsyncMock) as rpc:
            result = await mgr._fund_single_token(OWNER_ADDRESS, "definitely-not-a-token", Decimal("1"))

        assert result is False
        rpc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_known_mint_without_decimals_returns_false(self):
        mgr = _make_manager()

        # A mint entry with no matching decimals entry hits the decimals guard
        with (
            patch.dict(MINTS_PATH, {"FAKETOKEN": USDC_MINT}),
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock) as rpc,
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "FAKETOKEN", Decimal("1"))

        assert result is False
        rpc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_solders_returns_false(self):
        mgr = _make_manager()

        # None entries in sys.modules make `from solders.pubkey import ...` raise ImportError
        with patch.dict(sys.modules, {"solders": None, "solders.pubkey": None}):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "USDC", Decimal("1"))

        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ata_info", [None, {"value": None}])
    async def test_creates_ata_when_missing(self, ata_info):
        pytest.importorskip("solders")
        from solders.pubkey import Pubkey

        mgr = _make_manager()
        owner = Pubkey.from_string(OWNER_ADDRESS)
        mint = Pubkey.from_string(USDC_MINT)
        expected_ata = mgr._derive_ata(owner, mint)

        with (
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=ata_info) as rpc,
            patch.object(mgr, "_create_ata_and_mint", new_callable=AsyncMock, return_value=True) as create,
            patch.object(mgr, "_mint_to", new_callable=AsyncMock) as mint_to,
            patch.object(mgr, "_get_token_balance", new_callable=AsyncMock, return_value="1000"),
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "USDC", Decimal("1000"))

        assert result is True
        rpc.assert_awaited_once_with(
            "getAccountInfo",
            [str(expected_ata), {"encoding": "base64", "commitment": "confirmed"}],
        )
        create.assert_awaited_once_with(
            owner=owner,
            mint=mint,
            ata=expected_ata,
            amount=1_000_000_000,  # 1000 USDC at 6 decimals
        )
        mint_to.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_mints_to_existing_ata_with_lowercase_symbol(self):
        pytest.importorskip("solders")
        from solders.pubkey import Pubkey

        mgr = _make_manager()
        owner = Pubkey.from_string(OWNER_ADDRESS)
        mint = Pubkey.from_string(USDC_MINT)
        expected_ata = mgr._derive_ata(owner, mint)
        existing = {"value": {"data": ["", "base64"]}}

        with (
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=existing),
            patch.object(mgr, "_create_ata_and_mint", new_callable=AsyncMock) as create,
            patch.object(mgr, "_mint_to", new_callable=AsyncMock, return_value=True) as mint_to,
            patch.object(mgr, "_get_token_balance", new_callable=AsyncMock, return_value="5"),
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "usdc", Decimal("5"))

        assert result is True
        create.assert_not_awaited()
        mint_to.assert_awaited_once_with(
            mint=mint,
            destination=expected_ata,
            amount=5_000_000,
        )

    @pytest.mark.asyncio
    async def test_polls_balance_until_nonzero(self):
        pytest.importorskip("solders")

        mgr = _make_manager()

        with (
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=None),
            patch.object(mgr, "_create_ata_and_mint", new_callable=AsyncMock, return_value=True),
            patch.object(
                mgr,
                "_get_token_balance",
                new_callable=AsyncMock,
                side_effect=["0", "0", "42"],
            ) as balance,
            patch("asyncio.sleep", new_callable=AsyncMock) as sleep,
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "USDC", Decimal("42"))

        assert result is True
        assert balance.await_count == 3
        assert sleep.await_count == 2

    @pytest.mark.asyncio
    async def test_balance_stays_zero_returns_false(self):
        pytest.importorskip("solders")

        mgr = _make_manager()

        with (
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=None),
            patch.object(mgr, "_create_ata_and_mint", new_callable=AsyncMock, return_value=True),
            patch.object(mgr, "_get_token_balance", new_callable=AsyncMock, return_value="0") as balance,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "USDC", Decimal("1"))

        assert result is False
        assert balance.await_count == 20

    @pytest.mark.asyncio
    async def test_failed_transaction_skips_balance_poll(self):
        pytest.importorskip("solders")

        mgr = _make_manager()

        with (
            patch.object(mgr, "_rpc_call", new_callable=AsyncMock, return_value=None),
            patch.object(mgr, "_create_ata_and_mint", new_callable=AsyncMock, return_value=False),
            patch.object(mgr, "_get_token_balance", new_callable=AsyncMock) as balance,
        ):
            result = await mgr._fund_single_token(OWNER_ADDRESS, "USDC", Decimal("1"))

        assert result is False
        balance.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_address_returns_false(self):
        pytest.importorskip("solders")

        mgr = _make_manager()

        # Pubkey.from_string raises on non-base58 input -> generic except branch
        result = await mgr._fund_single_token("not-a-base58-address!!", "USDC", Decimal("1"))

        assert result is False
