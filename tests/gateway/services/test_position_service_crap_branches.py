from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.accounting.policy import MatchingPolicy
from almanak.framework.primitives.types import Primitive
from almanak.framework.state.ledger_registry_mode import LedgerRegistrySaveMode
from almanak.framework.state.registry_errors import RegistryAutoCollisionError
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services import position_service
from almanak.gateway.services.position_service import (
    PositionServiceServicer,
    _decode_lp_position_result,
    _parse_token_id,
    _PrimitiveErrorCollector,
    encode_cursor,
)


class _Context:
    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _servicer() -> PositionServiceServicer:
    return PositionServiceServicer(settings=None)  # type: ignore[arg-type]


def _encoded_position(*, liquidity: int = 100, invalid_fee: bool = False) -> str:
    words = [0] * 12
    words[2] = int("11" * 20, 16)
    words[3] = int("22" * 20, 16)
    words[4] = 500
    words[5] = 10
    words[6] = 20
    words[7] = liquidity
    encoded = [f"{word:064x}" for word in words]
    if invalid_fee:
        encoded[4] = "z" * 64
    return "0x" + "".join(encoded)


def _phantom(identity_hash: str = "phantom") -> dict[str, object]:
    return {
        "primitive": "lp",
        "accounting_category": "lp",
        "physical_identity_hash": identity_hash,
        "semantic_grouping_key": f"arbitrum:{identity_hash}",
        "payload": {"protocol": "uniswap_v3", "token_id": 42},
        "opened_at_block": 0,
        "opened_tx": "",
    }


def _collision() -> RegistryAutoCollisionError:
    return RegistryAutoCollisionError(
        semantic_grouping_key="arbitrum:pool",
        existing_physical_identity_hash="existing",
        opened_tx="0xopen",
        accounting_category="lp",
    )


def test_wallet_registry_accepts_local_and_matching_wallets() -> None:
    servicer = _servicer()
    assert servicer._wallet_matches_registry("arbitrum", "0xabc", _Context())

    servicer.wallet_registry = SimpleNamespace(
        resolve=MagicMock(return_value=SimpleNamespace(account_address="  0xAbC  "))
    )
    context = _Context()
    assert servicer._wallet_matches_registry("arbitrum", "0xABC", context)
    assert context.code is None


def test_wallet_registry_mismatch_is_permission_denied() -> None:
    servicer = _servicer()
    servicer.wallet_registry = SimpleNamespace(resolve=MagicMock(return_value=SimpleNamespace(account_address="0xabc")))
    context = _Context()

    assert not servicer._wallet_matches_registry("arbitrum", "0xdef", context)
    assert context.code == grpc.StatusCode.PERMISSION_DENIED
    assert "does not match" in context.details


@pytest.mark.parametrize(
    ("resolver", "expected_code", "detail"),
    [
        (MagicMock(side_effect=KeyError("arbitrum")), grpc.StatusCode.FAILED_PRECONDITION, "no registered"),
        (MagicMock(side_effect=RuntimeError("down")), grpc.StatusCode.INTERNAL, "unavailable"),
        (MagicMock(return_value=None), grpc.StatusCode.FAILED_PRECONDITION, "no registered"),
        (
            MagicMock(return_value=SimpleNamespace(account_address="  ")),
            grpc.StatusCode.FAILED_PRECONDITION,
            "has no address",
        ),
    ],
)
def test_wallet_registry_resolution_failures_are_closed(
    resolver: MagicMock,
    expected_code: grpc.StatusCode,
    detail: str,
) -> None:
    servicer = _servicer()
    servicer.wallet_registry = SimpleNamespace(resolve=resolver)
    context = _Context()

    assert not servicer._wallet_matches_registry("arbitrum", "0xabc", context)
    assert context.code == expected_code
    assert detail in context.details


@pytest.mark.asyncio
async def test_source_block_resolution_rejects_unwired_and_failed_head(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer()
    context = _Context()
    request = gateway_pb2.ReconcileRequest()

    assert await servicer._resolve_source_block_number(request, "arbitrum", context) is None
    assert context.code == grpc.StatusCode.INTERNAL
    assert "initialized" in context.details

    servicer.rpc_servicer = MagicMock()
    monkeypatch.setattr(position_service, "_get_chain_head", AsyncMock(return_value=None))
    context = _Context()
    assert await servicer._resolve_source_block_number(request, "arbitrum", context) is None
    assert context.code == grpc.StatusCode.INTERNAL
    assert "sample chain head" in context.details


@pytest.mark.asyncio
async def test_source_block_resolution_pins_first_page_and_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer()
    servicer.rpc_servicer = MagicMock()
    monkeypatch.setattr(position_service, "_get_chain_head", AsyncMock(return_value=120))

    first_context = _Context()
    assert (
        await servicer._resolve_source_block_number(
            gateway_pb2.ReconcileRequest(max_age_blocks=0), "arbitrum", first_context
        )
        == 120
    )

    cursor_context = _Context()
    request = gateway_pb2.ReconcileRequest(
        page_cursor=encode_cursor(source_block_number=100, last_primitive="lp", last_hash="hash"),
        max_age_blocks=20,
    )
    assert await servicer._resolve_source_block_number(request, "arbitrum", cursor_context) == 100
    assert cursor_context.code is None


def test_cursor_block_resolution_rejects_malformed_and_stale_cursor() -> None:
    malformed_context = _Context()
    malformed = gateway_pb2.ReconcileRequest(page_cursor=b"bad")
    assert PositionServiceServicer._source_block_from_cursor(malformed, 120, malformed_context) is None
    assert malformed_context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "malformed" in malformed_context.details

    stale_context = _Context()
    stale = gateway_pb2.ReconcileRequest(
        page_cursor=encode_cursor(source_block_number=90, last_primitive="lp", last_hash="hash"),
        max_age_blocks=20,
    )
    assert PositionServiceServicer._source_block_from_cursor(stale, 120, stale_context) is None
    assert stale_context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "head=120" in stale_context.details
    assert "cursor_block=90" in stale_context.details


def test_first_page_rejects_unsupported_freshness_guard() -> None:
    context = _Context()
    request = gateway_pb2.ReconcileRequest(max_age_blocks=1)
    assert PositionServiceServicer._source_block_for_first_page(request, 120, context) is None
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "first-page" in context.details


@pytest.mark.parametrize("raw", [None, 42, "0x", "not-hex"])
def test_parse_token_id_drops_unreadable_values(raw: object) -> None:
    assert _parse_token_id(raw) is None  # type: ignore[arg-type]


def test_parse_token_id_accepts_hex() -> None:
    assert _parse_token_id("0x2a") == 42


@pytest.mark.parametrize(
    "raw",
    [None, 42, "0x", "0x1234", _encoded_position(invalid_fee=True), _encoded_position(liquidity=0)],
)
def test_decode_lp_position_drops_unreadable_or_empty_values(raw: object) -> None:
    assert _decode_lp_position_result(raw) is None  # type: ignore[arg-type]


def test_decode_lp_position_returns_reconciliation_fields() -> None:
    decoded = _decode_lp_position_result(_encoded_position())
    assert decoded == {
        "token0": "0x" + "11" * 20,
        "token1": "0x" + "22" * 20,
        "fee": 500,
        "tick_lower": 10,
        "tick_upper": 20,
        "liquidity": 100,
    }


@pytest.mark.asyncio
async def test_read_lp_position_drops_failed_token_and_position_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer()
    servicer.rpc_servicer = MagicMock()

    call = AsyncMock(return_value=None)
    monkeypatch.setattr(position_service, "_eth_call_in_process", call)
    result = await servicer._read_lp_position(
        chain="arbitrum",
        wallet_address="0x" + "aa" * 20,
        protocol="uniswap_v3",
        npm="0x" + "bb" * 20,
        index=0,
        source_block_number=123,
        physical_identity_hashes_filter=None,
    )
    assert result is None
    assert call.await_count == 1

    call.reset_mock(side_effect=True)
    call.side_effect = ["0x2a", None]
    result = await servicer._read_lp_position(
        chain="arbitrum",
        wallet_address="0x" + "aa" * 20,
        protocol="uniswap_v3",
        npm="0x" + "bb" * 20,
        index=0,
        source_block_number=123,
        physical_identity_hashes_filter=None,
    )
    assert result is None
    assert call.await_count == 2


@pytest.mark.asyncio
async def test_read_lp_position_preserves_block_pin_and_hash_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    servicer = _servicer()
    servicer.rpc_servicer = MagicMock()
    call = AsyncMock(side_effect=["0x2a", _encoded_position()])
    monkeypatch.setattr(position_service, "_eth_call_in_process", call)
    kwargs = {
        "chain": "arbitrum",
        "wallet_address": "0x" + "aa" * 20,
        "protocol": "uniswap_v3",
        "npm": "0x" + "bb" * 20,
        "index": 3,
        "source_block_number": 123,
    }

    result = await servicer._read_lp_position(**kwargs, physical_identity_hashes_filter=None)
    assert result is not None
    assert result["payload"] == {
        "source": "reconciliation_discovery",
        "protocol": "uniswap_v3",
        "token_id": 42,
        "npm_address": "0x" + "bb" * 20,
        "token0": "0x" + "11" * 20,
        "token1": "0x" + "22" * 20,
        "fee": 500,
        "tick_lower": 10,
        "tick_upper": 20,
        "liquidity": "100",
    }
    assert result["semantic_grouping_key"] == f"arbitrum:0x{'11' * 20}:0x{'22' * 20}:500"
    assert [rpc_call.kwargs["block_number"] for rpc_call in call.await_args_list] == [123, 123]

    call.reset_mock(side_effect=True)
    call.side_effect = ["0x2a", _encoded_position()]
    filtered = await servicer._read_lp_position(
        **kwargs,
        physical_identity_hashes_filter=frozenset({"different"}),
    )
    assert filtered is None


@pytest.mark.asyncio
async def test_registry_reader_reports_unwired_state_servicer() -> None:
    errors = _PrimitiveErrorCollector()
    rows = await _servicer()._read_registry_rows(
        deployment_id="deployment-a",
        chain="arbitrum",
        primitives=["lp"],
        physical_identity_hashes=[],
        errors=errors,
    )

    assert rows == []
    error = errors.list()[0]
    assert (error.primitive, error.chain, error.code, error.recoverable) == (
        "lp",
        "arbitrum",
        "BACKEND_TIMEOUT",
        False,
    )


@pytest.mark.asyncio
async def test_registry_reader_preserves_postgres_order_and_malformed_payload_fallback() -> None:
    state_servicer = SimpleNamespace(
        _snapshot_pool=object(),
        _ensure_snapshot_pool=AsyncMock(),
        _snapshot_fetch=AsyncMock(
            return_value=[
                {"physical_identity_hash": "first", "payload_text": '{"token_id": 1}'},
                {"physical_identity_hash": "second", "payload_text": "not-json"},
                {"physical_identity_hash": "third", "payload_text": None},
            ]
        ),
    )
    servicer = _servicer()
    servicer.state_servicer = state_servicer
    errors = _PrimitiveErrorCollector()

    rows = await servicer._read_registry_rows(
        deployment_id="deployment-a",
        chain="arbitrum",
        primitives=["lp"],
        physical_identity_hashes=[],
        errors=errors,
    )

    assert [row["physical_identity_hash"] for row in rows] == ["first", "second", "third"]
    assert [row["payload"] for row in rows] == [{"token_id": 1}, {}, {}]
    assert all("payload_text" not in row for row in rows)
    sql, deployment_id, chain, primitive = state_servicer._snapshot_fetch.await_args.args
    assert "WHERE deployment_id = $1 AND status = 'open'" in sql
    assert "chain = $2 AND primitive = $3" in sql
    assert "ORDER BY opened_at_block ASC NULLS FIRST, opened_tx ASC NULLS FIRST" in sql
    assert (deployment_id, chain, primitive) == ("deployment-a", "arbitrum", "lp")
    assert errors.list() == []


@pytest.mark.asyncio
async def test_registry_reader_uses_sqlite_fallback_with_exact_scope() -> None:
    warm = SimpleNamespace(
        get_position_registry_open_rows=AsyncMock(return_value=[{"physical_identity_hash": "sqlite"}])
    )
    state_servicer = SimpleNamespace(
        _snapshot_pool=None,
        _ensure_snapshot_pool=AsyncMock(),
        _ensure_initialized=AsyncMock(),
        _state_manager=SimpleNamespace(warm_backend=warm),
    )
    servicer = _servicer()
    servicer.state_servicer = state_servicer
    errors = _PrimitiveErrorCollector()

    rows = await servicer._read_registry_rows(
        deployment_id="deployment-a",
        chain="arbitrum",
        primitives=["lp"],
        physical_identity_hashes=[],
        errors=errors,
    )

    assert rows == [{"physical_identity_hash": "sqlite"}]
    assert warm.get_position_registry_open_rows.await_args.args == ("deployment-a",)
    assert warm.get_position_registry_open_rows.await_args.kwargs == {
        "chain": "arbitrum",
        "primitive": "lp",
        "accounting_category": None,
    }
    assert errors.list() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("warm", [None, object()])
async def test_registry_reader_reports_missing_sqlite_capability(warm: object | None) -> None:
    state_servicer = SimpleNamespace(
        _snapshot_pool=None,
        _ensure_snapshot_pool=AsyncMock(),
        _ensure_initialized=AsyncMock(),
        _state_manager=SimpleNamespace(warm_backend=warm),
    )
    servicer = _servicer()
    servicer.state_servicer = state_servicer
    errors = _PrimitiveErrorCollector()

    rows = await servicer._read_registry_rows(
        deployment_id="deployment-a",
        chain="arbitrum",
        primitives=["lp"],
        physical_identity_hashes=[],
        errors=errors,
    )

    assert rows == []
    error = errors.list()[0]
    assert error.code == "BACKEND_TIMEOUT"
    assert not error.recoverable
    assert "lacks get_position_registry_open_rows" in error.message


@pytest.mark.asyncio
async def test_registry_reader_keeps_prior_rows_on_malformed_row_and_continues_primitives() -> None:
    servicer = _servicer()
    servicer.state_servicer = SimpleNamespace(
        _snapshot_pool=object(),
        _ensure_snapshot_pool=AsyncMock(),
        _snapshot_fetch=AsyncMock(
            side_effect=[
                [{"physical_identity_hash": "keep", "payload_text": "{}"}, object()],
                [
                    {"physical_identity_hash": "drop", "payload_text": "{}"},
                    {"physical_identity_hash": "keep-too", "payload_text": "{}"},
                ],
            ]
        ),
    )
    errors = _PrimitiveErrorCollector()

    rows = await servicer._read_registry_rows(
        deployment_id="deployment-a",
        chain="arbitrum",
        primitives=["lp", "other"],
        physical_identity_hashes=["keep", "keep-too"],
        errors=errors,
    )

    assert rows == [
        {"physical_identity_hash": "keep", "payload": {}},
        {"physical_identity_hash": "keep-too", "payload": {}},
    ]
    error = errors.list()[0]
    assert (error.primitive, error.code, error.recoverable) == ("lp", "BACKEND_TIMEOUT", True)
    assert "registry read failed" in error.message


@pytest.mark.asyncio
async def test_apply_phantoms_handles_empty_and_unwired_paths() -> None:
    servicer = _servicer()
    errors = _PrimitiveErrorCollector()
    kwargs = {
        "deployment_id": "deployment-a",
        "chain": "arbitrum",
        "source_block_number": 123,
        "reconciliation_id": "reconcile-a",
        "errors": errors,
    }

    assert await servicer._apply_phantom_missing(**kwargs, phantoms=[]) == []
    assert errors.list() == []
    assert await servicer._apply_phantom_missing(**kwargs, phantoms=[_phantom()]) == []
    error = errors.list()[0]
    assert (error.code, error.recoverable) == ("BACKEND_TIMEOUT", False)


@pytest.mark.asyncio
async def test_apply_phantom_uses_registry_only_mode_and_current_matching_policy() -> None:
    save = AsyncMock()
    state_servicer = SimpleNamespace(
        _ensure_initialized=AsyncMock(),
        _state_manager=SimpleNamespace(save_ledger_and_registry=save),
    )
    servicer = _servicer()
    servicer.state_servicer = state_servicer
    errors = _PrimitiveErrorCollector()
    phantom = _phantom()

    rebuilt = await servicer._apply_phantom_missing(
        deployment_id="deployment-a",
        chain="arbitrum",
        source_block_number=123,
        reconciliation_id="reconcile-a",
        phantoms=[phantom],
        errors=errors,
    )

    assert [row["physical_identity_hash"] for row in rebuilt] == ["phantom"]
    call = save.await_args.kwargs
    assert call["mode"] is LedgerRegistrySaveMode.REGISTRY_RECONCILIATION
    assert call["ledger"].deployment_id == "deployment-a"
    assert call["ledger"].tx_hash == ""
    assert call["registry"].deployment_id == "deployment-a"
    assert call["registry"].matching_policy_version == MatchingPolicy.for_primitive(Primitive.LP)
    assert call["registry"].matching_policy_version != 1
    assert call["registry"].last_reconciled_at_block == 123
    assert call["registry"].opened_at_block is None
    assert call["registry"].payload["reconciliation_id"] == "reconcile-a"
    assert phantom["payload"] == {"protocol": "uniswap_v3", "token_id": 42}
    assert errors.list() == []


@pytest.mark.asyncio
async def test_apply_phantoms_isolates_collision_and_backend_failure() -> None:
    save = AsyncMock(side_effect=[_collision(), RuntimeError("database down"), None])
    state_servicer = SimpleNamespace(
        _ensure_initialized=AsyncMock(),
        _state_manager=SimpleNamespace(save_ledger_and_registry=save),
    )
    servicer = _servicer()
    servicer.state_servicer = state_servicer
    errors = _PrimitiveErrorCollector()

    rebuilt = await servicer._apply_phantom_missing(
        deployment_id="deployment-a",
        chain="arbitrum",
        source_block_number=123,
        reconciliation_id="reconcile-a",
        phantoms=[_phantom("collision"), _phantom("backend"), _phantom("rebuilt")],
        errors=errors,
    )

    assert [row["physical_identity_hash"] for row in rebuilt] == ["rebuilt"]
    assert [error.code for error in errors.list()] == ["REGISTRY_AUTO_COLLISION", "BACKEND_TIMEOUT"]
    assert [error.recoverable for error in errors.list()] == [False, True]
    assert save.await_count == 3
