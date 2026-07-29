"""The gateway's SaveLedgerEntry RPC applies the Empty != Zero guard (VIB-6043).

`SaveLedgerEntry` is an **independent write boundary**: it persists the caller's
`success` / `amount_in` / `amount_out` into `transaction_ledger` in both the
PostgreSQL branch (hosted) and the SQLite branch (local), and it is the write
path a hosted deployment actually uses.

The gateway is the **trust boundary** — it must not assume its caller already
ran the guard. A differently-versioned strategy container, a replay tool, or any
other gRPC client could otherwise still write the exact shape the guard exists
to forbid: `success=True` with unmeasured money columns. (Flagged by CodeRabbit
on PR #3441; the strategy-side commit primitive alone was not sufficient.)

These tests drive the **SQLite branch**, which delegates a fully-formed
`LedgerEntry` to the warm backend — so the persisted object can be inspected
directly. The PostgreSQL branch consumes the same three `ledger_*` locals
computed once, before either branch runs; a static test pins that.
"""

from __future__ import annotations

import ast
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX

REPO_ROOT = Path(__file__).resolve().parents[3]
STATE_SERVICE = REPO_ROOT / "almanak" / "gateway" / "services" / "state_service.py"


class _CapturingBackend:
    """Warm backend that records the LedgerEntry it was asked to persist."""

    def __init__(self) -> None:
        self.saved: list[Any] = []

    async def save_ledger_entry(self, entry: Any) -> None:
        self.saved.append(entry)


class _Ctx:
    """Minimal grpc ServicerContext stand-in."""

    def __init__(self) -> None:
        self.code = None
        self.details_text = ""

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details_text = details


def _request(**overrides):
    """A SaveLedgerEntryRequest-shaped object (duck-typed, no proto needed)."""
    base = {
        "id": str(uuid.uuid4()),
        "cycle_id": "cycle-1",
        "deployment_id": "deployment:abc123abc123",
        "execution_mode": "live",
        "timestamp": int(datetime.now(UTC).timestamp()),
        "intent_type": "SWAP",
        "token_in": "USDC",
        "amount_in": "50",
        "token_out": "WETH",
        "amount_out": "0.0265",
        "effective_price": "",
        "slippage_bps": 0,
        "gas_used": 263_322,
        "gas_usd": "0.42",
        "tx_hash": "0x" + "ab" * 32,
        "chain": "arbitrum",
        "protocol": "pancakeswap_v3",
        "success": True,
        "error": "",
        "extracted_data_json": b"",
        "price_inputs_json": b"",
        "pre_state_json": b"",
        "post_state_json": b"",
    }
    base.update(overrides)

    class _Req(SimpleNamespace):
        """Adds the protobuf ``HasField`` the servicer probes for optionals."""

        def HasField(self, name: str) -> bool:  # noqa: N802 — protobuf API shape
            return bool(getattr(self, name, None))

    return _Req(**base)


async def _save(request) -> tuple[Any, _Ctx]:
    """Run the SQLite branch of SaveLedgerEntry and return the persisted entry."""
    from almanak.gateway.services.state_service import StateServiceServicer

    backend = _CapturingBackend()
    service = StateServiceServicer.__new__(StateServiceServicer)
    service._state_manager = SimpleNamespace(warm_backend=backend)
    service._snapshot_pool = None  # forces the SQLite branch

    async def _noop() -> None:
        return None

    service._ensure_snapshot_pool = _noop  # type: ignore[method-assign]
    service._ensure_initialized = _noop  # type: ignore[method-assign]

    ctx = _Ctx()
    await StateServiceServicer.SaveLedgerEntry(service, request, ctx)
    return (backend.saved[0] if backend.saved else None), ctx


@pytest.mark.asyncio
async def test_unmeasured_swap_is_degraded_at_the_gateway_boundary():
    """The shape a non-guard-applying client could send must not persist as-is."""
    entry, _ = await _save(_request(amount_out=""))

    assert entry is not None, "nothing was persisted"
    assert entry.success is False, "gateway persisted a clean success with an unmeasured amount"
    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "amount_out" in entry.error
    # Chain reality preserved — a books verdict, not a claim the tx failed.
    assert entry.tx_hash == "0x" + "ab" * 32
    assert entry.gas_used == 263_322
    # Empty != Zero: the unmeasured column stays unmeasured. Never "0".
    assert entry.amount_out == ""
    assert entry.amount_in == "50"

    marker = json.loads(entry.extracted_data_json)["accounting_degradation"]
    assert marker["reason"] == "amounts_unmeasured"
    assert marker["chain_success"] is True


@pytest.mark.asyncio
async def test_measured_zero_survives_the_gateway_boundary():
    """A measured zero is a measurement — degrading it would be the mirror bug."""
    entry, _ = await _save(_request(amount_in="0", amount_out="0"))

    assert entry.success is True
    assert entry.error == ""
    assert entry.amount_in == "0"
    assert entry.amount_out == "0"


@pytest.mark.asyncio
async def test_clean_row_passes_through_untouched():
    entry, _ = await _save(_request())

    assert entry.success is True
    assert entry.error == ""
    assert entry.amount_in == "50"
    assert entry.amount_out == "0.0265"
    assert entry.extracted_data_json == ""


@pytest.mark.asyncio
async def test_lp_close_with_no_measured_column_is_degraded():
    """VIB-6051's shape, arriving over the wire instead of via the runner."""
    entry, _ = await _save(_request(intent_type="LP_CLOSE", amount_in="", amount_out="", protocol="curve"))

    assert entry.success is False
    assert entry.error.startswith(DEGRADED_PREFIX)


@pytest.mark.asyncio
async def test_single_sided_lp_close_is_not_degraded():
    entry, _ = await _save(_request(intent_type="LP_CLOSE", amount_in="", amount_out="25", protocol="curve"))

    assert entry.success is True
    assert entry.error == ""


@pytest.mark.asyncio
async def test_existing_caller_error_text_is_preserved():
    entry, _ = await _save(_request(amount_out="", error="caller note"))

    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "caller note" in entry.error


def _arg_names(node: ast.AST) -> set[str]:
    """Every dotted name a call argument could bind, so ``request.success`` is comparable.

    Returns a SET, and unwraps both arms of a conditional. The earlier version
    returned ``""`` for any unrecognised node type, which made the negative
    assertions below passable: rewriting the INSERT to bind
    ``getattr(request, "success")`` yielded ``""`` and
    ``"request.success" not in bound`` passed vacuously. Unrecognised shapes
    now surface as a literal marker so an assertion cannot silently succeed
    against them.
    """
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {f"{base}.{node.attr}" for base in _arg_names(node.value)} or {f"?.{node.attr}"}
    if isinstance(node, ast.IfExp):
        return _arg_names(node.body) | _arg_names(node.orelse)
    if isinstance(node, ast.Call):
        # e.g. ``getattr(request, "success")`` — record the callee plus any
        # constant string argument, so this cannot be used to smuggle a raw
        # request field past the negative assertions unnoticed.
        names = _arg_names(node.func)
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                names.add(arg.value)
            else:
                names |= _arg_names(arg)
        return names
    if isinstance(node, ast.Constant):
        return set()
    return {f"<unrecognised:{type(node).__name__}>"}


def _transaction_ledger_insert_call(fn: ast.AST) -> ast.Call | None:
    """The Call whose first literal argument is the transaction_ledger INSERT."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        first = node.args[0]
        if (
            isinstance(first, ast.Constant)
            and isinstance(first.value, str)
            and "transaction_ledger" in first.value
            and "INSERT" in first.value.upper()
        ):
            return node
    return None


def test_both_persistence_branches_consume_the_guarded_values():
    """Static guard: the PostgreSQL branch must not bypass what SQLite applies.

    The PG branch builds an INSERT rather than a ``LedgerEntry``, so a unit test
    cannot inspect it without a live pool. Instead, pin that neither branch
    persists the raw ``request.success`` / ``request.error`` — both must use the
    ``ledger_*`` locals the guard computes once, before either branch runs.
    """
    source = STATE_SERVICE.read_text()
    tree = ast.parse(source)
    fn = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == "SaveLedgerEntry"
    )
    body = ast.dump(fn)

    assert "degrade_row_fields" in body, "SaveLedgerEntry does not apply the Empty != Zero guard"

    # Inspect the ACTUAL persistence call arguments, not merely the function
    # text. The previous form of this test asserted
    #   "attr='success'" not in body.split(...)[1] or "ledger_success" in body
    # which is a tautology — ``ledger_success`` is necessarily present once the
    # guard is applied, so the `or` arm always short-circuits true and a
    # regression to ``request.success`` in the INSERT would sail through it.
    pg_call = _transaction_ledger_insert_call(fn)
    assert pg_call is not None, "could not locate the transaction_ledger INSERT in SaveLedgerEntry"

    bound: set[str] = set()
    for a in pg_call.args:
        bound |= _arg_names(a)
    assert "ledger_success" in bound, "the PostgreSQL INSERT does not bind the guarded success"
    assert "ledger_error" in bound, "the PostgreSQL INSERT does not bind the guarded error"
    assert "request.success" not in bound, "the PostgreSQL INSERT still binds the caller's raw success"
    assert "request.error" not in bound, "the PostgreSQL INSERT still binds the caller's raw error"

    # And the SQLite branch, which builds a LedgerEntry from the same locals.
    #
    # Inspect the LedgerEntry(...) KEYWORDS, not a symbol count. The previous
    # form asserted ``body.count("ledger_success") >= 2`` — and ``ledger_success``
    # is ASSIGNED twice (state_service.py:1445 and :1453) before either branch
    # binds it, so the count was already satisfied by the assignments alone.
    # Measured: regressing the SQLite branch to ``success=request.success``
    # leaves the count at 2 and this assertion passing. The behavioural tests
    # do catch that regression, so it was never a coverage hole — but a static
    # check that cannot fail is exactly the shape this PR exists to remove.
    entry_call = next(
        (node for node in ast.walk(fn)
         if isinstance(node, ast.Call)
         and isinstance(node.func, ast.Name)
         and node.func.id == "LedgerEntry"),
        None,
    )
    assert entry_call is not None, "could not locate the SQLite LedgerEntry(...) construction"
    kw = {k.arg: _arg_names(k.value) for k in entry_call.keywords if k.arg}
    assert "ledger_success" in kw.get("success", set()), (
        "the SQLite LedgerEntry does not bind the guarded success"
    )
    assert "ledger_error" in kw.get("error", set()), (
        "the SQLite LedgerEntry does not bind the guarded error"
    )
    assert "request.success" not in kw.get("success", set()), (
        "the SQLite LedgerEntry still binds the caller's raw success"
    )
    assert "request.error" not in kw.get("error", set()), (
        "the SQLite LedgerEntry still binds the caller's raw error"
    )


# ---------------------------------------------------------------------------
# SaveLedgerAndRegistry — the SECOND gateway write boundary (VIB-6043 leg 2).
#
# The atomic/registry RPC rebuilds a LedgerEntry from the request and hands the
# SAME object to both the PostgreSQL INSERT and the SQLite delegate. It was
# persisting caller-supplied success/amounts verbatim while the whole
# `almanak/gateway` tree sat behind one chokepoint-test allowlist entry earned
# only by `SaveLedgerEntry` — the same allowlist-hides-a-bypass shape as the
# round that produced f99a618, one RPC over.
#
# The guard is applied at `_build_ledger_entry_from_request`, the single
# construction point, so BOTH branches inherit it structurally.
# ---------------------------------------------------------------------------


def _atomic_request(**overrides):
    """A SaveLedgerAndRegistryRequest-shaped object (duck-typed)."""
    return _request(**overrides)


def _build_atomic_entry(request):
    """Run the atomic RPC's ledger construction path."""
    from almanak.gateway.services.state_service import StateServiceServicer

    return StateServiceServicer._build_ledger_entry_from_request(
        request,
        str(uuid.uuid4()),
        "deployment:abc123abc123",
        datetime.now(UTC),
    )


def test_unmeasured_swap_is_degraded_on_the_atomic_rpc_path():
    """The bypass: an unmeasured successful SWAP over SaveLedgerAndRegistry."""
    entry = _build_atomic_entry(_atomic_request(amount_out=""))

    assert entry.success is False, "the atomic RPC persisted a clean success with an unmeasured amount"
    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "amount_out" in entry.error
    # Chain reality preserved — a books verdict, not a claim the tx failed.
    assert entry.tx_hash == "0x" + "ab" * 32
    assert entry.gas_used == 263_322
    # Empty != Zero: the unmeasured column stays unmeasured. Never "0".
    assert entry.amount_out == ""
    assert entry.amount_in == "50"

    marker = json.loads(entry.extracted_data_json)["accounting_degradation"]
    assert marker["reason"] == "amounts_unmeasured"
    assert marker["chain_success"] is True


def test_lp_close_with_no_measured_column_is_degraded_on_the_atomic_rpc_path():
    """VIB-6051's shape over the registry lane — the path LP rows actually take."""
    entry = _build_atomic_entry(
        _atomic_request(intent_type="LP_CLOSE", amount_in="", amount_out="", protocol="curve")
    )

    assert entry.success is False
    assert entry.error.startswith(DEGRADED_PREFIX)


def test_measured_zero_survives_the_atomic_rpc_path():
    """A measured zero is a measurement — degrading it would be the mirror bug."""
    entry = _build_atomic_entry(_atomic_request(amount_in="0", amount_out="0"))

    assert entry.success is True
    assert entry.error == ""
    assert entry.amount_in == "0"
    assert entry.amount_out == "0"


def test_single_sided_lp_close_is_not_degraded_on_the_atomic_rpc_path():
    entry = _build_atomic_entry(
        _atomic_request(intent_type="LP_CLOSE", amount_in="", amount_out="25", protocol="curve")
    )

    assert entry.success is True
    assert entry.error == ""


def test_clean_row_passes_through_the_atomic_rpc_path_untouched():
    entry = _build_atomic_entry(_atomic_request())

    assert entry.success is True
    assert entry.error == ""
    assert entry.amount_in == "50"
    assert entry.amount_out == "0.0265"


def test_atomic_rpc_preserves_existing_caller_error_text():
    entry = _build_atomic_entry(_atomic_request(amount_out="", error="caller note"))

    assert entry.error.startswith(DEGRADED_PREFIX)
    assert "caller note" in entry.error


def test_atomic_rpc_does_not_double_mark_an_already_degraded_row():
    """Idempotence — a retry must not stack markers."""
    already = f"{DEGRADED_PREFIX}amounts_unmeasured: previous"
    entry = _build_atomic_entry(_atomic_request(amount_out="", success=False, error=already))

    assert entry.error == already
    assert entry.error.count(DEGRADED_PREFIX) == 1
