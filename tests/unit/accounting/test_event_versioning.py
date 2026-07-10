"""VIB-4166 (T6) — primitive_version per-primitive stamp + bump-policy contract.

Frozen UAT card: ``docs/internal/uat-cards/VIB-4166.md``.
Phase 1 verdict: SPEC_OK at SHA c11ffeaf84aeb64583b82d0da698b132c83558fe
(see ``docs/internal/uat-runs/VIB-4166/phase1-verdict.md``).

The contract:

1. Every accounting_events row carries ``primitive_version`` — an int stamped
   by the single augment chokepoint (``writer.augment_accounting_payload``)
   invoked from BOTH state backends' ``save_accounting_event`` per VIB-3862.
2. The stamped value is per-primitive, resolved through the same taxonomy
   lookup used for ``matching_policy_version``. Bumps are isolated.
3. Mode-aware contract preserved: live raises on missing/unknown event_type,
   paper/dry-run logs ERROR + falls back to ``Primitive.UTILITY``.
4. Round-trip lossless for every typed event class with ``from_payload_json``.
5. Dataclass field default is ``int`` (not ``bool`` subclass, not ``float``).
6. JSON serialisation produces a JSON integer (not stringified, not float).
"""

from __future__ import annotations

import ast
import inspect
import json
import sqlite3
import textwrap
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
import pytest_asyncio

from almanak.framework.accounting import writer as writer_module
from almanak.framework.accounting.lp_accounting import LPAccountingEvent
from almanak.framework.accounting.models import (
    ALL_ACCOUNTING_EVENT_TYPES,
    AccountingConfidence,
    AccountingIdentity,
    LendingAccountingEvent,
    LendingEventType,
    LPEventType,
    PendleAccountingEvent,
    PendleEventType,
    PerpEventType,
    PredictionAccountingEvent,
    PredictionEventType,
    SettlementEventType,
    SwapAccountingEvent,
    SwapEventType,
    TransferAccountingEvent,
    TransferEventType,
    TransferSettlementStatus,
    VaultEventType,
)
from almanak.framework.accounting.payload_schemas import (
    _PAYLOAD_MODELS,
    MATCHING_POLICY_VERSIONS,
    PRIMITIVE_VERSION_DEFAULT,
    PRIMITIVE_VERSIONS,
)
from almanak.framework.accounting.perp_accounting import PerpAccountingEvent
from almanak.framework.accounting.settlement_accounting import SettlementAccountingEvent
from almanak.framework.accounting.vault_accounting import VaultAccountingEvent
from almanak.framework.accounting.writer import (
    AccountingEvent,
    AccountingWriter,
    augment_accounting_payload,
)
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.gateway_state_manager import GatewayStateManager

# ─── Fixtures and constants ────────────────────────────────────────────────


_NOW = datetime(2026, 5, 10, 12, 0, 0, tzinfo=UTC)
_TYPED_CLASSES_WITH_SERDE = (
    LendingAccountingEvent,
    PendleAccountingEvent,
    PredictionAccountingEvent,
    SwapAccountingEvent,
    TransferAccountingEvent,
)
_DUCK_TYPED_CLASSES = (LPAccountingEvent, PerpAccountingEvent, VaultAccountingEvent, SettlementAccountingEvent)
_ALL_EVENT_CLASSES = _TYPED_CLASSES_WITH_SERDE + _DUCK_TYPED_CLASSES


def _identity(*, event_type_str: str = "SUPPLY") -> AccountingIdentity:
    """Build a minimal AccountingIdentity for synthetic events."""
    return AccountingIdentity(
        id=str(uuid.uuid4()),
        deployment_id="strat-vib-4166",
        cycle_id="cycle-1",
        execution_mode="paper",
        timestamp=_NOW,
        chain="arbitrum",
        protocol="test_proto",
        wallet_address="0x" + "0" * 40,
        tx_hash="0x" + "1" * 64,
        ledger_entry_id="le-1",
    )


def _build_event(cls: type, *, primitive_version: int = 1) -> object:
    """Construct a minimal valid event instance of ``cls`` with the given
    ``primitive_version``. Each branch builds the smallest valid instance
    for one event_type within the primitive."""
    if cls is LendingAccountingEvent:
        return LendingAccountingEvent(
            identity=_identity(event_type_str="SUPPLY"),
            event_type=LendingEventType.SUPPLY,
            position_key="lending:arbitrum:aave_v3:wallet:USDC",
            market_id="aave_v3:USDC",
            asset="USDC",
            collateral_value_before_usd=None,
            collateral_value_after_usd=Decimal("1000"),
            debt_value_before_usd=None,
            debt_value_after_usd=None,
            net_equity_before_usd=None,
            net_equity_after_usd=Decimal("1000"),
            health_factor_before=None,
            health_factor_after=None,
            liquidation_threshold=None,
            lltv=None,
            supply_apr_bps=500,
            borrow_apr_bps=None,
            principal_delta_usd=Decimal("1000"),
            interest_delta_usd=None,
            gas_usd=Decimal("0.5"),
            primitive_version=primitive_version,
        )
    if cls is PendleAccountingEvent:
        return PendleAccountingEvent(
            identity=_identity(event_type_str="PT_BUY"),
            event_type=PendleEventType.PT_BUY,
            position_key="pendle:arbitrum:pendle:wallet:0xpt",
            market_id="pendle-market-1",
            pt_token="0xpt",
            maturity_timestamp=_NOW,
            pt_amount=Decimal("100"),
            sy_amount=Decimal("100"),
            pt_price=Decimal("0.95"),
            implied_apr_bps=600,
            days_to_maturity=180,
            realized_yield_usd=None,
            primitive_version=primitive_version,
        )
    if cls is SwapAccountingEvent:
        return SwapAccountingEvent(
            identity=_identity(event_type_str="SWAP"),
            event_type=SwapEventType.SWAP,
            protocol="enso",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal("1000"),
            amount_out=Decimal("0.3"),
            amount_in_usd=Decimal("1000"),
            amount_out_usd=Decimal("995"),
            effective_price=Decimal("3333.33"),
            slippage_bps=10,
            realized_pnl_usd=None,
            cost_basis_recorded=True,
            gas_usd=Decimal("0.5"),
            confidence=AccountingConfidence.HIGH,
            unavailable_reason="",
            swap_position_key="swap:arbitrum:wallet",
            primitive_version=primitive_version,
        )
    if cls is PredictionAccountingEvent:
        return PredictionAccountingEvent(
            identity=_identity(event_type_str="PREDICTION_OPEN"),
            event_type=PredictionEventType.PREDICTION_OPEN,
            position_key="prediction:polygon:polymarket:wallet:m1:YES",
            market_id="m1",
            outcome="YES",
            intent_type="PREDICTION_BUY",
            shares_delta=Decimal("100"),
            usd_delta=Decimal("60"),
            realized_pnl_usd=None,
            position_size_after=Decimal("100"),
            position_basis_after=Decimal("60"),
            primitive_version=primitive_version,
        )
    if cls is TransferAccountingEvent:
        return TransferAccountingEvent(
            identity=_identity(event_type_str="TRANSFER"),
            event_type=TransferEventType.TRANSFER,
            asset="USDC",
            amount=Decimal("1000"),
            amount_usd=Decimal("1000"),
            source_chain="arbitrum",
            destination_chain="optimism",
            settlement_status=TransferSettlementStatus.PENDING,
            primitive_version=primitive_version,
        )
    if cls is LPAccountingEvent:
        evt = LPAccountingEvent(
            identity=_identity(event_type_str="LP_OPEN"),
            event_type=LPEventType.LP_OPEN,
            position_key="lp:arbitrum:uniswap_v3:wallet:0xpool",
            pool_address="0xpool",
            token0="USDC",
            token1="WETH",
            amount0=Decimal("1000"),
            amount1=Decimal("0.3"),
            lp_token_amount=Decimal("1"),
            cost_basis_usd=Decimal("2000"),
            realized_pnl_usd=None,
            fees0_collected=None,
            fees1_collected=None,
            confidence=AccountingConfidence.HIGH,
        )
        # LP/Perp/Vault use class-attribute primitive_version; override per-instance.
        evt.primitive_version = primitive_version
        return evt
    if cls is PerpAccountingEvent:
        evt = PerpAccountingEvent(
            identity=_identity(event_type_str="PERP_OPEN"),
            event_type=PerpEventType.PERP_OPEN,
            position_key="perp:arbitrum:gmx_v2:wallet:ARB-USDC",
            market="ARB-USDC",
            collateral_token="USDC",
            size_usd=Decimal("1000"),
            collateral_amount=Decimal("100"),
            is_long=True,
            leverage=Decimal("10"),
            entry_price=Decimal("1.0"),
            realized_pnl_usd=None,
            funding_paid_usd=None,
            confidence=AccountingConfidence.HIGH,
        )
        evt.primitive_version = primitive_version
        return evt
    if cls is VaultAccountingEvent:
        evt = VaultAccountingEvent(
            identity=_identity(event_type_str="VAULT_DEPOSIT"),
            event_type=VaultEventType.VAULT_DEPOSIT,
            position_key="vault:arbitrum:erc4626:wallet:0xvault",
            vault_address="0xvault",
            asset_token="USDC",
            assets_amount=Decimal("1000"),
            shares_amount=Decimal("999"),
            share_price=Decimal("1.001"),
            cost_basis_usd=Decimal("1000"),
            yield_usd=None,
            confidence=AccountingConfidence.HIGH,
        )
        evt.primitive_version = primitive_version
        return evt
    if cls is SettlementAccountingEvent:
        evt = SettlementAccountingEvent(
            identity=_identity(event_type_str="SETTLE_DEPOSIT"),
            event_type=SettlementEventType.SETTLE_DEPOSIT,
            position_key="settlement:lagoon:arbitrum:wallet:0xvault",
            vault_address="0xvault",
            asset_token="USDC",
            assets_delta=Decimal("1000"),
            shares_delta=Decimal("999"),
            new_total_assets=Decimal("1000"),
            fee_shares=None,
            assets_usd=Decimal("1000"),
            epoch_id=1,
            confidence=AccountingConfidence.HIGH,
        )
        evt.primitive_version = primitive_version
        return evt
    raise AssertionError(f"unhandled class {cls!r}")


# ─── D1 Correctness — registry and baseline ────────────────────────────────


def test_primitive_versions_map_covers_every_primitive_member() -> None:
    """F3 — `PRIMITIVE_VERSIONS` exposes a positive int for every Primitive."""
    for member in Primitive:
        assert member in PRIMITIVE_VERSIONS, (
            f"Primitive.{member.name} missing from PRIMITIVE_VERSIONS — writer "
            f"lookup would KeyError on this primitive's events"
        )
        version = PRIMITIVE_VERSIONS[member]
        assert type(version) is int and not isinstance(version, bool)
        assert version >= 1, f"Primitive.{member.name} version must be >= 1, got {version}"


def test_primitive_versions_explicit_per_primitive_pinning() -> None:
    """Pin every per-primitive version explicitly so bumps require a deliberate
    update here.

    The original T6 ship had every primitive at ``PRIMITIVE_VERSION_DEFAULT``
    (``1``).  VIB-4905 / F1 bumps ``Primitive.SWAP`` to ``2`` as the
    SwapEventPayload now carries the partial-match field bundle
    (``realized_pnl_usd_matched`` / ``unmatched_amount_in`` /
    ``unmatched_proceeds_usd``).  Other primitives stay at the default
    until their own contract changes ship.

    Adding a new primitive to the enum must also add an entry here AND in
    ``PRIMITIVE_VERSIONS`` (``test_primitive_versions_map_covers_every_primitive_member``
    enforces the dict coverage side; this test enforces the explicit-pin
    side so a silent default-bump can't sneak through).
    """
    assert PRIMITIVE_VERSION_DEFAULT == 1
    expected: dict[Primitive, int] = {
        Primitive.LP: 1,
        Primitive.LP_V4: 1,
        Primitive.LENDING: 1,
        Primitive.CDP: 1,
        Primitive.LIQUIDATION: 1,
        Primitive.PERP: 1,
        Primitive.UTILITY: 1,
        # VIB-4905 (F1): bumped 1→2 — SwapEventPayload partial-match contract.
        # VIB-4988: bumped 2→3 — Pendle PT now emits PT_SELL / PT_REDEEM
        # (realized fixed-yield attribution) under the SWAP primitive.
        # VIB-4988: bumped 3→4 — PT_BUY/PT_SELL payloads moved raw-18 → human
        # units (uniform with PT_REDEEM) so PEN6 conservation holds on a redeem.
        # VIB-5316: bumped 4→5 — PT_BUY now populates the buy-time ``sy_price`` the
        # held-PT USD cost basis is anchored to (was re-marked at the current
        # underlying, sign-flipping PnL for volatile underlyings).
        # VIB-5314: bumped 5→6 — PT_SELL/PT_REDEEM ``realized_yield_usd`` is now
        # STRICTLY USD-or-None (never SY-units); new ``realized_yield_sy`` carries
        # the SY-denominated value separately.
        Primitive.SWAP: 6,
        Primitive.VAULT: 1,
        # VIB-5666: vault SETTLEMENT primitive (greenfield, v1).
        Primitive.SETTLEMENT: 1,
        Primitive.STAKING: 1,
        Primitive.BRIDGE: 1,
        # #2146: bumped 1→2 — PredictionAccountingEvent payload now carries
        # ``position_loaded_extras_after`` so replay restores the VIB-3710
        # loaded-extras accumulator across a runner restart.
        Primitive.PREDICTION: 2,
        Primitive.FLASH_LOAN: 1,
    }
    for member, want in expected.items():
        got = PRIMITIVE_VERSIONS[member]
        assert got == want, (
            f"Primitive.{member.name} = {got}, expected {want}. "
            f"If you bumped intentionally, update this test alongside "
            f"``PRIMITIVE_VERSIONS`` and document the bump in the dict's comment."
        )
    # Every PRIMITIVE_VERSIONS key must be pinned here too — surface accidental
    # additions to the dict that this baseline test forgot to mirror.
    assert set(expected.keys()) == set(PRIMITIVE_VERSIONS.keys()), (
        "PRIMITIVE_VERSIONS and the explicit-pin map drifted; update both."
    )


# ─── D1 Correctness — augment chokepoint stamps ────────────────────────────


@pytest.mark.parametrize("event_type", sorted(ALL_ACCOUNTING_EVENT_TYPES))
def test_augment_stamps_primitive_version_for_every_known_event_type(
    event_type: str,
) -> None:
    """Every event_type in the gateway whitelist resolves a primitive and
    stamps the right `primitive_version`."""
    payload = json.dumps({"event_type": event_type})
    out = augment_accounting_payload(payload, is_live=True)
    decoded = json.loads(out)
    expected = PRIMITIVE_VERSIONS[record_for(event_type).primitive]
    assert decoded["primitive_version"] == expected
    # Strict int check (rejects bool subclass and float).
    assert type(decoded["primitive_version"]) is int


@pytest.mark.parametrize(
    "event_type,primitive",
    [
        ("LP_OPEN", Primitive.LP),
        ("BORROW", Primitive.LENDING),
        ("PERP_OPEN", Primitive.PERP),
        ("SWAP", Primitive.SWAP),
        ("TRANSFER", Primitive.BRIDGE),
        ("VAULT_DEPOSIT", Primitive.VAULT),
        # PT_BUY is taxonomy-mapped to Primitive.SWAP (taxonomy.py:332-338) —
        # the writer's per-primitive stamping reads ``primitive_for("PT_BUY")``
        # which returns SWAP, not LENDING.  Pre-VIB-4905 this parametrization
        # said LENDING and passed only because both primitives were at version 1;
        # the SWAP bump to v2 surfaced the latent test bug.
        ("PT_BUY", Primitive.SWAP),
        ("PREDICTION_OPEN", Primitive.PREDICTION),
    ],
)
def test_augment_stamps_primitive_version_alongside_matching_policy_version(
    event_type: str, primitive: Primitive
) -> None:
    """F1 backstop — both versions land together, both as ints, on canonical events."""
    payload = json.dumps({"event_type": event_type})
    decoded = json.loads(augment_accounting_payload(payload, is_live=True))
    assert "primitive_version" in decoded
    assert "matching_policy_version" in decoded
    assert type(decoded["primitive_version"]) is int
    assert type(decoded["matching_policy_version"]) is int
    # Sanity — matches the per-primitive map (the test asserts the canonical
    # taxonomy lookup; bump in the map propagates here, not vice versa).
    assert decoded["primitive_version"] == PRIMITIVE_VERSIONS[record_for(event_type).primitive]
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[record_for(event_type).primitive]


def test_augment_stamps_both_versions_at_every_per_primitive_lookup_site() -> None:  # noqa: C901
    # noqa C901: complexity 21 > threshold 15 is intentional — the AST guard
    # has to walk function/block ancestors AND match assignment shapes for both
    # version stamps AND verify same dict-name + same primitive expression.
    # Decomposing into helpers fragments the assertion's locality and makes
    # the "both versions stamped together in the same block" invariant harder
    # to read. The function is the test, not a hot-path consumer.
    """F1 primary AST guard — every assignment of
    ``<dict>["matching_policy_version"]`` in writer.py must have a sibling
    ``<same dict>["primitive_version"]`` assignment in the same enclosing
    function and same direct-parent block, where the values reference the
    parallel typed accessors ``MatchingPolicy.for_primitive(<expr>)`` and
    ``PrimitiveVersion.for_primitive(<same expr>)`` (VIB-4195 + VIB-4166)."""
    src = inspect.getsource(writer_module)
    tree = ast.parse(src)

    def _is_subscript_assign(stmt: ast.stmt, key: str) -> tuple[str, ast.expr] | None:
        """Return (dict_name, value_expr) if `stmt` is `<Name>["key"] = value`,
        else None."""
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1:
            return None
        target = stmt.targets[0]
        if not isinstance(target, ast.Subscript):
            return None
        if not isinstance(target.value, ast.Name):
            return None
        slc = target.slice
        if not (isinstance(slc, ast.Constant) and slc.value == key):
            return None
        return (target.value.id, stmt.value)

    def _is_typed_accessor_call(expr: ast.expr, namespace: str, method: str = "for_primitive") -> ast.expr | None:
        """Return the single-arg expression if `expr` is `<namespace>.<method>(<arg>)`,
        else None. Catches the parallel typed accessors `MatchingPolicy.for_primitive`
        and `PrimitiveVersion.for_primitive`."""
        if not isinstance(expr, ast.Call):
            return None
        if not isinstance(expr.func, ast.Attribute):
            return None
        if expr.func.attr != method:
            return None
        if not isinstance(expr.func.value, ast.Name) or expr.func.value.id != namespace:
            return None
        if len(expr.args) != 1 or expr.keywords:
            return None
        return expr.args[0]

    def _ast_equal(a: ast.expr, b: ast.expr) -> bool:
        return ast.dump(a, annotate_fields=False) == ast.dump(b, annotate_fields=False)

    found_pairs = 0
    for func in ast.walk(tree):
        if not isinstance(func, ast.FunctionDef):
            continue
        # Walk every block container in the function: body, orelse, finalbody, etc.
        for parent_block in _iter_blocks(func):
            mpv_assigns = []
            pv_assigns = []
            for stmt in parent_block:
                mpv = _is_subscript_assign(stmt, "matching_policy_version")
                if mpv is not None:
                    mpv_assigns.append(mpv)
                    continue
                pv = _is_subscript_assign(stmt, "primitive_version")
                if pv is not None:
                    pv_assigns.append(pv)
            for mpv_name, mpv_expr in mpv_assigns:
                # Find a sibling primitive_version assignment in the SAME block.
                idx_mpv = _is_typed_accessor_call(mpv_expr, "MatchingPolicy")
                paired = False
                for pv_name, pv_expr in pv_assigns:
                    if pv_name != mpv_name:
                        continue  # different dict — dict-aliasing bypass
                    idx_pv = _is_typed_accessor_call(pv_expr, "PrimitiveVersion")
                    if idx_pv is None or idx_mpv is None:
                        continue
                    if _ast_equal(idx_mpv, idx_pv):
                        paired = True
                        break
                assert paired, (
                    f"writer.py {func.name}: assignment to "
                    f"{mpv_name}['matching_policy_version'] = MatchingPolicy.for_primitive(<expr>) "
                    f"has no sibling {mpv_name}['primitive_version'] = PrimitiveVersion.for_primitive(<same expr>) "
                    f"in the same direct-parent block. Asymmetric stamp would silently drop "
                    f"primitive_version on this code path."
                )
                found_pairs += 1
    assert found_pairs >= 1, (
        "AST guard found zero matching_policy_version/primitive_version assignment pairs "
        "in writer.py — either the chokepoint moved or the stamping was deleted."
    )


def _iter_blocks(node: ast.AST):
    """Yield every list-of-statements block reachable from `node` (the function
    body itself, plus every nested If/For/While/With/Try body / orelse /
    finalbody / handlers)."""
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        yield node.body
        for stmt in node.body:
            yield from _iter_blocks(stmt)
        return
    if isinstance(node, ast.If | ast.For | ast.AsyncFor | ast.While):
        yield node.body
        if node.orelse:
            yield node.orelse
        for child in node.body + node.orelse:
            yield from _iter_blocks(child)
        return
    if isinstance(node, ast.With | ast.AsyncWith):
        yield node.body
        for child in node.body:
            yield from _iter_blocks(child)
        return
    if isinstance(node, ast.Try):
        yield node.body
        yield node.orelse
        yield node.finalbody
        for handler in node.handlers:
            yield handler.body
        for child in node.body + node.orelse + node.finalbody + sum((h.body for h in node.handlers), []):
            yield from _iter_blocks(child)
        return


# ─── D1 Correctness — F1b production-path proofs ───────────────────────────


@pytest_asyncio.fixture
async def sqlite_store(tmp_path: Path):
    """Real SQLiteStore with full schema, file-backed for SQL inspection."""
    db_path = tmp_path / "vib4166.sqlite"
    s = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await s.initialize()
    yield s, db_path


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_class,primitive",
    [
        (LPAccountingEvent, Primitive.LP),
        (LendingAccountingEvent, Primitive.LENDING),
        # PT_BUY → SWAP per taxonomy.py:332-338 (corrected at VIB-4905 — the
        # prior LENDING parametrization was wrong, passed silently while both
        # primitives shared version 1).
        (PendleAccountingEvent, Primitive.SWAP),
        (PerpAccountingEvent, Primitive.PERP),
        (SwapAccountingEvent, Primitive.SWAP),
        (TransferAccountingEvent, Primitive.BRIDGE),
        (VaultAccountingEvent, Primitive.VAULT),
        (PredictionAccountingEvent, Primitive.PREDICTION),
    ],
)
async def test_real_writer_through_sqlite_stamps_primitive_version(
    sqlite_store, event_class: type, primitive: Primitive
) -> None:
    """F1b — drives the real ``AccountingWriter.write → SQLiteStore.save_accounting_event
    → augment_accounting_payload`` chain and SQL-extracts the stamped value
    from ``accounting_events.payload_json``. The test does NOT call
    ``augment_accounting_payload`` directly. Test invalidation rule (see card
    §5 F1b): any rewrite that bypasses ``writer.write`` (e.g. fixture
    generators or hand-stamped INSERTs) is invalid by definition."""
    store, db_path = sqlite_store
    writer = AccountingWriter(store)
    event = _build_event(event_class)

    ok = await writer.write(event)
    assert ok is True

    # SQL-extract via json_extract + typeof (SQLite-level int check).
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT event_type, "
            "json_extract(payload_json, '$.primitive_version'), "
            "typeof(json_extract(payload_json, '$.primitive_version')) "
            "FROM accounting_events"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1, f"expected exactly one accounting_event row, got {len(rows)}"
    event_type, stamped_pv, sql_type = rows[0]
    assert stamped_pv == PRIMITIVE_VERSIONS[primitive], (
        f"event_type={event_type}: stamped primitive_version={stamped_pv}, "
        f"expected PRIMITIVE_VERSIONS[Primitive.{primitive.name}]={PRIMITIVE_VERSIONS[primitive]}"
    )
    assert sql_type == "integer", (
        f"event_type={event_type}: SQLite sees payload_json.primitive_version as "
        f"{sql_type!r}, expected 'integer'. Stringified-int regression."
    )


def test_gateway_state_manager_save_accounting_event_invokes_augment_chokepoint() -> None:
    """F1b second-backend coverage — proves
    ``GatewayStateManager.save_accounting_event`` actually calls
    ``augment_accounting_payload``. Without this, a refactor that imports
    the helper but stops calling it would silently drop the stamp on the
    gateway/Postgres path. Source-level proof; no gRPC standup."""
    # Method source is class-indented; dedent before ast.parse.
    src = textwrap.dedent(inspect.getsource(GatewayStateManager.save_accounting_event))
    tree = ast.parse(src)
    found = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # Direct name (after `from ... import augment_accounting_payload`):
        if isinstance(func, ast.Name) and func.id == "augment_accounting_payload":
            found = True
            break
        # Attribute call (e.g. `writer.augment_accounting_payload(...)`):
        if isinstance(func, ast.Attribute) and func.attr == "augment_accounting_payload":
            found = True
            break
    assert found, (
        "GatewayStateManager.save_accounting_event MUST invoke "
        "augment_accounting_payload to stamp version fields onto every payload. "
        "Without it, the gateway/Postgres write path silently drops the per-primitive "
        "version stamps (matching_policy_version AND primitive_version)."
    )


# ─── D1 Correctness — mode-aware contract for primitive_version ────────────


def test_augment_paper_unknown_event_type_falls_back_to_utility_for_primitive_version(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F6 — paper + unknown event_type → both versions fall back to UTILITY,
    AND the writer emits an ERROR log (per VIB-3863's mode-aware contract).
    Asserts BOTH halves so a future regression that silently dropped the
    log line would still trip the test."""
    import logging

    payload = json.dumps({"event_type": "FROBNICATE"})
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.writer"):
        decoded = json.loads(augment_accounting_payload(payload, is_live=False))
    assert decoded["primitive_version"] == PRIMITIVE_VERSIONS[Primitive.UTILITY]
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.UTILITY]
    assert any(
        "FROBNICATE" in record.message and record.levelname == "ERROR"
        for record in caplog.records
    ), f"Expected ERROR log mentioning 'FROBNICATE'; got records: {[r.message for r in caplog.records]}"


def test_augment_live_unknown_event_type_raises() -> None:
    """Live mode raise contract preserved (no silent swallow regression)."""
    payload = json.dumps({"event_type": "FROBNICATE"})
    with pytest.raises(AccountingPersistenceError):
        augment_accounting_payload(payload, is_live=True)


def test_augment_live_missing_event_type_raises() -> None:
    """Missing event_type in live mode raises (preserves VIB-3863)."""
    payload = json.dumps({"asset": "USDC"})  # no event_type key
    with pytest.raises(AccountingPersistenceError):
        augment_accounting_payload(payload, is_live=True)


def test_augment_paper_missing_event_type_falls_back_to_utility_for_primitive_version() -> None:
    """F6 — paper + missing event_type → both versions fall back to UTILITY."""
    payload = json.dumps({})  # empty
    decoded = json.loads(augment_accounting_payload(payload, is_live=False))
    assert decoded["primitive_version"] == PRIMITIVE_VERSIONS[Primitive.UTILITY]
    assert decoded["matching_policy_version"] == MATCHING_POLICY_VERSIONS[Primitive.UTILITY]


# ─── D1 Per-class field + serde ────────────────────────────────────────────


@pytest.mark.parametrize("event_class", _ALL_EVENT_CLASSES)
def test_every_typed_event_class_declares_primitive_version_field(
    event_class: type,
) -> None:
    """F2 — every member of the AccountingEvent union has ``primitive_version``
    with default 1, accessible on a constructed instance."""
    # The class must declare primitive_version as a class- or instance-attribute
    # default of 1. Build a default instance and check.
    event = _build_event(event_class, primitive_version=1)
    assert hasattr(event, "primitive_version")
    pv = event.primitive_version
    assert type(pv) is int and not isinstance(pv, bool)
    assert pv == 1


def test_accounting_event_union_classes_all_appear_in_typed_classes() -> None:
    """Sanity — keep ``_ALL_EVENT_CLASSES`` aligned with the writer's
    ``AccountingEvent`` union. If the union grows but this list doesn't,
    F2 / F4 / F5 silently miss the new class."""
    union_args = AccountingEvent.__args__  # type: ignore[attr-defined]
    union_set = set(union_args)
    test_set = set(_ALL_EVENT_CLASSES)
    assert union_set == test_set, (
        f"AccountingEvent union has classes {union_set - test_set} not covered by "
        f"_ALL_EVENT_CLASSES, OR _ALL_EVENT_CLASSES has stale entries {test_set - union_set}. "
        f"Update tests/unit/accounting/test_event_versioning.py::_ALL_EVENT_CLASSES."
    )


@pytest.mark.parametrize("event_class", _ALL_EVENT_CLASSES)
def test_to_payload_json_includes_primitive_version_for_every_event_class(
    event_class: type,
) -> None:
    """F4 — every class's ``to_payload_json`` emits ``primitive_version`` as a
    strict ``int`` (NOT ``bool`` subclass, NOT ``float``)."""
    event = _build_event(event_class, primitive_version=1)
    decoded = json.loads(event.to_payload_json())
    assert "primitive_version" in decoded, (
        f"{event_class.__name__}.to_payload_json() omits primitive_version — "
        f"the augment chokepoint would still stamp it, but readers that consume "
        f"the unaugmented JSON (tests, debug logs) lose visibility."
    )
    assert decoded["primitive_version"] == 1
    # `type(...) is int` — explicitly forbids `bool` (subclass of int) and `float`.
    assert type(decoded["primitive_version"]) is int


@pytest.mark.parametrize("event_class", _TYPED_CLASSES_WITH_SERDE)
def test_from_payload_json_preserves_primitive_version_for_every_event_class(
    event_class: type,
) -> None:
    """F5 — round-trip per class with from_payload_json. Use a non-default
    value (7) to catch silent-default regressions."""
    event = _build_event(event_class, primitive_version=7)
    payload = event.to_payload_json()
    rehydrated = event_class.from_payload_json(event.identity, payload)
    assert rehydrated.primitive_version == 7
    assert type(rehydrated.primitive_version) is int


@pytest.mark.parametrize("event_class", _ALL_EVENT_CLASSES)
def test_to_payload_json_serialises_primitive_version_as_json_integer(
    event_class: type,
) -> None:
    """F4 type-strictness — JSON-level: dump+load round-trip preserves int.
    A future regression that stringified the int via `_enc` (e.g. `str(self.primitive_version)`)
    or replaced the field with `Decimal(...)` would land here."""
    event = _build_event(event_class, primitive_version=1)
    raw = event.to_payload_json()
    decoded = json.loads(raw)
    pv = decoded["primitive_version"]
    assert type(pv) is int
    # Re-encode + decode ensures JSON canonicalisation doesn't promote to float.
    assert type(json.loads(json.dumps(pv))) is int


# ─── D1 Read-rail acceptance ───────────────────────────────────────────────


@pytest.mark.parametrize("event_type,model_cls", sorted(_PAYLOAD_MODELS.items()))
def test_versioned_pydantic_base_accepts_primitive_version(
    event_type: str, model_cls: type
) -> None:
    """The pydantic _Versioned read rail must accept and preserve primitive_version."""
    # Build a minimal payload that satisfies each model's required fields.
    base = _minimal_payload_for_model(event_type)
    base["primitive_version"] = 2
    instance = model_cls.model_validate(base)
    assert instance.primitive_version == 2


@pytest.mark.parametrize("event_type,model_cls", sorted(_PAYLOAD_MODELS.items()))
def test_versioned_pydantic_base_defaults_primitive_version_to_one_when_absent(
    event_type: str, model_cls: type
) -> None:
    """Backwards-compat: pre-T6 payloads on disk lack the field; the read
    rail must default to 1 so old rows don't crash readers."""
    base = _minimal_payload_for_model(event_type)
    base.pop("primitive_version", None)
    instance = model_cls.model_validate(base)
    assert instance.primitive_version == 1


def _minimal_payload_for_model(event_type: str) -> dict:
    """Build the smallest dict that satisfies a payload-schema model's
    required fields. Mirrors `_PAYLOAD_MODELS` shape."""
    common = {
        "event_type": event_type,
        "confidence": "HIGH",
    }
    if event_type == "SUPPLY":
        return {**common, "protocol": "aave_v3", "asset": "USDC", "amount": "1000"}
    if event_type == "WITHDRAW":
        return {**common, "protocol": "aave_v3", "asset": "USDC", "amount": "1000"}
    if event_type == "BORROW":
        return {**common, "protocol": "aave_v3", "asset": "USDC", "borrowed_amount": "1000"}
    if event_type in ("REPAY", "DELEVERAGE"):
        return {**common, "protocol": "aave_v3", "asset": "USDC", "amount": "1000"}
    if event_type == "LP_OPEN":
        return {
            **common,
            "protocol": "uniswap_v3",
            "position_key": "lp:arbitrum:uniswap_v3:wallet:0xpool",
            "pool_address": "0xpool",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "1000",
            "amount1": "0.3",
        }
    if event_type == "LP_CLOSE":
        return {
            **common,
            "protocol": "uniswap_v3",
            "position_key": "lp:arbitrum:uniswap_v3:wallet:0xpool",
            "pool_address": "0xpool",
            "token0": "USDC",
            "token1": "WETH",
            "amount0": "1000",
            "amount1": "0.3",
        }
    if event_type == "PERP_OPEN":
        return {
            **common,
            "protocol": "gmx_v2",
            "position_key": "perp:arbitrum:gmx_v2:wallet:ARB-USDC",
            "market": "ARB-USDC",
            "is_long": True,
            "size": "1000",
        }
    if event_type == "PERP_CLOSE":
        return {
            **common,
            "protocol": "gmx_v2",
            "position_key": "perp:arbitrum:gmx_v2:wallet:ARB-USDC",
            "market": "ARB-USDC",
            "is_long": True,
            "size": "1000",
        }
    if event_type == "SWAP":
        return {
            **common,
            "protocol": "enso",
            "token_in": "USDC",
            "token_out": "WETH",
            "amount_in": "1000",
            "amount_out": "0.3",
        }
    raise AssertionError(f"unhandled event_type {event_type!r}")


# ─── D2 Variance / scalability ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "bumped_primitive,probe_event_type",
    [
        (Primitive.LP, "LP_OPEN"),
        (Primitive.LENDING, "BORROW"),
        (Primitive.PERP, "PERP_OPEN"),
        (Primitive.SWAP, "SWAP"),
        (Primitive.BRIDGE, "TRANSFER"),
        (Primitive.VAULT, "VAULT_DEPOSIT"),
        (Primitive.PREDICTION, "PREDICTION_OPEN"),
    ],
)
def test_per_primitive_isolation_under_bump(
    monkeypatch: pytest.MonkeyPatch,
    bumped_primitive: Primitive,
    probe_event_type: str,
) -> None:
    """F7 — bumping one primitive's version stamps that bump on its events
    AND leaves every other primitive's events at their declared version. No
    cross-primitive contamination (mirror of test_lp_bump_isolation)."""
    monkeypatch.setitem(PRIMITIVE_VERSIONS, bumped_primitive, 99)

    sibling_event_types = {
        Primitive.LP: "LP_OPEN",
        Primitive.LENDING: "BORROW",
        Primitive.PERP: "PERP_OPEN",
        Primitive.SWAP: "SWAP",
        Primitive.BRIDGE: "TRANSFER",
        Primitive.VAULT: "VAULT_DEPOSIT",
        Primitive.PREDICTION: "PREDICTION_OPEN",
    }
    # Bumped event gets v99.
    bumped_decoded = json.loads(
        augment_accounting_payload(json.dumps({"event_type": probe_event_type}), is_live=True)
    )
    assert bumped_decoded["primitive_version"] == 99

    # Every sibling primitive still gets its declared (non-bumped) version.
    for sibling_primitive, sibling_et in sibling_event_types.items():
        if sibling_primitive == bumped_primitive:
            continue
        decoded = json.loads(
            augment_accounting_payload(json.dumps({"event_type": sibling_et}), is_live=True)
        )
        assert decoded["primitive_version"] == PRIMITIVE_VERSIONS[sibling_primitive], (
            f"Bumping {bumped_primitive.name} to v99 contaminated {sibling_primitive.name}: "
            f"got {decoded['primitive_version']}, expected {PRIMITIVE_VERSIONS[sibling_primitive]}"
        )


@pytest.mark.parametrize("event_type", sorted(ALL_ACCOUNTING_EVENT_TYPES))
def test_every_event_type_in_whitelist_resolves_a_primitive_version(
    event_type: str,
) -> None:
    """Sanity over the whitelist surface — augment + decode for every
    canonical event_type produces a positive int primitive_version. Catches
    "added a new event_type to the enum but forgot to add a TAXONOMY row"
    at the writer level."""
    decoded = json.loads(
        augment_accounting_payload(json.dumps({"event_type": event_type}), is_live=True)
    )
    pv = decoded["primitive_version"]
    assert type(pv) is int and pv >= 1, (
        f"event_type={event_type!r} produced primitive_version={pv!r} "
        f"(type={type(pv).__name__}); augment chokepoint must stamp a positive int"
    )


# ─── Accessor discipline (parallel to VIB-4195's MatchingPolicy guards) ───
# These two tests mirror `test_no_other_production_code_reads_raw_dict` and
# `test_writer_imports_accessor_and_does_not_index_raw_dict` from
# `test_matching_policy.py`, applied to the parallel `PRIMITIVE_VERSIONS`
# map / `PrimitiveVersion.for_primitive` accessor that VIB-4166 introduced.


def test_no_production_code_reads_raw_primitive_versions_dict() -> None:
    """No production-code module under ``almanak/`` may reference
    ``PRIMITIVE_VERSIONS`` outside the source (``payload_schemas.py``) and
    the wrapper (``policy.py``). Catches future code that would bypass the
    typed accessor and read the raw dict — the same defense-in-depth
    VIB-4195 set up for ``MATCHING_POLICY_VERSIONS``.
    """
    repo_root = Path(__file__).resolve().parents[3]
    almanak_root = repo_root / "almanak"
    allowed = {
        almanak_root / "framework" / "accounting" / "payload_schemas.py",
        almanak_root / "framework" / "accounting" / "policy.py",
    }

    violations: list[tuple[str, int, str]] = []
    for path in almanak_root.rglob("*.py"):
        if path in allowed:
            continue
        src = path.read_text()
        if "PRIMITIVE_VERSIONS" not in src:
            continue
        rel = path.relative_to(repo_root).as_posix()
        tree = ast.parse(src, filename=rel)
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "PRIMITIVE_VERSIONS":
                violations.append((rel, node.lineno, "bare-name"))
            if isinstance(node, ast.Attribute) and node.attr == "PRIMITIVE_VERSIONS":
                violations.append((rel, node.lineno, "attr"))
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "PRIMITIVE_VERSIONS":
                        violations.append((rel, node.lineno, "import"))
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
            ):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "PRIMITIVE_VERSIONS":
                        violations.append((rel, node.lineno, "getattr-string"))

    assert not violations, (
        "Production code outside payload_schemas.py + policy.py references "
        "PRIMITIVE_VERSIONS — every read should go through "
        "PrimitiveVersion.for_primitive(). Violations: " + repr(violations)
    )


def test_writer_imports_primitive_version_accessor_and_does_not_index_raw_dict() -> None:
    """``almanak/framework/accounting/writer.py`` must (a) not reference
    ``PRIMITIVE_VERSIONS`` in any AST-detectable form, (b) import
    ``PrimitiveVersion``, and (c) actually call
    ``PrimitiveVersion.for_primitive(...)``.
    """
    repo_root = Path(__file__).resolve().parents[3]
    src = (repo_root / "almanak" / "framework" / "accounting" / "writer.py").read_text()
    tree = ast.parse(src)

    bypasses: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "PRIMITIVE_VERSIONS":
            bypasses.append((node.lineno, "bare-name"))
        if isinstance(node, ast.Attribute) and node.attr == "PRIMITIVE_VERSIONS":
            bypasses.append((node.lineno, "attr"))
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "PRIMITIVE_VERSIONS":
                    bypasses.append((node.lineno, "import"))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
        ):
            for arg in node.args:
                if isinstance(arg, ast.Constant) and arg.value == "PRIMITIVE_VERSIONS":
                    bypasses.append((node.lineno, "getattr-string"))
    assert not bypasses, f"writer.py still references PRIMITIVE_VERSIONS at {bypasses}"

    imports_accessor = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "almanak.framework.accounting.policy"
        and any(alias.name == "PrimitiveVersion" for alias in node.names)
        for node in ast.walk(tree)
    )
    assert imports_accessor, "writer.py must import PrimitiveVersion from accounting.policy"

    calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "for_primitive"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "PrimitiveVersion"
    ]
    assert calls, "writer.py imports PrimitiveVersion but never calls .for_primitive(...)"
