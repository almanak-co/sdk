"""PositionService implementation — on-chain reconciliation of position_registry.

Implements ``PositionService.Reconcile`` (T24 / VIB-4210), the control-plane RPC
that closes user-facing bug GH #2131. Distinct service from StateService per
ADR §2.1 (different cost profile, different auth posture, different rate-limit
class).

Full design contract: ``docs/internal/adr/VIB-4221-position-service-reconcile.md``
Companion proto draft: ``docs/internal/proto-drafts/VIB-4221-position-service.proto``

Reconciliation algorithm (ADR §4):

1. Validate request (deployment_id, chain, wallet_address, primitives ⊆ {"lp"}).
2. Mint reconciliation_id (UUID4); sample source_block_number from chain head.
3. Enumerate on-chain LP positions via the gateway's chain RPCs (NPM enumeration —
   matching ``almanak/framework/teardown/discovery.py`` but in-process).
4. Read ``position_registry`` rows scoped to the deployment.
5. Compute four-bucket diff: matched / phantom_missing / stranded / rebuilt.
6. If ``apply=true``, insert phantom_missing rows via
   ``save_ledger_and_registry(mode='registry_reconciliation')`` — the ledger
   write is SKIPPED on this path (ADR §2.3 #1+#2, ADR §8.1 Option (c)).
7. Build response.

What Reconcile MUST NOT do (ADR §2.3, copied here for emphasis):
- MUST NOT read ``transaction_ledger``.
- MUST NOT write ``transaction_ledger`` (mode=registry_reconciliation enforces).
- MUST NOT mutate ``accounting_events`` / ``position_events``.
- MUST NOT flip an already-terminal registry row (priority guard enforces).
- MUST NOT be invoked from strategy code — control-plane only.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    resolve_agent_id,
    validate_chain,
    validate_strategy_id,
)

if TYPE_CHECKING:
    from almanak.gateway.services.rpc_service import RpcServiceServicer
    from almanak.gateway.services.state_service import StateServiceServicer

logger = logging.getLogger(__name__)

# v1 supported primitives — UniV3 LP only. Per ADR §2.4, Aave / Pendle / GMX
# reconciliation lands in T24-followups, one ticket per primitive, gated on
# each primitive's mode='registry' cutover (T28 / T23 / T16 respectively).
_V1_SUPPORTED_PRIMITIVES = frozenset({"lp"})

# Pagination cap matches the teardown discovery cap (_MAX_POSITIONS_PER_NPM in
# almanak/framework/teardown/discovery.py). Mirroring the cap keeps a single
# operational limit for "how many positions can the gateway enumerate per call".
MAX_RECONCILIATION_PAGE_SIZE = 256
DEFAULT_RECONCILIATION_PAGE_SIZE = 64

# Cursor schema version. Bumped iff the cursor payload layout changes
# (gateway rejects mismatching schema_version with FAILED_PRECONDITION so a
# rolling deploy doesn't replay stale cursors against a newer gateway).
_CURSOR_SCHEMA_VERSION = 1

# Allowed values for ReconcileRequest.trigger (proto-documented enumeration —
# see gateway.proto §ReconcileRequest.trigger). Empty string is also accepted
# but the gateway emits a WARN when it sees one (the proto explicitly says
# "do not rely on this"). Unknown non-empty values are rejected with
# INVALID_ARGUMENT.
_ALLOWED_TRIGGERS = frozenset(
    {
        "operator_cli",  # `ax positions reconcile` (default for human-run)
        "hosted_boot",  # runner startup auto-trigger (T24+1 follow-up)
        "dashboard",  # operator dashboard
        "ci",  # test/CI invocation
    }
)


class _PrimitiveErrorCollector:
    """Helper that aggregates per-primitive PrimitiveError protos.

    Reconcile NEVER fails the whole RPC on a single-primitive failure
    (ADR §5.1). Failures are surfaced via ``ReconcileResponse.primitive_errors``
    so the operator can see partial diffs from the primitives that succeeded.
    """

    def __init__(self) -> None:
        self._errors: list[gateway_pb2.PrimitiveError] = []

    def add(self, *, primitive: str, chain: str, code: str, message: str, recoverable: bool) -> None:
        self._errors.append(
            gateway_pb2.PrimitiveError(
                primitive=primitive,
                chain=chain,
                code=code,
                message=message,
                recoverable=recoverable,
            )
        )

    def list(self) -> list[gateway_pb2.PrimitiveError]:
        return list(self._errors)


# =============================================================================
# Cursor encoding (ADR §4.2 — opaque base64-JSON, schema-versioned)
# =============================================================================


def encode_cursor(*, source_block_number: int, last_primitive: str, last_hash: str) -> bytes:
    """Encode an opaque pagination cursor (ADR §4.2)."""
    payload = {
        "source_block_number": int(source_block_number),
        "last_primitive": last_primitive,
        "last_physical_identity_hash": last_hash,
        "schema_version": _CURSOR_SCHEMA_VERSION,
    }
    return base64.b64encode(json.dumps(payload, sort_keys=True).encode("utf-8"))


def decode_cursor(raw: bytes) -> dict[str, Any] | None:
    """Decode a pagination cursor; return None on any parse failure.

    The opaque-cursor contract (ADR §4.2) is: clients treat the bytes as opaque
    and pass them back verbatim. A None return from this function means the
    cursor is malformed; the caller treats it as ``FAILED_PRECONDITION``
    rather than silently restart at page 0 (a silent-restart would mask
    pagination bugs).
    """
    if not raw:
        return None
    try:
        decoded = json.loads(base64.b64decode(raw).decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(decoded, dict):
        return None
    if decoded.get("schema_version") != _CURSOR_SCHEMA_VERSION:
        return None
    # Structural validation: every cursor field encode_cursor produces must
    # be present with the right type. A schema-version-1 cursor missing
    # source_block_number (or carrying a non-int) would otherwise raise
    # KeyError/ValueError downstream in _resolve_source_block_number; we
    # treat it as "malformed cursor" → caller surfaces FAILED_PRECONDITION
    # (the same code path as schema-version mismatch). CodeRabbit MAJOR
    # PR #2240.
    if not isinstance(decoded.get("source_block_number"), int):
        return None
    if not isinstance(decoded.get("last_primitive"), str):
        return None
    if not isinstance(decoded.get("last_physical_identity_hash"), str):
        return None
    return decoded


# =============================================================================
# Diff classifier (pure function — easy to unit-test in isolation)
# =============================================================================


def classify_diff(
    *,
    on_chain: list[dict[str, Any]],
    registry: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Four-bucket diff between on-chain truth and registry rows.

    Returns ``(matched, phantom_missing, stranded)`` — the ``rebuilt`` bucket
    is populated by the writer path when ``apply=true`` and is NOT computed
    here (a pure-function diff has no side effects).

    Input shape (both lists):
        Each dict has at minimum:
        - ``physical_identity_hash``: str (the join key)
        - ``primitive``: str
        - ``accounting_category``: str
        Registry rows additionally carry the full row dict from
        ``GetPositionRegistryOpenRows``.

    Semantics (ADR §4 step 5):
        matched          = on_chain_hashes ∩ registry_hashes
        phantom_missing  = on_chain_hashes \\ registry_hashes  (the GH #2131 case)
        stranded         = registry_hashes \\ on_chain_hashes
    """
    on_chain_by_hash = {p["physical_identity_hash"]: p for p in on_chain}
    registry_by_hash = {r["physical_identity_hash"]: r for r in registry}

    matched_hashes = set(on_chain_by_hash) & set(registry_by_hash)
    phantom_hashes = set(on_chain_by_hash) - set(registry_by_hash)
    stranded_hashes = set(registry_by_hash) - set(on_chain_by_hash)

    matched = [on_chain_by_hash[h] for h in sorted(matched_hashes)]
    phantom_missing = [on_chain_by_hash[h] for h in sorted(phantom_hashes)]
    stranded = [registry_by_hash[h] for h in sorted(stranded_hashes)]
    return matched, phantom_missing, stranded


# =============================================================================
# In-process chain enumeration (mirrors discovery.py but uses RpcServiceServicer
# in-process — avoids a TCP loopback hop from the gateway to itself).
# =============================================================================


async def _eth_call_in_process(
    rpc_servicer: RpcServiceServicer,
    *,
    chain: str,
    to: str,
    data: str,
    block_number: int | str = "latest",
    network: str = "",
    timeout: float = 15.0,
) -> str | None:
    """Issue eth_call via the in-process RpcServiceServicer.

    Mirrors :func:`almanak.framework.teardown.discovery._eth_call` semantics —
    returns the hex result string on success or ``None`` on any failure (so
    callers can iterate over multiple NPMs without one failure masking
    others). Errors are logged at DEBUG so the gateway audit log isn't
    flooded; the partial-failure surface lives in ``primitive_errors``.

    Why in-process: PositionService runs inside the gateway server. The
    gateway IS the egress layer (CLAUDE.md gateway-boundary rule §"Code
    under almanak/gateway/ IS the egress layer"). A TCP loopback hop to
    self would add latency, double-count auth, and complicate the
    rate-limit picture — calling the in-process servicer is the correct
    architectural choice.

    Reconciliation invariant (Gemini high, PR #2240): callers MUST pin
    ``block_number`` to the ``source_block_number`` sampled at the start of
    the RPC so every eth_call in the same Reconcile invocation reads from
    the same chain state. The ``"latest"`` default is reserved for sampling
    the head itself (where the value of "now" is the answer); production
    enumeration paths pass the pinned int.
    """
    block_tag = hex(block_number) if isinstance(block_number, int) else block_number
    request = gateway_pb2.RpcRequest(
        chain=chain,
        method="eth_call",
        params=json.dumps([{"to": to, "data": data}, block_tag]),
        id="position_reconcile",
        network=network,
    )
    try:
        response = await asyncio.wait_for(
            rpc_servicer.Call(request, _NoopContext()),  # in-process; no real grpc context
            timeout=timeout,
        )
    except TimeoutError:
        logger.debug("position_reconcile eth_call timeout for %s on %s", to, chain)
        return None
    except Exception as e:  # noqa: BLE001 — discovery is intentionally fail-open per call
        logger.debug("position_reconcile eth_call failed for %s on %s: %s", to, chain, e)
        return None
    if not response.success:
        logger.debug("position_reconcile eth_call returned error for %s on %s: %s", to, chain, response.error)
        return None
    try:
        return json.loads(response.result)
    except (ValueError, json.JSONDecodeError):
        logger.debug("position_reconcile eth_call returned unparsable result for %s on %s", to, chain)
        return None


class _NoopContext:
    """In-process gRPC ServicerContext stand-in.

    The async ``RpcServiceServicer.Call`` accepts a context argument it uses
    only on the failure path (``set_code`` / ``set_details``). For in-process
    invocation we don't have a real context and don't need to bridge any
    error code to the caller's wire — the typed response already carries
    ``success=False`` + ``error=...``. This stand-in absorbs the set_code /
    set_details calls so the servicer's failure path doesn't NPE.
    """

    def set_code(self, code: Any) -> None:  # pragma: no cover — diagnostic-only
        pass

    def set_details(self, details: str) -> None:  # pragma: no cover — diagnostic-only
        pass


async def _get_chain_head(
    rpc_servicer: RpcServiceServicer,
    *,
    chain: str,
    network: str = "",
) -> int | None:
    """Return the current chain head block number (best-effort).

    Used both for sampling ``source_block_number`` on the first page and for
    enforcing ``max_age_blocks`` freshness checks. Returns ``None`` on any
    RPC failure — caller decides whether to fail the request with
    INTERNAL or proceed with a 0 sentinel.
    """
    request = gateway_pb2.RpcRequest(
        chain=chain,
        method="eth_blockNumber",
        params="[]",
        id="position_reconcile_head",
        network=network,
    )
    try:
        response = await asyncio.wait_for(
            rpc_servicer.Call(request, _NoopContext()),
            timeout=5.0,
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("position_reconcile eth_blockNumber failed on %s: %s", chain, e)
        return None
    if not response.success:
        return None
    try:
        raw = json.loads(response.result)
        return int(raw, 16) if isinstance(raw, str) else int(raw)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


# =============================================================================
# PositionServiceServicer
# =============================================================================


class PositionServiceServicer(gateway_pb2_grpc.PositionServiceServicer):
    """Implements PositionService gRPC interface (T24 / VIB-4210)."""

    def __init__(self, settings: GatewaySettings):
        """Initialize PositionService.

        Cross-servicer references are wired by ``GatewayServer._register_services``
        after the servicer is constructed (matching the existing pattern used
        for ExecutionService → market_servicer).
        """
        self.settings = settings
        self.rpc_servicer: RpcServiceServicer | None = None
        self.state_servicer: StateServiceServicer | None = None
        self.wallet_registry: Any = None

    async def Reconcile(
        self,
        request: gateway_pb2.ReconcileRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ReconcileResponse:
        """Reconcile a deployment's position_registry against on-chain truth.

        See module docstring for the algorithm. Failure modes are documented
        on :class:`PositionServiceServicer._algorithm_step_N` helpers below.
        """
        started_at = time.perf_counter()

        # Step 1: validate request.
        validated = self._validate_request(request, context)
        if validated is None:
            return self._empty_response_with_no_writes()
        deployment_id, chain, wallet_address, primitives, page_size = validated

        # Step 2: mint reconciliation_id, sample source_block_number.
        reconciliation_id = str(uuid.uuid4())
        source_block_number = await self._resolve_source_block_number(request, chain, context)
        if source_block_number is None:
            # set_code already called inside _resolve_source_block_number.
            return self._empty_response_with_no_writes(reconciliation_id=reconciliation_id)

        errors = _PrimitiveErrorCollector()

        # Step 3: enumerate on-chain positions per primitive.
        on_chain_positions: list[dict[str, Any]] = []
        oversize = False
        oversize_detail = ""
        # Codex P2 (PR #2240): the hash filter must constrain BOTH
        # registry rows AND on-chain enumeration. Pre-compute the
        # frozenset once so we don't rebuild it per-primitive.
        hash_filter = frozenset(request.physical_identity_hashes) if request.physical_identity_hashes else None
        for primitive in primitives:
            if primitive != "lp":
                # v1 hard-rejects non-lp; defensive check matches
                # _validate_request which already rejects with INVALID_ARGUMENT.
                errors.add(
                    primitive=primitive,
                    chain=chain,
                    code="PARSER_UNSUPPORTED",
                    message=(
                        f"primitive={primitive!r} not supported in v1; "
                        "Aave / Pendle / GMX reconciliation lands in T24-followups"
                    ),
                    recoverable=False,
                )
                continue
            lp_positions, lp_oversize, lp_oversize_detail = await self._enumerate_lp_positions(
                chain=chain,
                wallet_address=wallet_address,
                source_block_number=source_block_number,
                physical_identity_hashes_filter=hash_filter,
                errors=errors,
            )
            on_chain_positions.extend(lp_positions)
            if lp_oversize:
                oversize = True
                oversize_detail = lp_oversize_detail

        # Step 4: read registry rows.
        registry_rows = await self._read_registry_rows(
            deployment_id=deployment_id,
            chain=chain,
            primitives=primitives,
            physical_identity_hashes=list(request.physical_identity_hashes),
            errors=errors,
        )

        # Step 5: compute four-bucket diff.
        matched_dicts, phantom_dicts, stranded_dicts = classify_diff(
            on_chain=on_chain_positions, registry=registry_rows
        )

        # Step 6: when apply=true, write phantom-missing rows via the
        # registry_reconciliation mode (ledger NOT touched).
        rebuilt_dicts: list[dict[str, Any]] = []
        if request.apply:
            rebuilt_dicts = await self._apply_phantom_missing(
                deployment_id=deployment_id,
                chain=chain,
                source_block_number=source_block_number,
                reconciliation_id=reconciliation_id,
                phantoms=phantom_dicts,
                errors=errors,
            )

        # Step 7: build response.
        #
        # Pagination contract (CodeRabbit MAJOR, PR #2240): v1 ships as
        # SINGLE-PAGE — the entire diff for the requested
        # (deployment, chain, primitives) tuple is returned in one
        # response, capped server-side by
        # ``_MAX_POSITIONS_PER_NPM`` (256 per NPM). The proto's
        # ``next_page_cursor`` / ``page_size`` fields are reserved for
        # forward-compat: if a future deployment needs > 256 LPs on a
        # single NPM, ``oversize=true`` flags the truncation today and
        # multi-page paging lands as T24+1. The cursor field is therefore
        # always emitted empty in v1. Reconciliation IS the operator
        # safety net (GH #2131); a wallet with > 256 positions on one NPM
        # is far enough from the typical case that single-page is the
        # right v1 ship.
        duration = time.perf_counter() - started_at
        return self._build_response(
            reconciliation_id=reconciliation_id,
            source_block_number=source_block_number,
            matched=matched_dicts,
            phantom_missing=phantom_dicts,
            stranded=stranded_dicts,
            rebuilt=rebuilt_dicts,
            oversize=oversize,
            oversize_detail=oversize_detail,
            duration_seconds=duration,
            primitive_errors=errors.list(),
            next_page_cursor=b"",  # v1: single-page only (see comment above).
            page_size=page_size,
        )

    # =========================================================================
    # Step 1: Request validation
    # =========================================================================

    def _validate_request(
        self,
        request: gateway_pb2.ReconcileRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, str, list[str], int] | None:
        """Validate ReconcileRequest. Returns parsed fields or sets context error."""
        deployment_id = (request.deployment_id or "").strip()
        if not deployment_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("deployment_id is required")
            return None
        try:
            validate_strategy_id(deployment_id)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"deployment_id invalid: {e}")
            return None
        deployment_id = resolve_agent_id(deployment_id)

        chain = (request.chain or "").strip()
        if not chain:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("chain is required")
            return None
        # Authoritative chain validation against the gateway's allowlist
        # (CodeRabbit MAJOR, PR #2240) — reject unknown chains with
        # INVALID_ARGUMENT instead of forwarding garbage to the RPC layer.
        try:
            chain = validate_chain(chain)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"chain invalid: {e}")
            return None

        wallet_address = (request.wallet_address or "").strip()
        if not wallet_address:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("wallet_address is required")
            return None
        if not self._wallet_matches_registry(chain, wallet_address, context):
            return None

        # Primitive filter — empty repeated field defaults to v1's supported set.
        requested_primitives = [p.strip() for p in request.primitives if p and p.strip()]
        if not requested_primitives:
            primitives = sorted(_V1_SUPPORTED_PRIMITIVES)
        else:
            unknown = [p for p in requested_primitives if p not in _V1_SUPPORTED_PRIMITIVES]
            if unknown:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(
                    f"unsupported primitives in v1: {unknown!r}; supported: {sorted(_V1_SUPPORTED_PRIMITIVES)!r}"
                )
                return None
            primitives = requested_primitives

        # operator_note size cap (proto comment says 256 bytes).
        if len(request.operator_note.encode("utf-8")) > 256:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("operator_note exceeds 256-byte cap")
            return None

        # Trigger field enumeration check (CodeRabbit minor, PR #2240). The
        # proto comment §ReconcileRequest.trigger enumerates valid values;
        # gateway emits WARN on empty and rejects unknown non-empty values
        # with INVALID_ARGUMENT.
        trigger = (request.trigger or "").strip()
        if not trigger:
            logger.warning(
                "PositionService.Reconcile called with empty trigger; "
                "telemetry will label as 'unspecified'. Set request.trigger "
                "to one of: %s",
                sorted(_ALLOWED_TRIGGERS),
            )
        elif trigger not in _ALLOWED_TRIGGERS:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid trigger={trigger!r}; expected one of {sorted(_ALLOWED_TRIGGERS)!r} or empty")
            return None

        # page_size: 0 → default; >cap → clamp silently per proto comment.
        raw_page_size = int(request.page_size or 0)
        if raw_page_size <= 0:
            page_size = DEFAULT_RECONCILIATION_PAGE_SIZE
        else:
            page_size = min(raw_page_size, MAX_RECONCILIATION_PAGE_SIZE)

        return deployment_id, chain, wallet_address, primitives, page_size

    # crap-allowlist: VIB-4210 — fail-closed security boundary structural
    # cc=8 (5 error classes → 5 distinct gRPC status codes, one success
    # case, plus the no-registry local-mode skip). CodeRabbit MAJOR round 2
    # required per-class typed status mapping (see docstring). Decomposing
    # into helpers loses the per-class status mapping or invents an internal
    # error-enum, neither of which fits the gRPC "set_code-then-return-False"
    # pattern used elsewhere on this servicer. Coverage rises once
    # hosted-boot wires a real wallet_registry (T24+1).
    def _wallet_matches_registry(
        self,
        chain: str,
        wallet_address: str,
        context: grpc.aio.ServicerContext,
    ) -> bool:
        """Verify caller-supplied ``wallet_address`` matches the registry.

        Gemini security-high + CodeRabbit MAJOR (PR #2240, ADR §6): when a
        wallet registry is configured (hosted multi-tenant posture), the
        supplied ``wallet_address`` MUST match the registry-resolved wallet
        for this chain. The 1:1 strategy:gateway rule (blueprint 06) makes
        the registry the source of truth for "which wallet does this
        deployment own?". Local-mode gateways may run without a registry;
        we skip the check rather than fail-closed because the local posture
        has no multi-tenant blast radius.

        Fail-closed posture (CodeRabbit MAJOR, round 2, PR #2240): once a
        registry IS configured, ANY error path (no mapping for the chain,
        registry plugin raised, resolved is None / has no address) MUST
        return False with a typed gRPC status. Swallowing errors and
        returning True would let a misconfigured / partially-failed
        registry silently bypass ownership validation — exactly the
        multi-tenant blast-radius case the registry exists to prevent.

        Returns ``True`` only when (a) no registry is configured, or
        (b) the registry-resolved wallet for ``chain`` matches the
        caller's ``wallet_address``. All other paths return ``False``
        after setting one of:
            FAILED_PRECONDITION — no wallet mapping for this chain, or
                                  registry resolved without an address.
            INTERNAL            — registry plugin raised unexpectedly.
            PERMISSION_DENIED   — registry mapped to a different wallet.
        """
        if self.wallet_registry is None:
            return True
        try:
            resolved = self.wallet_registry.resolve(chain)
        except KeyError:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"no registered wallet mapping for chain {chain!r}")
            return False
        except Exception as e:  # noqa: BLE001 — registry plugin contract is "anything"
            logger.error("wallet_registry.resolve(%s) failed: %s", chain, e)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("wallet registry unavailable; cannot validate wallet ownership")
            return False
        if resolved is None:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"no registered wallet mapping for chain {chain!r}")
            return False
        expected = (getattr(resolved, "account_address", "") or "").strip().lower()
        if not expected:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"registered wallet for chain {chain!r} has no address")
            return False
        if expected == wallet_address.lower():
            return True
        context.set_code(grpc.StatusCode.PERMISSION_DENIED)
        context.set_details("wallet_address does not match the deployment's registered wallet for this chain")
        return False

    # =========================================================================
    # Step 2: Chain head sampling + freshness check + cursor validation
    # =========================================================================

    # crap-allowlist: VIB-4210 — gRPC handler structural cc=8 (rpc-not-wired
    # guard + head-sample failure + page_cursor optional branch with malformed
    # vs stale sub-branches + first-page anchor). Same "undecomposable gRPC
    # handler boilerplate" carve-out as ``GetMigrationState`` /
    # ``UpdateMigrationState`` in ``state_service.py``. Coverage rises once
    # the hosted-boot trigger lands (T24+1).
    async def _resolve_source_block_number(
        self,
        request: gateway_pb2.ReconcileRequest,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> int | None:
        """Sample chain head; honor max_age_blocks + page_cursor freshness.

        Returns the source_block_number to anchor this reconciliation pass
        against, or ``None`` after setting context to FAILED_PRECONDITION
        (stale head or stale cursor).
        """
        if self.rpc_servicer is None:
            logger.error("PositionService.Reconcile: rpc_servicer not wired")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("gateway not fully initialized")
            return None

        head = await _get_chain_head(self.rpc_servicer, chain=chain)
        if head is None:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to sample chain head for {chain!r}")
            return None

        # Optional cursor: if present, anchor on the cursor's block (so the
        # entire paginated pass is consistent against ONE chain state). The
        # gateway then checks the cursor isn't too stale relative to head.
        if request.page_cursor:
            decoded = decode_cursor(request.page_cursor)
            if decoded is None:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details("page_cursor malformed or schema_version mismatch — restart from page 0")
                return None
            cursor_block = int(decoded["source_block_number"])
            max_age = int(request.max_age_blocks or 0)
            if max_age > 0 and (head - cursor_block) > max_age:
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(
                    f"stale cursor: head={head}, cursor_block={cursor_block}, "
                    f"max_age_blocks={max_age} — restart from page 0"
                )
                return None
            return cursor_block

        # First page: anchor on current head.
        #
        # CodeRabbit MAJOR (round 2, PR #2240): in v1 we have ONE RPC
        # source for both the observed head and the "reference head", so
        # there is no independent freshness oracle to enforce
        # ``max_age_blocks`` against on a first-page request. Silently
        # accepting non-zero values would let a caller think the
        # guardrail is active when it isn't — strictly worse than no
        # guardrail. Reject explicitly with INVALID_ARGUMENT so the
        # caller can either drop ``max_age_blocks`` or supply a
        # ``page_cursor`` (which DOES anchor against the previously
        # observed head and is enforced above). The freshness check
        # against an independent reference RPC is reserved for T24+1.
        max_age = int(request.max_age_blocks or 0)
        if max_age > 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                "max_age_blocks > 0 is not supported on first-page requests in v1 "
                "(single-RPC-source gateway has no independent reference head). "
                "Either omit max_age_blocks or supply a page_cursor; "
                "reference-head freshness is reserved for T24+1."
            )
            return None
        return head

    # =========================================================================
    # Step 3: On-chain LP enumeration (in-process via RpcServiceServicer)
    # =========================================================================

    # crap-allowlist: VIB-4210 — fanned-RPC enumerator structural cc=18.
    # The per-NPM + per-position loop is a strict mirror of
    # ``almanak.framework.teardown.discovery.discover_lp_positions`` (see
    # the docstring "Mirrors discover_lp_positions" comment) and inherits
    # its 4-class fail-open-per-call topology: balanceOf-failed,
    # balanceOf-unparsable, count-oversize, per-position-read-failed.
    # Decomposing further would mean either inventing a different
    # discovery contract from the teardown's or splitting fail-open
    # branches across functions (loses the per-NPM error-grouping
    # invariant ADR §5.1 depends on). Same "undecomposable
    # public-contract / hot-path budget" carve-out as
    # ``GatewayStateManager.save_accounting_event`` (VIB-4196). Coverage
    # rises once the integration test covers the oversize + unparsable
    # branches (T24 follow-up — UAT card D3.F1 only covers RPC_FANOUT_FAILED).
    async def _enumerate_lp_positions(
        self,
        *,
        chain: str,
        wallet_address: str,
        source_block_number: int,
        physical_identity_hashes_filter: frozenset[str] | None,
        errors: _PrimitiveErrorCollector,
    ) -> tuple[list[dict[str, Any]], bool, str]:
        """Enumerate LP positions for ``wallet_address`` on ``chain``.

        Mirrors :func:`almanak.framework.teardown.discovery.discover_lp_positions`
        but calls the in-process RpcServiceServicer directly. Returns:
        - list of position dicts (matches what classify_diff expects)
        - oversize bool (per-NPM cap hit)
        - oversize detail string

        Failure modes (ADR §5.1, mirroring discovery strict=False semantics):
        - ``balanceOf`` failure on one NPM → record PrimitiveError(RPC_FANOUT_FAILED,
          recoverable=True), continue with remaining NPMs.
        - per-position read failure → continue (matches teardown discovery's
          fail-open-per-NPM behaviour with strict=False).
        - per-NPM count exceeds _MAX_POSITIONS_PER_NPM → oversize=True,
          truncate at the cap, surface PrimitiveError(RECONCILIATION_OVERSIZE).
        """
        if self.rpc_servicer is None:
            errors.add(
                primitive="lp",
                chain=chain,
                code="RPC_FANOUT_FAILED",
                message="gateway rpc servicer not wired",
                recoverable=False,
            )
            return [], False, ""

        # Lazy import to avoid module-load cycles + to keep the discovery
        # helpers (NPM registries, ABI selectors, decoders) in one place.
        from almanak.framework.teardown.discovery import (
            _MAX_POSITIONS_PER_NPM,
            _SELECTOR_BALANCE_OF,
            _npms_for_chain,
            _pad_address,
        )

        npms = _npms_for_chain(chain)
        if not npms:
            logger.info("No V3-fork NPMs registered for chain=%s; LP discovery skipped", chain)
            return [], False, ""

        out: list[dict[str, Any]] = []
        oversize = False
        oversize_detail = ""

        for protocol, npm in npms:
            count_raw = await _eth_call_in_process(
                self.rpc_servicer,
                chain=chain,
                to=npm,
                data=_SELECTOR_BALANCE_OF + _pad_address(wallet_address),
                block_number=source_block_number,
            )
            if count_raw is None:
                errors.add(
                    primitive="lp",
                    chain=chain,
                    code="RPC_FANOUT_FAILED",
                    message=f"balanceOf unreadable on {protocol}/{npm}",
                    recoverable=True,
                )
                continue
            if count_raw == "0x":
                continue
            try:
                count = int(count_raw, 16)
            except ValueError:
                errors.add(
                    primitive="lp",
                    chain=chain,
                    code="RPC_FANOUT_FAILED",
                    message=f"balanceOf returned unparsable hex {count_raw!r} on {protocol}/{npm}",
                    recoverable=True,
                )
                continue
            if count == 0:
                continue
            if count > _MAX_POSITIONS_PER_NPM:
                oversize = True
                oversize_detail = (
                    f"{protocol}/{npm} on {chain}: wallet owns {count} positions "
                    f">_MAX_POSITIONS_PER_NPM={_MAX_POSITIONS_PER_NPM}; truncated"
                )
                errors.add(
                    primitive="lp",
                    chain=chain,
                    code="RECONCILIATION_OVERSIZE",
                    message=oversize_detail,
                    recoverable=False,
                )
                count = _MAX_POSITIONS_PER_NPM

            for i in range(count):
                position = await self._read_lp_position(
                    chain=chain,
                    wallet_address=wallet_address,
                    protocol=protocol,
                    npm=npm,
                    index=i,
                    source_block_number=source_block_number,
                    physical_identity_hashes_filter=physical_identity_hashes_filter,
                )
                if position is not None:
                    out.append(position)

        return out, oversize, oversize_detail

    # crap-allowlist: VIB-4210 — per-slot fail-open reader structural cc=11;
    # strict mirror of ``discovery._read_position`` (see docstring). 5
    # distinct early-return classes (tokenOfOwnerByIndex failure / unparsable
    # hex, positions() failure, layout-too-short, zero-liquidity, hash-filter
    # exclusion) maintain the per-slot fail-open invariant ADR §5.1 requires
    # — decomposing splits these branches across functions and loses the
    # per-slot dropping the enumerator depends on. Same "undecomposable
    # public-contract mirror" carve-out as ``_enumerate_lp_positions`` above.
    async def _read_lp_position(
        self,
        *,
        chain: str,
        wallet_address: str,
        protocol: str,
        npm: str,
        index: int,
        source_block_number: int,
        physical_identity_hashes_filter: frozenset[str] | None,
    ) -> dict[str, Any] | None:
        """Read one UniV3 LP position from an NPM at index ``i``.

        Returns the position dict shape ``_enumerate_lp_positions`` appends
        to its output list, or ``None`` to skip this slot (RPC failure,
        unparsable hex, zero liquidity, or hash-filter exclusion). Pulled
        out of the per-NPM loop to keep ``_enumerate_lp_positions``'s
        cyclomatic complexity inside the C901 budget while preserving the
        per-slot fail-open semantics ADR §5.1 specifies (each ``None``
        return is silently dropped — partial failures don't abort the
        whole enumeration). Mirrors ``discovery._read_position`` in
        ``almanak/framework/teardown/discovery.py``.
        """
        # Lazy imports here too — the helper is on the hot path so we avoid
        # the module-load cost when there are zero positions to read.
        from almanak.framework.migration.backfill import physical_identity_hash_univ3
        from almanak.framework.teardown.discovery import (
            _SELECTOR_POSITIONS,
            _SELECTOR_TOKEN_OF_OWNER_BY_INDEX,
            _decode_int24,
            _pad_address,
            _pad_uint256,
        )

        # The caller (``_enumerate_lp_positions``) already guarded on
        # ``self.rpc_servicer is not None`` before entering the per-NPM
        # loop and short-circuited with a PrimitiveError if it was. This
        # assert is for mypy — the typed contract on this private helper
        # is "rpc_servicer is wired"; an unwired call here is a logic bug,
        # not a runtime condition.
        assert self.rpc_servicer is not None, "_read_lp_position called without rpc_servicer wired"

        token_id_raw = await _eth_call_in_process(
            self.rpc_servicer,
            chain=chain,
            to=npm,
            data=_SELECTOR_TOKEN_OF_OWNER_BY_INDEX + _pad_address(wallet_address) + _pad_uint256(index),
            block_number=source_block_number,
        )
        if token_id_raw is None or token_id_raw == "0x":
            return None
        try:
            token_id = int(token_id_raw, 16)
        except ValueError:
            return None

        position_raw = await _eth_call_in_process(
            self.rpc_servicer,
            chain=chain,
            to=npm,
            data=_SELECTOR_POSITIONS + _pad_uint256(token_id),
            block_number=source_block_number,
        )
        if position_raw is None or position_raw == "0x":
            return None

        # positions(tokenId) returns 12 fields per UniV3 NPM ABI; the
        # layout we care about (matching discovery._read_position):
        # token0[2], token1[3], fee[4], tickLower[5], tickUpper[6], liquidity[7].
        # Each word is 64 hex chars (32 bytes). Strip the leading 0x.
        raw_hex = position_raw[2:] if position_raw.startswith("0x") else position_raw
        if len(raw_hex) < 64 * 12:
            return None
        token0 = "0x" + raw_hex[64 * 2 + 24 : 64 * 3].lower()
        token1 = "0x" + raw_hex[64 * 3 + 24 : 64 * 4].lower()
        fee = int(raw_hex[64 * 4 : 64 * 5], 16)
        tick_lower = _decode_int24(raw_hex[64 * 5 : 64 * 6])
        tick_upper = _decode_int24(raw_hex[64 * 6 : 64 * 7])
        liquidity = int(raw_hex[64 * 7 : 64 * 8], 16)

        if liquidity == 0:
            # Skip burned / fully withdrawn positions — same default
            # as discover_lp_positions(include_zero_liquidity=False).
            return None

        # Canonical UniV3 LP physical_identity_hash — MUST match the helper
        # used by ``strategy_runner._register_lp_open`` (chatgpt-codex P1,
        # PR #2240). Mismatched hashing schemes would misclassify every
        # runner-tracked LP as phantom_missing and, under apply=true,
        # create a duplicate registry row colliding with the canonical one.
        physical_identity_hash = physical_identity_hash_univ3(
            chain=chain,
            nft_manager_addr=npm,
            token_id=token_id,
        )
        # Codex P2 (PR #2240): when the caller passed a hash filter, drop
        # on-chain positions outside that set BEFORE they enter
        # classify_diff. Without this, a one-row filter would report every
        # other wallet position as phantom_missing against the one-row
        # registry subset.
        if (
            physical_identity_hashes_filter is not None
            and physical_identity_hash not in physical_identity_hashes_filter
        ):
            return None
        payload = {
            "source": "reconciliation_discovery",
            "protocol": protocol,
            "token_id": token_id,
            "npm_address": npm,
            "token0": token0,
            "token1": token1,
            "fee": fee,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "liquidity": str(liquidity),
        }
        return {
            "physical_identity_hash": physical_identity_hash,
            "primitive": "lp",
            "accounting_category": "lp",
            "semantic_grouping_key": f"{chain}:{token0}:{token1}:{fee}",
            "payload": payload,
            "opened_at_block": 0,  # best-effort back-derivation deferred
            "opened_tx": "",
        }

    # =========================================================================
    # Step 4: Read registry rows
    # =========================================================================

    # crap-allowlist: VIB-4210 — dual-backend registry reader structural
    # cc=15 (state-servicer-not-wired guard + per-primitive loop +
    # Postgres branch with per-row payload-JSON decode + SQLite branch
    # with warm-backend capability check + try/except partial-failure
    # surfacing + hash-filter post-pass). Mirrors the dual-backend
    # dispatch pattern of ``StateServiceServicer.GetPositionRegistryOpenRows``
    # (also crap-allowlisted; see ``state_service.py:3296``). Refactor
    # target is collapsing the Postgres + SQLite branches into one
    # backend-trait, planned as a sibling deliverable to the VIB-4297
    # GetMigrationState collapse follow-up.
    async def _read_registry_rows(
        self,
        *,
        deployment_id: str,
        chain: str,
        primitives: list[str],
        physical_identity_hashes: list[str],
        errors: _PrimitiveErrorCollector,
    ) -> list[dict[str, Any]]:
        """Read OPEN registry rows scoped to the deployment, chain, primitives.

        Uses the StateService's existing accessor (T22 / VIB-4208 for SQLite,
        T19 / VIB-4205 for Postgres). On read failure, surfaces a PrimitiveError
        with code='BACKEND_TIMEOUT' and returns []. ADR §5.1 partial-failure
        semantics: a read failure should NOT abort the whole RPC.
        """
        if self.state_servicer is None:
            errors.add(
                primitive=",".join(primitives),
                chain=chain,
                code="BACKEND_TIMEOUT",
                message="state servicer not wired",
                recoverable=False,
            )
            return []

        out: list[dict[str, Any]] = []
        for primitive in primitives:
            try:
                # Reuse the existing GetPositionRegistryOpenRows handler shape.
                # Since we're in-process, call StateManager directly via the
                # state servicer's _ensure_initialized + warm backend accessor.
                await self.state_servicer._ensure_snapshot_pool()
                if self.state_servicer._snapshot_pool is not None:
                    # Postgres path (T19 / VIB-4205).
                    sql = (
                        "SELECT deployment_id, chain, primitive, accounting_category, "
                        "physical_identity_hash, semantic_grouping_key, "
                        "grouping_policy_version, handle, status, "
                        "payload::text AS payload_text, "
                        "opened_at_block, opened_tx, "
                        "closed_at_block, closed_tx, "
                        "last_reconciled_at_block, matching_policy_version "
                        "FROM position_registry "
                        "WHERE deployment_id = $1 AND status = 'open' "
                        "  AND chain = $2 AND primitive = $3 "
                        "ORDER BY opened_at_block ASC NULLS FIRST, opened_tx ASC NULLS FIRST"
                    )
                    rows = await self.state_servicer._snapshot_fetch(sql, deployment_id, chain, primitive)
                    for row in rows:
                        row_dict = dict(row)
                        payload_text = row_dict.pop("payload_text", None) or "{}"
                        try:
                            row_dict["payload"] = json.loads(payload_text)
                        except (TypeError, ValueError):
                            row_dict["payload"] = {}
                        out.append(row_dict)
                else:
                    # SQLite path (T22 / VIB-4208).
                    await self.state_servicer._ensure_initialized()
                    assert self.state_servicer._state_manager is not None
                    warm = self.state_servicer._state_manager.warm_backend
                    if warm is None or not hasattr(warm, "get_position_registry_open_rows"):
                        errors.add(
                            primitive=primitive,
                            chain=chain,
                            code="BACKEND_TIMEOUT",
                            message="warm backend lacks get_position_registry_open_rows",
                            recoverable=False,
                        )
                        continue
                    rows = await warm.get_position_registry_open_rows(
                        deployment_id,
                        chain=chain,
                        primitive=primitive,
                        accounting_category=None,
                    )
                    for row in rows:
                        out.append(dict(row))
            except Exception as e:  # noqa: BLE001 — partial-failure semantics
                logger.warning(
                    "PositionService.Reconcile read_registry_rows failed (primitive=%s): %s",
                    primitive,
                    e,
                )
                errors.add(
                    primitive=primitive,
                    chain=chain,
                    code="BACKEND_TIMEOUT",
                    message=f"registry read failed: {e}",
                    recoverable=True,
                )

        if physical_identity_hashes:
            hash_filter = set(physical_identity_hashes)
            out = [row for row in out if row.get("physical_identity_hash") in hash_filter]
        return out

    # =========================================================================
    # Step 6: Apply path — write phantom-missing rows via mode='registry_reconciliation'
    # =========================================================================

    # crap-allowlist: VIB-4210 — phantom-missing writer structural cc=10
    # (no-phantoms shortcut + state-servicer guard + per-phantom loop +
    # typed-collision vs generic-exception branching). The per-phantom
    # error-class split is ADR §5.1's partial-failure contract — collapsing
    # it loses the typed-collision surface UAT D3.F9 depends on. Coverage
    # rises with the apply=true SQLite path test (T24+1 follow-up).
    async def _apply_phantom_missing(
        self,
        *,
        deployment_id: str,
        chain: str,
        source_block_number: int,
        reconciliation_id: str,
        phantoms: list[dict[str, Any]],
        errors: _PrimitiveErrorCollector,
    ) -> list[dict[str, Any]]:
        """Write phantom-missing rows to position_registry via mode='registry_reconciliation'.

        Uses the existing atomic primitive ``save_ledger_and_registry`` with
        the new ``mode='registry_reconciliation'`` parameter (ADR §8.1
        Option (c) — ratified single-registry-writer rule). The ledger
        INSERT is SKIPPED on this path; only the registry UPSERT + handle
        backfill run, atomically.

        Per-phantom failures are isolated: a RegistryAutoCollisionError on
        one phantom surfaces as a PrimitiveError(code='REGISTRY_AUTO_COLLISION',
        recoverable=False) and the remaining phantoms still process. The
        ADR §5.1 partial-failure rule again.
        """
        if not phantoms:
            return []
        if self.state_servicer is None:
            errors.add(
                primitive="lp",
                chain=chain,
                code="BACKEND_TIMEOUT",
                message="state servicer not wired; cannot apply phantom_missing",
                recoverable=False,
            )
            return []

        from datetime import UTC, datetime

        from almanak.framework.accounting.commit import RegistryRow
        from almanak.framework.observability.ledger import LedgerEntry
        from almanak.framework.state.registry_errors import RegistryAutoCollisionError

        rebuilt: list[dict[str, Any]] = []
        await self.state_servicer._ensure_initialized()
        assert self.state_servicer._state_manager is not None
        state_manager = self.state_servicer._state_manager

        for phantom in phantoms:
            payload = dict(phantom.get("payload") or {})
            # Stamp provenance fields — distinguishes chain-derived rows
            # from intent-derived rows downstream.
            payload["source"] = "reconciliation_discovery"
            payload["reconciliation_id"] = reconciliation_id

            registry = RegistryRow(
                deployment_id=deployment_id,
                chain=chain,
                primitive=phantom["primitive"],
                accounting_category=phantom["accounting_category"],
                physical_identity_hash=phantom["physical_identity_hash"],
                semantic_grouping_key=phantom.get("semantic_grouping_key", ""),
                grouping_policy_version="univ3_lp@v1",
                status="open",
                payload=payload,
                matching_policy_version=1,
                handle=None,
                opened_at_block=phantom.get("opened_at_block") or None,
                opened_tx=phantom.get("opened_tx") or None,
                closed_at_block=None,
                closed_tx=None,
                last_reconciled_at_block=int(source_block_number),
            )
            # The atomic primitive still requires a LedgerEntry argument for
            # signature uniformity even though mode='registry_reconciliation'
            # skips the ledger write. We construct a sentinel ledger that
            # would fail if it were ever accidentally written (empty
            # intent_type / tx_hash) — a defensive layer in case a future
            # refactor regresses the skip behaviour.
            sentinel_ledger = LedgerEntry(
                id=f"reconciliation:{reconciliation_id}:{phantom['physical_identity_hash']}",
                cycle_id=f"reconciliation:{reconciliation_id}",
                strategy_id=deployment_id,
                deployment_id=deployment_id,
                execution_mode="reconciliation",
                timestamp=datetime.now(UTC),
                intent_type="",
                token_in="",
                amount_in="",
                token_out="",
                amount_out="",
                effective_price="",
                slippage_bps=None,
                gas_used=0,
                gas_usd="",
                tx_hash="",
                chain=chain,
                protocol=phantom.get("payload", {}).get("protocol", ""),
                success=True,
                error="",
                extracted_data_json="",
                price_inputs_json="",
                pre_state_json="",
                post_state_json="",
            )
            try:
                await state_manager.save_ledger_and_registry(
                    ledger=sentinel_ledger,
                    registry=registry,
                    handle=None,
                    mode="registry_reconciliation",
                )
            except RegistryAutoCollisionError as e:
                # ADR §5.1 + UAT D3.F9: surface collision as a typed
                # PrimitiveError but DO NOT fail the whole RPC. The existing
                # handle-less open row is preserved; the operator must add
                # a registry_handle to disambiguate before re-running.
                errors.add(
                    primitive="lp",
                    chain=chain,
                    code="REGISTRY_AUTO_COLLISION",
                    message=(f"auto-mode collision for phantom_missing pih={phantom['physical_identity_hash']!r}: {e}"),
                    recoverable=False,
                )
                continue
            except Exception as e:  # noqa: BLE001 — wrap as primitive_error
                logger.error(
                    "PositionService.Reconcile apply failed for pih=%s: %s",
                    phantom["physical_identity_hash"],
                    e,
                )
                errors.add(
                    primitive="lp",
                    chain=chain,
                    code="BACKEND_TIMEOUT",
                    message=f"registry write failed: {e}",
                    recoverable=True,
                )
                continue

            rebuilt.append(
                {
                    "physical_identity_hash": phantom["physical_identity_hash"],
                    "primitive": phantom["primitive"],
                    "accounting_category": phantom["accounting_category"],
                    "source": "reconciliation_discovery",
                    "last_reconciled_at_block": int(source_block_number),
                    "reconciliation_id": reconciliation_id,
                    "registry_row": {
                        "deployment_id": deployment_id,
                        "chain": chain,
                        "primitive": phantom["primitive"],
                        "physical_identity_hash": phantom["physical_identity_hash"],
                        "payload": payload,
                    },
                }
            )
        return rebuilt

    # =========================================================================
    # Step 7: Response construction
    # =========================================================================

    def _build_response(
        self,
        *,
        reconciliation_id: str,
        source_block_number: int,
        matched: list[dict[str, Any]],
        phantom_missing: list[dict[str, Any]],
        stranded: list[dict[str, Any]],
        rebuilt: list[dict[str, Any]],
        oversize: bool,
        oversize_detail: str,
        duration_seconds: float,
        primitive_errors: list[gateway_pb2.PrimitiveError],
        next_page_cursor: bytes,
        page_size: int,
    ) -> gateway_pb2.ReconcileResponse:
        response = gateway_pb2.ReconcileResponse(
            reconciliation_id=reconciliation_id,
            source_block_number=int(source_block_number),
            next_page_cursor=next_page_cursor,
            oversize=oversize,
            oversize_detail=oversize_detail,
            matched_count=len(matched),
            phantom_missing_count=len(phantom_missing),
            stranded_count=len(stranded),
            rebuilt_count=len(rebuilt),
            duration_seconds=float(duration_seconds),
        )
        for m in matched:
            response.matched.append(
                gateway_pb2.MatchedPosition(
                    physical_identity_hash=m["physical_identity_hash"],
                    primitive=m["primitive"],
                    accounting_category=m["accounting_category"],
                    confirmed_at_block=int(source_block_number),
                )
            )
        for p in phantom_missing:
            response.phantom_missing.append(
                gateway_pb2.PhantomMissingPosition(
                    physical_identity_hash=p["physical_identity_hash"],
                    primitive=p["primitive"],
                    accounting_category=p["accounting_category"],
                    semantic_grouping_key=p.get("semantic_grouping_key", ""),
                    payload_json=json.dumps(p.get("payload") or {}, sort_keys=True).encode("utf-8"),
                    opened_at_block=int(p.get("opened_at_block") or 0),
                    opened_tx=p.get("opened_tx") or "",
                )
            )
        for s in stranded:
            response.stranded.append(
                gateway_pb2.StrandedRow(
                    physical_identity_hash=s["physical_identity_hash"],
                    primitive=s["primitive"],
                    accounting_category=s["accounting_category"],
                    handle=s.get("handle") or "",
                    registry_row_json=json.dumps(_jsonify(s), sort_keys=True).encode("utf-8"),
                    confirmed_absent_at_block=int(source_block_number),
                    absent_reason="position not held on-chain",
                )
            )
        for r in rebuilt:
            response.rebuilt.append(
                gateway_pb2.RebuiltRow(
                    physical_identity_hash=r["physical_identity_hash"],
                    primitive=r["primitive"],
                    accounting_category=r["accounting_category"],
                    source=r["source"],
                    last_reconciled_at_block=int(r["last_reconciled_at_block"]),
                    reconciliation_id=r["reconciliation_id"],
                    registry_row_json=json.dumps(_jsonify(r["registry_row"]), sort_keys=True).encode("utf-8"),
                )
            )
        for err in primitive_errors:
            response.primitive_errors.append(err)
        _ = page_size  # carried for future paginated implementation (T24+1)
        return response

    @staticmethod
    def _empty_response_with_no_writes(*, reconciliation_id: str | None = None) -> gateway_pb2.ReconcileResponse:
        """Empty response used on validation failure (already-set context error).

        Returns a fully-zero envelope so the response shape is uniform across
        success / failure. Counts are zero; bucket lists are empty; primitive
        errors are empty (the failure is communicated via the gRPC status code
        + details). NO writes happened — the four-surface invariant holds.
        """
        return gateway_pb2.ReconcileResponse(reconciliation_id=reconciliation_id or "")


def _jsonify(obj: Any) -> Any:
    """Coerce a registry-row dict into JSON-serializable form (bytes → str, datetime → ISO)."""
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.hex()
    # Decimal, datetime — best-effort str fallback (registry rows shouldn't
    # carry these, but defensive).
    try:
        json.dumps(obj)
        return obj
    except TypeError:
        return str(obj)
