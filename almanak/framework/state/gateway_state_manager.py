"""Gateway-backed StateManager implementation.

This module provides a StateManager that persists state through the gateway
sidecar instead of directly accessing the database. Used in strategy containers
that have no access to database credentials.

Portfolio snapshots are persisted via gateway gRPC (SavePortfolioSnapshot,
GetLatestSnapshot, GetSnapshotsSince) which routes to PostgreSQL in deployed
mode.  Portfolio metrics (PnL baseline) are persisted via SavePortfolioMetrics
and GetPortfolioMetrics.  Local mode uses the regular StateManager with
SQLiteStore.
"""

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

import grpc

from almanak.framework.gateway_client import GatewayClient
from almanak.framework.state.exceptions import AccountingPersistenceError, AccountingWriteKind
from almanak.framework.state.state_manager import StateData
from almanak.gateway.proto import gateway_pb2

if TYPE_CHECKING:
    from almanak.framework.accounting.models import LendingAccountingEvent, PendleAccountingEvent
    from almanak.framework.observability.ledger import LedgerEntry
    from almanak.framework.observability.position_events import PositionEvent
    from almanak.framework.portfolio.models import PortfolioMetrics
    from almanak.framework.state.portfolio import PortfolioSnapshot

logger = logging.getLogger(__name__)


def _apply_synth_position_guard(snapshot: "PortfolioSnapshot") -> None:
    """Degrade a HIGH-confidence snapshot to ESTIMATED if it carries a
    measured-zero-cost-basis position with non-zero value (VIB-3917 / 4098).

    Pre-3.7 the predicate also fired when ``cost_basis_usd is None`` — a
    legal "unmeasured" state for a freshly-discovered position. That
    overreach silently re-degraded every snapshot back to ESTIMATED on
    every iteration, producing the May 3 production class where the
    dashboard's confidence pill never went green even with a healthy
    accounting pipeline.

    Truth table after 3.7:

        cb              v               action
        --------------  --------------  ------
        None            *               leave HIGH (unmeasured ≠ zero)
        Decimal("0")    None            leave HIGH (oracle silent)
        Decimal("0")    Decimal("0")    leave HIGH (genuine zero position)
        Decimal("0")    > 0             degrade   (true basis violation)
        > 0             *               leave HIGH

    Per-position degradation (instead of snapshot-wide) is a follow-up;
    requires adding ``value_confidence`` to ``PositionValue``. This PRD
    ships the ``None`` / ``Decimal(0)`` distinction first.
    """
    from almanak.framework.portfolio.models import ValueConfidence

    if getattr(snapshot, "value_confidence", None) is None:
        return
    if snapshot.value_confidence != ValueConfidence.HIGH:
        return

    for pos in snapshot.positions or []:
        cb = getattr(pos, "cost_basis_usd", None)
        v = getattr(pos, "value_usd", None)
        # True basis violation: handler measured zero cost (Decimal('0'))
        # while oracle reports non-zero value. ``cb is None`` means
        # "unmeasured" — legal for a freshly-discovered position; do
        # NOT degrade.
        if isinstance(cb, Decimal) and cb == Decimal("0") and v is not None and v > Decimal("0"):
            logger.warning(
                "snapshot for %s carries HIGH+measured-zero-basis "
                "position (value_usd=%s, cost_basis_usd=%s) — degrading to ESTIMATED",
                snapshot.deployment_id,
                v,
                cb,
            )
            snapshot.value_confidence = ValueConfidence.ESTIMATED
            return


class GatewayStateManager:
    """StateManager that persists state through the gateway.

    This implementation routes all state operations to the gateway sidecar,
    which has access to the actual storage backends (PostgreSQL, SQLite).

    The interface mirrors the standard StateManager but works via gRPC.

    Example:
        from almanak.framework.gateway_client import GatewayClient
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        with GatewayClient() as client:
            state_manager = GatewayStateManager(client)
            state = await state_manager.load_state("my-strategy")
            if state:
                print(f"Loaded state version {state.version}")
    """

    def __init__(self, client: GatewayClient, timeout: float = 30.0):
        """Initialize gateway-backed state manager.

        Args:
            client: Connected GatewayClient instance
            timeout: RPC timeout in seconds
        """
        self._client = client
        self._timeout = timeout

    async def initialize(self) -> None:
        """Initialize the state manager.

        For the gateway-backed version, this is a no-op since the actual
        initialization happens in the gateway.
        """
        logger.debug("Gateway state manager initialized (no-op)")

    async def close(self) -> None:
        """Close the state manager."""
        logger.debug("Gateway state manager closed")

    async def load_state(self, deployment_id: str) -> StateData | None:
        """Load strategy state from gateway.

        Args:
            deployment_id: Unique deployment identifier

        Returns:
            StateData if found, None if not found

        Raises:
            StateError: If gateway request fails
        """
        try:
            request = gateway_pb2.LoadStateRequest(deployment_id=deployment_id)
            response = self._client.state.LoadState(request, timeout=self._timeout)

            if not response.deployment_id:
                return None

            # Deserialize state from JSON bytes
            state_dict = json.loads(response.data.decode("utf-8"))

            return StateData(
                deployment_id=response.deployment_id,
                version=response.version,
                state=state_dict,
                schema_version=response.schema_version,
                checksum=response.checksum or "",
                created_at=datetime.fromtimestamp(response.created_at, tz=UTC)
                if response.created_at
                else datetime.now(UTC),
            )

        except Exception as e:
            error_msg = str(e)
            # NOT_FOUND is expected for new strategies
            if "NOT_FOUND" in error_msg:
                return None

            logger.error(f"Gateway load state failed for {deployment_id}: {error_msg}")
            raise

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    async def save_state(self, state: StateData, expected_version: int | None = None) -> StateData:
        """Save strategy state through gateway.

        Uses optimistic locking: if expected_version is provided, the save
        will fail if the current version doesn't match.

        Args:
            state: State data to save
            expected_version: Expected current version for CAS semantics

        Returns:
            Updated StateData with new version

        Raises:
            StateConflictError: If version conflict (CAS failure)
            StateError: If gateway request fails
        """
        try:
            # Serialize state to JSON bytes
            state_bytes = json.dumps(state.state, default=str, sort_keys=True).encode("utf-8")

            request = gateway_pb2.SaveStateRequest(
                deployment_id=state.deployment_id,
                expected_version=expected_version or 0,
                data=state_bytes,
                schema_version=state.schema_version,
            )
            response = self._client.state.SaveState(request, timeout=self._timeout)

            if not response.success:
                error_msg = response.error or "Unknown save error"

                # Check for version conflict
                if "version" in error_msg.lower() or "conflict" in error_msg.lower():
                    from almanak.framework.state.state_manager import StateConflictError

                    raise StateConflictError(
                        deployment_id=state.deployment_id,
                        expected_version=expected_version or 0,
                        actual_version=response.new_version,
                    )

                raise RuntimeError(f"State save failed: {error_msg}")

            # Return updated state with new version
            return StateData(
                deployment_id=state.deployment_id,
                version=response.new_version,
                state=state.state,
                schema_version=state.schema_version,
                checksum=response.checksum or "",
                created_at=state.created_at,
            )

        except Exception as e:
            if "StateConflictError" in type(e).__name__:
                raise
            logger.error(f"Gateway save state failed for {state.deployment_id}: {e}")
            raise

    async def delete_state(self, deployment_id: str) -> bool:
        """Delete strategy state through gateway.

        Args:
            deployment_id: Unique deployment identifier

        Returns:
            True if deleted, False if not found
        """
        try:
            request = gateway_pb2.DeleteStateRequest(deployment_id=deployment_id)
            response = self._client.state.DeleteState(request, timeout=self._timeout)

            return response.success

        except Exception as e:
            logger.error(f"Gateway delete state failed for {deployment_id}: {e}")
            raise

    def invalidate_hot_cache(self, deployment_id: str | None = None) -> None:
        """Invalidate hot cache.

        For the gateway-backed version, this is a no-op since caching
        is handled in the gateway.

        Args:
            deployment_id: Strategy to invalidate, or None for all
        """
        logger.debug(f"Cache invalidation requested for {deployment_id or 'all'} (no-op)")

    async def save_portfolio_snapshot(self, snapshot: "PortfolioSnapshot") -> int:
        """Save portfolio snapshot via gateway gRPC → PostgreSQL.

        Args:
            snapshot: Portfolio snapshot to save

        Returns:
            Snapshot ID from the database
        """
        try:
            # VIB-3917 / VIB-4098 (3.7) — guard extracted into module-level
            # helper for testability and narrowed predicate (None ≠
            # Decimal("0")). See ``_apply_synth_position_guard`` docstring
            # for the full truth table.
            _apply_synth_position_guard(snapshot)

            # VIB-3923 — to_positions_payload() now ALWAYS emits envelope shape.
            # The state_service on the receiving end unpacks it and persists
            # each field to its own column. Token prices and wallet balances
            # ride alongside as sibling envelope fields.
            payload = snapshot.to_positions_payload()
            # VIB-3894 — SaveSnapshotRequest is missing deployed_capital_usd
            # and wallet_total_value_usd on the proto wire. Pre-fix the
            # gateway-side state_service constructed PortfolioSnapshot with
            # those fields defaulted to ``Decimal("0")`` even when the
            # framework valuer had computed them from open-position cost
            # basis. Smuggle them through the envelope's ``metadata`` dict
            # so the proto contract stays additive (backwards-compat with
            # snapshots written before this change). state_service reads
            # them back during unpack.
            payload.setdefault("metadata", {})
            payload["metadata"]["__deployed_capital_usd__"] = str(snapshot.deployed_capital_usd)
            payload["metadata"]["__wallet_total_value_usd__"] = str(snapshot.wallet_total_value_usd)
            # Attach accounting data to the envelope
            if snapshot.token_prices:
                payload["token_prices"] = snapshot.token_prices
            if snapshot.wallet_balances:
                payload["wallet_balances"] = [
                    {
                        "symbol": b.symbol,
                        "balance": str(b.balance),
                        "value_usd": str(b.value_usd),
                        "address": b.address,
                        "price_usd": str(b.price_usd) if b.price_usd is not None else None,
                    }
                    for b in snapshot.wallet_balances
                ]

            positions_bytes = json.dumps(payload, default=str, sort_keys=True).encode("utf-8")

            request = gateway_pb2.SaveSnapshotRequest(
                timestamp=int(snapshot.timestamp.timestamp()),
                iteration_number=snapshot.iteration_number,
                total_value_usd=str(snapshot.total_value_usd),
                available_cash_usd=str(snapshot.available_cash_usd),
                value_confidence=snapshot.value_confidence.value,
                positions_json=positions_bytes,
                chain=snapshot.chain or "",
                # VIB-4091/4094 — Phase 4 identity. Source of truth: the runner
                # stamps these onto the snapshot before calling save (VIB-4099),
                # mirroring how it stamps PortfolioMetrics today.
                deployment_id=snapshot.deployment_id or "",
                cycle_id=snapshot.cycle_id or "",
                execution_mode=snapshot.execution_mode or "",
            )
            response = self._client.state.SavePortfolioSnapshot(request, timeout=self._timeout)

            if not response.success:
                # VIB-3157: treat gateway-side write failure as a first-class accounting
                # error. The previous "return 0" path caused silent accounting loss --
                # on-chain trades with no durable snapshot.
                logger.error("SavePortfolioSnapshot failed: %s", response.error)
                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.SNAPSHOT,
                    deployment_id=snapshot.deployment_id,
                    message=f"SavePortfolioSnapshot failed: {response.error}",
                )

            logger.debug(
                "Portfolio snapshot saved via gateway: strategy=%s, value=$%.2f, confidence=%s",
                snapshot.deployment_id,
                snapshot.total_value_usd,
                snapshot.value_confidence.value,
            )
            return response.snapshot_id
        except AccountingPersistenceError:
            raise
        except Exception as e:
            logger.exception("Failed to save portfolio snapshot via gateway")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.SNAPSHOT,
                deployment_id=getattr(snapshot, "deployment_id", "") or "",
                cause=e,
            ) from e

    async def get_latest_snapshot(self, deployment_id: str) -> "PortfolioSnapshot | None":
        """Get most recent portfolio snapshot via gateway gRPC."""
        try:
            request = gateway_pb2.GetLatestSnapshotRequest(deployment_id=deployment_id)
            response = self._client.state.GetLatestSnapshot(request, timeout=self._timeout)

            if not response.found:
                return None

            return self._proto_to_snapshot(response)
        except Exception as e:
            logger.debug("Failed to get latest snapshot via gateway: %s", e)
            return None

    async def get_snapshots_since(
        self, deployment_id: str, since: datetime, limit: int = 168
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a given time via gateway gRPC."""
        try:
            request = gateway_pb2.GetSnapshotsSinceRequest(
                deployment_id=deployment_id,
                since=int(since.timestamp()),
                limit=limit,
            )
            response = self._client.state.GetSnapshotsSince(request, timeout=self._timeout)

            return [self._proto_to_snapshot(s) for s in response.snapshots if s.found]
        except Exception as e:
            logger.debug("Failed to get snapshots via gateway: %s", e)
            return []

    async def save_ledger_entry(self, entry: "LedgerEntry") -> None:
        """Save a transaction ledger entry via gateway gRPC → PostgreSQL.

        VIB-3201 closes the VIB-3157 gap. Mirrors
        :meth:`save_portfolio_snapshot`: on ``response.success == False`` or
        any gRPC/transport exception, raises
        :class:`AccountingPersistenceError` so the runner halts the iteration
        with ``ACCOUNTING_FAILED`` rather than losing the trade record.
        """
        try:
            request = gateway_pb2.SaveLedgerEntryRequest(
                id=getattr(entry, "id", "") or "",
                cycle_id=getattr(entry, "cycle_id", "") or "",
                deployment_id=getattr(entry, "deployment_id", "") or "",
                execution_mode=getattr(entry, "execution_mode", "") or "",
                timestamp=int(entry.timestamp.timestamp()),
                intent_type=getattr(entry, "intent_type", "") or "",
                token_in=getattr(entry, "token_in", "") or "",
                amount_in=getattr(entry, "amount_in", "") or "",
                token_out=getattr(entry, "token_out", "") or "",
                amount_out=getattr(entry, "amount_out", "") or "",
                effective_price=getattr(entry, "effective_price", "") or "",
                gas_used=int(getattr(entry, "gas_used", 0) or 0),
                gas_usd=getattr(entry, "gas_usd", "") or "",
                tx_hash=getattr(entry, "tx_hash", "") or "",
                chain=getattr(entry, "chain", "") or "",
                protocol=getattr(entry, "protocol", "") or "",
                success=bool(getattr(entry, "success", True)),
                error=getattr(entry, "error", "") or "",
                extracted_data_json=(getattr(entry, "extracted_data_json", "") or "").encode("utf-8"),
                price_inputs_json=(getattr(entry, "price_inputs_json", "") or "").encode("utf-8"),
                pre_state_json=(getattr(entry, "pre_state_json", "") or "").encode("utf-8"),
                post_state_json=(getattr(entry, "post_state_json", "") or "").encode("utf-8"),
            )
            # slippage_bps is ``optional`` in the proto so None stays
            # distinguishable from 0.0 on the wire.
            slippage = getattr(entry, "slippage_bps", None)
            if slippage is not None:
                request.slippage_bps = float(slippage)

            response = self._client.state.SaveLedgerEntry(request, timeout=self._timeout)

            if not response.success:
                logger.error("SaveLedgerEntry failed: %s", response.error)
                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.LEDGER,
                    deployment_id=getattr(entry, "deployment_id", "") or "",
                    message=f"SaveLedgerEntry failed: {response.error}",
                )

            logger.debug(
                "Ledger entry saved via gateway: strategy=%s, id=%s, intent=%s, success=%s",
                getattr(entry, "deployment_id", ""),
                getattr(entry, "id", ""),
                getattr(entry, "intent_type", ""),
                getattr(entry, "success", True),
            )
        except AccountingPersistenceError:
            raise
        except Exception as e:
            logger.exception("Failed to save ledger entry via gateway")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.LEDGER,
                deployment_id=getattr(entry, "deployment_id", "") or "",
                cause=e,
            ) from e

    async def sum_ledger_gas_usd(
        self,
        deployment_id: str,
    ) -> Decimal:
        """Σ transaction_ledger.gas_usd via gateway gRPC (VIB-4247)."""
        request = gateway_pb2.SumLedgerGasUsdRequest(
            deployment_id=deployment_id,
        )
        try:
            response = self._client.state.SumLedgerGasUsd(request, timeout=self._timeout)
        except grpc.RpcError as exc:
            code = exc.code() if hasattr(exc, "code") else None
            if code == grpc.StatusCode.UNIMPLEMENTED:
                raise NotImplementedError("gateway does not implement SumLedgerGasUsd") from exc
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                deployment_id=deployment_id,
                message=f"SumLedgerGasUsd RPC failed for {deployment_id}",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                deployment_id=deployment_id,
                message=f"SumLedgerGasUsd RPC failed for {deployment_id}",
                cause=exc,
            ) from exc

        if not response.success:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                deployment_id=deployment_id,
                message=f"SumLedgerGasUsd failed for {deployment_id}: {response.error or 'unknown error'}",
            )

        try:
            return Decimal(response.gas_usd_total or "0")
        except Exception as exc:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                deployment_id=deployment_id,
                message=f"SumLedgerGasUsd returned invalid Decimal for {deployment_id}",
                cause=exc,
            ) from exc

    async def save_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
        """Save portfolio metrics via gateway gRPC.

        Args:
            metrics: PortfolioMetrics to persist.

        Returns:
            True if save succeeded.
        """
        try:
            request = gateway_pb2.SaveMetricsRequest(
                initial_value_usd=str(metrics.initial_value_usd),
                initial_timestamp=int(metrics.timestamp.timestamp()),
                deposits_usd=str(metrics.deposits_usd),
                withdrawals_usd=str(metrics.withdrawals_usd),
                gas_spent_usd=str(metrics.gas_spent_usd),
                # Phase 4 accounting identity fields (VIB-2835/2837/2839)
                deployment_id=getattr(metrics, "deployment_id", "") or "",
                cycle_id=getattr(metrics, "cycle_id", "") or "",
                execution_mode=getattr(metrics, "execution_mode", "") or "",
                is_complete=getattr(metrics, "is_complete", True),
            )
            response = self._client.state.SavePortfolioMetrics(request, timeout=self._timeout)

            if not response.success:
                # VIB-3157: mirror save_portfolio_snapshot -- silent False returns caused
                # baseline drift between ledger and metrics tables.
                logger.error("SavePortfolioMetrics failed: %s", response.error)
                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.METRICS,
                    deployment_id=metrics.deployment_id,
                    message=f"SavePortfolioMetrics failed: {response.error}",
                )

            logger.debug("Portfolio metrics saved via gateway for strategy=%s", metrics.deployment_id)
            return True
        except AccountingPersistenceError:
            raise
        except Exception as e:
            logger.exception("Failed to save portfolio metrics via gateway")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                deployment_id=getattr(metrics, "deployment_id", "") or "",
                cause=e,
            ) from e

    async def get_portfolio_metrics(self, deployment_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics via gateway gRPC.

        Args:
            deployment_id: Deployment identifier.

        Returns:
            PortfolioMetrics or None if not found.
        """
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioMetrics

        try:
            request = gateway_pb2.GetMetricsRequest(deployment_id=deployment_id)
            response = self._client.state.GetPortfolioMetrics(request, timeout=self._timeout)

            if not response.found:
                return None

            # VIB-2475: the ``PortfolioMetricsData`` proto does NOT carry
            # ``total_value_usd`` (VIB-2765 — it is derived from the most recent
            # snapshot, saved moments before this RPC). Source it from the
            # latest snapshot, exactly as the comment historically promised and
            # as the gateway WRITE path already does
            # (``_save_metrics_helpers.resolve_total_value_usd``). Empty≠Zero:
            # when no snapshot exists the value is genuinely UNMEASURED, so the
            # field stays ``None`` — never ``Decimal("0")``, which would
            # fabricate a measured-zero NAV and poison ``pnl_before_gas`` as
            # ≈ −initial (a confident-wrong −100% loss). The framework READ path
            # diverges intentionally from the gateway write path's
            # ``Decimal("0")``-on-miss: a read must surface "unmeasured", not a
            # fabricated zero.
            snapshot = await self.get_latest_snapshot(deployment_id)
            total_value_usd = snapshot.total_value_usd if snapshot is not None else None

            return PortfolioMetrics(
                timestamp=datetime.fromtimestamp(response.updated_at, tz=UTC)
                if response.updated_at
                else datetime.now(UTC),
                total_value_usd=total_value_usd,
                initial_value_usd=Decimal(response.initial_value_usd or "0"),
                deposits_usd=Decimal(response.deposits_usd or "0"),
                withdrawals_usd=Decimal(response.withdrawals_usd or "0"),
                gas_spent_usd=Decimal(response.gas_spent_usd or "0"),
                # Phase 4 accounting identity fields (VIB-2835/2837/2839)
                deployment_id=response.deployment_id or "",
                cycle_id=response.cycle_id or "",
                execution_mode=response.execution_mode or "",
                is_complete=response.is_complete,
            )
        except Exception as e:
            logger.debug("Failed to get portfolio metrics via gateway: %s", e)
            return None

    @staticmethod
    def _proto_to_snapshot(data: gateway_pb2.SnapshotData) -> "PortfolioSnapshot":
        """Convert a SnapshotData protobuf message to a PortfolioSnapshot."""
        from almanak.framework.portfolio.models import PortfolioSnapshot

        positions_payload = json.loads(data.positions_json.decode("utf-8")) if data.positions_json else []
        positions_list, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)

        # Extract accounting data from envelope (Phase 1c)
        token_prices: dict = {}
        wallet_balances_raw: list[dict] = []
        if isinstance(positions_payload, dict):
            token_prices = positions_payload.get("token_prices", {})
            wallet_balances_raw = positions_payload.get("wallet_balances", [])

        snapshot_dict = {
            "timestamp": datetime.fromtimestamp(data.timestamp, tz=UTC).isoformat(),
            "total_value_usd": data.total_value_usd or "0",
            "available_cash_usd": data.available_cash_usd or "0",
            "value_confidence": data.value_confidence or "HIGH",
            "error": None,
            "positions": positions_list,
            "wallet_balances": wallet_balances_raw,
            "token_prices": token_prices,
            "chain": data.chain or "",
            "iteration_number": data.iteration_number,
            "snapshot_metadata": snapshot_metadata,
            "deployment_id": getattr(data, "deployment_id", "") or "",
            "cycle_id": getattr(data, "cycle_id", "") or "",
            "execution_mode": getattr(data, "execution_mode", "") or "",
        }

        return PortfolioSnapshot.from_dict(snapshot_dict)

    # crap-allowlist: VIB-4196 — public-contract chokepoint, gRPC-marshalling sibling of SQLiteStore.save_accounting_event; cc=6 cov=4% are structural per .claude/rules/crap-refactor.md "undecomposable public contract / hot-path budget".
    async def save_accounting_event(self, event: "LendingAccountingEvent | PendleAccountingEvent") -> bool:
        """Save a typed accounting event via gateway gRPC → SQLite / PostgreSQL.

        Mirrors :meth:`save_ledger_entry` in error handling: non-blocking in
        non-live modes (logs warning, returns False); raises in live mode so the
        runner halts with ACCOUNTING_FAILED rather than silently dropping records.

        Args:
            event: A typed accounting event (LendingAccountingEvent or PendleAccountingEvent).

        Returns:
            True if the event was persisted successfully.
        """
        identity = event.identity
        is_live = getattr(identity, "execution_mode", "") == "live"
        try:
            # Augment payload before gRPC send so the version stamps + lending
            # aliases survive every downstream path (G13, L1/L4 — see
            # accounting.writer.augment_accounting_payload). Without this,
            # mainnet runs produced rows missing matching_policy_version
            # despite the SQLite chokepoint augmenter — the gateway routing
            # has multiple consumers, not all of which re-serialize through
            # SQLiteStore.save_accounting_event.
            #
            # Mode-aware error contract (VIB-3863): live raises
            # AccountingPersistenceError on a malformed payload so the runner
            # halts; paper/dry-run logs ERROR and pass-throughs.
            from ..accounting.writer import augment_accounting_payload

            # VIB-4278: the strategy-side gateway client cannot query
            # ``position_registry`` directly — it has no DB connection (that
            # lives on the gateway sidecar). The gateway-side registry
            # lookup over gRPC lands with T19 / VIB-4205. Until then we pass
            # ``registry_lookup=None`` and the chokepoint falls back to the
            # legacy reference. In LOCAL mode the registry stamp still
            # lands because the gateway sidecar's state_service re-augments
            # via ``SQLiteStore.save_accounting_event`` which DOES wire the
            # lookup (see ``SQLiteStore._build_registry_lookup_for_event``).
            # In HOSTED Postgres mode every accounting event ships
            # ``source="legacy"`` until T19 lands the gateway RPC + DDL.
            augmented = augment_accounting_payload(
                event.to_payload_json(),
                is_live=is_live,
                registry_lookup=None,
            )
            payload_bytes = augmented.encode("utf-8")
            # VIB-4196 / T10: position_reference column carried alongside
            # payload_json over the gateway gRPC. The hosted Postgres half
            # of this column lands in T19 / VIB-4205; until then the gateway
            # stores it on its local SQLite backend (unchanged code path
            # below — `payload_json` already carries the same JSON
            # sub-document, so a deferred migration can re-extract from
            # there if needed).
            request = gateway_pb2.SaveAccountingEventRequest(
                id=identity.id,
                deployment_id=identity.deployment_id,
                cycle_id=identity.cycle_id,
                execution_mode=identity.execution_mode,
                timestamp=int(identity.timestamp.timestamp()),
                chain=identity.chain,
                protocol=identity.protocol,
                wallet_address=identity.wallet_address,
                tx_hash=identity.tx_hash,
                ledger_entry_id=identity.ledger_entry_id,
                event_type=str(getattr(event, "event_type", "UNKNOWN")),
                position_key=getattr(event, "position_key", ""),
                confidence=str(event.confidence),
                payload_json=payload_bytes,
                schema_version=event.schema_version,
            )
            response = self._client.state.SaveAccountingEvent(request, timeout=self._timeout)
            if not response.success:
                logger.warning(
                    "SaveAccountingEvent failed: strategy=%s, id=%s, error=%s",
                    identity.deployment_id,
                    identity.id,
                    response.error,
                )
                if is_live:
                    raise AccountingPersistenceError(
                        write_kind=AccountingWriteKind.LEDGER,
                        deployment_id=identity.deployment_id,
                        message=f"SaveAccountingEvent failed: {response.error}",
                    )
                return False
            logger.debug(
                "Accounting event saved via gateway: strategy=%s, id=%s, type=%s",
                identity.deployment_id,
                identity.id,
                getattr(event, "event_type", ""),
            )
            return True
        except AccountingPersistenceError:
            raise
        except Exception as e:
            logger.warning("Failed to save accounting event via gateway: %s", e)
            if is_live:
                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.LEDGER,
                    deployment_id=getattr(identity, "deployment_id", ""),
                    cause=e,
                ) from e
            return False

    async def get_position_history(
        self,
        deployment_id: str,
        position_id: str,
    ) -> list[dict]:
        """Fetch full position lifecycle via gateway gRPC (VIB-3944).

        Returns events ordered chronologically (OPEN -> SNAPSHOT* -> CLOSE).
        Used by ``pnl_attributor.run_attribution_on_close`` and
        ``recompute_attribution`` to pair a CLOSE event with its matching
        OPEN for FIFO realised-PnL attribution.

        Read-side fail-quiet: on RPC error returns ``[]`` rather than
        raising. The caller (pnl_attributor) escalates a missing OPEN to a
        warning + skipped attribution rather than halting the runner — same
        contract as the prior in-process StateManager path.

        Args:
            deployment_id: Strategy deployment identifier (the runner-stable id).
            position_id: The position to query.

        Returns:
            List of position event dicts in chronological order. Empty list
            on RPC failure or when no events exist for the position.
        """
        deployment_id = (deployment_id or "").strip()
        position_id = (position_id or "").strip()
        if not deployment_id or not position_id:
            logger.warning(
                "get_position_history called with empty deployment_id=%r or position_id=%r — returning []",
                deployment_id,
                position_id,
            )
            return []

        try:
            request = gateway_pb2.GetPositionHistoryRequest(
                deployment_id=deployment_id,
                position_id=position_id,
            )
            response = self._client.state.GetPositionHistory(request, timeout=self._timeout)
            return [_proto_position_event_to_dict(e) for e in response.events]
        except Exception as e:
            # Bump from debug to warning (Claude pr-auditor Important #2):
            # the runner-side caller (pnl_attributor) will degrade to "no
            # OPEN found" if this returns []; a transient gRPC failure is
            # therefore invisible at debug level. Warning matches the
            # server-side log level for symmetry, and matches the
            # pnl_attributor convention (VIB-3205 audit fix Important #5).
            logger.warning("GetPositionHistory via gateway failed: %s", e)
            return []

    async def update_position_attribution(
        self,
        event_id: str,
        attribution_json: str,
        attribution_version: int,
        deployment_id: str = "",
    ) -> bool:
        """Partial-update of attribution columns via gateway gRPC (VIB-3944).

        Companion to :meth:`get_position_history`. Without this method,
        ``pnl_attributor.run_attribution_on_close`` falls back to
        :meth:`save_position_event` which is ``INSERT OR IGNORE`` and
        silently NO-OPs on the existing row — attribution_json never reaches
        disk in gateway-sidecar mode.

        Mirrors the SQLite signature so ``hasattr(store,
        "update_position_attribution")`` resolves True on a GSM-backed
        runner and the partial-update path is taken instead of the
        INSERT-OR-IGNORE fallback.

        Non-blocking write: returns False on RPC failure rather than
        raising. pnl_attributor wraps the call in a logged try/except.

        Args:
            event_id: UUID of the position_events row to update.
            attribution_json: Serialized attribution payload (overwrites column).
            attribution_version: Formula version stamp (``CURRENT_VERSION``).

        Returns:
            True iff the gateway reports a row was matched and updated.
        """
        event_id = (event_id or "").strip()
        if not event_id:
            logger.warning("update_position_attribution called with empty event_id — returning False")
            return False

        try:
            request = gateway_pb2.UpdatePositionAttributionRequest(
                event_id=event_id,
                attribution_json=attribution_json or "{}",
                attribution_version=int(attribution_version or 0),
                deployment_id=(deployment_id or "").strip(),
            )
            response = self._client.state.UpdatePositionAttribution(request, timeout=self._timeout)
            if not response.success and response.error:
                logger.warning(
                    "UpdatePositionAttribution returned success=False for event_id=%s: %s",
                    event_id,
                    response.error,
                )
            return bool(response.success)
        except Exception as e:
            logger.warning(
                "UpdatePositionAttribution via gateway failed for event_id=%s: %s",
                event_id,
                e,
            )
            return False

    async def save_position_event(self, event: "PositionEvent") -> bool:
        """Save a position lifecycle event via gateway gRPC → SQLite / PostgreSQL.

        Non-blocking write: logs a warning on failure and returns False rather
        than raising, since position events are observability data and should
        not halt the strategy loop on transient errors.

        Args:
            event: PositionEvent to persist.

        Returns:
            True if the event was persisted successfully.
        """
        try:
            request = gateway_pb2.SavePositionEventRequest(
                id=event.id,
                deployment_id=event.deployment_id,
                cycle_id=getattr(event, "cycle_id", "") or "",
                execution_mode=getattr(event, "execution_mode", "") or "",
                position_id=event.position_id,
                position_type=event.position_type,
                event_type=event.event_type,
                timestamp=int(event.timestamp.timestamp()),
                protocol=event.protocol,
                chain=event.chain,
                token0=event.token0,
                token1=event.token1,
                amount0=event.amount0,
                amount1=event.amount1,
                value_usd=event.value_usd,
                liquidity=event.liquidity,
                fees_token0=event.fees_token0,
                fees_token1=event.fees_token1,
                leverage=event.leverage,
                entry_price=event.entry_price,
                mark_price=event.mark_price,
                unrealized_pnl=event.unrealized_pnl,
                tx_hash=event.tx_hash,
                gas_usd=event.gas_usd,
                ledger_entry_id=event.ledger_entry_id,
                protocol_fees_usd=(
                    "" if getattr(event, "protocol_fees_usd", None) is None else event.protocol_fees_usd
                ),
                attribution_json=event.attribution_json or "{}",
                attribution_version=event.attribution_version,
            )
            # Set optional proto fields only when the source has them set (None = absent on wire)
            if event.tick_lower is not None:
                request.tick_lower = event.tick_lower
            if event.tick_upper is not None:
                request.tick_upper = event.tick_upper
            if event.in_range is not None:
                request.in_range = event.in_range
            if event.is_long is not None:
                request.is_long = event.is_long

            response = self._client.state.SavePositionEvent(request, timeout=self._timeout)
            if not response.success:
                logger.warning(
                    "SavePositionEvent failed: id=%s, position=%s, error=%s",
                    event.id,
                    event.position_id,
                    response.error,
                )
                return False
            logger.debug(
                "Position event saved via gateway: id=%s, type=%s, position=%s",
                event.id,
                event.event_type,
                event.position_id,
            )
            return True
        except Exception as e:
            logger.warning("Failed to save position event via gateway: %s", e)
            return False

    # Optional string-serialised fields on PositionStateSnapshotRow — bulk-set
    # via getattr/setattr in ``_position_state_row_to_proto``. Hoisted to
    # class scope so the tuple isn't re-allocated per row in a hot bulk save
    # (Gemini, 2026-05-17).
    _POSITION_STATE_OPTIONAL_STR_FIELDS: ClassVar[tuple[str, ...]] = (
        "liquidity",
        "sqrt_price_x96",
        "supply_balance",
        "borrow_balance",
        "health_factor",
        "supply_apy_pct",
        "borrow_apy_pct",
        "interest_accrued_since_last",
        "mark_price",
        "unrealized_pnl",
        "funding_accrued_since_last",
        "liquidation_price",
        "margin_utilisation_pct",
        "delta_vs_protocol_pct",
    )

    @classmethod
    def _position_state_row_to_proto(cls, r: Any) -> "gateway_pb2.PositionStateSnapshotRow":
        """Map a ``PositionStateRow`` dataclass to its proto wire shape.

        Each nullable field is only set when the source value is non-None
        so the wire preserves "unmeasured" (HasField==False) vs "measured
        zero" (HasField==True, value="0") per CLAUDE.md §Accounting
        "Empty != Zero". Extracted from
        :meth:`save_position_state_snapshots` to keep that caller's
        cyclomatic complexity in check — it would otherwise be a flat
        16-branch optional-field setter ladder.
        """
        proto_row = gateway_pb2.PositionStateSnapshotRow(
            deployment_id=r.deployment_id,
            cycle_id=r.cycle_id,
            captured_at=r.timestamp.isoformat() if r.timestamp else "",
            position_id=r.position_id,
            position_type=r.position_type,
            value_confidence=r.value_confidence,
            schema_version=int(r.schema_version),
            formula_version=int(r.formula_version),
            matching_policy_version=int(r.matching_policy_version),
        )
        # Optional integer + bool — typed setters; checking via "is not None"
        # is the only way to distinguish missing vs 0 / False.
        if r.current_tick is not None:
            proto_row.current_tick = int(r.current_tick)
        if r.in_range is not None:
            proto_row.in_range = bool(r.in_range)
        # Optional string (Decimal/int serialised) — uniform str(...) cast over
        # the class-level tuple so the method's CC stays linear in the field
        # count rather than branchy.
        for fname in cls._POSITION_STATE_OPTIONAL_STR_FIELDS:
            v = getattr(r, fname, None)
            if v is not None:
                setattr(proto_row, fname, str(v))
        return proto_row

    async def save_position_state_snapshots(
        self,
        snapshot_id: int,
        rows: list,
    ) -> int:
        """Track-C bulk write of per-iteration position state rows (VIB-4541).

        Routes the runner's ``_persist_position_state_snapshots`` call
        (runner_state.py:565) through the gateway's
        ``SavePositionStateSnapshots`` RPC, which delegates to the warm
        backend's ``save_position_state_snapshots`` (SQLite implementation
        at sqlite.py:2686). Returns the number of rows written.

        Capability semantics — gRPC UNIMPLEMENTED on the wire (the hosted
        Postgres warm backend lacks the method until the metrics-database
        migration lands, PRD T-DRAFT-25) is mapped to a silent ``return 0``
        so the runner's deployment-time capability gate at
        runner_state.py:480 stays observationally identical to the
        pre-RPC behaviour for hosted runs. Other gRPC errors propagate as
        exceptions so the runner's live-mode handler can convert them
        into ``AccountingPersistenceError`` and halt with
        ACCOUNTING_FAILED rather than masking a real backend regression
        as "0 rows written".

        Empty ``rows`` returns 0 without sending an RPC.
        """
        if not rows:
            return 0

        proto_rows = [self._position_state_row_to_proto(r) for r in rows]
        request = gateway_pb2.SavePositionStateSnapshotsRequest(
            snapshot_id=int(snapshot_id),
            rows=proto_rows,
        )

        try:
            response = self._client.state.SavePositionStateSnapshots(request, timeout=self._timeout)
        except grpc.RpcError as e:
            # Hosted Postgres warm backend doesn't yet implement the method
            # (PRD T-DRAFT-25, Infra-owned metrics-database migration). The
            # server returns UNIMPLEMENTED; degrade to the capability-gate
            # equivalent (silent zero) so the cell read side keeps reporting
            # XFAIL rather than the runner's live-mode handler converting
            # this into AccountingPersistenceError. All other gRPC errors
            # propagate so the runner can decide between halt and log per
            # execution mode.
            code = e.code() if callable(getattr(e, "code", None)) else None
            if code == grpc.StatusCode.UNIMPLEMENTED:
                logger.debug(
                    "SavePositionStateSnapshots returned UNIMPLEMENTED — backend lacks Track-C "
                    "support (hosted PG pre-metrics-database migration); skipping %d rows for "
                    "snapshot_id=%d.",
                    len(proto_rows),
                    snapshot_id,
                )
                return 0
            raise

        if not response.success:
            logger.warning(
                "SavePositionStateSnapshots returned success=False for snapshot_id=%d: %s",
                snapshot_id,
                response.error,
            )
            return 0
        return int(response.rows_written)

    # -------------------------------------------------------------------------
    # Cutover storage — VIB-4208 / T22 (SQLite half of T19 / VIB-4205).
    # -------------------------------------------------------------------------
    #
    # Round-trips through the gateway's State RPCs. The gateway routes to
    # the WARM backend's SQLite implementation; on Postgres the gateway
    # returns gRPC UNIMPLEMENTED and these adapters translate that back
    # into :class:`almanak.framework.migration.CutoverStorageNotSupported`
    # so the cutover boot guard degrades cleanly on hosted runs (cutover
    # spec §2.4 pre-T19 contract).
    #
    # The Postgres half + the cross-repo metrics-database migration land
    # in T19 (VIB-4205). One pair of dataclasses (``MigrationStateRow``,
    # ``RegistryRow`` dict-shape) is reconstructed from the wire bytes here
    # so the runner sees an identical surface across local and gateway-
    # backed runs.
    #
    # NOTE on `insert_position_registry_row_if_absent`: this method is
    # only called by `BackfillReader.run()` when there is legacy
    # position_events data to migrate. For fresh deployments
    # (Anvil + lp_dual) there is no legacy data, so the backfill loop
    # iterates zero groups and never calls this. T19's Postgres half
    # will ship the corresponding RPC when legacy-data backfill on
    # hosted backends becomes needed.

    @staticmethod
    def _translate_unimplemented(exc: Exception) -> Exception:
        """Map gRPC UNIMPLEMENTED → CutoverStorageNotSupported.

        Audit M3: a backend that does not host cutover storage must
        signal its absence via :class:`CutoverStorageNotSupported`, not
        via a silent empty result or a generic RpcError. The cutover
        boot guard catches that specific exception class and degrades
        controlled-ly (cutover spec §2.4). Any other gRPC error
        propagates unchanged — those are loud infrastructure failures
        the runner must halt on.
        """
        from almanak.framework.migration import CutoverStorageNotSupported

        code = getattr(exc, "code", None)
        if callable(code):
            try:
                status = code()
                if status == grpc.StatusCode.UNIMPLEMENTED:
                    details = getattr(exc, "details", None)
                    details_str = details() if callable(details) else str(exc)
                    return CutoverStorageNotSupported(
                        f"GatewayStateManager: gateway returned UNIMPLEMENTED — {details_str}"
                    )
            except Exception:  # noqa: BLE001 — defensive
                pass
        return exc

    async def upsert_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> None:
        """Idempotent baseline migration_state row (cutover spec §2.1).

        Raises :class:`CutoverStorageNotSupported` when the gateway's
        WARM backend is Postgres (T19 / VIB-4205 ships the hosted half).
        Other failures propagate as the underlying gRPC error so the
        runner halts loud per the cutover spec.
        """
        request = gateway_pb2.UpsertMigrationStateRequest(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
        )
        try:
            response = self._client.state.UpsertMigrationState(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        if not response.success:
            from almanak.framework.migration import CutoverStorageNotSupported

            # An explicit failure return without UNIMPLEMENTED is still
            # treated as a degrade signal: every server-side path that
            # returns success=False is either a contract violation
            # (caller bug) or the documented Postgres-not-supported path.
            raise CutoverStorageNotSupported(f"GatewayStateManager.upsert_migration_state failed: {response.error}")

    async def get_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> Any | None:
        """Return the parsed ``MigrationStateRow``, or ``None`` when absent.

        Postgres backend raises :class:`CutoverStorageNotSupported` via
        the gRPC UNIMPLEMENTED translation; the runner's boot guard
        catches it and degrades to the legacy path.
        """
        from almanak.framework.migration.backfill import MigrationStateRow

        request = gateway_pb2.GetMigrationStateRequest(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
        )
        try:
            response = self._client.state.GetMigrationState(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        if not response.found:
            return None
        data = response.data
        try:
            notes_obj = json.loads(data.notes.decode("utf-8")) if data.notes else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            notes_obj = {}
        if not isinstance(notes_obj, dict):
            notes_obj = {}
        return MigrationStateRow(
            deployment_id=data.deployment_id,
            primitive=data.primitive,
            cutover_key=data.cutover_key,
            position_registry_backfill_complete=bool(data.position_registry_backfill_complete),
            backfill_started_at=data.backfill_started_at or None,
            backfill_completed_at=data.backfill_completed_at or None,
            backfill_source_table=data.backfill_source_table or "position_events",
            backfill_reader_version=int(data.backfill_reader_version or 1),
            rows_synthesized=int(data.rows_synthesized or 0),
            rows_skipped_already_present=int(data.rows_skipped_already_present or 0),
            notes=notes_obj,
            created_at=data.created_at or "",
            updated_at=data.updated_at or "",
        )

    async def update_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_started_at: str | None = None,
        rows_synthesized: int | None = None,
        rows_skipped_already_present: int | None = None,
    ) -> None:
        """Partial-update of migration_state in-flight progress columns."""
        request = gateway_pb2.UpdateMigrationStateRequest(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            backfill_started_at=backfill_started_at or "",
        )
        if rows_synthesized is not None:
            request.rows_synthesized = int(rows_synthesized)
        if rows_skipped_already_present is not None:
            request.rows_skipped_already_present = int(rows_skipped_already_present)
        try:
            response = self._client.state.UpdateMigrationState(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        if not response.success:
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(f"GatewayStateManager.update_migration_state failed: {response.error}")

    async def mark_backfill_complete(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        rows_synthesized: int,
        rows_skipped_already_present: int,
        backfill_completed_at: str,
    ) -> None:
        """Terminal flip — set ``complete=1`` + final counters + completed_at."""
        request = gateway_pb2.MarkBackfillCompleteRequest(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            rows_synthesized=int(rows_synthesized),
            rows_skipped_already_present=int(rows_skipped_already_present),
            backfill_completed_at=backfill_completed_at,
        )
        try:
            response = self._client.state.MarkBackfillComplete(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        if not response.success:
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(f"GatewayStateManager.mark_backfill_complete failed: {response.error}")

    async def get_position_events_filtered(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
    ) -> list[dict]:
        """Streamed position_events read used by the backfill loop's fold pass.

        Returns rows in (position_id ASC, timestamp ASC, id ASC) order
        per the SQLite accessor's contract (cutover spec §3.5 fold
        determinism). Empty list when there are no rows matching the
        filter (fresh deployment → no legacy data → backfill is a
        no-op).
        """
        request = gateway_pb2.GetPositionEventsFilteredRequest(
            deployment_id=deployment_id,
            position_types=sorted(position_types),
        )
        try:
            response = self._client.state.GetPositionEventsFiltered(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        return [_proto_position_event_to_dict(ev) for ev in response.events]

    @staticmethod
    def _decode_position_registry_payload(payload_bytes: bytes) -> tuple[Any, str, str, str]:
        """Decode the proto ``payload`` bytes into ``(obj, raw, decode_error, shape_error)``.

        Mirrors the SQLite-backend's diagnostic fields so a malformed
        wire payload preserves an audit signal rather than silently
        producing ``{}``.
        """
        if not payload_bytes:
            return {}, "", "", ""
        try:
            parsed = json.loads(payload_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {}, payload_bytes.decode("utf-8", errors="replace"), str(exc), ""
        if isinstance(parsed, dict):
            return parsed, "", "", ""
        return (
            {},
            payload_bytes.decode("utf-8", errors="replace"),
            "",
            f"expected JSON object, got {type(parsed).__name__}",
        )

    @staticmethod
    def _attach_payload_diagnostics(
        entry: dict[str, Any],
        row: Any,
        payload_raw: str,
        payload_decode_error: str,
        payload_shape_error: str,
    ) -> None:
        """Surface raw/decode/shape diagnostics on the result dict.

        Server-side fields take precedence; client-side decoded values
        are the fallback when the server did not stamp them (older
        gateway builds, or local SQLite already rendered them).
        Iterating the (server, client, key) tuples keeps the helper
        flat (no per-field if-tree) so CRAP scores cleanly.
        """
        fields = (
            ("payload_raw", row.payload_raw, payload_raw),
            ("payload_decode_error", row.payload_decode_error, payload_decode_error),
            ("payload_shape_error", row.payload_shape_error, payload_shape_error),
        )
        for key, server_val, client_val in fields:
            chosen = server_val or client_val
            if chosen:
                entry[key] = chosen

    @classmethod
    def _proto_row_to_registry_dict(cls, row: Any) -> dict[str, Any]:
        """Map a single ``PositionRegistryRow`` proto message to a dict.

        Pure projection; ``payload`` is JSON-decoded via
        :meth:`_decode_position_registry_payload` so the runner sees the
        same shape it would see from the SQLite-direct path.
        """
        payload_obj, payload_raw, payload_decode_error, payload_shape_error = cls._decode_position_registry_payload(
            row.payload
        )
        entry: dict[str, Any] = {
            "deployment_id": row.deployment_id,
            "chain": row.chain,
            "primitive": row.primitive,
            "accounting_category": row.accounting_category,
            "physical_identity_hash": row.physical_identity_hash,
            "semantic_grouping_key": row.semantic_grouping_key,
            "grouping_policy_version": row.grouping_policy_version,
            "handle": row.handle or None,
            "status": row.status,
            "payload": payload_obj,
            "opened_at_block": row.opened_at_block or None,
            "opened_tx": row.opened_tx or None,
            "closed_at_block": row.closed_at_block or None,
            "closed_tx": row.closed_tx or None,
            "last_reconciled_at_block": row.last_reconciled_at_block or None,
            "matching_policy_version": int(row.matching_policy_version),
        }
        cls._attach_payload_diagnostics(entry, row, payload_raw, payload_decode_error, payload_shape_error)
        return entry

    async def get_position_registry_open_rows(
        self,
        deployment_id: str,
        *,
        chain: str | None = None,
        primitive: str | None = None,
        accounting_category: str | None = None,
    ) -> list[dict]:
        """Return OPEN ``position_registry`` rows for a deployment.

        The wire shape carries the JSON-encoded payload; this adapter
        parses it back to a dict so the runner sees a result identical
        to the SQLite-direct path. Postgres backend raises
        :class:`CutoverStorageNotSupported` via the UNIMPLEMENTED
        translation.

        Row-level conversion is delegated to
        :meth:`_proto_row_to_registry_dict` so this orchestrator stays
        small. Payload-decode + diagnostic stamping live in the two
        helpers above.
        """
        request = gateway_pb2.GetPositionRegistryOpenRowsRequest(
            deployment_id=deployment_id,
            chain=chain or "",
            primitive=primitive or "",
            accounting_category=accounting_category or "",
        )
        try:
            response = self._client.state.GetPositionRegistryOpenRows(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e
        return [self._proto_row_to_registry_dict(row) for row in response.rows]

    @staticmethod
    def _build_save_ledger_and_registry_request(
        ledger: "LedgerEntry",
        registry: Any,
        handle: Any,
        mode: str = "commit",
    ) -> gateway_pb2.SaveLedgerAndRegistryRequest:
        """Marshal LedgerEntry + RegistryRow + optional HandleMapping → proto.

        ``ledger`` is a dataclass — fields are declared and present, so
        direct attribute access is correct. Optional integer columns
        (slippage_bps, opened_at_block, closed_at_block,
        last_reconciled_at_block) are set post-construction via the
        proto's ``HasField`` semantics so unset values stay unset
        rather than collapsing to zero.
        """

        def _str_or_empty(val: Any) -> str:
            if val is None:
                return ""
            if isinstance(val, str):
                return val
            return str(val)

        def _bytes_or_empty(val: Any) -> bytes:
            if val is None:
                return b""
            if isinstance(val, bytes):
                return val
            return str(val).encode("utf-8")

        handle_handle = getattr(handle, "handle", "") if handle is not None else ""
        handle_deployment_id = getattr(handle, "deployment_id", "") if handle is not None else ""
        handle_accounting_category = str(getattr(handle, "accounting_category", "")) if handle is not None else ""

        request = gateway_pb2.SaveLedgerAndRegistryRequest(
            id=ledger.id,
            cycle_id=ledger.cycle_id or "",
            deployment_id=ledger.deployment_id or "",
            execution_mode=ledger.execution_mode or "",
            timestamp=int(ledger.timestamp.timestamp()),
            intent_type=_str_or_empty(ledger.intent_type),
            token_in=_str_or_empty(ledger.token_in),
            amount_in=_str_or_empty(ledger.amount_in),
            token_out=_str_or_empty(ledger.token_out),
            amount_out=_str_or_empty(ledger.amount_out),
            effective_price=_str_or_empty(ledger.effective_price),
            gas_used=int(ledger.gas_used or 0),
            gas_usd=_str_or_empty(ledger.gas_usd),
            tx_hash=_str_or_empty(ledger.tx_hash),
            chain=_str_or_empty(ledger.chain),
            protocol=_str_or_empty(ledger.protocol),
            success=bool(ledger.success),
            error=_str_or_empty(ledger.error),
            extracted_data_json=_bytes_or_empty(ledger.extracted_data_json),
            price_inputs_json=_bytes_or_empty(ledger.price_inputs_json),
            pre_state_json=_bytes_or_empty(ledger.pre_state_json),
            post_state_json=_bytes_or_empty(ledger.post_state_json),
            registry_chain=registry.chain,
            registry_primitive=registry.primitive_value(),
            registry_accounting_category=registry.accounting_category_value(),
            registry_physical_identity_hash=registry.physical_identity_hash,
            registry_semantic_grouping_key=registry.semantic_grouping_key,
            registry_grouping_policy_version=registry.grouping_policy_version,
            registry_handle=registry.handle or "",
            registry_status=registry.status,
            registry_payload_json=registry.payload_json().encode("utf-8"),
            registry_matching_policy_version=int(registry.matching_policy_version),
            registry_opened_tx=registry.opened_tx or "",
            registry_closed_tx=registry.closed_tx or "",
            handle_mapping_handle=handle_handle,
            handle_mapping_deployment_id=handle_deployment_id,
            handle_mapping_accounting_category=handle_accounting_category,
            # T24 / VIB-4210: proto3 default "" + server-side normalization
            # to "commit" means existing callers (who don't pass mode) keep
            # bit-identical wire behaviour.
            mode=mode if mode != "commit" else "",
        )
        if ledger.slippage_bps is not None:
            request.slippage_bps = float(ledger.slippage_bps)
        if registry.opened_at_block is not None:
            request.registry_opened_at_block = int(registry.opened_at_block)
        if registry.closed_at_block is not None:
            request.registry_closed_at_block = int(registry.closed_at_block)
        if registry.last_reconciled_at_block is not None:
            request.registry_last_reconciled_at_block = int(registry.last_reconciled_at_block)
        return request

    @staticmethod
    def _raise_for_save_ledger_and_registry_response(
        response: Any,
        registry: Any,
        deployment_id: str,
    ) -> None:
        """Translate a non-success response to the right typed exception.

        Discriminates VIB-4200 collisions (RegistryAutoCollisionError),
        Postgres-degrade signals (CutoverStorageNotSupported via
        UNIMPLEMENTED), and generic infra failures
        (AccountingPersistenceError). See the in-line note on the
        ``existing_physical_identity_hash`` sentinel — the rendered
        message preserves the structured PIH text via the chained
        ``__cause__``.
        """
        from almanak.framework.state.registry_errors import RegistryAutoCollisionError

        if response.error_class == "RegistryAutoCollisionError":
            raise RegistryAutoCollisionError(
                semantic_grouping_key=registry.semantic_grouping_key,
                existing_physical_identity_hash="<unavailable-from-gateway: see error message>",
                opened_tx="",
                accounting_category=registry.accounting_category_value(),
            ) from RuntimeError(response.error)
        if response.error_class == "UNIMPLEMENTED":
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(f"GatewayStateManager.save_ledger_and_registry: {response.error}")
        raise AccountingPersistenceError(
            write_kind=AccountingWriteKind.ACCOUNTING,
            deployment_id=deployment_id,
            message=f"SaveLedgerAndRegistry failed via gateway: {response.error or response.error_class}",
        )

    async def save_ledger_and_registry(
        self,
        *,
        ledger: "LedgerEntry",
        registry: Any,  # RegistryRow — lazy import to avoid module-load cycle
        handle: Any = None,  # HandleMapping | None
        mode: str = "commit",
    ) -> None:
        """Atomic ledger + position_registry + handle commit (T11 / VIB-4197).

        Mirrors ``StateManager.save_ledger_and_registry`` on the local
        path. Failures preserve T11's error contract:

        - ``RegistryAutoCollisionError`` (VIB-4200 programming bug)
          propagates as that exact class.
        - ``AccountingPersistenceError`` propagates with
          ``write_kind=ACCOUNTING``.
        - Other backend errors wrap in ``AccountingPersistenceError``.

        Postgres backend raises :class:`CutoverStorageNotSupported` via
        the UNIMPLEMENTED translation. Request marshalling and response
        error translation live in :meth:`_build_save_ledger_and_registry_request`
        and :meth:`_raise_for_save_ledger_and_registry_response`.
        """
        deployment_id = ledger.deployment_id or ""
        request = self._build_save_ledger_and_registry_request(ledger, registry, handle, mode)

        try:
            response = self._client.state.SaveLedgerAndRegistry(request, timeout=self._timeout)
        except Exception as e:
            raise self._translate_unimplemented(e) from e

        if response.success:
            logger.debug(
                "SaveLedgerAndRegistry ok: id=%s strategy=%s pih=%s",
                ledger.id,
                deployment_id,
                registry.physical_identity_hash,
            )
            return

        self._raise_for_save_ledger_and_registry_response(response, registry, deployment_id)

    async def insert_position_registry_row_if_absent(self, *, row: Any) -> bool:
        """Not wired for T22 — legacy-data backfill on the gateway lands with T19.

        The backfill loop only calls this method when ``get_position_events_filtered``
        returns non-empty results. For fresh deployments (the lp_dual /
        managed-Anvil scope of T22) the event list is empty, the group
        loop iterates zero times, and this method is never reached. T19
        / VIB-4205 ships the hosted backfill (including this RPC) when
        legacy-data migration on hosted Postgres becomes necessary.
        """
        from almanak.framework.migration import CutoverStorageNotSupported

        raise CutoverStorageNotSupported(
            "GatewayStateManager.insert_position_registry_row_if_absent: "
            "legacy-data backfill on the gateway lands with T19 / VIB-4205. "
            "Fresh deployments (lp_dual / managed-Anvil) never reach this path."
        )

    # -------------------------------------------------------------------------
    # Accounting outbox — gateway gRPC layer (SaveOutboxEntry / GetOutboxEntry /
    # GetOutboxPending / UpdateOutboxEntry). DDL landed in metrics-database PR #24.
    # -------------------------------------------------------------------------

    async def save_outbox_entry(
        self,
        outbox_id: str,
        deployment_id: str,
        cycle_id: str,
        ledger_entry_id: str,
        intent_type: str,
        wallet_address: str,
        position_key: str,
        market_id: str,
        created_at: str,
    ) -> None:
        """Write one accounting_outbox row via gateway gRPC (idempotent INSERT OR IGNORE).

        Raises AccountingPersistenceError on RPC failure so write_outbox_entry()
        can propagate it as a fail-closed error in live mode.
        """
        request = gateway_pb2.SaveOutboxEntryRequest(
            outbox_id=outbox_id,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            ledger_entry_id=ledger_entry_id,
            intent_type=intent_type,
            wallet_address=wallet_address,
            position_key=position_key,
            market_id=market_id,
            created_at=created_at,
        )
        try:
            response = self._client.state.SaveOutboxEntry(request, timeout=self._timeout)
        except Exception as e:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.OUTBOX,
                deployment_id=deployment_id,
                cause=e,
            ) from e
        if not response.success:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.OUTBOX,
                deployment_id=deployment_id,
                cause=Exception(response.error),
            )

    async def get_outbox_by_ledger_id(self, ledger_entry_id: str) -> dict | None:
        """Fetch the outbox row for a ledger entry via gateway gRPC, or None."""
        try:
            request = gateway_pb2.GetOutboxEntryRequest(ledger_entry_id=ledger_entry_id)
            response = self._client.state.GetOutboxEntry(request, timeout=self._timeout)
            if not response.found:
                return None
            e = response.entry
            return {
                "id": e.id,
                "deployment_id": e.deployment_id,
                "cycle_id": e.cycle_id,
                "ledger_entry_id": e.ledger_entry_id,
                "intent_type": e.intent_type,
                "wallet_address": e.wallet_address,
                "position_key": e.position_key,
                "market_id": e.market_id,
                "status": e.status,
                "attempts": e.attempts,
                "error": e.error,
                "created_at": e.created_at,
                "updated_at": e.updated_at,
            }
        except Exception as exc:
            logger.warning("get_outbox_by_ledger_id failed: %s", exc)
            return None

    async def get_outbox_pending(self, deployment_id: str, max_retries: int = 3) -> list[dict]:
        """Return pending/failed/stuck-processing outbox rows via gateway gRPC."""
        try:
            request = gateway_pb2.GetOutboxPendingRequest(deployment_id=deployment_id, max_retries=max_retries)
            response = self._client.state.GetOutboxPending(request, timeout=self._timeout)
            result = []
            for e in response.entries:
                result.append(
                    {
                        "id": e.id,
                        "deployment_id": e.deployment_id,
                        "cycle_id": e.cycle_id,
                        "ledger_entry_id": e.ledger_entry_id,
                        "intent_type": e.intent_type,
                        "wallet_address": e.wallet_address,
                        "position_key": e.position_key,
                        "market_id": e.market_id,
                        "status": e.status,
                        "attempts": e.attempts,
                        "error": e.error,
                        "created_at": e.created_at,
                        "updated_at": e.updated_at,
                    }
                )
            return result
        except Exception as exc:
            logger.warning("get_outbox_pending failed: %s", exc)
            return []

    async def update_outbox_entry(
        self, outbox_id: str, status: str, error: str = "", attempts: int | None = None
    ) -> None:
        """Update outbox row status via gateway gRPC. Logs on failure (non-blocking)."""
        try:
            request = gateway_pb2.UpdateOutboxEntryRequest(
                outbox_id=outbox_id,
                status=status,
                error=error,
            )
            if attempts is not None:
                request.attempts = attempts
            response = self._client.state.UpdateOutboxEntry(request, timeout=self._timeout)
            if not response.success:
                logger.warning("UpdateOutboxEntry failed for outbox_id=%s: %s", outbox_id, response.error)
        except Exception as exc:
            logger.warning("update_outbox_entry failed for outbox_id=%s: %s", outbox_id, exc)

    async def has_accounting_events_for_ledger(self, ledger_entry_id: str) -> bool:
        """Return True if accounting_events already has a row for this ledger entry.

        Raises on RPC failure rather than returning False. Returning False on error
        would conflate "no row" with "lookup failed" and risk re-processing an already
        written ledger entry (duplicate accounting events). The caller (drain_one) leaves
        the outbox row in 'processing' status so the retry loop can attempt again.
        """
        request = gateway_pb2.HasAccountingEventsForLedgerRequest(ledger_entry_id=ledger_entry_id)
        response = self._client.state.HasAccountingEventsForLedger(request, timeout=self._timeout)
        return bool(response.has_events)

    async def get_ledger_entry_by_id(self, ledger_entry_id: str) -> dict | None:
        """Fetch a transaction_ledger row by id via gateway gRPC for AccountingProcessor."""
        try:
            request = gateway_pb2.GetLedgerEntryRequest(ledger_entry_id=ledger_entry_id)
            response = self._client.state.GetLedgerEntry(request, timeout=self._timeout)
            if not response.found:
                return None
            e = response.entry

            def _decode(b: bytes) -> str:
                return b.decode("utf-8") if b else ""

            row: dict = {
                "id": e.id,
                "cycle_id": e.cycle_id,
                "deployment_id": e.deployment_id,
                "execution_mode": e.execution_mode,
                # Category handlers parse timestamp as ISO string (ts_str.replace(...)).
                # The proto carries epoch seconds (0 when unset); always return ISO so
                # handlers never fall through to datetime.now(UTC) and corrupt timestamps.
                "timestamp": datetime.fromtimestamp(e.timestamp or 0, UTC).isoformat(),
                "intent_type": e.intent_type,
                "token_in": e.token_in,
                "amount_in": e.amount_in,
                "token_out": e.token_out,
                "amount_out": e.amount_out,
                "effective_price": e.effective_price,
                "gas_used": e.gas_used,
                "gas_usd": e.gas_usd,
                "tx_hash": e.tx_hash,
                "chain": e.chain,
                "protocol": e.protocol,
                "success": e.success,
                "error": e.error,
                "extracted_data_json": _decode(e.extracted_data_json),
                "price_inputs_json": _decode(e.price_inputs_json),
                "pre_state_json": _decode(e.pre_state_json),
                "post_state_json": _decode(e.post_state_json),
            }
            if e.HasField("slippage_bps"):
                row["slippage_bps"] = e.slippage_bps
            return row
        except Exception as exc:
            logger.warning("get_ledger_entry_by_id failed for %s: %s", ledger_entry_id, exc)
            return None

    def get_accounting_events_sync(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> list[dict]:
        """Read typed accounting events via gateway gRPC → Postgres / SQLite.

        Mirrors :meth:`SQLiteStore.get_accounting_events_sync` so callers
        (``PortfolioValuer`` cost-basis enrichment, ``_run_loop_helpers``
        FIFO basis-store reconstruction at startup) can swap backends
        without code changes.

        Read-side fail-quiet: on gRPC error returns ``[]`` rather than
        raising. Stale PnL is preferred over halting snapshot building.

        Note on ``deployment_id`` over the wire: the gRPC contract still
        requires a deployment_id field for format validation. We pass
        ``deployment_id`` as that wire value until the proto rename lands.

        Args:
            deployment_id: Strategy deployment identifier.
            position_key: Optional filter by position_key.

        Returns:
            List of dicts shaped like ``SQLiteStore.get_accounting_events_sync``
            so PortfolioValuer's duck-typed access (``e.get("event_type")``,
            ``e.get("payload_json")`` etc.) is unchanged.
        """
        rows, _measured = self._fetch_accounting_events(deployment_id, position_key)
        return rows

    def _fetch_accounting_events(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> tuple[list[dict], bool]:
        """Single gateway round-trip for accounting events + a MEASURED flag.

        Shared engine for :meth:`get_accounting_events_sync` (which drops the
        flag, keeping its ``[]``-on-any-failure contract) and
        :meth:`read_accounting_events_measured` (which surfaces it).

        Returns ``(rows, measured)`` where ``measured`` is ``True`` ONLY when
        the gateway reports ``ACCOUNTING_BACKEND_STATUS_AVAILABLE`` — backend
        present AND the read succeeded, so an empty ``rows`` is a real measured
        zero. Every other status (VIB-5185: ``ABSENT`` / ``ERRORED``, the
        UNSPECIFIED default of an OLD gateway that predates the signal, or a
        transport-level gRPC error) yields ``measured=False`` — UNMEASURED, the
        Empty ≠ Zero fail-closed direction. Never raises.
        """
        deployment_id = (deployment_id or "").strip()
        if not deployment_id:
            logger.warning("GetAccountingEvents called with empty deployment_id — returning []")
            # An empty deployment id is a caller error, not a backend read —
            # UNMEASURED, so the swap-back clamp fails closed rather than
            # treating it as a measured-zero inventory.
            return [], False
        try:
            request = gateway_pb2.GetAccountingEventsRequest(
                deployment_id=deployment_id,
                position_key=position_key or "",
            )
            response = self._client.state.GetAccountingEvents(request, timeout=self._timeout)
            rows = [_proto_event_to_dict(e) for e in response.events]
            # Stable ordering for deterministic FIFO replay; secondary key on id breaks timestamp ties.
            rows.sort(key=lambda r: (r.get("timestamp") or "", r.get("id") or ""))
            # VIB-5185: trust an empty list as a measured zero ONLY when the
            # gateway affirmatively reports AVAILABLE. UNSPECIFIED (old gateway),
            # ABSENT, and ERRORED all mean UNMEASURED.
            measured = response.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE
            return rows, measured
        except Exception as e:
            # VIB-3944: was debug; bumped to warning so an empty result on a
            # startup-critical call (FIFO basis-store reconstruction, lending
            # PnL enrichment) is visible to operators without re-instrumenting.
            # The fail-quiet contract still holds — we still return [] — but
            # the operator can now see WHY the downstream caller saw an empty
            # event list.
            logger.warning("GetAccountingEvents via gateway failed: %s", e)
            # Transport-level failure → UNMEASURED.
            return [], False

    def read_accounting_events_measured(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> tuple[list[dict], bool]:
        """Accounting events plus whether the read is MEASURED (VIB-5185).

        Returns ``(rows, measured)``. ``measured=True`` means the gateway
        reported ``ACCOUNTING_BACKEND_STATUS_AVAILABLE`` (backend present AND
        the read succeeded), so an empty ``rows`` is an authoritative zero.
        ``measured=False`` means UNMEASURED — backend structurally absent (e.g.
        hosted before the metrics-database migration), the read errored, or an
        old gateway that does not emit the signal.

        This is the production wiring for the ALM-2766 / VIB-5173 teardown
        swap-back clamp: it carries the absent-vs-errored-vs-empty distinction
        end-to-end over the gateway proto, in the SAME read that returns the
        events (no separate structural probe, so no TOCTOU and a single
        round-trip). Callers MUST map ``measured=False`` to the UNMEASURED
        ``None`` sentinel (fail closed → ``accounting_degraded``), never to
        measured-zero inventory. Never raises.
        """
        return self._fetch_accounting_events(deployment_id, position_key)

    def read_ledger_entries_measured(
        self,
        deployment_id: str,
    ) -> tuple[list[dict], bool]:
        """transaction_ledger rows plus whether the read is MEASURED (VIB-5416).

        The teardown swap-back clamp folds this deployment's NO_ACCOUNTING ledger
        rows (STAKE→wstETH, WRAP→WETH, CDP MINT→stablecoin) into its tracked map —
        those primitives write a transaction_ledger row but ZERO accounting_events,
        so the accounting-event FIFO read (:meth:`read_accounting_events_measured`)
        cannot see their wallet inventory and the clamp would strand the strategy's
        own closing swap as ``untracked_token``.

        Returns ``(rows, measured)`` with the SAME Empty ≠ Zero contract as
        :meth:`read_accounting_events_measured`: ``measured=True`` only on
        ``ACCOUNTING_BACKEND_STATUS_AVAILABLE``; ABSENT / ERRORED / UNSPECIFIED
        (old gateway) / transport error all yield ``measured=False``. The clamp
        drops the NO_ACCOUNTING tracked lane on ``measured=False`` (the token
        strands — the safe under-sweep direction), and NEVER over-/under-sweeps a
        shared wallet on an unmeasured read. ``limit`` is unset (0 → full history)
        so a STAKE acquisition is never paginated out while a later disposal is
        kept. Never raises.
        """
        deployment_id = (deployment_id or "").strip()
        if not deployment_id:
            return [], False
        try:
            request = gateway_pb2.GetLedgerEntriesMeasuredRequest(deployment_id=deployment_id)
            response = self._client.state.GetLedgerEntriesMeasured(request, timeout=self._timeout)
            rows = [_proto_ledger_to_dict(e) for e in response.entries]
            measured = response.backend_status == gateway_pb2.ACCOUNTING_BACKEND_STATUS_AVAILABLE
            return rows, measured
        except Exception as e:  # noqa: BLE001 — transport failure → UNMEASURED, never raise.
            logger.warning("GetLedgerEntriesMeasured via gateway failed: %s", e)
            return [], False


def _proto_ledger_to_dict(entry: "gateway_pb2.LedgerEntryInfo") -> dict:
    """Convert a wire ``LedgerEntryInfo`` to the dict shape the clamp's synthetic
    NO_ACCOUNTING projection reads (VIB-5416, ``basis.synthetic_wallet_movement_events``).

    Only the fields the projection consumes are mapped (id / intent_type / token &
    amount legs / chain / success / timestamp). ``timestamp`` stays an int epoch —
    ``basis._timestamp_to_iso`` normalises it.
    """
    return {
        "id": entry.id,
        "deployment_id": entry.deployment_id,
        "intent_type": entry.intent_type,
        "token_in": entry.token_in,
        "amount_in": entry.amount_in,
        "token_out": entry.token_out,
        "amount_out": entry.amount_out,
        "chain": entry.chain,
        "timestamp": entry.timestamp,
        "success": entry.success,
    }


def _proto_position_event_to_dict(event: "gateway_pb2.PositionEventData") -> dict:
    """Convert proto PositionEventData -> dict matching SQLite ``position_events`` row shape.

    Mirrors :func:`_proto_event_to_dict` for the position-events table so
    callers like ``pnl_attributor._pair_close_with_open`` can read both
    backends identically. Timestamp is converted from epoch seconds to a
    tz-aware ISO string because SQLiteStore stores ISO strings and
    ``pnl_attributor`` parses with ``datetime.fromisoformat``.
    """
    from datetime import UTC
    from datetime import datetime as _dt

    timestamp_iso = _dt.fromtimestamp(event.timestamp or 0, tz=UTC).isoformat()
    return {
        "id": event.id,
        "deployment_id": event.deployment_id,
        "cycle_id": event.cycle_id,
        "execution_mode": event.execution_mode,
        "position_id": event.position_id,
        "position_type": event.position_type,
        "event_type": event.event_type,
        "timestamp": timestamp_iso,
        "protocol": event.protocol,
        "chain": event.chain,
        "token0": event.token0,
        "token1": event.token1,
        "amount0": event.amount0,
        "amount1": event.amount1,
        "value_usd": event.value_usd,
        "tick_lower": event.tick_lower if event.HasField("tick_lower") else None,
        "tick_upper": event.tick_upper if event.HasField("tick_upper") else None,
        "liquidity": event.liquidity,
        "in_range": event.in_range if event.HasField("in_range") else None,
        "fees_token0": event.fees_token0,
        "fees_token1": event.fees_token1,
        "leverage": event.leverage,
        "entry_price": event.entry_price,
        "mark_price": event.mark_price,
        "unrealized_pnl": event.unrealized_pnl,
        "is_long": event.is_long if event.HasField("is_long") else None,
        "tx_hash": event.tx_hash,
        "gas_usd": event.gas_usd,
        "ledger_entry_id": event.ledger_entry_id,
        "protocol_fees_usd": event.protocol_fees_usd,
        "attribution_json": event.attribution_json or "{}",
        "attribution_version": event.attribution_version,
    }


def _proto_event_to_dict(event: "gateway_pb2.AccountingEvent") -> dict:
    """Convert proto AccountingEvent → dict matching SQLiteStore return shape.

    Keys mirror the SQLite ``accounting_events`` row dict so callers like
    ``PortfolioValuer._enrich_lending_pnl`` and ``FIFOBasisStore.
    reconstruct_from_events`` can read both backends identically.

    The proto carries ``timestamp`` as an int (epoch seconds); the SQLite
    contract is an ISO string. ``FIFOBasisStore.reconstruct_from_events``
    parses the ISO string with ``datetime.fromisoformat``, so we convert
    epoch → tz-aware ISO unconditionally. Even epoch=0 must yield a parseable
    ISO string (``1970-01-01T00:00:00+00:00``); returning an empty string
    here would crash downstream ISO parsing on any defaulted-zero proto field.

    ``payload_json`` defaults to ``"{}"`` (not ``""``) so consumers that
    parse it with ``json.loads`` never see a JSONDecodeError on an empty
    payload. The SQLite store uses the same ``"{}"`` default.
    """
    from datetime import UTC
    from datetime import datetime as _dt

    payload_bytes = event.payload_json or b""
    timestamp_iso = _dt.fromtimestamp(event.timestamp or 0, tz=UTC).isoformat()
    return {
        "id": event.id,
        "deployment_id": event.deployment_id,
        "cycle_id": event.cycle_id,
        "execution_mode": event.execution_mode,
        "timestamp": timestamp_iso,
        "chain": event.chain,
        "protocol": event.protocol,
        "wallet_address": event.wallet_address,
        "event_type": event.event_type,
        "position_key": event.position_key,
        "ledger_entry_id": event.ledger_entry_id,
        "tx_hash": event.tx_hash,
        "confidence": event.confidence,
        "payload_json": payload_bytes.decode("utf-8") if payload_bytes else "{}",
        "schema_version": event.schema_version,
    }
