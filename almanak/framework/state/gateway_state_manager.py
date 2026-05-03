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
from typing import TYPE_CHECKING

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

    async def load_state(self, strategy_id: str) -> StateData | None:
        """Load strategy state from gateway.

        Args:
            strategy_id: Unique strategy identifier

        Returns:
            StateData if found, None if not found

        Raises:
            StateError: If gateway request fails
        """
        try:
            request = gateway_pb2.LoadStateRequest(strategy_id=strategy_id)
            response = self._client.state.LoadState(request, timeout=self._timeout)

            if not response.strategy_id:
                return None

            # Deserialize state from JSON bytes
            state_dict = json.loads(response.data.decode("utf-8"))

            return StateData(
                strategy_id=response.strategy_id,
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

            logger.error(f"Gateway load state failed for {strategy_id}: {error_msg}")
            raise

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
                strategy_id=state.strategy_id,
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
                        strategy_id=state.strategy_id,
                        expected_version=expected_version or 0,
                        actual_version=response.new_version,
                    )

                raise RuntimeError(f"State save failed: {error_msg}")

            # Return updated state with new version
            return StateData(
                strategy_id=state.strategy_id,
                version=response.new_version,
                state=state.state,
                schema_version=state.schema_version,
                checksum=response.checksum or "",
                created_at=state.created_at,
            )

        except Exception as e:
            if "StateConflictError" in type(e).__name__:
                raise
            logger.error(f"Gateway save state failed for {state.strategy_id}: {e}")
            raise

    async def delete_state(self, strategy_id: str) -> bool:
        """Delete strategy state through gateway.

        Args:
            strategy_id: Unique strategy identifier

        Returns:
            True if deleted, False if not found
        """
        try:
            request = gateway_pb2.DeleteStateRequest(strategy_id=strategy_id)
            response = self._client.state.DeleteState(request, timeout=self._timeout)

            return response.success

        except Exception as e:
            logger.error(f"Gateway delete state failed for {strategy_id}: {e}")
            raise

    def invalidate_hot_cache(self, strategy_id: str | None = None) -> None:
        """Invalidate hot cache.

        For the gateway-backed version, this is a no-op since caching
        is handled in the gateway.

        Args:
            strategy_id: Strategy to invalidate, or None for all
        """
        logger.debug(f"Cache invalidation requested for {strategy_id or 'all'} (no-op)")

    async def save_portfolio_snapshot(self, snapshot: "PortfolioSnapshot") -> int:
        """Save portfolio snapshot via gateway gRPC → PostgreSQL.

        Args:
            snapshot: Portfolio snapshot to save

        Returns:
            Snapshot ID from the database
        """
        try:
            # VIB-3917 — writer-side raise: refuse to ship a snapshot that
            # claims HIGH confidence while carrying a synthesised position
            # whose cost_basis_usd is null. The May 3 production trace
            # showed exactly this combination — a HIGH-confidence snapshot
            # with a fabricated $0-cost-basis LP position because the
            # accounting pipeline hadn't finished. CONF (VIB-3886) covers
            # accounting_events; this covers snapshot_writes.
            if getattr(snapshot, "value_confidence", None) and snapshot.value_confidence.value == "HIGH":
                for pos in snapshot.positions or []:
                    cb = getattr(pos, "cost_basis_usd", None)
                    if cb is None or (isinstance(cb, Decimal) and cb == Decimal("0")):
                        # cost_basis=0 by itself is legal (e.g. a freshly
                        # tracked position whose handler hasn't priced yet).
                        # But if value_usd is non-zero AND cost_basis is
                        # zero/null while we claim HIGH confidence, the
                        # books are lying. Degrade in-place.
                        v = getattr(pos, "value_usd", None)
                        if v is not None and v > Decimal("0"):
                            from almanak.framework.portfolio.models import ValueConfidence

                            logger.warning(
                                "VIB-3917: snapshot for %s carries HIGH+synth position "
                                "(value_usd=%s, cost_basis_usd=%s) — degrading to ESTIMATED",
                                snapshot.strategy_id,
                                v,
                                cb,
                            )
                            snapshot.value_confidence = ValueConfidence.ESTIMATED
                            break

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
                strategy_id=snapshot.strategy_id,
                timestamp=int(snapshot.timestamp.timestamp()),
                iteration_number=snapshot.iteration_number,
                total_value_usd=str(snapshot.total_value_usd),
                available_cash_usd=str(snapshot.available_cash_usd),
                value_confidence=snapshot.value_confidence.value,
                positions_json=positions_bytes,
                chain=snapshot.chain or "",
            )
            response = self._client.state.SavePortfolioSnapshot(request, timeout=self._timeout)

            if not response.success:
                # VIB-3157: treat gateway-side write failure as a first-class accounting
                # error. The previous "return 0" path caused silent accounting loss --
                # on-chain trades with no durable snapshot.
                logger.error("SavePortfolioSnapshot failed: %s", response.error)
                raise AccountingPersistenceError(
                    write_kind=AccountingWriteKind.SNAPSHOT,
                    strategy_id=snapshot.strategy_id,
                    message=f"SavePortfolioSnapshot failed: {response.error}",
                )

            logger.debug(
                "Portfolio snapshot saved via gateway: strategy=%s, value=$%.2f, confidence=%s",
                snapshot.strategy_id,
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
                strategy_id=getattr(snapshot, "strategy_id", "") or "",
                cause=e,
            ) from e

    async def get_latest_snapshot(self, strategy_id: str) -> "PortfolioSnapshot | None":
        """Get most recent portfolio snapshot via gateway gRPC."""
        try:
            request = gateway_pb2.GetLatestSnapshotRequest(strategy_id=strategy_id)
            response = self._client.state.GetLatestSnapshot(request, timeout=self._timeout)

            if not response.found:
                return None

            return self._proto_to_snapshot(response)
        except Exception as e:
            logger.debug("Failed to get latest snapshot via gateway: %s", e)
            return None

    async def get_snapshots_since(
        self, strategy_id: str, since: datetime, limit: int = 168
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a given time via gateway gRPC."""
        try:
            request = gateway_pb2.GetSnapshotsSinceRequest(
                strategy_id=strategy_id,
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
                strategy_id=getattr(entry, "strategy_id", "") or "",
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
                    strategy_id=getattr(entry, "strategy_id", "") or "",
                    message=f"SaveLedgerEntry failed: {response.error}",
                )

            logger.debug(
                "Ledger entry saved via gateway: strategy=%s, id=%s, intent=%s, success=%s",
                getattr(entry, "strategy_id", ""),
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
                strategy_id=getattr(entry, "strategy_id", "") or "",
                cause=e,
            ) from e

    async def save_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
        """Save portfolio metrics via gateway gRPC.

        Args:
            metrics: PortfolioMetrics to persist.

        Returns:
            True if save succeeded.
        """
        try:
            request = gateway_pb2.SaveMetricsRequest(
                strategy_id=metrics.strategy_id,
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
                    strategy_id=metrics.strategy_id,
                    message=f"SavePortfolioMetrics failed: {response.error}",
                )

            logger.debug("Portfolio metrics saved via gateway for strategy=%s", metrics.strategy_id)
            return True
        except AccountingPersistenceError:
            raise
        except Exception as e:
            logger.exception("Failed to save portfolio metrics via gateway")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=getattr(metrics, "strategy_id", "") or "",
                cause=e,
            ) from e

    async def get_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics via gateway gRPC.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            PortfolioMetrics or None if not found.
        """
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioMetrics

        try:
            request = gateway_pb2.GetMetricsRequest(strategy_id=strategy_id)
            response = self._client.state.GetPortfolioMetrics(request, timeout=self._timeout)

            if not response.found:
                return None

            return PortfolioMetrics(
                strategy_id=response.strategy_id,
                timestamp=datetime.fromtimestamp(response.updated_at, tz=UTC)
                if response.updated_at
                else datetime.now(UTC),
                total_value_usd=Decimal("0"),  # Not stored in metrics, get from latest snapshot
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
            "strategy_id": data.strategy_id,
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
        }

        return PortfolioSnapshot.from_dict(snapshot_dict)

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

            payload_bytes = augment_accounting_payload(event.to_payload_json(), is_live=is_live).encode("utf-8")
            request = gateway_pb2.SaveAccountingEventRequest(
                id=identity.id,
                deployment_id=identity.deployment_id,
                strategy_id=identity.strategy_id,
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
                    identity.strategy_id,
                    identity.id,
                    response.error,
                )
                if is_live:
                    raise AccountingPersistenceError(
                        write_kind=AccountingWriteKind.LEDGER,
                        strategy_id=identity.strategy_id,
                        message=f"SaveAccountingEvent failed: {response.error}",
                    )
                return False
            logger.debug(
                "Accounting event saved via gateway: strategy=%s, id=%s, type=%s",
                identity.strategy_id,
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
                    strategy_id=getattr(identity, "strategy_id", ""),
                    cause=e,
                ) from e
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

    # -------------------------------------------------------------------------
    # Accounting outbox — gateway gRPC layer (SaveOutboxEntry / GetOutboxEntry /
    # GetOutboxPending / UpdateOutboxEntry). DDL landed in metrics-database PR #24.
    # -------------------------------------------------------------------------

    async def save_outbox_entry(
        self,
        outbox_id: str,
        deployment_id: str,
        strategy_id: str,
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
            strategy_id=strategy_id,
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
                strategy_id=strategy_id,
                cause=e,
            ) from e
        if not response.success:
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.OUTBOX,
                strategy_id=strategy_id,
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
                "strategy_id": e.strategy_id,
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
                        "strategy_id": e.strategy_id,
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
                "strategy_id": e.strategy_id,
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

        Note on ``strategy_id`` over the wire: the gRPC contract requires a
        strategy_id field for format validation, but in hosted mode the
        gateway always prefers the platform-injected ``AGENT_ID`` env var
        when filtering, and in local SQLite mode the value is unused for
        filtering. We pass ``deployment_id`` as the wire value because it
        follows the same alphanumeric format and is always available at
        the call site.

        Args:
            deployment_id: Strategy deployment identifier.
            position_key: Optional filter by position_key.

        Returns:
            List of dicts shaped like ``SQLiteStore.get_accounting_events_sync``
            so PortfolioValuer's duck-typed access (``e.get("event_type")``,
            ``e.get("payload_json")`` etc.) is unchanged.
        """
        deployment_id = (deployment_id or "").strip()
        if not deployment_id:
            logger.warning("get_accounting_events_sync called with empty deployment_id — returning []")
            return []
        try:
            request = gateway_pb2.GetAccountingEventsRequest(
                strategy_id=deployment_id,
                deployment_id=deployment_id,
                position_key=position_key or "",
            )
            response = self._client.state.GetAccountingEvents(request, timeout=self._timeout)
            rows = [_proto_event_to_dict(e) for e in response.events]
            # Stable ordering for deterministic FIFO replay; secondary key on id breaks timestamp ties.
            rows.sort(key=lambda r: (r.get("timestamp") or "", r.get("id") or ""))
            return rows
        except Exception as e:
            logger.debug("GetAccountingEvents via gateway failed: %s", e)
            return []


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
        "strategy_id": event.strategy_id,
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
