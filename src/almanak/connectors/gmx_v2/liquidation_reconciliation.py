"""Unsolicited liquidation clears only the proved owned position and attributes receipt cash."""

from almanak.connectors.gmx_v2 import liquidation_receipts
from almanak.connectors.gmx_v2.liquidation_reader import financial_before
from almanak.connectors.gmx_v2.liquidation_types import LiquidationOutcome, scope
from almanak.connectors.gmx_v2.observations import snapshot
from almanak.connectors.gmx_v2.reconciliation import (
    cash_effects,
    changed_ownership,
    validate_wallets,
)
from almanak.connectors.identifiers import ConnectorId
from almanak.external_effects import ExternalIdentity
from almanak.gateway.ethereum import DataError


def verify_handler(reader, target, block):
    from almanak.connectors.gmx_v2.deployments import verify_deployments

    verify_deployments(reader, target.market, block)
    handler = liquidation_receipts.handler(target)
    expected = {
        "decreaseOrderExecutor": target.market.contracts.decrease_order_executor,
        "dataStore": target.market.contracts.data_store,
        "oracle": target.market.contracts.oracle,
    }
    for getter, address in expected.items():
        found = reader.call(handler, getter + "()", block, outputs=("address",))[0]
        if found != address:
            raise DataError("revision", "GMX liquidation handler execution closure differs")
    return handler


def reconcile(backend, state, target, event, block):
    reader = backend.reader
    before = financial_before(state, target)
    owned = next(o for o in state.owned if o.target == target)
    if before is None or before.owned != owned or owned.position is None:
        raise DataError("ownership", "GMX liquidation lacks its retained owned financial preimage")
    if before.block.number >= block.number:
        raise DataError("ordering", "GMX liquidation cannot precede its financial reference")
    handler = verify_handler(reader, target, block)
    receipt = reader._rpc("eth_getTransactionReceipt", [event.transaction_hash], allow_none=True)
    if receipt is None or receipt.get("blockHash") != block.hash or event.block_hash != block.hash:
        raise DataError(
            "reorganization", "GMX liquidation receipt is absent from canonical history"
        )
    trace = reader._rpc(
        "debug_traceTransaction", [event.transaction_hash, {"tracer": "callTracer"}]
    )
    created, executed, decreased = liquidation_receipts.validate_events(receipt, target, before)
    if decreased != event:
        raise DataError("integrity", "GMX liquidation query differs from its canonical receipt")
    fees = liquidation_receipts.fee_events(receipt, target, created.topics[0])
    native = liquidation_receipts.native_received(trace, receipt, target)
    after = snapshot(backend, target.allocation, block, owned)
    if after.position is not None or after.allowance != before.allowance:
        raise DataError(
            "ownership", "GMX liquidation did not fully close its position or changed allowance"
        )
    validate_native_measurement(before, after, native)
    assets = cash_effects(receipt, target, owned.inventory, native)
    claims, issues = liquidation_receipts.accrue_funding(before, after, receipt)
    changed = changed_ownership(before, assets, None).model_copy(
        update={"claimable_funding": claims}
    )
    after = after.model_copy(update={"owned": changed})
    return LiquidationOutcome(
        identity=ExternalIdentity(
            connector=ConnectorId("gmx_v2"),
            scope=scope(target),
            reference=f"{event.transaction_hash}:{event.log_index}",
        ),
        effective_at=after.effective_at,
        target=target,
        before=before,
        after=after,
        tx_hash=event.transaction_hash,
        handler=handler,
        receipt=receipt,
        trace=trace,
        creation_event=created,
        execution_event=executed,
        position_event=decreased,
        fee_events=fees,
        assets=assets,
        accounting_issues=issues + validate_wallets(before, after, assets, 0),
    )


def validate_native_measurement(before, after, native):
    old = next(value for value in before.wallets if value.asset.namespace == "slip44")
    new = next(value for value in after.wallets if value.asset == old.asset)
    if new.amount != old.amount + native:
        raise DataError(
            "accounting",
            "GMX liquidation native residual lacks exact receipt-call attribution; "
            "retain the position obligation until canonical attribution is available",
        )
