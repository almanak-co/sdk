"""A keeper liquidation is a scoped external effect, not an admitted SDK request."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Target
from almanak.connectors.gmx_v2.events import Event
from almanak.connectors.gmx_v2.observations import Snapshot
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import AssetDelta
from almanak.external_effects import ExternalEffect, ExternalIdentity, ExternalScan
from almanak.models import Address, Block, Hash32, Model
from almanak.work import ObservationScope


def scope(target):
    return ObservationScope(
        chain=target.fund.chain, account=target.account, allocation=target.allocation
    )


class LiquidationResult(Model):
    kind: Literal["external_liquidation"] = "external_liquidation"
    identity: ExternalIdentity
    target: Target
    assets: tuple[AssetDelta, ...]
    block: Block
    accounting_issues: tuple[str, ...] = ()

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)


class LiquidationOutcome(ExternalEffect):
    kind: Literal["gmx_liquidation_outcome"] = "gmx_liquidation_outcome"
    target: Target
    before: Snapshot
    after: Snapshot
    tx_hash: Hash32
    handler: Address
    receipt: dict
    trace: dict
    position_event: Event
    creation_event: Event
    execution_event: Event
    fee_events: tuple[Event, ...]
    assets: tuple[AssetDelta, ...]
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def identity_scope(self):
        if (
            self.identity.connector != ConnectorId("gmx_v2")
            or self.identity.scope != scope(self.target)
            or self.before.target != self.target
            or self.after.target != self.target
            or self.effective_at != self.after.effective_at
        ):
            raise ValueError("liquidation effect differs from its exact fund, chain or time")
        expected = {
            "transactionHash": self.tx_hash,
            "blockHash": self.after.block.hash,
            "blockNumber": hex(self.after.block.number),
            "status": "0x1",
        }
        if any(self.receipt.get(key) != value for key, value in expected.items()):
            raise ValueError("liquidation effect lacks its canonical successful receipt")
        if any(
            (value.key.account, value.key.chain) != (self.target.account, self.target.fund.chain)
            for value in self.assets
        ):
            raise ValueError("liquidation cash effects differ from the affected fund scope")
        return self

    def strategy_result(self):
        return LiquidationResult(
            identity=self.identity,
            target=self.target,
            assets=self.assets,
            block=self.after.block,
            accounting_issues=self.accounting_issues,
        )


class Scan(ExternalScan):
    kind: Literal["gmx_external_scan"] = "gmx_external_scan"
    target: Target
    start: Block
    end: Block
    events: tuple[Event, ...]
    outcome: LiquidationOutcome | None = None

    @model_validator(mode="after")
    def bounded_scope(self):
        if (
            self.connector != ConnectorId("gmx_v2")
            or self.scope != scope(self.target)
            or self.start.chain != self.target.fund.chain
            or self.end.chain != self.target.fund.chain
            or self.start.number > self.end.number
        ):
            raise ValueError("GMX scan page requires one ordered explicit execution scope")
        if self.outcome is not None and self.outcome.after.block != self.end:
            raise ValueError("GMX liquidation scan ends at its financial evidence block")
        return self
