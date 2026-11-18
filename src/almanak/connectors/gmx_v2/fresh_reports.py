"""Bounded acquisition of unchanged post-origin signatures for an explicitly selected source."""

import json
import time
from typing import Annotated

from eth_utils import keccak
from pydantic import Field

from almanak.connectors.gmx_v2 import report_source as source
from almanak.connectors.gmx_v2.deployments import verify_deployments
from almanak.connectors.gmx_v2.events import decode_event
from almanak.connectors.gmx_v2.oracle_reader import stored, stream_configuration
from almanak.connectors.gmx_v2.report_source import SourceSelection
from almanak.gateway.ethereum import DataError
from almanak.models import Model


class Limits(Model):
    page_blocks: Annotated[int, Field(gt=0, le=2000)]
    maximum_blocks: Annotated[int, Field(gt=0)]
    maximum_transactions: Annotated[int, Field(gt=0)]
    maximum_logs: Annotated[int, Field(gt=0)]
    wait_seconds: Annotated[int, Field(gt=0)]
    poll_seconds: Annotated[int, Field(gt=0, le=30)]


def bound_selections(reader, market, origin, selection):
    """One reviewed handler, or several sharing one Oracle (see report_source.selections_for),
    with the stream bindings they all resolve to at the origin."""
    selections = (selection,) if isinstance(selection, SourceSelection) else tuple(selection)
    if len({s.oracle for s in selections}) != 1:
        raise ValueError("report source handlers must share one selected Oracle")
    bound = [source.bindings(reader, market, origin, s) for s in selections]
    if any(other != bound[0] for other in bound[1:]):
        raise DataError("identity", "report source handlers bind different streams")
    return selections, bound[0]


class FreshReports:
    def __init__(self, reader, market, origin, selection, limits, output):
        if reader.chain != market.chain or origin.chain != market.chain:
            raise ValueError("fresh report acquisition requires the selected market chain")
        if reader.pin_block(origin.number) != origin:
            raise DataError("reorganization", "fresh report origin is no longer canonical")
        self.reader, self.market, self.origin = reader, market, origin
        self.selections, (self.destination, self.original) = bound_selections(
            reader, market, origin, selection
        )
        self.selection, self.limits, self.output = self.selections[0], limits, output
        self.identities = self.source_identities(origin)
        self.target_identities = verify_deployments(reader, market, origin)
        self.expiration = stored(
            reader, origin, market.contracts.data_store, "Uint", "REQUEST_EXPIRATION_TIME"
        )
        self.cursor, self.transactions, self.log_count = origin.number + 1, 0, 0
        self.start = origin.number
        self.seen, self.candidates, self.selected, self.pages = set(), [], [], []
        self.examined = []
        self.retain()

    def seek(self, block):
        """Read from `block` on: a fork whose clock runs behind the chain head creates
        requests long after its origin, and earlier blocks cannot hold their reports."""
        if block.chain != self.market.chain:
            raise ValueError("report source seek requires the selected market chain")
        if block.number > self.cursor:
            self.cursor, self.start = block.number, block.number - 1
            self.candidates.clear()

    def acquire(self, request, *, not_before):
        if request.market != self.market.share_asset:
            raise ValueError("fresh report request differs from its configured market")
        timestamp = request.events[0].identity.block.timestamp
        cutoff = max(timestamp, not_before, self.origin.timestamp)
        if cutoff >= timestamp + self.expiration:
            raise DataError(
                "unavailable", "fresh request has no remaining report eligibility window"
            )
        deadline = time.monotonic() + self.limits.wait_seconds
        try:
            return self.wait_for_source(timestamp, cutoff, deadline)
        except BaseException:
            self.retain()
            raise

    def wait_for_source(self, timestamp, cutoff, deadline):
        while time.monotonic() < deadline:
            result = self.inspect_candidates(timestamp, cutoff)
            if result is not None:
                return result
            if not self.read_page():
                time.sleep(self.limits.poll_seconds)
        raise DataError(
            "unavailable",
            "no eligible authentic report from the explicit source within its wait bound",
        )

    def inspect_candidates(self, timestamp, cutoff):
        while self.candidates:
            self.transactions += 1
            if self.transactions > self.limits.maximum_transactions:
                raise DataError(
                    "unavailable", "fresh report source exhausted its transaction bound"
                )
            transaction = self.reader._rpc("eth_getTransactionByHash", [self.candidates.pop(0)])
            self.examined.append(transaction)
            vectors = source.transaction_vectors(transaction, self.selections)
            if vectors is None:
                continue
            reports = source.reports_for(vectors, self.destination, self.original)
            if reports is not None:
                result = self.accept(transaction, reports, timestamp, cutoff)
                if result is not None:
                    return result
        return None

    def accept(self, transaction, reports, timestamp, cutoff):
        selected, original = reports
        if not source.eligible(
            selected, self.destination, cutoff, self.expiration - (cutoff - timestamp)
        ):
            return None
        block, receipt = source.canonical_receipt(self.reader, transaction, self.origin)
        self.audit_source(block)
        events = source.source_events(receipt, self.market, self.selection)
        for binding, report in zip(self.original, original, strict=True):
            source.verify_report_event(events, binding, report)
        role_store = source.keeper_authorized(
            self.reader, self.market, self.origin, transaction["from"]
        )
        row = {
            "oracle_source": transaction,
            "reports": [report.model_dump(mode="json") for report in selected],
            "source_reports": [report.model_dump(mode="json") for report in original],
            "publication_block": block.model_dump(mode="json"),
            "original_receipt": receipt,
            "target_keeper_role_store": role_store,
            "target_signature_verification": "required in unchanged target fork Oracle execution",
        }
        self.selected.append(row)
        self.retain()
        return row

    def audit_source(self, block):
        if self.reader.pin_block(self.origin.number) != self.origin:
            raise DataError("reorganization", "fresh report origin is no longer canonical")
        if self.source_identities(block) != self.identities:
            raise DataError(
                "revision", "selected report source code changed after the fresh origin"
            )
        for expected in self.original:
            actual = stream_configuration(
                self.reader, block, expected.asset, expected.data_store, expected.oracle
            )
            if actual.model_dump(exclude={"block"}) != expected.model_dump(exclude={"block"}):
                raise DataError(
                    "identity", "selected source stream binding changed after the fresh origin"
                )

    def source_identities(self, block):
        identities: dict[str, str] = {}
        for selection in self.selections:
            identities |= source.identities(
                self.reader, self.market, block, selection, self.original
            )
        return identities

    def read_page(self):
        head = self.reader.pin_block("latest")
        if head.number < self.cursor:
            return False
        end = min(head.number, self.cursor + self.limits.page_blocks - 1)
        if end - self.start > self.limits.maximum_blocks:
            raise DataError("unavailable", "fresh report source exhausted its block bound")
        self.read_events(self.cursor, end)
        self.cursor = end + 1
        return True

    def read_events(self, start, end):
        query = {
            "address": self.market.contracts.event_emitter,
            "topics": [
                None,
                "0x" + keccak(text="OraclePriceUpdate").hex(),
                "0x" + self.market.index_asset.reference[2:].rjust(64, "0"),
            ],
            "fromBlock": hex(start),
            "toBlock": hex(end),
        }
        beginning, ending = self.reader.pin_block(start), self.reader.pin_block(end)
        logs = self.reader._rpc("eth_getLogs", [query])
        self.log_count += len(logs)
        if self.log_count > self.limits.maximum_logs:
            raise DataError("unavailable", "fresh report source exhausted its log bound")
        self.pages.append(
            {
                "query": query,
                "logs": logs,
                "start": beginning.model_dump(mode="json"),
                "end": ending.model_dump(mode="json"),
            }
        )
        for log in logs:
            event = decode_event(log, emitter=self.market.contracts.event_emitter)
            self.admit_event(event, start, end)

    def admit_event(self, event, start, end):
        if event is None or event.sender != self.selection.oracle:
            return
        if not start <= event.block_number <= end or event.name != "OraclePriceUpdate":
            raise DataError("identity", "report source log lies outside the requested page")
        if self.reader.pin_block(event.block_number).hash != event.block_hash:
            raise DataError("reorganization", "source log block is no longer canonical")
        if event.transaction_hash not in self.seen:
            self.seen.add(event.transaction_hash)
            self.candidates.append(event.transaction_hash)

    def retain(self):
        value = {
            "scope": "explicit signed-report fixture source with separate target verification",
            "origin": self.origin.model_dump(mode="json"),
            "selection": self.selection.model_dump(mode="json"),
            "selections": [selection.model_dump(mode="json") for selection in self.selections],
            "limits": self.limits.model_dump(mode="json"),
            "bindings": [binding.model_dump(mode="json") for binding in self.destination],
            "source_bindings": [binding.model_dump(mode="json") for binding in self.original],
            "source_identities": self.identities,
            "target_identities": [
                identity.model_dump(mode="json") for identity in self.target_identities
            ],
            "request_expiration_seconds": self.expiration,
            "cursor": self.cursor,
            "transactions_examined": self.transactions,
            "source_transactions": self.examined,
            "pages": self.pages,
            "selected": self.selected,
        }
        self.output.parent.mkdir(parents=True, exist_ok=True)
        self.output.write_text(json.dumps(value, indent=2) + "\n")
