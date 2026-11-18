"""Bounded canonical publication scans preserve explicit handler and Oracle selection."""

from typing import Annotated

from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.backtest.history_authority import TransactionSourceScope
from almanak.connectors.gmx_v2.events import EVENT_SIGNATURES, decode_event
from almanak.connectors.gmx_v2.published_prices import read_prices
from almanak.gateway.ethereum import DataError
from almanak.models import Hash32, Model, UInt

Positive = Annotated[UInt, Field(gt=0)]


class PublicationLimits(Model):
    page_blocks: Positive
    maximum_blocks: Positive
    maximum_pages: Positive
    maximum_logs: Positive
    maximum_transactions: Positive

    @model_validator(mode="after")
    def bounds(self):
        if self.page_blocks > self.maximum_blocks:
            raise ValueError("GMX publication page exceeds the explicit block bound")
        return self


def publication_scope(market, first, last):
    if first.chain != market.chain or last.chain != market.chain or first.number > last.number:
        raise ValueError("GMX publication range differs from its configured historical chain")
    signature = next(value for value, count in EVENT_SIGNATURES.items() if count == 1)
    return TransactionSourceScope(
        address=market.contracts.event_emitter,
        topics=(
            signature,
            "0x" + keccak(text="OraclePriceUpdate").hex(),
            "0x" + "0" * 24 + market.index_asset.reference[2:],
        ),
        first_block=first.number,
        last_block=last.number,
    )


def selected_transactions(logs, market, source):
    found: dict[Hash32, tuple[int, Hash32, int]] = {}
    for log in logs:
        if log.get("removed") is not False:
            raise DataError("reorganization", "GMX publication log is removed or not canonical")
        event = decode_event(log, emitter=market.contracts.event_emitter)
        if event is None or event.name != "OraclePriceUpdate":
            raise DataError("identity", "GMX publication log differs from its exact event query")
        if event.sender != source.oracle:
            continue
        expected_topic = "0x" + "0" * 24 + market.index_asset.reference[2:]
        if (
            event.topics != (expected_topic,)
            or event.value("address", "token") != market.index_asset.reference
        ):
            raise DataError("identity", "GMX publication log differs from its indexed market asset")
        identity = event.block_number, event.block_hash, int(log["transactionIndex"], 16)
        previous = found.setdefault(event.transaction_hash, identity)
        if previous != identity:
            raise DataError(
                "identity", "GMX publication transaction has conflicting block identities"
            )
    return found


def publication_prices(reader, market, source, transaction_hash, identity):
    number, block_hash, transaction_index = identity
    block = reader.pin_block(number)
    if block.hash != block_hash:
        raise DataError("reorganization", "GMX publication differs from its canonical block")
    transaction = reader._rpc("eth_getTransactionByHash", [transaction_hash])
    receipt = reader._rpc("eth_getTransactionReceipt", [transaction_hash])
    if transaction is None or receipt is None or transaction.get("hash") != transaction_hash:
        raise DataError("identity", "GMX publication requires its exact transaction and receipt")
    if any(
        int(value["transactionIndex"], 16) != transaction_index for value in (transaction, receipt)
    ):
        raise DataError("identity", "GMX publication changed its transaction ordering")
    return read_prices(reader, market, block, source, transaction, receipt)


def scan_publications(reader, market, source, first, last, limits):
    scope = publication_scope(market, first, last)
    blocks = last.number - first.number + 1
    pages = (blocks + limits.page_blocks - 1) // limits.page_blocks
    if blocks > limits.maximum_blocks or pages > limits.maximum_pages:
        raise ValueError("GMX publication range exceeds its explicit block or page limit")
    require_boundaries(reader, first, last)
    found: dict[Hash32, tuple[int, Hash32, int]] = {}
    count = 0
    for start in range(first.number, last.number + 1, limits.page_blocks):
        end = min(last.number, start + limits.page_blocks - 1)
        logs = reader._rpc(
            "eth_getLogs",
            [
                {
                    "address": scope.address,
                    "topics": list(scope.topics),
                    "fromBlock": hex(start),
                    "toBlock": hex(end),
                }
            ],
        )
        count += len(logs)
        if count > limits.maximum_logs:
            raise DataError("unavailable", "GMX publication scan exceeds its explicit log limit")
        selected = selected_transactions(logs, market, source)
        if any(not start <= number <= end for number, _, _ in selected.values()):
            raise DataError("identity", "GMX publication escaped its authorized page")
        if found.keys() & selected.keys():
            raise DataError("identity", "GMX publication transaction appeared in distinct pages")
        found.update(selected)
        if len(found) > limits.maximum_transactions:
            raise DataError("unavailable", "GMX publication scan exceeds its transaction limit")
    require_boundaries(reader, first, last)
    return found


def require_boundaries(reader, first, last):
    if reader.pin_block(first.number) != first or reader.pin_block(last.number) != last:
        raise DataError("reorganization", "GMX publication range changed canonical boundaries")


def collect_publications(reader, market, source, first, last, limits):
    selected = scan_publications(reader, market, source, first, last, limits)
    results = tuple(
        prices
        for transaction, identity in sorted(
            selected.items(), key=lambda item: (item[1][0], item[1][2])
        )
        if (prices := publication_prices(reader, market, source, transaction, identity)) is not None
    )
    require_boundaries(reader, first, last)
    return results
