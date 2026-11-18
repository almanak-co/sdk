"""Measure the elapsed-interest branch without changing execution or observed state."""

from eth_abi import encode
from eth_utils import keccak
from pydantic import model_validator

from almanak.connectors.morpho_blue.operations import ApproveOperation, RevokeOperation
from almanak.gateway.ethereum import DataError
from almanak.models import Hash32, Model, UInt


class GasEstimation(Model):
    pinned_estimate: UInt
    accrued_estimate: UInt
    slot: Hash32
    observed_word: Hash32
    estimated_word: Hash32

    @model_validator(mode="after")
    def timestamp_only(self):
        observed, estimated = int(self.observed_word, 16), int(self.estimated_word, 16)
        if observed >> 128 != estimated >> 128 or estimated != observed - 1:
            raise ValueError("Morpho gas estimate may only move lastUpdate back one second")
        if not self.pinned_estimate or not self.accrued_estimate:
            raise ValueError("Morpho gas estimates must be positive")
        return self


def requires_accrual(operation, before):
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        return False
    if operation.kind == "supply" and not operation.target.reserve.is_loan:
        return False
    return before.account.market.accounting.last_update == before.block.timestamp


def timestamp_override(reader, before, selector):
    market = before.account.market
    # Verified v1 core layout: mapping market at slot 3; the third struct word
    # packs lastUpdate in the low uint128 and fee in the high uint128.
    base = int.from_bytes(
        keccak(
            encode(("bytes32", "uint256"), (bytes.fromhex(str(market.source.market_id)[2:]), 3))
        ),
        "big",
    )
    slot = "0x" + ((base + 2) % 2**256).to_bytes(32, "big").hex()
    word = int(reader._rpc("eth_getStorageAt", [market.source.core, slot, selector]), 16)
    accounting = market.accounting
    if word != accounting.last_update + (accounting.fee << 128) or not accounting.last_update:
        raise DataError("gas_estimation", "Morpho timestamp storage differs from its observation")
    return slot, "0x" + word.to_bytes(32, "big").hex(), "0x" + (word - 1).to_bytes(32, "big").hex()


def estimate_gas(reader, operation, before, tx):
    selector = {"blockHash": before.block.hash, "requireCanonical": True}
    pinned = int(reader._rpc("eth_estimateGas", [tx, selector]), 16)
    if not requires_accrual(operation, before):
        return {"gas_estimate": pinned}
    slot, observed, estimated = timestamp_override(reader, before, selector)
    overrides = {before.target.reserve.market.core: {"stateDiff": {slot: estimated}}}
    accrued = int(reader._rpc("eth_estimateGas", [tx, selector, overrides]), 16)
    evidence = GasEstimation(
        pinned_estimate=pinned,
        accrued_estimate=accrued,
        slot=slot,
        observed_word=observed,
        estimated_word=estimated,
    )
    return {"gas_estimate": max(pinned, accrued), "gas_estimation": evidence}
