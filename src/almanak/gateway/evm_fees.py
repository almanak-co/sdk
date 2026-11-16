"""Chain-owned mined execution costs, retaining OP fee calculation evidence."""

from eth_utils import keccak
from pydantic import model_validator

from almanak.chains import evm_network
from almanak.gateway.accounts import HexData
from almanak.gateway.environments import origin_chain
from almanak.gateway.ethereum import DataError
from almanak.gateway.op_fees import FjordParameters, read_parameters
from almanak.models import AssetAmount, Hash32, Model, UInt


class OpFeeEvidence(Model):
    transaction_hash: Hash32
    raw_transaction: HexData
    parameters: FjordParameters
    gas_used: UInt
    l1_fee_wei: UInt
    operator_fee_wei: UInt

    @model_validator(mode="after")
    def reproducible(self):
        raw = bytes.fromhex(self.raw_transaction[2:])
        if not raw or "0x" + keccak(raw).hex() != self.transaction_hash:
            raise ValueError("OP fee evidence must retain its exact signed transaction")
        if self.l1_fee_wei != self.parameters.l1_fee(
            raw
        ) or self.operator_fee_wei != self.parameters.operator_fee(self.gas_used):
            raise ValueError("OP fee amounts must reproduce from their retained inputs")
        return self


def charged_fees(backend, pending, receipt, block):
    gas_used = int(receipt["gasUsed"], 16)
    total = gas_used * int(receipt["effectiveGasPrice"], 16)
    origin = origin_chain(backend.config, pending.operation.target.fund.chain)
    evidence = None
    if evm_network(origin.chain_id).family == "optimism":
        parameters = read_parameters(backend.reader, block)
        raw = bytes.fromhex(pending.raw_transaction[2:])
        evidence = OpFeeEvidence(
            transaction_hash=pending.tx_hash,
            raw_transaction=pending.raw_transaction,
            parameters=parameters,
            gas_used=gas_used,
            l1_fee_wei=parameters.l1_fee(raw),
            operator_fee_wei=parameters.operator_fee(gas_used),
        )
        if "l1Fee" in receipt and int(receipt["l1Fee"], 16) != evidence.l1_fee_wei:
            raise DataError("inconsistent", "receipt L1 fee differs from pinned fee calculation")
        total += evidence.l1_fee_wei + evidence.operator_fee_wei
    return AssetAmount(asset=pending.before.native_balance.asset, amount=total), evidence


def validate_fee_evidence(evidence, tx_hash, block, receipt, cost):
    if evidence is None:
        return
    if (
        evidence.transaction_hash != tx_hash
        or evidence.parameters.block != block
        or evidence.gas_used != int(receipt["gasUsed"], 16)
    ):
        raise ValueError("OP fee evidence differs from the retained receipt identity")
    expected = (
        evidence.gas_used * int(receipt["effectiveGasPrice"], 16)
        + evidence.l1_fee_wei
        + evidence.operator_fee_wei
    )
    if cost.amount != expected:
        raise ValueError("payer cost differs from retained OP fee evidence")
