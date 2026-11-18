"""Explicit fixed native cost assumptions backed by retained successful Safe receipts."""

import hashlib
import json
from typing import get_args

from eth_abi import decode
from eth_utils import keccak
from pydantic import Field

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.backtest_config import OperationKind
from almanak.connectors.uniswap_v4.deployments import ARTIFACTS
from almanak.gateway.accounts import ContractCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.models import AssetAmount, AssetType, Block, Model, SafeRoles


class Measurement(Model):
    origin: Block
    profile: SafeRoles
    asset: AssetType
    call: ContractCall
    receipt: dict
    op_fee_evidence: OpFeeEvidence | None = Field(default=None, exclude_if=lambda v: v is None)

    def cost(self):
        if (
            self.receipt["status"] != "0x1"
            or self.receipt["from"].lower() != self.profile.submitter.address
            or self.receipt["to"].lower() != self.profile.roles_module
            or self.call.fund != self.profile.fund
        ):
            raise ValueError("calibration requires a successful fund-scoped Safe receipt")
        native = evm_network(self.origin.chain.chain_id).native_asset
        if self.asset != AssetType(f"{self.profile.fund.chain.chain_id}/slip44:{native.reference}"):
            raise ValueError("calibration gas must identify the source fork native asset")
        execution = int(self.receipt["gasUsed"], 16) * int(self.receipt["effectiveGasPrice"], 16)
        return execution + self.additional_cost(execution)

    def additional_cost(self, execution):
        network = evm_network(self.origin.chain.chain_id)
        evidence = self.op_fee_evidence
        if network.family != "optimism":
            if evidence is not None:
                raise ValueError("non-OP calibration cannot contain OP fee evidence")
            return 0
        if evidence is None:
            raise ValueError("OP calibration requires retained L1 and operator fee evidence")
        block = evidence.parameters.block
        if (block.chain, block.hash, block.number) != (
            self.profile.fund.chain,
            self.receipt["blockHash"],
            int(self.receipt["blockNumber"], 16),
        ):
            raise ValueError("OP calibration fee block differs from its mined receipt")
        additional = evidence.l1_fee_wei + evidence.operator_fee_wei
        validate_fee_evidence(
            evidence,
            self.receipt["transactionHash"],
            block,
            self.receipt,
            AssetAmount(asset=self.asset, amount=execution + additional),
        )
        return additional


def selector(signature):
    return "0x" + keccak(text=signature)[:4].hex()


def operation_kind(call):
    prefix, raw = call.data[:10], bytes.fromhex(call.data[10:])
    if prefix == selector("approve(address,uint256)"):
        _, amount = decode(("address", "uint256"), raw)
        return "approve_erc20" if amount else "revoke_erc20"
    if prefix == selector("approve(address,address,uint160,uint48)"):
        _, _, amount, _ = decode(("address", "address", "uint160", "uint48"), raw)
        return "approve_permit2" if amount else "revoke_permit2"
    if prefix == selector(abi.ROUTER_EXECUTE):
        return "swap"
    if prefix == selector(abi.MODIFY):
        unlock, _ = decode(("bytes", "uint256"), raw)
        actions, _ = decode(("bytes", "bytes[]"), unlock)
        return {
            abi.MINT_ACTIONS: "mint",
            abi.COLLECT_ACTIONS: "collect",
            abi.CLOSE_ACTIONS: "burn",
        }[actions]
    raise ValueError("calibration contains an unsupported protocol call")


def measured_maxima(measurements):
    costs: dict[str, int] = {}
    for item in measurements:
        kind = operation_kind(item.call)
        costs[kind] = max(costs.get(kind, 0), item.cost())
    return costs


CALIBRATION_FILES = {
    ChainId("eip155:1"): "safe-gas-calibration.json",
    ChainId("eip155:43114"): "avalanche-safe-gas-calibration-complete.json",
    ChainId("eip155:8453"): "base-safe-gas-calibration.json",
    ChainId("eip155:42161"): "arbitrum-safe-gas-calibration.json",
    ChainId("eip155:10"): "optimism-safe-gas-calibration.json",
    # Candidate chains whose Safe receipts are not measured yet. The loader names
    # the artifact a proof runner must retain; it never substitutes estimated costs.
    ChainId("eip155:56"): "bnb-safe-gas-calibration.json",
    ChainId("eip155:137"): "polygon-safe-gas-calibration.json",
    ChainId("eip155:4663"): "robinhood-safe-gas-calibration.json",
}


def load_calibration(chain: ChainId):
    if chain not in CALIBRATION_FILES:
        raise ValueError("no retained Safe gas calibration for the selected chain")
    path = ARTIFACTS / CALIBRATION_FILES[chain]
    if not path.is_file():
        raise ValueError(
            f"no retained Safe gas calibration for {chain}; measured Safe receipts "
            f"must be retained as {path.name} before detailed costs can be selected"
        )
    raw = path.read_bytes()
    payload = json.loads(raw)
    measurements = tuple(
        Measurement.model_validate_json(json.dumps(v)) for v in payload["measurements"]
    )
    if not measurements or any(m.origin.chain.chain_id != chain for m in measurements):
        raise ValueError("calibration contains another origin chain")
    measured = measured_maxima(measurements)
    if payload["maximum_wei"] != measured:
        raise ValueError("calibration maxima differ from retained receipt costs")
    return "0x" + hashlib.sha256(raw).hexdigest(), measured


def validate_costs(settings):
    validate_operation_costs(settings)
    chains = {f.fund.chain.chain_id for f in settings.funds}
    if len(chains) != 1:
        raise ValueError("detailed Uniswap costs require one chain-specific calibration")
    digest, maximum = load_calibration(next(iter(chains)))
    if settings.costs.calibration_sha256 != digest or settings.costs.per_operation != maximum:
        raise ValueError("explicit fixed costs must match the selected retained Safe calibration")


def validate_operation_costs(settings):
    missing = set(get_args(OperationKind)) - settings.costs.per_operation.keys()
    if missing:
        raise ValueError("explicit fixed costs missing operations: " + ", ".join(sorted(missing)))
