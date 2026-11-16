"""Correlated Morpho core, AdaptiveCurve IRM and explicit oracle revisions."""

import json
from pathlib import Path
from typing import Literal

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.evm_revision import read_code
from almanak.connectors.morpho_blue.domain import Market
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import Model, address

REVISIONS = json.loads(Path(__file__).with_name("revisions.json").read_text())


class Deployment(Model):
    contracts: tuple[ContractIdentity, ...]
    oracle_inputs: dict[str, int]
    oracle_kind: Literal["chainlink_v2", "erc4626_conversion"] = "chainlink_v2"


def oracle_dependencies(deployment):
    roles = (
        ("STAVAX",)
        if deployment.oracle_kind == "erc4626_conversion"
        else ("BASE_FEED_1", "BASE_FEED_2", "QUOTE_FEED_1", "QUOTE_FEED_2")
    )
    return tuple(
        dict.fromkeys(
            immutable_address(deployment.oracle_inputs[role])
            for role in roles
            if deployment.oracle_inputs[role]
        )
    )


def immutable_address(value):
    if type(value) is not int or not 0 <= value < 2**160:
        raise DataError("revision", "Morpho constructor address is malformed")
    return address("0x" + value.to_bytes(20).hex())


def verify_domain(market, observed):
    domain = keccak(
        encode(
            ["bytes32", "uint256", "address"],
            [
                keccak(text="EIP712Domain(uint256 chainId,address verifyingContract)"),
                chain_number(market.chain.chain_id),
                market.core,
            ],
        )
    )
    if observed != int.from_bytes(domain):
        raise DataError(
            "revision", "Morpho core domain differs from its canonical chain and address"
        )


def verify_parameters(reader, market, block):
    values = reader.call(
        market.core,
        "idToMarketParams(bytes32)",
        block,
        inputs=("bytes32",),
        args=(bytes.fromhex(str(market.market_id)[2:]),),
        outputs=("address", "address", "address", "address", "uint256"),
    )
    if tuple(values) != market.parameters:
        raise DataError("market", "Morpho stored market parameters differ from the explicit target")


def verify_oracle_scope(values):
    if values["BASE_VAULT"] or values["QUOTE_VAULT"]:
        raise DataError("revision", "Morpho vault-based oracle conversions are unsupported")
    if values["BASE_VAULT_CONVERSION_SAMPLE"] != 1 or values["QUOTE_VAULT_CONVERSION_SAMPLE"] != 1:
        raise DataError("revision", "Morpho non-vault oracle conversion samples must be one")
    if not 0 < values["SCALE_FACTOR"] < 2**256:
        raise DataError("revision", "Morpho oracle requires a positive scale factor")
    for key in ("BASE_FEED_1", "BASE_FEED_2", "QUOTE_FEED_1", "QUOTE_FEED_2"):
        immutable_address(values[key])


def read_deployment(reader, market: Market, block) -> Deployment:
    if reader.chain != market.chain or market.chain != block.chain or reader._pinned != block:
        raise DataError(
            "block_mismatch", "Morpho verification requires its configured pinned chain"
        )
    revision = REVISIONS.get(str(market.chain.chain_id))
    if revision is None:
        raise DataError("revision", "Morpho has no verified deployment revision on this chain")
    core_values, core = read_code(reader, market.core, block, revision["morpho"])
    verify_domain(market, core_values["DOMAIN_SEPARATOR"])
    irm_values, irm = read_code(reader, market.irm, block, revision["irm"])
    if immutable_address(irm_values["MORPHO"]) != market.core:
        raise DataError("revision", "Morpho IRM is bound to another core contract")
    oracle_values, oracle = read_code(reader, market.oracle, block, revision["oracle"])
    kind = revision.get("oracle_kind", "chainlink_v2")
    if kind == "erc4626_conversion":
        if immutable_address(oracle_values["STAVAX"]) != market.collateral_asset.reference:
            raise DataError("revision", "Morpho conversion oracle is bound to another collateral")
    else:
        verify_oracle_scope(oracle_values)
    verify_parameters(reader, market, block)
    return Deployment(contracts=(core, irm, oracle), oracle_inputs=oracle_values, oracle_kind=kind)
