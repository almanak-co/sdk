"""EVM-owned canonical anchors and bounded resolver read evidence."""

import re
import time
from typing import Literal

from pydantic import JsonValue

from almanak.chains import ChainReference, CheckpointReference
from almanak.configuration.resolution import AnchorSelection
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import erc20_address
from almanak.models import Block, Model


class ResolutionRead(Model):
    method: Literal[
        "eth_chainId",
        "eth_getBlockByNumber",
        "eth_getBlockByHash",
        "eth_getCode",
        "eth_getStorageAt",
        "eth_call",
        # A HyperCore `/info` answer (blueprint 30, section 12): HyperCore market facts are
        # not EVM state, so they resolve from the venue's API beside the chain's reads.
        "hypercore_info",
    ]
    params: tuple[JsonValue, ...]
    result: JsonValue
    retrieved_at: float


class ResolutionReader(EthereumReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.evidence: list[ResolutionRead] = []

    def hypercore(self, query) -> JsonValue:
        """One whitelisted mainnet `/info` answer, retained like the chain's reads."""
        from almanak.gateway.hypercore_info import HyperCoreInfo, InfoRequest, body

        with HyperCoreInfo() as client:
            result = client.fetch(InfoRequest(info=query))
        self.evidence.append(
            ResolutionRead(
                method="hypercore_info",
                params=(body(query),),
                result=result.result,
                retrieved_at=result.received_at / 1000,
            )
        )
        return result.result

    def _rpc(self, method, params, *, allow_none=False):
        value = super()._rpc(method, params, allow_none=allow_none)
        self.evidence.append(
            ResolutionRead(
                method=method,
                params=tuple(params),
                result=_canonical_block_result(method, value),
                retrieved_at=time.time(),
            )
        )
        return value


# Non-consensus block fields a provider's node pool computes independently, so
# the same pinned block can read back differently per node. Retained evidence
# must compare only what downstream code consumes, or the same anchor fails.
_VOLATILE_BLOCK_FIELDS = ("size", "totalDifficulty", "milliTimestamp")


def _canonical_block_result(method, value):
    if not isinstance(value, dict) or not method.startswith("eth_getBlockBy"):
        return value
    return {key: item for key, item in value.items() if key not in _VOLATILE_BLOCK_FIELDS}


def checkpoint(block: Block) -> CheckpointReference:
    return CheckpointReference(
        chain=block.chain,
        reference=ChainReference(block.hash),
        height=block.number,
        timestamp=block.timestamp,
    )


def evm_block(anchor: CheckpointReference) -> Block:
    if anchor.chain.chain_id.namespace != "eip155" or anchor.height is None:
        raise ValueError("EVM resolution requires an EVM checkpoint")
    return Block(
        chain=anchor.chain,
        number=anchor.height,
        hash=str(anchor.reference),
        timestamp=anchor.timestamp,
    )


def resolve_anchor(reader: EthereumReader, selection: AnchorSelection) -> CheckpointReference:
    if reader.chain.chain_id != selection.chain or reader.chain.environment != "mainnet":
        raise ValueError("origin resolution reader differs from the selected chain")
    height = selection.height
    selected = None
    if selection.at is not None:
        height = floor_height(reader, selection.at)
    elif height is None and selection.reference is not None:
        reader.verify_chain()
        header = reader._rpc("eth_getBlockByHash", [str(selection.reference), False])
        try:
            selected = Block(
                chain=reader.chain,
                number=int(header["number"], 16),
                hash=header["hash"],
                timestamp=int(header["timestamp"], 16),
            )
        except (ValueError, KeyError, TypeError):
            raise DataError("malformed", "selected block header is invalid") from None
        selection.require(checkpoint(selected))
        # Canonical lookup by height rejects a retained hash from an orphaned block.
        height = selected.number
    if not selection.fresh and height is None:
        raise ValueError("fixed EVM resolution requires a block height or hash")
    block = reader.pin_block("latest" if height is None else height)
    if selected is not None and selected != block:
        raise DataError("block_mismatch", "hash-selected block differs from its canonical header")
    result = checkpoint(block)
    selection.require(result)
    return result


def floor_height(reader: EthereumReader, at: int) -> int:
    """The historical model's own boundary search selects the last block at a time."""
    from almanak.backtest.evm import EvmHistory
    from almanak.backtest.types import UnixTime

    return EvmHistory(reader).floor(UnixTime(at)).number


class RetainedResolutionReader(EthereumReader):
    """Replay exact successful reads; this transport never contacts a provider."""

    def __init__(self, chain, evidence):
        import json

        self.responses: dict[str, JsonValue] = {}
        for row in evidence:
            key = json.dumps([row.method, row.params], sort_keys=True)
            if key in self.responses and self.responses[key] != row.result:
                raise DataError("resolution_evidence", "retained reads disagree at the same anchor")
            self.responses[key] = row.result
        super().__init__(chain, "https://retained-resolution.invalid")

    def _rpc(self, method, params, *, allow_none=False):
        import json

        key = json.dumps([method, params], sort_keys=True)
        if key not in self.responses:
            raise DataError("resolution_evidence", "retained resolution lacks a required read")
        result = self.responses[key]
        if result is None and not allow_none:
            raise DataError("resolution_evidence", "retained resolution read is unavailable")
        return result

    def hypercore(self, query) -> JsonValue:
        """The retained `/info` answer, checked against its closed shape again."""
        from almanak.gateway.hypercore_info import body, matched

        return matched(query, self._rpc("hypercore_info", [body(query)]))


# A display symbol is untrusted contract text: one short token without markup characters.
SYMBOL = re.compile(r"[^\s<>&\"'`\\]{1,32}")


def asset_symbols(reader: EthereumReader, assets, block: Block) -> dict:
    """ERC-20 display symbols at the anchor; a token without a readable one has none.

    Symbols name assets for people. They are never verified facts, so an unanswered or
    malformed `symbol()` leaves the asset unnamed instead of failing the resolution.
    """
    symbols = {}
    for asset in assets:
        token = asset.identifier
        if token.namespace != "erc20" or token in symbols:
            continue
        try:
            (value,) = reader.call(
                erc20_address(token, block.chain.chain_id), "symbol()", block, outputs=("string",)
            )
        except DataError:
            continue
        if isinstance(value, str) and SYMBOL.fullmatch(value.strip()) and value.isprintable():
            symbols[token] = value.strip()
    return symbols
