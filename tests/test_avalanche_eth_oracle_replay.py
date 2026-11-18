"""The added ETH feed reconstructs from retained, exact verified dependency runtimes."""

import copy
import json
import sqlite3
from pathlib import Path

import pytest

from almanak.backtest.economic_oracles import OracleReader
from almanak.backtest.history import ReadEvidence, ReadRequest
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.models import Block

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-09-finish-lending"
    / "avalanche-oracle-expansion-qualification"
)


class Replay(EthereumReader):
    def __init__(self, block):
        super().__init__(block.chain, "http://retained.invalid")
        with sqlite3.connect(f"file:{EVIDENCE / 'rpc.sqlite'}?mode=ro", uri=True) as database:
            self.rows = {
                row.request.key: row.result
                for (body,) in database.execute("SELECT body FROM observations")
                for row in (ReadEvidence.model_validate_json(body),)
            }

    def _rpc(self, method, params, *, allow_none=False):
        request = ReadRequest(chain=self.chain, method=method, params=tuple(params))
        if request.key not in self.rows:
            raise AssertionError("missing captured RPC input; network fallback forbidden")
        return copy.deepcopy(self.rows[request.key])


def recorded(index):
    raw = json.loads((EVIDENCE / "result.json").read_text())["blocks"][index]
    return raw, Block.model_validate_json(json.dumps(raw["block"]))


@pytest.mark.parametrize("index", [0, 1, 2])
def test_eth_source_reconstructs_at_archive_fixed_and_fresh_boundaries(index):
    raw, block = recorded(index)
    with Replay(block) as reader:
        assert reader.pin_block(block.number) == block
        oracle = OracleReader(reader)
        observed = oracle.answer(
            raw["feed"]["address"], "0x84b50b131a82da689c0205c00d603c1c92a5f8a4", block
        )
        assert observed == raw["feed"]["latest"]["value"][0] > 0
        assert oracle.dependencies == {raw["feed"]["address"], raw["aggregator"]["address"]}
        assert not oracle.deadlines


@pytest.mark.parametrize("role", ["feed", "aggregator"])
def test_changed_dependency_runtime_refuses_reconstruction(role):
    raw, block = recorded(0)
    with Replay(block) as reader:
        reader.pin_block(block.number)
        request = ReadRequest(
            chain=block.chain,
            method="eth_getCode",
            params=(raw[role]["address"], {"blockHash": str(block.hash), "requireCanonical": True}),
        )
        reader.rows[request.key] = "0x00" + reader.rows[request.key][4:]
        with pytest.raises(DataError, match="differs from its verified revision"):
            OracleReader(reader).answer(raw["feed"]["address"], raw["feed"]["address"], block)
