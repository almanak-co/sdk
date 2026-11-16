"""The read recorder merges retained evidence at one anchor and never writes provider text."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.gateway.resolution_evm import ResolutionRead
from almanak.models import Block
from scripts import record_resolution_reads as recorder

BLOCK = Block.model_validate_json(
    json.dumps(
        {
            "chain": {"chain_id": "eip155:8453", "environment": "mainnet"},
            "number": 50995893,
            "hash": "0x" + "57" * 32,
            "timestamp": 1788781133,
        }
    )
)


def read(method, params, result):
    return ResolutionRead(method=method, params=tuple(params), result=result, retrieved_at=1.0)


def progress(*markets):
    return SimpleNamespace(result=SimpleNamespace(markets=list(markets)))


def market(block, *rows):
    return SimpleNamespace(resolved=SimpleNamespace(block=block), evidence=list(rows))


def test_recorder_merges_distinct_reads_of_every_reference_at_the_shared_anchor(
    tmp_path, monkeypatch
):
    shared = read("eth_chainId", [], "0x2105")
    calls = []

    def resolve(config, operator, env_file, directory):
        calls.append((config.name, operator.name, env_file))
        specific = read("eth_call", [{"to": "0x" + "ab" * 20}, "latest"], "0x01")
        return progress(market(BLOCK, shared, specific), market(BLOCK, shared))

    monkeypatch.setattr(recorder, "resolve_reference", resolve)
    output = tmp_path / "reads.json.gz"
    assert (
        recorder.main(
            ["--reference", "a", "--reference", "b", "--env-file", "env", "--output", str(output)]
        )
        == 0
    )
    fixture = json.loads(gzip.decompress(output.read_bytes()))
    assert fixture["block"] == BLOCK.model_dump(mode="json")
    assert [row["method"] for row in fixture["evidence"]] == ["eth_chainId", "eth_call"]
    assert [call[0] for call in calls] == ["config.yaml", "config.yaml"]
    assert [call[1] for call in calls] == ["a.fork.yaml", "b.fork.yaml"]


def test_recorder_refuses_mixed_anchors_and_disagreeing_reads():
    other = BLOCK.model_copy(update={"number": BLOCK.number + 1})
    with pytest.raises(ValueError, match="one shared anchor"):
        recorder.shared_block([BLOCK, other])
    with pytest.raises(ValueError, match="one shared anchor"):
        recorder.shared_block([])
    rows = [read("eth_chainId", [], "0x2105"), read("eth_chainId", [], "0x01")]
    with pytest.raises(ValueError, match="disagree"):
        recorder.merged(rows)


def test_recorder_never_writes_provider_text(tmp_path):
    fixture = {"block": BLOCK.model_dump(mode="json"), "evidence": [{"result": "https://x"}]}
    with pytest.raises(ValueError, match="provider text"):
        recorder.write_fixture(tmp_path / "out.json.gz", fixture)
    assert not (tmp_path / "out.json.gz").exists()
