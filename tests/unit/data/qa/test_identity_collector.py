"""Gateway-backed Data identity collection produces replayable exact evidence."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode as abi_encode

from almanak.framework.data.qa import identity_bundle
from almanak.framework.data.qa import identity_collector as collector
from almanak.framework.data.qa.production_identity import (
    DirectChainlinkFeedRequirement,
    TokenRequirement,
    V3PoolRequirement,
    observation_from_dict,
    requirement_from_dict,
)

TOKEN0 = "0x0000000000000000000000000000000000000001"
TOKEN1 = "0x0000000000000000000000000000000000000002"
FEED = "0x0000000000000000000000000000000000000003"
POOL = "0x0000000000000000000000000000000000000004"
BLOCK_HASH = "0x" + "ab" * 32


class _Rpc:
    def Call(self, request):
        assert request.method == "eth_getBlockByNumber"
        return SimpleNamespace(
            success=True,
            error="",
            result=json.dumps({"number": "0x7b", "hash": BLOCK_HASH}),
        )


class _Client:
    rpc = _Rpc()

    def __init__(self, results: dict[tuple[str, str], str]):
        self.results = results
        self.calls: list[tuple[str, str, str, int | str | None, bool]] = []

    def block_number(self, chain: str, *, timeout: float | None = None) -> int | None:
        assert chain == "ethereum"
        return 123

    def eth_call(
        self,
        chain: str,
        to: str,
        data: str,
        block: int | str | None = None,
        *,
        raise_on_error: bool = False,
    ) -> str | None:
        self.calls.append((chain, to, data, block, raise_on_error))
        return self.results[(to, data)]


def _requirements():
    return (
        TokenRequirement(chain="ethereum", address=TOKEN0, decimals=18, symbols=("AAA",)),
        DirectChainlinkFeedRequirement(chain="ethereum", address=FEED, pair="AAA/USD", decimals=8, feed_kind="usd"),
        V3PoolRequirement(
            protocol="example_v3",
            chain="ethereum",
            address=POOL,
            token_pair=(TOKEN0, TOKEN1),
            fee_tier=500,
        ),
    )


def _word(value: int) -> str:
    return "0x" + value.to_bytes(32, "big").hex()


def _address_word(value: str) -> str:
    return "0x" + int(value, 16).to_bytes(32, "big").hex()


def test_collects_all_selected_resources_at_one_pinned_block(monkeypatch, tmp_path: Path) -> None:
    requirements = _requirements()
    monkeypatch.setattr(collector, "derive_production_requirements", lambda: requirements)
    client = _Client(
        {
            (TOKEN0, collector.DECIMALS_SELECTOR): _word(18),
            (FEED, collector.DESCRIPTION_SELECTOR): "0x" + abi_encode(["string"], ["AAA / USD"]).hex(),
            (FEED, collector.DECIMALS_SELECTOR): _word(8),
            (POOL, collector.TOKEN0_SELECTOR): _address_word(TOKEN0),
            (POOL, collector.TOKEN1_SELECTOR): _address_word(TOKEN1),
            (POOL, collector.FEE_SELECTOR): _word(500),
        }
    )

    output = collector.collect_identity_bundle(
        client,
        chain="ethereum",
        output=tmp_path,
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
    )

    requirement_payload = json.loads((output / "requirements.json").read_text())
    observation_payload = json.loads((output / "observations.json").read_text())
    assert requirement_payload["scope"] == {"chain": "ethereum", "complete_chain_inventory": True}
    assert len(observation_payload["observations"]) == 3
    assert {requirement_from_dict(row) for row in requirement_payload["requirements"]} == set(requirements)
    parsed = [observation_from_dict(row) for row in observation_payload["observations"]]
    assert all(item.provenance.block_number == 123 for item in parsed)
    assert all(item.provenance.block_hash == BLOCK_HASH for item in parsed)
    assert all(call[3:] == (123, True) for call in client.calls)
    for item in parsed:
        artifact = output / "raw" / f"{item.provenance.artifact_sha256}.json"
        assert artifact.is_file()
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == item.provenance.artifact_sha256

    monkeypatch.setattr(identity_bundle, "derive_production_requirements", lambda: requirements)
    validated = identity_bundle.validate_identity_bundle(
        output,
        sealed_at=datetime(2026, 8, 15, 1, tzinfo=UTC),
    )
    assert validated.evaluation.passed
    assert validated.complete_chain_inventory


def test_unknown_requirement_cannot_be_collected(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(collector, "derive_production_requirements", _requirements)

    with pytest.raises(ValueError, match="Unknown production identity requirement"):
        collector.collect_identity_bundle(
            _Client({}),
            chain="ethereum",
            output=tmp_path,
            requirement_ids=["token:ethereum:attacker"],
        )


def test_missing_gateway_measurement_fails_closed(monkeypatch, tmp_path: Path) -> None:
    requirement = _requirements()[0]
    monkeypatch.setattr(collector, "derive_production_requirements", lambda: (requirement,))
    client = _Client({(TOKEN0, collector.DECIMALS_SELECTOR): None})  # type: ignore[dict-item]

    with pytest.raises(ValueError, match="returned no result"):
        collector.collect_identity_bundle(client, chain="ethereum", output=tmp_path)


def test_sealer_rejects_observation_not_entailed_by_raw_artifact(monkeypatch, tmp_path: Path) -> None:
    requirement = _requirements()[0]
    monkeypatch.setattr(collector, "derive_production_requirements", lambda: (requirement,))
    monkeypatch.setattr(identity_bundle, "derive_production_requirements", lambda: (requirement,))
    collector.collect_identity_bundle(
        _Client({(TOKEN0, collector.DECIMALS_SELECTOR): _word(18)}),
        chain="ethereum",
        output=tmp_path,
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    payload = json.loads((tmp_path / "observations.json").read_text())
    payload["observations"][0]["decimals"] = 6
    (tmp_path / "observations.json").write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="not entailed"):
        identity_bundle.validate_identity_bundle(
            tmp_path,
            sealed_at=datetime(2026, 8, 15, 1, tzinfo=UTC),
        )


def test_sealer_rejects_unreferenced_raw_artifact(monkeypatch, tmp_path: Path) -> None:
    requirement = _requirements()[0]
    monkeypatch.setattr(collector, "derive_production_requirements", lambda: (requirement,))
    monkeypatch.setattr(identity_bundle, "derive_production_requirements", lambda: (requirement,))
    collector.collect_identity_bundle(
        _Client({(TOKEN0, collector.DECIMALS_SELECTOR): _word(18)}),
        chain="ethereum",
        output=tmp_path,
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    (tmp_path / "raw" / f"{'ff' * 32}.json").write_text("{}")

    with pytest.raises(ValueError, match="not bijective"):
        identity_bundle.validate_identity_bundle(
            tmp_path,
            sealed_at=datetime(2026, 8, 15, 1, tzinfo=UTC),
        )


# --- Raw-artifact tamper matrix -------------------------------------------
#
# _validate_raw_artifact is the guard that stops a forged bundle from sealing:
# it re-checks that the raw gateway capture is the one the observation claims,
# was taken at the pinned block, and actually contains the bytes the observation
# reports. Every branch below is a rejection path, and none of them had a test --
# the function was 66% covered incidentally, through the happy path only. An
# untested reject branch in a tamper detector is indistinguishable from one that
# does not fire, which is the failure mode this whole bundle format exists to
# prevent.


def _sealed_bundle(monkeypatch, tmp_path: Path) -> Path:
    """One valid single-requirement bundle, built by the real collector."""
    requirement = _requirements()[0]
    monkeypatch.setattr(collector, "derive_production_requirements", lambda: (requirement,))
    monkeypatch.setattr(identity_bundle, "derive_production_requirements", lambda: (requirement,))
    collector.collect_identity_bundle(
        _Client({(TOKEN0, collector.DECIMALS_SELECTOR): _word(18)}),
        chain="ethereum",
        output=tmp_path,
        captured_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    return tmp_path


def _raw_path(bundle: Path) -> Path:
    (path,) = (bundle / "raw").glob("*.json")
    return path


def _reseal_raw(bundle: Path, raw: dict) -> None:
    """Rewrite the raw capture and re-point the digest chain at it.

    The bundle is content-addressed, so simply editing the file trips the digest
    check before the branch under test is ever reached. Re-naming the artifact to
    its new hash and updating the observation's provenance moves the mutation
    PAST that guard, which is the only way to prove the deeper guards fire.
    """
    old = _raw_path(bundle)
    body = json.dumps(raw).encode()
    digest = hashlib.sha256(body).hexdigest()
    old.unlink()
    (bundle / "raw" / f"{digest}.json").write_bytes(body)
    payload = json.loads((bundle / "observations.json").read_text())
    payload["observations"][0]["provenance"]["artifact_sha256"] = digest
    (bundle / "observations.json").write_text(json.dumps(payload))


def _expect(bundle: Path, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        identity_bundle.validate_identity_bundle(bundle, sealed_at=datetime(2026, 8, 15, 1, tzinfo=UTC))


def test_sealer_rejects_missing_raw_artifact(monkeypatch, tmp_path: Path) -> None:
    bundle = _sealed_bundle(monkeypatch, tmp_path)
    _raw_path(bundle).unlink()
    _expect(bundle, "missing")


def test_sealer_rejects_raw_artifact_edited_in_place(monkeypatch, tmp_path: Path) -> None:
    """The content-address guard itself: same filename, different bytes."""
    bundle = _sealed_bundle(monkeypatch, tmp_path)
    path = _raw_path(bundle)
    raw = json.loads(path.read_text())
    raw["observed"] = {"decimals": 6}
    path.write_text(json.dumps(raw))
    _expect(bundle, "digest mismatch")


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda raw: raw.__setitem__("requirement", {"requirement_id": "forged"}), "requirement mismatch"),
        (lambda raw: raw.__setitem__("pinned_block", "not-an-object"), "lacks pinned block"),
        (lambda raw: raw["pinned_block"].__setitem__("number", 999), "block number mismatch"),
        (lambda raw: raw["pinned_block"].__setitem__("hash", "0x" + "cd" * 32), "block hash mismatch"),
        (lambda raw: raw.__setitem__("calls", []), "no RPC calls"),
        (lambda raw: raw.__setitem__("calls", "not-a-list"), "no RPC calls"),
        (lambda raw: raw.__setitem__("calls", ["not-an-object"]), "Malformed gateway call"),
        (lambda raw: raw["calls"][0].__setitem__("to", TOKEN1), "Unbound gateway call"),
        (lambda raw: raw["calls"][0].__setitem__("chain", "base"), "Unbound gateway call"),
        (lambda raw: raw["calls"][0].__setitem__("method", "eth_getLogs"), "Unbound gateway call"),
        (lambda raw: raw["calls"][0].__setitem__("block", "0x1"), "Unbound gateway call"),
        (lambda raw: raw["calls"][0].__setitem__("result", None), "request/result bytes"),
        (lambda raw: raw["calls"][0].__setitem__("data", 1234), "request/result bytes"),
    ],
    ids=[
        "requirement-swapped",
        "pinned-block-not-an-object",
        "pinned-block-number",
        "pinned-block-hash",
        "calls-empty",
        "calls-not-a-list",
        "call-not-an-object",
        "call-rebound-to-another-address",
        "call-rebound-to-another-chain",
        "call-rebound-to-another-method",
        "call-rebound-to-another-block",
        "call-result-missing",
        "call-data-not-a-string",
    ],
)
def test_sealer_rejects_tampered_raw_artifact(monkeypatch, tmp_path: Path, mutate, match: str) -> None:
    bundle = _sealed_bundle(monkeypatch, tmp_path)
    raw = json.loads(_raw_path(bundle).read_text())
    mutate(raw)
    _reseal_raw(bundle, raw)
    _expect(bundle, match)


def test_sealer_rejects_raw_return_bytes_that_disagree_with_the_observation(monkeypatch, tmp_path: Path) -> None:
    """The last guard, and the one a careful forger runs into.

    Editing only the observation trips the `observed` comparison earlier in the
    function. This mutation instead keeps `observed` and the observation in
    agreement at 18 decimals and rewrites the eth_call RETURN BYTES to 6 -- a
    bundle that is internally consistent everywhere except against the chain
    data it claims to have captured. Nothing above this line can see that, so
    without the final decode the two edits would cancel and the bundle would
    seal.
    """
    bundle = _sealed_bundle(monkeypatch, tmp_path)
    raw = json.loads(_raw_path(bundle).read_text())
    assert raw["observed"] == {"decimals": 18}, raw["observed"]
    raw["calls"][0]["result"] = _word(6)
    _reseal_raw(bundle, raw)
    _expect(bundle, "do not entail observation")
