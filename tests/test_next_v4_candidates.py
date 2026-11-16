"""New V4 candidates bind exact compiled revisions and selected archival pool quotes."""

import gzip
import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.connectors.uniswap_v4 import deployments
from almanak.connectors.uniswap_v4.models import Target
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity

ROOT = Path(__file__).parents[1]


@pytest.fixture(params=((137, "polygon"), (4663, "robinhood")))
def selected(request):
    number, name = request.param
    return ROOT / f"docs/internal/reports/evidence/2026-09-09-dex-finish/v4-{number}-source", name


def candidate(capture):
    discovery = json.loads(
        gzip.decompress((capture / "selected-provider-discovery.json.gz").read_bytes())
    )
    assert discovery["status"] == "qualified_revision_market"
    assert len(discovery["quotes"]) == 2 and all(
        row["values"][0] > 0 for row in discovery["quotes"]
    )
    pool = discovery["source"]
    roles = json.loads((capture / "roles.json").read_text())
    return Target.model_validate_json(
        json.dumps(
            {
                "allocation": "trade",
                "account": "fund",
                "fund": {"chain": pool["chain"], "address": "0x" + "12" * 20},
                "pool": pool,
                "contracts": {
                    k: v for k, v in roles.items() if k not in ("pool_manager", "state_view")
                },
                "range": None,
                "funding_asset": pool["currency0"],
            }
        )
    )


def identities(name):
    path = deployments.ARTIFACTS / f"{name}-protocol.json"
    return tuple(ContractIdentity.model_validate(v) for v in json.loads(path.read_text()).values())


def test_archived_alchemy_runtimes_match_the_reconstructed_source_bindings(selected):
    capture, name = selected
    target = candidate(capture)
    expected = identities(name)
    assert deployments.infrastructure(target) == {i.address for i in expected}
    deployments.verify_identities(target, expected)
    reads = json.loads(
        gzip.decompress((capture / "selected-provider-discovery.json.gz").read_bytes())
    )["reads"]
    rebuilt = json.loads((capture / "recompiled-bindings.json").read_text())
    assert len(rebuilt) == 6
    for identity in expected:
        codes = {
            r["result"]
            for r in reads
            if r["method"] == "eth_getCode" and r["params"][0].lower() == identity.address
        }
        assert len(codes) == 1
        assert "0x" + keccak(bytes.fromhex(codes.pop()[2:])).hex() == identity.code_hash


@pytest.mark.parametrize("index", range(6))
def test_each_changed_infrastructure_runtime_is_refused(selected, index):
    capture, name = selected
    observed = list(identities(name))
    observed[index] = observed[index].model_copy(update={"code_hash": "0x" + "34" * 32})
    with pytest.raises(DataError, match="code differs"):
        deployments.verify_identities(candidate(capture), tuple(observed))
