"""The BNB candidate binds six exact source-compiled infrastructure roles."""

import json
from pathlib import Path

import pytest
from eth_utils import keccak

from almanak.connectors.uniswap_v4 import deployments
from almanak.connectors.uniswap_v4.models import Target
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity

ROOT = Path(__file__).parents[1]
CAPTURE = ROOT / "docs/internal/reports/evidence/2026-09-09-dex-finish/bnb-v4-source"


def candidate():
    discovery = json.loads((CAPTURE / "discovery.json").read_text())
    pool = next(
        row["pool"]
        for row in discovery["markets"]
        if row["period"] == "latest" and row["pool"]["fee"] == 500
    )
    roles = json.loads((CAPTURE / "roles.json").read_text())
    return Target.model_validate_json(
        json.dumps(
            {
                "allocation": "trade",
                "account": "fund",
                "fund": {"chain": discovery["chain"], "address": "0x" + "12" * 20},
                "pool": pool,
                "contracts": {
                    k: v for k, v in roles.items() if k not in ("pool_manager", "state_view")
                },
                "range": None,
                "funding_asset": pool["currency0"],
            }
        )
    )


def identities():
    path = deployments.ARTIFACTS / "bnb-protocol.json"
    return tuple(ContractIdentity.model_validate(v) for v in json.loads(path.read_text()).values())


def test_archived_alchemy_runtimes_match_the_reconstructed_source_bindings():
    target = candidate()
    expected = identities()
    assert deployments.infrastructure(target) == {i.address for i in expected}
    deployments.verify_identities(target, expected)
    reads = json.loads((CAPTURE / "identity-rpc-capture.json").read_text())
    rebuilt = json.loads((CAPTURE / "recompiled-bindings.json").read_text())
    assert len(rebuilt) == 6 and all(r["original_provider_runtime_exact"] for r in rebuilt)
    for identity in expected:
        codes = {
            r["result"]
            for r in reads
            if r["method"] == "eth_getCode" and r["params"][0].lower() == identity.address
        }
        assert len(codes) == 1
        assert "0x" + keccak(bytes.fromhex(codes.pop()[2:])).hex() == identity.code_hash


@pytest.mark.parametrize("index", range(6))
def test_each_changed_bnb_infrastructure_runtime_is_refused(index):
    observed = list(identities())
    observed[index] = observed[index].model_copy(update={"code_hash": "0x" + "34" * 32})
    with pytest.raises(DataError, match="code differs"):
        deployments.verify_identities(candidate(), tuple(observed))
