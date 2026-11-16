"""Native GM authority and independently observed balances reproduce offline."""

import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_observations, permissions
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import gm_completed as gm_completed
from tests.gmx_retired import current_json
from tests.scope_trees import assert_same_scope
from tests.test_gmx_swap_gateway_replay import Reader, allowance_terms, keeper_fees


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_gm_permissions_regenerate_the_installed_native_safe_scope(chain):
    path = (
        Path(__file__).parents[1]
        / "docs/internal/reports/evidence/2026-09-09-gmx-gm"
        / (chain + "-safe-authority-runtime/permissions/manifest.json.gz")
    )
    manifest = permissions.Permissions.manifest_type.model_validate_json(
        current_json(gzip.decompress(path.read_bytes()))
    )
    for partition in manifest.partitions:
        # The installed package carried non-refilling quotas. Scope-only derivation
        # (blueprint 27) keeps every installed scope word by value and the keeper fee
        # bounds; only author amount bounds and their allowances are history.
        grants = permissions.derive(partition.profile, manifest.capabilities)
        assert_same_scope(grants, partition.grants, keeper_fees(manifest.capabilities))
        assert allowance_terms(partition.grants) and not allowance_terms(grants)
        assert {grant.signature for grant in grants} == {
            "approve(address,uint256)",
            "multicall(bytes[])",
            "cancelDeposit(bytes32)",
            "cancelWithdrawal(bytes32)",
        }


def test_gm_gateway_replays_fund_shares_and_rejects_changed_share_units(completed):
    observed = completed.baseline[0]
    backend = SimpleNamespace(config=completed.config, reader=Reader(completed, observed))
    assert exchange_observations.snapshot(backend, "Main", observed.block) == observed
    metadata = dict(completed.config.asset_metadata)
    asset = observed.target.market.share_asset
    metadata[asset] = metadata[asset].model_copy(update={"decimals": 6})
    backend.config = completed.config.model_copy(update={"asset_metadata": metadata})
    with pytest.raises(DataError, match="configured decimals"):
        exchange_observations.snapshot(backend, "Main", observed.block)
