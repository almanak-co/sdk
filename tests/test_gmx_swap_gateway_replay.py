"""Native authority and scoped wallet snapshots reproduce exactly offline."""

import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import exchange_observations, permissions
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json
from tests.scope_trees import assert_same_scope
from tests.test_gmx_swap_replay import completed as completed


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_swap_permissions_regenerate_the_installed_native_safe_scope(chain):
    path = (
        Path(__file__).parents[1]
        / "docs/internal/reports/evidence/2026-09-09-gmx-exchange"
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
            "cancelOrder(bytes32)",
        }


def keeper_fees(capabilities):
    """The keeper fee bounds scope-only grants keep from the author configuration."""
    return {capability.maximum_execution_fee for capability in capabilities.requirements}


def allowance_terms(grants):
    return [c for grant in grants for c in grant.conditions if c.operator == 28]


class Reader:
    def __init__(self, state, observed):
        self.state, self.observed = state, observed

    def _rpc(self, method, params):
        assert method == "eth_getBalance"
        assert params[1] == {"blockHash": self.observed.block.hash, "requireCanonical": True}
        if params[0] == self.observed.payer.address:
            return hex(self.observed.native_balance.amount)
        assert params[0] == self.observed.target.fund.address
        return hex(self.observed.wallets[-1].amount)

    def call(self, address, method, block, **kwargs):
        assert block == self.observed.block
        if method == "decimals()":
            return (
                next(
                    value.decimals
                    for asset, value in self.state.config.asset_metadata.items()
                    if asset.reference == address
                ),
            )
        if method == "allowance(address,address)":
            assert kwargs["args"] == (
                self.observed.target.fund.address,
                self.observed.target.market.contracts.token_router,
            )
            return (
                next(
                    value.amount
                    for value in self.observed.allowances
                    if value.asset.reference == address
                ),
            )
        assert method == "balanceOf(address)" and kwargs["args"] == (
            self.observed.target.fund.address,
        )
        return (
            next(
                value.amount
                for value in (*self.observed.wallets, self.observed.market_tokens)
                if value.asset.reference == address
            ),
        )


def test_swap_snapshot_replays_fund_and_payer_measurements(completed):
    original = completed.baseline[0]
    backend = SimpleNamespace(config=completed.config, reader=Reader(completed, original))
    result = exchange_observations.snapshot(backend, "Main", original.block)
    assert result == original
    assert exchange_observations.snapshot(backend, "Main", original.block, result) == original
    bad = original.owned.model_copy(
        update={"target": original.target.model_copy(update={"allocation": "foreign"})}
    )
    with pytest.raises(ValueError, match="reference differs"):
        exchange_observations.snapshot(backend, "Main", original.block, bad)
    metadata = dict(completed.config.asset_metadata)
    asset = original.target.market.long_asset
    metadata[asset] = metadata[asset].model_copy(update={"decimals": 3})
    backend.config = completed.config.model_copy(update={"asset_metadata": metadata})
    with pytest.raises(DataError, match="configured decimals"):
        exchange_observations.snapshot(backend, "Main", original.block)
