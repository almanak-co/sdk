"""Captured balances remain scoped to the fund and independent transaction submitter."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2 import observations
from almanak.connectors.gmx_v2.positions import PositionObservation
from almanak.gateway.ethereum import DataError
from tests.gmx_evidence import checkpoint


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
            assert kwargs["args"][0] == self.observed.target.fund.address
            return (self.observed.allowance,)
        assert method == "balanceOf(address)" and kwargs["args"] == (
            self.observed.target.fund.address,
        )
        return (next(a.amount for a in self.observed.wallets if a.asset.reference == address),)


def test_snapshot_uses_exact_owned_reference_and_independent_payer_balance(monkeypatch):
    state = checkpoint()
    original = state.baseline[0]
    backend = SimpleNamespace(config=state.config, reader=Reader(state, original))
    position = PositionObservation(
        target=original.target, block=original.block, position=original.position
    )
    monkeypatch.setattr(observations, "read_position", lambda *_: position)
    monkeypatch.setattr(observations, "claimable_funding", lambda *_: original.claimable_funding)
    result = observations.snapshot(backend, "Main", original.block)
    assert result == original
    assert result.payer != result.target.fund
    assert result.native_balance.amount > result.wallets[-1].amount
    assert observations.snapshot(backend, "Main", original.block, result) == original
    bad = original.owned.model_copy(
        update={"target": original.target.model_copy(update={"allocation": "foreign"})}
    )
    with pytest.raises(ValueError, match="reference differs"):
        observations.snapshot(backend, "Main", original.block, bad)
    metadata = dict(state.config.asset_metadata)
    asset = original.target.collateral_asset
    metadata[asset] = metadata[asset].model_copy(update={"decimals": 6})
    backend.config = state.config.model_copy(update={"asset_metadata": metadata})
    with pytest.raises(DataError, match="configured decimals"):
        observations.snapshot(backend, "Main", original.block)
