"""Native-currency V4 custody through resolution, allocation, fork funding and composition."""

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.connectors.composed.fork import allocation_assets
from almanak.connectors.uniswap_v4 import reconciliation
from almanak.connectors.uniswap_v4.deployments import NETWORK_ROLES
from almanak.connectors.uniswap_v4.fork import fork_allocation
from almanak.connectors.uniswap_v4.models import Allocation, Deployment
from almanak.connectors.uniswap_v4.observations import initial_owned
from almanak.connectors.uniswap_v4.receipts import movement
from almanak.connectors.uniswap_v4.resolution import ResolvedPool, custody
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import Asset, AssetAmount
from tests.test_uniswap_v4_resolution import recorded as recorded
from tests.test_uniswap_v4_resolution import replay as replay
from tests.test_uniswap_v4_resolution import resolve_market, selected
from tests.uniswap_native_support import (
    BNB,
    ETHEREUM,
    USDC,
    USDG,
    erc20,
    eth_in_receipt,
    native,
    pool,
    swap,
    target,
    transfer,
    wrapped,
)


class Assets:
    """A reader stand-in answering asset metadata reads."""

    def __init__(self, decimals, chain=ETHEREUM):
        self.decimals, self.chain = decimals, chain

    def asset(self, token, block):
        return Asset(chain=self.chain, identifier=token, decimals=self.decimals)


def native_resolution(result, **custody_fields):
    """A retained ERC-20 resolution re-keyed to the native ETH/USDC pool, as a dict."""
    raw = result.model_dump(mode="json")
    key = {"currency0": str(native(ETHEREUM)), "currency1": str(erc20(ETHEREUM, USDC))}
    usdc = next(a for a in (raw["asset0"], raw["asset1"]) if a["identifier"] == key["currency1"])
    raw["selection"].update(key, fee=3000, tick_spacing=60)
    raw["pool"].update(key, fee=3000, tick_spacing=60)
    raw["asset0"] = {
        "chain": raw["asset1"]["chain"],
        "identifier": key["currency0"],
        "decimals": 18,
    }
    raw["asset1"] = usdc
    weth = {"chain": usdc["chain"], "identifier": str(wrapped(ETHEREUM)), "decimals": 18}
    return {**raw, "custody0": weth, **custody_fields}


def test_a_resolved_native_pool_holds_the_wrapped_token_it_read(recorded, replay):
    block, _ = recorded
    result = ResolvedPool.model_validate_json(
        json.dumps(native_resolution(resolve_market(replay(), selected(), block)))
    )
    assert [a.identifier for a in result.currencies] == [native(ETHEREUM), erc20(ETHEREUM, USDC)]
    assert [a.identifier for a in result.assets] == [wrapped(ETHEREUM), erc20(ETHEREUM, USDC)]
    assert result.summary()["currencies"][0] == str(native(ETHEREUM))


@pytest.mark.parametrize(
    "change",
    [
        {"custody0": None},
        {"custody0": {"identifier": str(erc20(ETHEREUM, USDG)), "decimals": 18}},
        {"custody0": {"decimals": 6}},
        {"custody1": {"identifier": str(erc20(ETHEREUM, USDC)), "decimals": 6}},
    ],
)
def test_a_retained_native_resolution_must_carry_its_exact_custody(recorded, replay, change):
    block, _ = recorded
    raw = native_resolution(resolve_market(replay(), selected(), block))
    for field, update in change.items():
        raw[field] = None if update is None else {**(raw.get(field) or raw["asset1"]), **update}
    with pytest.raises(ValidationError, match="custody differs"):
        ResolvedPool.model_validate_json(json.dumps(raw))


def test_custody_reads_the_wrapped_token_and_refuses_a_precision_mismatch():
    block = SimpleNamespace()
    held = custody(
        Assets(18), Asset(chain=ETHEREUM, identifier=native(ETHEREUM), decimals=18), block
    )
    assert held.identifier == wrapped(ETHEREUM)
    assert (
        custody(Assets(18), Asset(chain=ETHEREUM, identifier=wrapped(ETHEREUM), decimals=18), block)
        is None
    )
    with pytest.raises(DataError, match="precision differs"):
        custody(Assets(6), Asset(chain=ETHEREUM, identifier=native(ETHEREUM), decimals=18), block)
    bnb = custody(Assets(18, BNB), Asset(chain=BNB, identifier=native(BNB), decimals=18), block)
    assert bnb.identifier == wrapped(BNB)


def allocation(capital_asset=None):
    source = pool("eth_usdc")
    roles = NETWORK_ROLES["eip155:1"]
    return Allocation(
        name="trade",
        account="fund",
        pool=source,
        contracts=Deployment(
            **{k: roles[k] for k in ("router", "position_manager", "quoter", "permit2")}
        ),
        capital=AssetAmount(asset=capital_asset or wrapped(ETHEREUM), amount=5 * 10**16),
        hold_seconds=60,
        slippage_bps=50,
        deadline_seconds=300,
        native_asset=native_asset(ETHEREUM.chain_id),
        minimum_native_balance=0,
    )


def test_capital_names_the_wrapped_token_not_the_native_coin():
    assert allocation().capital.asset == wrapped(ETHEREUM)
    with pytest.raises(ValidationError, match="capital must identify"):
        allocation(capital_asset=native(ETHEREUM))


def test_initial_inventory_fork_funding_and_composition_hold_wrapped_native():
    selected_allocation = allocation()
    config = SimpleNamespace(
        allocation=lambda name: selected_allocation, target=lambda name: target("eth_usdc")
    )
    owned = initial_owned(config, "trade")
    assert (owned.inventory0.asset, owned.inventory0.amount) == (wrapped(ETHEREUM), 5 * 10**16)
    assert (owned.inventory1.asset, owned.inventory1.amount) == (erc20(ETHEREUM, USDC), 0)
    forked = fork_allocation(selected_allocation, ETHEREUM)
    assert forked.capital.asset == wrapped(ETHEREUM) and forked.pool.currency0 == native(ETHEREUM)
    assert allocation_assets(selected_allocation) == (wrapped(ETHEREUM), erc20(ETHEREUM, USDC))


def test_an_exit_through_changed_hook_code_on_a_native_leg_is_recorded_from_fund_transfers(
    monkeypatch,
):
    operation = swap("fables_weth_usdg", eth_in=True, amount=10**16, minimum=1)
    receipt = eth_in_receipt(operation, 25 * 10**6)
    manager, hooks = operation.target.pool.pool_manager, operation.target.pool.hooks
    # The changed code now also takes some output, outside the reviewed override-fee model.
    receipt["logs"].insert(
        -1, transfer(erc20(operation.target.pool.chain, USDG).reference, manager, hooks, 10)
    )
    with pytest.raises(DataError):
        movement(receipt, operation)
    monkeypatch.setattr(reconciliation, "AuthorityReader", lambda reader, block: None)
    monkeypatch.setattr(reconciliation, "require_entry_code", lambda *a, entering: False)
    pending = SimpleNamespace(operation=operation, before=SimpleNamespace(hook_terms=None))
    deltas, nft, measured, (issue,) = reconciliation.receipt_movement(
        SimpleNamespace(reader=None), pending, receipt, None
    )
    assert (deltas, nft, measured) == ((-(10**16), 25 * 10**6), None, None)
    assert "differs from its pin" in issue and "reviewed model" in issue
