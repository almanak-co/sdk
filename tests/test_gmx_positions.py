"""Aggregate position identity cannot change with allocation labels or missing balances."""

import pytest

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.connectors.gmx_v2.domain import Contracts, Market, Target
from almanak.connectors.gmx_v2.intents import DecreasePosition, IncreasePosition
from almanak.connectors.gmx_v2.position_reader import decoded_position, position_key
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Account, AssetAmount, Chain


def target():
    chain = Chain(chain_id=ChainId("eip155:42161"), environment="anvil:gmx-identity")
    assets = tuple(AssetType(f"eip155:42161/erc20:0x{i:040x}") for i in range(1, 5))
    contracts = Contracts(
        **{field: f"0x{i:040x}" for i, field in enumerate(Contracts.model_fields, 10)}
    )
    market = Market(
        chain=chain,
        share_asset=assets[0],
        index_asset=assets[1],
        long_asset=assets[1],
        short_asset=assets[2],
        wrapped_native=assets[1],
        contracts=contracts,
    )
    return Target(
        allocation="Main",
        account="fund",
        fund=Account(chain=chain, address=AccountReference("0x" + "55" * 20)),
        market=market,
        collateral_asset=assets[1],
        side="long",
    )


def test_allocation_labels_do_not_split_protocol_position_ownership():
    original = target()
    renamed = original.model_copy(update={"allocation": "Second"})
    assert original.position_scope == renamed.position_scope
    assert position_key(original) == position_key(renamed)
    assert position_key(original) != position_key(original.model_copy(update={"side": "short"}))


def test_absent_position_requires_all_contract_fields_to_be_empty():
    value = target()
    assert (
        decoded_position(value, position_key(value), (ZERO_ADDRESS,) * 3, (0,) * 10, (False,))
        is None
    )
    with pytest.raises(DataError, match="inconsistent accounting"):
        decoded_position(value, position_key(value), (ZERO_ADDRESS,) * 3, (1,) + (0,) * 9, (False,))
    addresses = (
        value.fund.address,
        value.market.share_asset.reference,
        value.collateral_asset.reference,
    )
    with pytest.raises(DataError, match="exact aggregate ownership"):
        decoded_position(value, position_key(value), addresses, (0,) * 10, (False,))


@pytest.mark.parametrize(
    "kind,field", [(IncreasePosition, "collateral"), (DecreasePosition, "collateral_withdrawal")]
)
def test_position_intents_refuse_wrong_units_and_empty_changes(kind, field):
    value = target()
    with pytest.raises(ValueError, match="target"):
        kind(
            target=value,
            **{field: AssetAmount(asset=value.market.short_asset, amount=1)},
            size_delta_usd_e30=1,
        )
    with pytest.raises(ValueError, match="requires collateral or notional"):
        kind(
            target=value,
            **{field: AssetAmount(asset=value.collateral_asset, amount=0)},
            size_delta_usd_e30=0,
        )
