"""Compound retained ownership distinguishes signed base units and remaining collateral."""

import pytest

from almanak.connectors.compound_v3.domain import Reserve, Target
from almanak.connectors.compound_v3.observations import Ownership, Snapshot, owned, target_asset
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from tests.test_compound_v3_risk import account


def snapshot(base=False):
    observed = account()
    market = observed.market
    collateral = next(c for c in market.collaterals if observed.collaterals[c.offset].balance)
    reserve = Reserve(
        market=market.source,
        asset=market.source.base_asset if base else collateral.asset_type,
        price_feed=market.source.base_price_feed if base else collateral.price_feed,
    )
    target = Target(allocation="position", account="fund", fund=observed.fund, reserve=reserve)
    return Snapshot(
        target=target,
        account=observed,
        wallet_balance=0,
        allowance=0,
        payer=observed.fund,
        native_balance=AssetAmount(asset=native_asset(observed.fund.chain.chain_id), amount=0),
    )


def test_owned_collateral_survives_cleared_asset_membership_bits():
    before = snapshot()
    after = before.model_copy(
        update={
            "account": before.account.model_copy(update={"assets_in": 0, "extended_assets_in": 0})
        }
    )
    assert owned(before) == owned(after)
    assert owned(after).collateral == 10**18 and not owned(after).empty
    assert before.asset.identifier == before.target.asset
    assert before.effective_at.timestamp() == before.block.timestamp
    assert before.allocation == "position"


def test_base_ownership_retains_signed_principal_separately_from_projected_debt():
    observed = snapshot(True)
    ownership = owned(observed)
    assert ownership.principal == observed.account.principal < 0
    assert ownership.collateral == 0
    assert observed.asset == observed.account.market.base_asset
    assert Ownership(target=observed.target, principal=0, collateral=0).empty
    with pytest.raises(ValueError, match="units"):
        Ownership(target=observed.target, principal=0, collateral=1)
    with pytest.raises(ValueError, match="units"):
        Ownership(target=snapshot().target, principal=1, collateral=0)


@pytest.mark.parametrize("fault", ["fund", "feed", "asset", "payer", "gas"])
def test_snapshot_refuses_foreign_fund_market_asset_and_payer_scope(fault):
    observed = snapshot()
    target = observed.target
    if fault == "fund":
        target = target.model_copy(
            update={"fund": target.fund.model_copy(update={"address": "0x" + "12" * 20})}
        )
    elif fault == "feed":
        target = target.model_copy(
            update={"reserve": target.reserve.model_copy(update={"price_feed": "0x" + "12" * 20})}
        )
    elif fault == "asset":
        from almanak.caip import AssetType

        target = target.model_copy(
            update={
                "reserve": target.reserve.model_copy(
                    update={"asset": AssetType("eip155:8453/erc20:0x" + "12" * 20)}
                )
            }
        )
    elif fault == "payer":
        observed = observed.model_copy(
            update={
                "payer": observed.payer.model_copy(
                    update={
                        "chain": observed.payer.chain.model_copy(update={"environment": "other"})
                    }
                )
            }
        )
    else:
        observed = observed.model_copy(
            update={"native_balance": AssetAmount(asset=target.asset, amount=0)}
        )
    observed = observed.model_copy(update={"target": target})
    with pytest.raises(ValueError):
        Snapshot.model_validate_json(observed.model_dump_json())
    if fault in ("fund", "feed", "asset"):
        with pytest.raises(ValueError):
            target_asset(target, observed.account)
