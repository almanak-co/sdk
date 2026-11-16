"""Pool-wide quantities preserve order and swaps select a concrete pair of coins."""

from itertools import permutations

import pytest
from pydantic import ValidationError

from almanak.asset_amounts import AssetAmounts
from almanak.caip import AssetType, ChainId
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    DexTarget,
    IncreasePoolLiquidity,
    OpenLiquidity,
    OpenPoolLiquidity,
    SwapExactInput,
)
from almanak.models import Account, AssetAmount, Chain


class SyntheticPool(DexTarget):
    coins: tuple[AssetType, ...]

    @property
    def assets(self):
        return self.coins


@pytest.fixture
def target():
    chain = Chain(chain_id=ChainId("eip155:1"), environment="synthetic-amounts")
    return SyntheticPool(
        allocation="pool",
        account="fund",
        fund=Account(chain=chain, address="0x" + "11" * 20),
        coins=tuple(AssetType(f"eip155:1/erc20:0x{n:040x}") for n in (1, 2, 3)),
    )


def quantities(target, values):
    return AssetAmounts(
        tuple(
            AssetAmount(asset=asset, amount=value)
            for asset, value in zip(target.assets, values, strict=True)
        )
    )


def test_each_three_coin_swap_pair_retains_its_input_and_output_identity(target):
    for first, second in permutations(target.assets, 2):
        intent = SwapExactInput[SyntheticPool](
            target=target,
            amount=AssetAmount(asset=first, amount=17),
            minimum_output=AssetAmount(asset=second, amount=5),
            deadline=123,
        )
        assert type(intent).model_validate_json(intent.model_dump_json()) == intent
    with pytest.raises(ValidationError, match="two different target assets"):
        type(intent).model_validate(
            {**intent.model_dump(), "minimum_output": intent.amount.model_dump()}
        )


@pytest.mark.parametrize("kind", [OpenPoolLiquidity, IncreasePoolLiquidity])
def test_additions_retain_zero_coins_and_the_configured_order(target, kind):
    amounts = quantities(target, (0, 17, 0))
    intent = kind[SyntheticPool](target=target, maximums=amounts, deadline=123)
    assert type(intent).model_validate_json(intent.model_dump_json()) == intent
    with pytest.raises(ValidationError, match="currency order"):
        kind[SyntheticPool](target=target, maximums=AssetAmounts(amounts.root[::-1]), deadline=123)
    with pytest.raises(ValidationError, match="positive inventory"):
        kind[SyntheticPool](target=target, maximums=quantities(target, (0, 0, 0)), deadline=123)


def test_removal_minima_can_be_zero_but_must_cover_every_coin(target):
    amounts = quantities(target, (0, 0, 0))
    close = ClosePoolLiquidity[SyntheticPool](target=target, minimums=amounts, deadline=123)
    decrease = DecreasePoolLiquidity[SyntheticPool](
        target=target, minimums=amounts, liquidity=100, deadline=123
    )
    assert close.minimums == decrease.minimums
    with pytest.raises(ValidationError, match="currency order"):
        ClosePoolLiquidity[SyntheticPool](
            target=target, minimums=AssetAmounts(amounts.root[:2]), deadline=123
        )


def test_single_coin_removal_cannot_hide_a_second_required_output(target):
    selected = target.coins[1]
    close = ClosePoolLiquidity[SyntheticPool](
        target=target,
        minimums=quantities(target, (0, 10, 0)),
        output_asset=selected,
        deadline=123,
    )
    assert type(close).model_validate_json(close.model_dump_json()) == close
    with pytest.raises(ValidationError, match="other assets"):
        ClosePoolLiquidity[SyntheticPool](
            target=target,
            minimums=quantities(target, (1, 10, 0)),
            output_asset=selected,
            deadline=123,
        )
    with pytest.raises(ValidationError, match="configured pool asset"):
        ClosePoolLiquidity[SyntheticPool](
            target=target,
            minimums=quantities(target, (0, 0, 0)),
            output_asset=AssetType("eip155:1/erc20:0x" + "ff" * 20),
            deadline=123,
        )


def test_duplicate_or_cross_chain_quantities_are_invalid(target):
    amounts = quantities(target, (1, 2, 3)).root
    with pytest.raises(ValidationError, match="repeat an asset"):
        AssetAmounts((amounts[0], amounts[0]))
    foreign = amounts[1].model_copy(update={"asset": AssetType("eip155:8453/erc20:0x" + "22" * 20)})
    with pytest.raises(ValidationError, match="same CAIP chain"):
        AssetAmounts((amounts[0], foreign))


def test_pair_intents_use_the_same_ordered_quantity_checks(target):
    pair = target.model_copy(update={"coins": target.coins[:2]})
    first, second = quantities(pair, (1, 2)).root
    intent = OpenLiquidity[SyntheticPool](
        target=pair, maximum0=first, maximum1=second, deadline=123
    )
    assert type(intent).model_validate_json(intent.model_dump_json()) == intent
    with pytest.raises(ValidationError, match="currency order"):
        OpenLiquidity[SyntheticPool](target=pair, maximum0=second, maximum1=first, deadline=123)
