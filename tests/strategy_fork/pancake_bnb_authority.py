"""Pristine installed Roles probes for the BNB Pancake V3 references."""

from types import SimpleNamespace

from almanak.caip import AssetId
from almanak.connectors.pancakeswap_v3 import abi
from almanak.connectors.pancakeswap_v3.models import Target
from almanak.connectors.pancakeswap_v3.operations import Collect
from almanak.connectors.pancakeswap_v3.validation import validate_operation
from almanak.dex_intents import CollectLiquidityFees
from almanak.models import SafeRoles
from tests.strategy_fork.bnb_v3_authority import (
    ANY_AMOUNT,
    LARGE_DEPOSIT,
    _allowed,
    _approval,
    _denied,
    _inner,
    _safe_profile,
    _wrapped,
    _wrapped_result,
)
from tests.strategy_fork.robinhood_v3_authority import retain_observation


def _swap_values(target, direction, amount=1):
    assets = target.assets
    return (
        assets[direction].reference,
        assets[1 - direction].reference,
        target.pool.fee,
        target.fund.address,
        2**64,
        amount,
        1,
        0,
    )


def _swap(values):
    return _inner(
        abi.calldata(f"exactInputSingle({abi.SWAP_PARAMS})", (abi.SWAP_PARAMS,), (values,))
    )


def _swap_rejections(node, profile, target, direction):
    values = _swap_values(target, direction)
    prefix = f"swap{direction}"
    changes = (
        ("other_input", 0, profile.operator.address),
        ("other_output", 1, profile.operator.address),
        ("other_fee", 2, target.pool.fee + 1),
        ("other_recipient", 3, profile.operator.address),
        ("zero_input", 5, 0),
        ("price_limit", 7, 1),
    )
    rejected = []
    for label, index, replacement in changes:
        malformed = list(values)
        malformed[index] = replacement
        rejected.append(
            _denied(
                node,
                f"{prefix}_{label}",
                _wrapped(profile, target.contracts.router, _swap(tuple(malformed))),
            )
        )
    return rejected


def exchange_permissions(node, config, allocation_name):
    """Probe both direct Pancake swap directions on freshly installed scope-only grants."""
    target = config.target(allocation_name)
    profile = _safe_profile(config, target.account)
    node._validate_origin()
    allowed = []
    rejected = []
    for direction, asset in enumerate(target.assets):
        allowed.extend(
            (
                _allowed(
                    node,
                    f"router_approval_{direction}",
                    _wrapped(profile, asset.reference, _approval(target.contracts.router, 1)),
                ),
                _allowed(
                    node,
                    f"router_approval_any_amount_{direction}",
                    _wrapped(
                        profile, asset.reference, _approval(target.contracts.router, ANY_AMOUNT)
                    ),
                ),
                _allowed(
                    node,
                    f"configured_swap{direction}_template",
                    _wrapped_result(
                        profile,
                        target.contracts.router,
                        _swap(_swap_values(target, direction)),
                    ),
                    return_data=True,
                ),
            )
        )
        rejected.extend(_swap_rejections(node, profile, target, direction))
        rejected.append(
            _denied(
                node,
                f"router_approval_other_spender_{direction}",
                _wrapped(profile, asset.reference, _approval(profile.operator.address, 1)),
            )
        )
    result = {
        "stage": "before_runtime_entry",
        "allowed_controls": allowed,
        "rejected": rejected,
    }
    if target.range is not None:
        result["liquidity"] = liquidity_permissions(node, config, allocation_name)
    return result


def _mint(target, amounts, *, shift=0):
    values = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        target.range.lower + shift,
        target.range.upper + shift,
        *amounts,
        0,
        0,
        target.fund.address,
        2**64,
    )
    return values, _inner(abi.calldata(f"mint({abi.MINT_PARAMS})", (abi.MINT_PARAMS,), (values,)))


def _mint_rejections(node, profile, target, values):
    changes = (
        ("mint_other_asset", 0, target.pool.currency1.reference),
        ("mint_other_fee", 2, target.pool.fee + 1),
        ("mint_other_recipient", 9, profile.operator.address),
    )
    return [
        _denied(
            node,
            label,
            _wrapped(
                profile,
                target.contracts.position_manager,
                _inner(
                    abi.calldata(
                        f"mint({abi.MINT_PARAMS})",
                        (abi.MINT_PARAMS,),
                        (tuple((*values[:index], replacement, *values[index + 1 :])),),
                    )
                ),
            ),
        )
        for label, index, replacement in changes
    ]


def _exit_calls(target, nft, *, burn_nft=None):
    decrease = abi.calldata(
        f"decreaseLiquidity({abi.DECREASE_PARAMS})",
        (abi.DECREASE_PARAMS,),
        ((nft, 1, 0, 0, 2**64),),
    )
    collect = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((nft, target.fund.address, abi.MAX128, abi.MAX128),),
    )
    burn = abi.calldata("burn(uint256)", ("uint256",), (nft if burn_nft is None else burn_nft,))
    return decrease, collect, burn


def _multicall(*calls):
    return _inner(abi.calldata("multicall(bytes[])", ("bytes[]",), (calls,)))


def liquidity_permissions(node, config, allocation_name):
    """Probe Pancake mint/increase and its intentionally broad NFT exit templates."""
    target = config.target(allocation_name)
    profile = _safe_profile(config, target.account)
    values, mint = _mint(target, (1, 1))
    _, one_sided0 = _mint(target, (0, 1))
    _, one_sided1 = _mint(target, (1, 0))
    # Ticks and deposit amounts are gateway checks (blueprint 27 section 4.3).
    _, other_range = _mint(target, (1, 1), shift=target.pool.tick_spacing)
    _, large = _mint(target, (LARGE_DEPOSIT, LARGE_DEPOSIT))
    nft = 2**200
    increase = _inner(
        abi.calldata(
            f"increaseLiquidity({abi.INCREASE_PARAMS})",
            (abi.INCREASE_PARAMS,),
            ((nft, 1, 1, 0, 0, 2**64),),
        )
    )
    decrease, collect, burn = _exit_calls(target, nft)
    mismatched = _exit_calls(target, nft, burn_nft=nft + 1)
    allowed = [
        *(
            _allowed(
                node,
                f"position_manager_approval_{index}_{label}",
                _wrapped(
                    profile,
                    asset.reference,
                    _approval(target.contracts.position_manager, amount),
                ),
            )
            for index, asset in enumerate(target.assets)
            for label, amount in (("one", 1), ("any_amount", ANY_AMOUNT))
        ),
        *(
            _allowed(
                node,
                label,
                _wrapped_result(profile, target.contracts.position_manager, call),
                return_data=True,
            )
            for label, call in (
                ("configured_mint_template", mint),
                ("configured_mint_zero_amount0_template", one_sided0),
                ("configured_mint_zero_amount1_template", one_sided1),
                ("mint_other_range_template", other_range),
                ("mint_large_deposit_template", large),
                ("broad_arbitrary_nft_increase_template", increase),
                ("broad_arbitrary_nft_collect_template", _inner(collect)),
                ("broad_arbitrary_nft_partial_decrease_template", _multicall(decrease, collect)),
                ("broad_arbitrary_nft_close_template", _multicall(decrease, collect, burn)),
                ("broad_uncorrelated_nft_close_template", _multicall(*mismatched)),
            )
        ),
    ]
    rejected = _mint_rejections(node, profile, target, values)
    rejected.extend(
        _denied(
            node,
            f"position_manager_approval_other_spender_{index}",
            _wrapped(profile, asset.reference, _approval(profile.operator.address, 1)),
        )
        for index, asset in enumerate(target.assets)
    )
    wrong_collect = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((nft, profile.operator.address, abi.MAX128, abi.MAX128),),
    )
    rejected.extend(
        (
            _denied(
                node,
                "collect_other_recipient",
                _wrapped(profile, target.contracts.position_manager, _inner(wrong_collect)),
            ),
            _denied(
                node,
                "close_wrong_order",
                _wrapped(
                    profile,
                    target.contracts.position_manager,
                    _multicall(collect, decrease, burn),
                ),
            ),
            _denied(
                node,
                "close_extra_call",
                _wrapped(
                    profile,
                    target.contracts.position_manager,
                    _multicall(decrease, collect, burn, burn),
                ),
            ),
        )
    )
    return {
        "allowed_controls": allowed,
        "rejected": rejected,
        "accepted_onchain_scope": (
            "Roles admits compatible arbitrary and uncorrelated NFT IDs, any ticks and any "
            "deposit amounts; inner refusals are not native position success. Gateway economic "
            "admission enforces the configured range, inventory and the retained NFT."
        ),
    }


def _latest_observation(state, target):
    observations = [
        outcome.after
        for outcome in state.outcomes
        if getattr(outcome.operation, "target", None) == target
    ]
    if not observations:
        raise AssertionError("Pancake LP ownership probe requires a successful retained outcome")
    return observations[-1]


def _retained_position(state, owned):
    position = owned.position
    if position is None:
        raise AssertionError("Pancake LP ownership probe requires the retained strategy NFT")
    before = _latest_observation(state, owned.target)
    if before.owned != owned or before.position != position:
        raise AssertionError("latest Pancake outcome does not retain the held NFT")
    return position, before


def _foreign_collect(state, owned, before, foreign):
    intent = CollectLiquidityFees[Target](
        target=owned.target,
        deadline=before.block.timestamp
        + state.config.allocation(owned.target.allocation).deadline_seconds,
    )
    view = SimpleNamespace(
        config=state.config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=SimpleNamespace(intent=intent, observations=(before,)),
        outcomes=state.outcomes,
        mode=state.mode,
    )
    operation = Collect(target=owned.target, nft=foreign, deadline=intent.deadline)
    return view, operation


def _require_foreign_refusal(state, view, operation, before):
    try:
        validate_operation(SimpleNamespace(config=state.config), view, operation, before)
    except ValueError as error:
        if "operation differs from its retained economic intent or NFT" not in str(error):
            raise
    else:
        raise AssertionError("gateway admitted a foreign Pancake NFT")


def foreign_nft_gateway_refusal(state, owned):
    """Refuse a foreign Pancake NFT without mutating the retained checkpoint."""
    position, before = _retained_position(state, owned)
    retained = position.nft
    foreign = AssetId(f"{retained.asset_type}/{int(retained.token_id) + 1}")
    view, operation = _foreign_collect(state, owned, before, foreign)
    _require_foreign_refusal(state, view, operation, before)
    retained_after = next(value for value in state.owned if value.target == owned.target)
    if retained_after.position.nft != retained:
        raise AssertionError("foreign NFT probe changed retained Pancake ownership")
    return {
        "status": "rejected",
        "retained_nft": str(retained),
        "foreign_nft": str(foreign),
        "ownership_source": "unchanged retained checkpoint and latest successful outcome",
        "error": "operation differs from its retained economic intent or NFT",
    }


def retain_lp_gateway_ownership(node, journal, process, allocation_name):
    """Retain the post-entry gateway NFT boundary without making a quota claim."""
    state = journal.load()
    if process.poll() is not None:
        raise AssertionError("coordinator exited before the Pancake LP ownership probe")
    target = state.config.target(allocation_name)
    profile = state.config.accounts.binding(target.account).profile
    if isinstance(profile, SafeRoles):
        owned = next(value for value in state.owned if value.target == target)
        observation = {
            "profile": "safe_roles",
            "status": "passed",
            "gateway_foreign_nft": foreign_nft_gateway_refusal(state, owned),
            "quota_claim": "none; this observation exercises only retained gateway ownership",
        }
    else:
        observation = {
            "profile": "eoa",
            "status": "not_applicable",
            "reason": "EOA execution has no installed Roles authority",
        }
    retain_observation(journal, "paused_and_resumed_lp_ownership", observation)
