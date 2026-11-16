"""Pristine installed Roles probes for the BNB V3 reference package."""

from eth_abi import decode
from eth_abi.exceptions import DecodingError

from almanak.connectors.uniswap_v3 import abi
from almanak.gateway.safe_fixture import calldata
from almanak.models import SafeRoles
from tests.strategy_fork.pool_lending_authority import forbidden_calls as lending_rejections
from tests.strategy_fork.robinhood_v3_authority import (
    _allowed,
    _approval,
    _inner,
    _swap,
    _wrapped,
    _wrapped_result,
    foreign_nft_gateway_refusal,
    retain_observation,
)
from tests.strategy_fork.robinhood_v3_authority import _denied as _structured_denied

PREDICATE_STATUSES = frozenset(range(5, 19))


def _denied(node, label, call, *, expected_statuses=PREDICATE_STATUSES):
    result = _structured_denied(node, label, call)
    try:
        raw = bytes.fromhex(result["revert_data"][2:])
        status, info = decode(("uint8", "bytes32"), raw[4:])
    except (KeyError, TypeError, ValueError, DecodingError) as error:
        raise AssertionError("Roles ConditionViolation returned malformed arguments") from error
    if len(raw) != 68 or status not in expected_statuses:
        raise AssertionError(
            f"malformed {label} call returned unexpected Roles condition status {status}"
        )
    return {**result, "condition_status": status, "condition_info": "0x" + info.hex()}


def _safe_profile(config, account):
    profile = config.accounts.binding(account).profile
    if not isinstance(profile, SafeRoles):
        raise TypeError("pristine authority probes require a generated Safe Roles profile")
    return profile


# Amounts no author field bounds (blueprint 27): the largest approval, a uint128 deposit.
ANY_AMOUNT = 2**256 - 1
LARGE_DEPOSIT = 2**128 - 1


def _swap_values(target, amount):
    return (
        target.funding_currency.reference,
        target.counter_currency.reference,
        target.pool.fee,
        target.fund.address,
        amount,
        1,
        0,
    )


def _swap_rejections(node, profile, target, values):
    changes = (
        ("swap_other_input", 0, target.counter_currency.reference),
        ("swap_other_output", 1, target.funding_currency.reference),
        ("swap_other_fee", 2, target.pool.fee + 1),
        ("swap_other_recipient", 3, profile.submitter.address),
        ("swap_zero_input", 4, 0),
        ("swap_price_limit", 6, 1),
    )
    rejected = []
    for label, index, replacement in changes:
        malformed = list(values)
        malformed[index] = replacement
        rejected.append(
            _denied(
                node,
                label,
                _wrapped(profile, target.contracts.router, _swap(target, malformed)),
            )
        )
    rejected.append(
        _denied(
            node,
            "swap_extra_nested_call",
            _wrapped(profile, target.contracts.router, _swap(target, values, count=2)),
        )
    )
    return rejected


def exchange_permissions(node, config, allocation_name):
    """Probe actual generated scope-only swap grants on the installed Safe."""
    target = config.target(allocation_name)
    profile = _safe_profile(config, target.account)
    node._validate_origin()
    values = _swap_values(target, 1)
    allowed = [
        _allowed(
            node,
            "router_approval",
            _wrapped(
                profile, target.funding_currency.reference, _approval(target.contracts.router, 1)
            ),
        ),
        _allowed(
            node,
            "router_approval_any_amount",
            _wrapped(
                profile,
                target.funding_currency.reference,
                _approval(target.contracts.router, ANY_AMOUNT),
            ),
        ),
        _allowed(
            node,
            "configured_swap_template",
            _wrapped_result(profile, target.contracts.router, _swap(target, values)),
            return_data=True,
        ),
    ]
    rejected = _swap_rejections(node, profile, target, values)
    rejected.append(
        _denied(
            node,
            "router_approval_other_spender",
            _wrapped(
                profile,
                target.funding_currency.reference,
                _approval(profile.submitter.address, 1),
            ),
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
        ("mint_other_asset", 0, target.counter_currency.reference),
        ("mint_other_fee", 2, target.pool.fee + 1),
        ("mint_other_recipient", 9, profile.submitter.address),
    )
    rejected = []
    for label, index, replacement in changes:
        malformed = list(values)
        malformed[index] = replacement
        inner = _inner(
            abi.calldata(f"mint({abi.MINT_PARAMS})", (abi.MINT_PARAMS,), (tuple(malformed),))
        )
        rejected.append(
            _denied(node, label, _wrapped(profile, target.contracts.position_manager, inner))
        )
    return rejected


def liquidity_permissions(node, config, allocation_name):
    """Probe mint correlations and the accepted unrestricted NFT-ID exit scope."""
    target = config.target(allocation_name)
    profile = _safe_profile(config, target.account)
    values, mint = _mint(target, (1, 1))
    _, one_sided0 = _mint(target, (0, 1))
    _, one_sided1 = _mint(target, (1, 0))
    # Ticks and deposit amounts are gateway checks (blueprint 27 section 4.3).
    _, other_range = _mint(target, (1, 1), shift=target.pool.tick_spacing)
    _, large = _mint(target, (LARGE_DEPOSIT, LARGE_DEPOSIT))
    arbitrary_nft = 2**200
    increase = _inner(
        abi.calldata(
            f"increaseLiquidity({abi.INCREASE_PARAMS})",
            (abi.INCREASE_PARAMS,),
            ((arbitrary_nft, 1, 1, 0, 0, 2**64),),
        )
    )
    collect = _inner(
        abi.calldata(
            f"collect({abi.COLLECT_PARAMS})",
            (abi.COLLECT_PARAMS,),
            ((arbitrary_nft, target.fund.address, abi.MAX128, abi.MAX128),),
        )
    )
    decrease = abi.calldata(
        f"decreaseLiquidity({abi.DECREASE_PARAMS})",
        (abi.DECREASE_PARAMS,),
        ((arbitrary_nft, 1, 0, 0, 2**64),),
    )
    collect_bytes = bytes.fromhex(collect[2:])
    partial_decrease = _inner(
        abi.calldata(
            "multicall(bytes[])",
            ("bytes[]",),
            ((decrease, collect_bytes),),
        )
    )
    burn = abi.calldata("burn(uint256)", ("uint256",), (arbitrary_nft,))
    close = _inner(
        abi.calldata(
            "multicall(bytes[])",
            ("bytes[]",),
            ((decrease, collect_bytes, burn),),
        )
    )
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
        _allowed(
            node,
            "configured_mint_template",
            _wrapped_result(profile, target.contracts.position_manager, mint),
            return_data=True,
        ),
        _allowed(
            node,
            "configured_mint_zero_amount0_template",
            _wrapped_result(profile, target.contracts.position_manager, one_sided0),
            return_data=True,
        ),
        _allowed(
            node,
            "configured_mint_zero_amount1_template",
            _wrapped_result(profile, target.contracts.position_manager, one_sided1),
            return_data=True,
        ),
        _allowed(
            node,
            "mint_other_range_template",
            _wrapped_result(profile, target.contracts.position_manager, other_range),
            return_data=True,
        ),
        _allowed(
            node,
            "mint_large_deposit_template",
            _wrapped_result(profile, target.contracts.position_manager, large),
            return_data=True,
        ),
        _allowed(
            node,
            "broad_arbitrary_nft_increase_template",
            _wrapped_result(profile, target.contracts.position_manager, increase),
            return_data=True,
        ),
        _allowed(
            node,
            "broad_arbitrary_nft_collect_template",
            _wrapped_result(profile, target.contracts.position_manager, collect),
            return_data=True,
        ),
        _allowed(
            node,
            "broad_arbitrary_nft_partial_decrease_template",
            _wrapped_result(profile, target.contracts.position_manager, partial_decrease),
            return_data=True,
        ),
        _allowed(
            node,
            "broad_arbitrary_nft_close_template",
            _wrapped_result(profile, target.contracts.position_manager, close),
            return_data=True,
        ),
    ]
    rejected = _mint_rejections(node, profile, target, values)
    rejected.extend(
        _denied(
            node,
            f"position_manager_approval_other_spender_{index}",
            _wrapped(profile, asset.reference, _approval(profile.submitter.address, 1)),
        )
        for index, asset in enumerate(target.assets)
    )
    wrong_collect = _inner(
        abi.calldata(
            f"collect({abi.COLLECT_PARAMS})",
            (abi.COLLECT_PARAMS,),
            ((arbitrary_nft, profile.submitter.address, abi.MAX128, abi.MAX128),),
        )
    )
    rejected.append(
        _denied(
            node,
            "collect_other_recipient",
            _wrapped(profile, target.contracts.position_manager, wrong_collect),
        )
    )
    wrong_order = _inner(
        abi.calldata(
            "multicall(bytes[])",
            ("bytes[]",),
            ((collect_bytes, decrease, burn),),
        )
    )
    rejected.append(
        _denied(
            node,
            "close_wrong_order",
            _wrapped(profile, target.contracts.position_manager, wrong_order),
        )
    )
    return {
        "allowed_controls": allowed,
        "rejected": rejected,
        "accepted_onchain_scope": (
            "Roles leaves compatible NFT IDs, ticks and deposit amounts unrestricted; an inner "
            "refusal is retained and is not native position success. Gateway economic admission "
            "enforces the configured range, inventory and the retained NFT."
        ),
    }


def _lending_control_calls(config):
    collateral = config.allocation("collateral")
    loan = config.allocation("loan")
    profile = _safe_profile(config, collateral.account)
    pool = collateral.reserve.pool
    fund = profile.fund.address
    return (
        (
            "configured_supply_template",
            pool,
            calldata(
                "supply(address,uint256,address,uint16)",
                ("address", "uint256", "address", "uint16"),
                (collateral.reserve.asset.reference, 1, fund, 0),
            ),
        ),
        (
            "configured_borrow_template",
            pool,
            calldata(
                "borrow(address,uint256,uint256,uint16,address)",
                ("address", "uint256", "uint256", "uint16", "address"),
                (loan.reserve.asset.reference, 1, 2, 0, fund),
            ),
        ),
        (
            "configured_repay_template",
            pool,
            calldata(
                "repay(address,uint256,uint256,address)",
                ("address", "uint256", "uint256", "address"),
                (loan.reserve.asset.reference, 1, 2, fund),
            ),
        ),
        (
            "configured_withdraw_template",
            pool,
            calldata(
                "withdraw(address,uint256,address)",
                ("address", "uint256", "address"),
                (collateral.reserve.asset.reference, 1, fund),
            ),
        ),
        (
            "configured_collateral_template",
            pool,
            calldata(
                "setUserUseReserveAsCollateral(address,bool)",
                ("address", "bool"),
                (collateral.reserve.asset.reference, True),
            ),
        ),
    )


def lending_permissions(node, config):
    """Probe Aave supply/borrow/exit predicates while installed quotas are pristine."""
    collateral = config.allocation("collateral")
    profile = _safe_profile(config, collateral.account)
    node._validate_origin()
    loan = config.allocation("loan")
    allowed = [
        _allowed(
            node,
            label,
            _wrapped(
                profile, allocation.reserve.asset.reference, _approval(allocation.reserve.pool, 1)
            ),
        )
        for label, allocation in (
            ("supply_approval", collateral),
            ("repay_approval", loan),
        )
    ]
    allowed.extend(
        _allowed(node, label, _wrapped_result(profile, target, inner), return_data=True)
        for label, target, inner in _lending_control_calls(config)
    )
    outer_statuses = {"delegatecall": {1}, "native_value": {4}, "other_target": {2}}
    rejected = [
        _denied(
            node,
            label,
            _wrapped(profile, target, inner, value=value, operation=operation),
            expected_statuses=outer_statuses.get(label, PREDICATE_STATUSES),
        )
        for label, target, inner, value, operation in lending_rejections(config)
    ]
    return {
        "stage": "before_runtime_entry",
        "quota_basis": "freshly installed non-refilling supply and borrow quotas",
        "allowed_controls": allowed,
        "rejected": rejected,
    }


def retain_lp_gateway_ownership(node, journal, process, allocation_name):
    """Retain the post-entry gateway NFT boundary without making a quota claim."""
    state = journal.load()
    if process.poll() is not None:
        raise AssertionError("coordinator exited before the LP ownership probe")
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
