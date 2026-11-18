"""Pre-entry installed-authority probes for the private Robinhood candidate."""

import ast
from dataclasses import dataclass
from typing import Any

from eth_abi import decode, encode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak

from almanak.connectors.curve.pool_abi import layout, signature
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.operations import (
    BorrowOperation,
    MorphoOperation,
    RepayOperation,
    SupplyOperation,
    WithdrawOperation,
    call_arguments,
    plan_call,
)
from almanak.connectors.uniswap_v3 import abi as v3_abi
from almanak.connectors.uniswap_v4 import abi as v4_abi
from almanak.gateway.accounts import ContractCall
from almanak.gateway.safe_fixture import calldata
from almanak.models import AssetAmount, SafeRoles
from tests.strategy_fork import curve_authority, uniswap_v4_base_authority

ROLES_ENTRYPOINT = "execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)"
CONDITION_VIOLATION = "0xd0a9bf58"
RPC_ERROR_PREFIX = "fork RPC failed: eth_call: "
STATUS = (
    "Ok",
    "DelegateCallNotAllowed",
    "TargetAddressNotAllowed",
    "FunctionNotAllowed",
    "SendNotAllowed",
    "OrViolation",
    "NorViolation",
    "ParameterNotAllowed",
    "ParameterLessThanAllowed",
    "ParameterGreaterThanAllowed",
    "ParameterNotAMatch",
    "NotEveryArrayElementPasses",
    "NoArrayElementPasses",
    "ParameterNotSubsetOfAllowed",
    "BitmaskOverflow",
    "BitmaskNotAllowed",
    "CustomConditionViolation",
    "AllowanceExceeded",
    "CallAllowanceExceeded",
    "EtherAllowanceExceeded",
)


@dataclass(frozen=True)
class ProbeGroup:
    name: str
    controls: tuple[ContractCall, ...]
    attacks: tuple[tuple[str, ContractCall], ...]


def wrapped(profile: SafeRoles, call: ContractCall):
    return {
        "from": profile.operator.address,
        "to": profile.roles_module,
        "data": calldata(
            ROLES_ENTRYPOINT,
            ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
            (
                call.to,
                call.value,
                bytes.fromhex(call.data[2:]),
                0,
                bytes.fromhex(profile.role_key[2:]),
                False,
            ),
        ),
    }


def admitted(node, profile, label, call):
    request = wrapped(profile, call)
    try:
        result = node.rpc("eth_call", [request, "latest"])
        raw = bytes.fromhex(result[2:])
        success, returned = decode(("bool", "bytes"), raw)
    except (RuntimeError, AttributeError, ValueError, DecodingError) as error:
        raise AssertionError(f"valid {label} template was not admitted") from error
    if encode(("bool", "bytes"), (success, returned)) != raw:
        raise AssertionError(f"valid {label} template returned noncanonical Roles data")
    return {
        "case": label,
        "call": request,
        "outcome": "native_success" if success else "authorized_downstream_refusal",
        "inner_return_data": "0x" + returned.hex(),
    }


def condition_violation(error: RuntimeError):
    message = str(error)
    if not message.startswith(RPC_ERROR_PREFIX):
        raise AssertionError("denied call returned an unstructured RPC error") from error
    try:
        payload = ast.literal_eval(message.removeprefix(RPC_ERROR_PREFIX))
        data = payload["data"]
        raw = bytes.fromhex(data[2:])
        status, info = decode(("uint8", "bytes32"), raw[4:])
    except (KeyError, TypeError, SyntaxError, ValueError, DecodingError) as parse_error:
        raise AssertionError("denied call returned an unstructured RPC error") from parse_error
    if (
        not isinstance(payload, dict)
        or payload.get("code") != 3
        or not isinstance(payload.get("message"), str)
        or not payload["message"].startswith("execution reverted")
        or not isinstance(data, str)
        or not data.startswith(CONDITION_VIOLATION)
        or raw[:4] != bytes.fromhex(CONDITION_VIOLATION[2:])
        or encode(("uint8", "bytes32"), (status, info)) != raw[4:]
        or status == 0
        or status >= len(STATUS)
    ):
        raise AssertionError("denied call did not return a canonical Roles ConditionViolation")
    return data, status, STATUS[status], "0x" + info.hex()


def denied(node, profile, label, call, *, expected_status=None):
    request = wrapped(profile, call)
    try:
        node.rpc("eth_call", [request, "latest"])
    except RuntimeError as error:
        data, status, status_name, info = condition_violation(error)
    else:
        raise AssertionError(f"malformed {label} call was admitted")
    if expected_status is not None and status != expected_status:
        raise AssertionError(
            f"malformed {label} returned {status_name}, expected {STATUS[expected_status]}"
        )
    return {
        "case": label,
        "call": request,
        "error": "ConditionViolation",
        "status": status,
        "status_name": status_name,
        "info": info,
        "revert_data": data,
    }


def changed(values, index, replacement):
    result = list(values)
    result[index] = replacement
    return tuple(result)


def encoded_call(name, types, values):
    payload = keccak(text=f"{name}({','.join(types)})")[:4] + encode(types, values)
    return "0x" + payload.hex()


def morpho_operations(config, allocation):
    target = config.target(allocation.name)
    amount = AssetAmount(asset=target.asset, amount=1)
    # Supply and withdrawal are granted for every allocation; borrowing only with `borrow`.
    result: list[MorphoOperation] = [
        SupplyOperation(target=target, amount=amount),
        WithdrawOperation(target=target, amount=amount, full=False),
    ]
    if target.reserve.is_loan:
        result.append(
            WithdrawOperation(
                target=target,
                amount=AssetAmount(asset=target.asset, amount=0),
                full=True,
                shares=1,
            )
        )
    if allocation.borrow:
        result.append(BorrowOperation(target=target, amount=amount))
        result.append(RepayOperation(target=target, amount=amount))
        result.append(RepayOperation(target=target, amount=amount, shares=1))
    return tuple(result)


def morpho_group(config, allocation, operation):
    name, types, values = call_arguments(operation)
    target = operation.target
    core = target.reserve.market.core
    control = plan_call(operation)
    wrong_market = list(values[0])
    wrong_market[0] = config.accounts.binding(allocation.account).profile.submitter.address
    owner_index = 3 if target.reserve.is_loan else 2
    tail_index = owner_index + 1
    wrong_tail = b"\x01" if name in {"supply", "supplyCollateral", "repay"} else target.fund.address
    if not isinstance(wrong_tail, bytes):
        wrong_tail = config.accounts.binding(allocation.account).profile.submitter.address
    active_index = 2 if target.reserve.is_loan and values[2] else 1
    quantity = "shares" if active_index == 2 else "assets"
    if name.startswith("supply") or name == "borrow" or (name == "repay" and active_index == 1):
        # Scope-only grants take any positive supply, borrow or asset repayment, so
        # no excess probe applies; Morpho reverts on an asset repayment above the debt.
        maximum = None
    else:
        maximum = 2**128 - 1
    excess = (
        () if maximum is None else (("above_quantity", changed(values, active_index, maximum + 1)),)
    )
    mutations = (
        ("wrong_market", changed(values, 0, tuple(wrong_market))),
        (
            "zero_quantity",
            changed(changed(values, 1, 0), 2, 0)
            if target.reserve.is_loan
            else changed(values, 1, 0),
        ),
        *excess,
        (
            "wrong_owner",
            changed(
                values,
                owner_index,
                config.accounts.binding(allocation.account).profile.submitter.address,
            ),
        ),
        ("wrong_callback_or_receiver", changed(values, tail_index, wrong_tail)),
    )
    attacks = tuple(
        (
            f"{allocation.name}_{name}_{quantity}_{label}",
            ContractCall(
                fund=target.fund,
                to=core,
                value=0,
                data=encoded_call(name, types, mutated),
            ),
        )
        for label, mutated in mutations
    )
    return ProbeGroup(f"morpho_{allocation.name}_{name}_{quantity}", (control,), attacks)


def morpho_groups(config):
    economic = tuple(
        morpho_group(config, allocation, operation)
        for allocation in config.positions
        for operation in morpho_operations(config, allocation)
    )
    # Approvals admit any amount to the exact core, so only the spender is probed.
    approvals: dict[tuple[str, str, str], Any] = {}
    for allocation in config.positions:
        target = config.target(allocation.name)
        key = (allocation.account, target.asset.reference, target.reserve.market.core)
        approvals.setdefault(key, target.fund)
    approval_groups = []
    for index, ((account, token, core), fund) in enumerate(approvals.items()):
        profile = config.accounts.binding(account).profile
        control = ContractCall(
            fund=fund,
            to=token,
            value=0,
            data=calldata("approve(address,uint256)", ("address", "uint256"), (core, 1)),
        )
        attacks = (
            (
                f"morpho_approval_{index}_wrong_spender",
                control.model_copy(
                    update={
                        "data": calldata(
                            "approve(address,uint256)",
                            ("address", "uint256"),
                            (profile.submitter.address, 1),
                        )
                    }
                ),
            ),
        )
        approval_groups.append(ProbeGroup(f"morpho_approval_{index}", (control,), attacks))
    return (*economic, *approval_groups)


# Amounts no author field bounds: the largest approval and a uint128 deposit word.
ANY_AMOUNT = 2**256 - 1
LARGE_DEPOSIT = 2**128 - 1


def v3_call(target, destination, payload):
    return ContractCall(
        fund=target.fund,
        to=destination,
        value=0,
        data="0x" + payload.hex(),
    )


def v3_approval(target, asset, spender, amount):
    return v3_call(
        target,
        asset.reference,
        v3_abi.calldata("approve(address,uint256)", ("address", "uint256"), (spender, amount)),
    )


def v3_swap(target, values, *, count=1):
    exact = v3_abi.calldata(
        f"exactInputSingle({v3_abi.SWAP_PARAMS})",
        (v3_abi.SWAP_PARAMS,),
        (tuple(values),),
    )
    payload = v3_abi.calldata(
        "multicall(uint256,bytes[])",
        ("uint256", "bytes[]"),
        (2**64, (exact,) * count),
    )
    return v3_call(target, target.contracts.router, payload)


def v3_swap_groups(config):
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    profile = config.accounts.binding(allocation.account).profile
    controls = []
    attacks = []
    for direction in range(2):
        assets = target.assets
        values = (
            assets[direction].reference,
            assets[1 - direction].reference,
            target.pool.fee,
            target.fund.address,
            1,
            1,
            0,
        )
        controls.append(v3_swap(target, values))
        for label, index, replacement in (
            ("wrong_input", 0, assets[1 - direction].reference),
            ("wrong_output", 1, assets[direction].reference),
            ("wrong_fee", 2, target.pool.fee + 1),
            ("wrong_recipient", 3, profile.submitter.address),
            ("zero_input", 4, 0),
            ("price_limit", 6, 1),
        ):
            attacks.append(
                (
                    f"v3_swap{direction}_{label}",
                    v3_swap(target, changed(values, index, replacement)),
                )
            )
        attacks.append((f"v3_swap{direction}_extra_nested_call", v3_swap(target, values, count=2)))
    # Scope-only grants (blueprint 27): an approval pins the exact spender with any amount.
    approvals = tuple(
        v3_approval(target, asset, target.contracts.router, amount)
        for asset in target.assets
        for amount in (1, ANY_AMOUNT)
    )
    approval_attacks = tuple(
        (
            f"v3_router_approval{index}_wrong_spender",
            v3_approval(target, asset, profile.submitter.address, 1),
        )
        for index, asset in enumerate(target.assets)
    )
    return (
        ProbeGroup("uniswap_v3_swap", tuple(controls), tuple(attacks)),
        ProbeGroup("uniswap_v3_router_approval", approvals, approval_attacks),
    )


def v3_mint(target, amount0, amount1, *, shift=0):
    values = (
        target.pool.currency0.reference,
        target.pool.currency1.reference,
        target.pool.fee,
        target.range.lower + shift,
        target.range.upper + shift,
        amount0,
        amount1,
        0,
        0,
        target.fund.address,
        2**64,
    )
    payload = v3_abi.calldata(f"mint({v3_abi.MINT_PARAMS})", (v3_abi.MINT_PARAMS,), (values,))
    return values, v3_call(target, target.contracts.position_manager, payload)


def v3_increase(target, token, amount0, amount1):
    values = (token, amount0, amount1, 0, 0, 2**64)
    payload = v3_abi.calldata(
        f"increaseLiquidity({v3_abi.INCREASE_PARAMS})",
        (v3_abi.INCREASE_PARAMS,),
        (values,),
    )
    return v3_call(target, target.contracts.position_manager, payload)


def v3_collect(target, token, *, recipient=None, amount0=v3_abi.MAX128, amount1=v3_abi.MAX128):
    values = (token, recipient or target.fund.address, amount0, amount1)
    payload = v3_abi.calldata(
        f"collect({v3_abi.COLLECT_PARAMS})", (v3_abi.COLLECT_PARAMS,), (values,)
    )
    return payload, v3_call(target, target.contracts.position_manager, payload)


def v3_exit(
    target,
    decrease_token,
    collect_token,
    *,
    burn_token=None,
    liquidity=1,
    recipient=None,
):
    decrease = v3_abi.calldata(
        f"decreaseLiquidity({v3_abi.DECREASE_PARAMS})",
        (v3_abi.DECREASE_PARAMS,),
        ((decrease_token, liquidity, 0, 0, 2**64),),
    )
    collect, _ = v3_collect(target, collect_token, recipient=recipient)
    calls = (decrease, collect)
    if burn_token is not None:
        calls += (v3_abi.calldata("burn(uint256)", ("uint256",), (burn_token,)),)
    payload = v3_abi.calldata("multicall(bytes[])", ("bytes[]",), (calls,))
    return calls, v3_call(target, target.contracts.position_manager, payload)


def v3_liquidity_groups(config):
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    profile = config.accounts.binding(allocation.account).profile
    mint_values, mint = v3_mint(target, 1, 1)
    # Ticks and deposit amounts are not scope (blueprint 27 section 4.3): the gateway
    # enforces the configured range, so another aligned range is an on-chain control.
    mint_controls = (
        mint,
        v3_mint(target, 0, 1)[1],
        v3_mint(target, 1, 0)[1],
        v3_mint(target, 1, 1, shift=target.pool.tick_spacing)[1],
        v3_mint(target, LARGE_DEPOSIT, LARGE_DEPOSIT)[1],
    )
    mint_attacks = tuple(
        (
            f"v3_mint_{label}",
            v3_call(
                target,
                target.contracts.position_manager,
                v3_abi.calldata(
                    f"mint({v3_abi.MINT_PARAMS})",
                    (v3_abi.MINT_PARAMS,),
                    (changed(mint_values, index, replacement),),
                ),
            ),
        )
        for label, index, replacement in (
            ("wrong_token0", 0, target.pool.currency1.reference),
            ("wrong_token1", 1, target.pool.currency0.reference),
            ("wrong_fee", 2, target.pool.fee + 1),
            ("wrong_recipient", 9, profile.submitter.address),
        )
    )
    token0, token1, token2 = 2**200, 2**200 + 1, 2**200 + 2
    increase_controls = tuple(
        v3_increase(target, token0, amount0, amount1)
        for amount0, amount1 in ((1, 1), (0, 1), (1, 0), (LARGE_DEPOSIT, LARGE_DEPOSIT))
    )
    increase_attacks = (("v3_increase_zero_nft", v3_increase(target, 0, 1, 1)),)
    _, collect = v3_collect(target, token0)
    collect_attacks = (
        ("v3_collect_zero_nft", v3_collect(target, 0)[1]),
        (
            "v3_collect_wrong_recipient",
            v3_collect(target, token0, recipient=profile.submitter.address)[1],
        ),
        (
            "v3_collect_nonmax_amount0",
            v3_collect(target, token0, amount0=v3_abi.MAX128 - 1)[1],
        ),
        (
            "v3_collect_nonmax_amount1",
            v3_collect(target, token0, amount1=v3_abi.MAX128 - 1)[1],
        ),
    )
    _, partial = v3_exit(target, token0, token0)
    correlated_calls, correlated = v3_exit(target, token0, token0, burn_token=token0)
    _, uncorrelated_partial = v3_exit(target, token0, token1)
    _, uncorrelated_close = v3_exit(target, token0, token1, burn_token=token2)
    _, zero_liquidity = v3_exit(target, token0, token1, liquidity=0)
    # Exit grants mask every NFT word; positive-ID checks apply only to standalone calls.
    _, zero_nft_partial = v3_exit(target, 0, 0)
    _, zero_nft_close = v3_exit(target, 0, 0, burn_token=0)
    _, wrong_recipient = v3_exit(
        target,
        token0,
        token0,
        burn_token=token0,
        recipient=profile.submitter.address,
    )
    wrong_order = v3_abi.calldata(
        "multicall(bytes[])",
        ("bytes[]",),
        ((correlated_calls[1], correlated_calls[0], correlated_calls[2]),),
    )
    extra = v3_abi.calldata(
        "multicall(bytes[])",
        ("bytes[]",),
        ((correlated_calls + (correlated_calls[2],)),),
    )
    exit_attacks = (
        ("v3_exit_wrong_recipient", wrong_recipient),
        (
            "v3_exit_wrong_order",
            v3_call(target, target.contracts.position_manager, wrong_order),
        ),
        (
            "v3_exit_extra_call",
            v3_call(target, target.contracts.position_manager, extra),
        ),
    )
    approvals = tuple(
        v3_approval(target, asset, target.contracts.position_manager, amount)
        for asset in target.assets
        for amount in (1, ANY_AMOUNT)
    )
    approval_attacks = tuple(
        (
            f"v3_position_approval{index}_wrong_spender",
            v3_approval(target, asset, profile.submitter.address, 1),
        )
        for index, asset in enumerate(target.assets)
    )
    return (
        ProbeGroup("uniswap_v3_position_approval", approvals, approval_attacks),
        ProbeGroup("uniswap_v3_mint", mint_controls, mint_attacks),
        ProbeGroup("uniswap_v3_increase", increase_controls, increase_attacks),
        ProbeGroup("uniswap_v3_collect", (collect,), collect_attacks),
        ProbeGroup(
            "uniswap_v3_exit",
            (
                partial,
                correlated,
                uncorrelated_partial,
                uncorrelated_close,
                zero_liquidity,
                zero_nft_partial,
                zero_nft_close,
            ),
            exit_attacks,
        ),
    )


def v3_groups(config):
    groups = list(v3_swap_groups(config))
    if config.allocations[0].range is not None:
        groups.extend(v3_liquidity_groups(config))
    return tuple(groups)


def v4_groups(config, deadline):
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    assets = target.assets
    swaps = tuple(
        v4_abi.swap(target, AssetAmount(asset=asset, amount=1), 1, deadline) for asset in assets
    )
    # Scope-only authority (blueprint 27): no excess-amount or other-range probe exists.
    attacks = tuple(uniswap_v4_base_authority.swap_attacks(target, deadline))
    swap_attacks = tuple(
        item for item in attacks if item[0] not in {"wrong_spender", "wrong_asset"}
    )
    approval_attacks = tuple(item for item in attacks if item[0] == "wrong_asset")
    permit_attacks = tuple(item for item in attacks if item[0] == "wrong_spender")
    spenders = (target.contracts.router,) + (
        (target.contracts.position_manager,) if target.range is not None else ()
    )
    groups = [
        ProbeGroup("uniswap_v4_swap", swaps, swap_attacks),
        ProbeGroup(
            "uniswap_v4_erc20_approval",
            tuple(v4_abi.approve(target, asset, 1) for asset in assets),
            approval_attacks,
        ),
        ProbeGroup(
            "uniswap_v4_permit2_approval",
            tuple(
                v4_abi.permit(target, asset, spender, 1, deadline)
                for asset in assets
                for spender in spenders
            ),
            permit_attacks,
        ),
    ]
    if target.range is not None:
        cases = dict(uniswap_v4_base_authority.liquidity_attacks(target, deadline))
        groups.extend(
            (
                ProbeGroup(
                    "uniswap_v4_mint",
                    (v4_abi.mint(target, 1, 1, 1, deadline),),
                    tuple((name, cases[name]) for name in cases if name.startswith("mint_")),
                ),
                ProbeGroup(
                    "uniswap_v4_collect",
                    (v4_abi.collect(target, 1, deadline),),
                    tuple((name, cases[name]) for name in ("collect_recipient", "zero_nft")),
                ),
                ProbeGroup(
                    "uniswap_v4_close",
                    (v4_abi.close(target, 1, 0, 0, deadline),),
                    (("close_recipient", cases["close_recipient"]),),
                ),
            )
        )
    return tuple(groups)


def curve_groups(config):
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    pool_layout = layout(target.pool)
    calls = []
    for index in range(len(target.assets)):
        output = (index + 1) % len(target.assets)
        values = (index, output, 1, 1) + pool_layout.receiver(target.fund.address)
        calls.append(
            ContractCall(
                fund=target.fund,
                to=target.pool.pool,
                value=0,
                data=calldata(
                    signature("exchange", pool_layout.exchange),
                    pool_layout.exchange,
                    values,
                ),
            )
        )
    attacks = tuple(
        (label, ContractCall(fund=target.fund, to=destination, value=0, data=inner))
        for label, destination, inner in curve_authority.swap_attacks(target)
    )
    approvals = tuple(
        ContractCall(
            fund=target.fund,
            to=asset.reference,
            value=0,
            data=calldata(
                "approve(address,uint256)",
                ("address", "uint256"),
                (target.pool.pool, 1),
            ),
        )
        for asset in target.assets
    )
    # Scope-only authority (blueprint 27): an approval takes any amount to the exact pool.
    submitter = config.accounts.bindings[0].profile.submitter.address
    approval_attacks = tuple(
        (
            f"curve_approval_{index}_wrong_spender",
            control.model_copy(
                update={
                    "data": calldata(
                        "approve(address,uint256)",
                        ("address", "uint256"),
                        (submitter, 1),
                    )
                }
            ),
        )
        for index, control in enumerate(approvals)
    )
    return (
        ProbeGroup("curve_swap", tuple(calls), attacks),
        ProbeGroup("curve_approval", approvals, approval_attacks),
    )


def outer_boundary_groups(config):
    groups = []
    for binding in config.accounts.bindings:
        profile = binding.profile
        assert isinstance(profile, SafeRoles)
        allocation = next(a for a in config.allocations if a.account == binding.name)
        target = config.target(allocation.name)
        token = target.asset if hasattr(target, "asset") else target.assets[0]
        groups.append(
            ProbeGroup(
                f"roles_outer_boundary_{binding.name}",
                (),
                (
                    (
                        "arbitrary_outer_target",
                        ContractCall(
                            fund=profile.fund,
                            to=profile.submitter.address,
                            value=0,
                            data="0x",
                        ),
                    ),
                    (
                        "arbitrary_outer_function",
                        ContractCall(
                            fund=profile.fund,
                            to=token.reference,
                            value=0,
                            data=calldata(
                                "transfer(address,uint256)",
                                ("address", "uint256"),
                                (profile.submitter.address, 1),
                            ),
                        ),
                    ),
                ),
            )
        )
    return tuple(groups)


def component(config, connector):
    identity = ConnectorId(connector)
    return config.component(identity) if hasattr(config, "component") else config


def probe_groups(node, config):
    groups = []
    configured = getattr(config, "connector", None)
    components = getattr(config, "parameter_components", {})
    if configured == "morpho_blue" or ConnectorId("morpho_blue") in components:
        groups.extend(morpho_groups(component(config, "morpho_blue")))
    if configured == "uniswap_v4" or ConnectorId("uniswap_v4") in components:
        deadline = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16) + 300
        groups.extend(v4_groups(component(config, "uniswap_v4"), deadline))
    if configured == "uniswap_v3" or ConnectorId("uniswap_v3") in components:
        groups.extend(v3_groups(component(config, "uniswap_v3")))
    if configured == "curve" or ConnectorId("curve") in components:
        groups.extend(curve_groups(component(config, "curve")))
    groups.extend(outer_boundary_groups(config))
    return tuple(groups)


def run(node, config):
    node._validate_origin()
    records = []
    for group in probe_groups(node, config):
        account = group.controls[0].fund if group.controls else group.attacks[0][1].fund
        binding = next(
            binding for binding in config.accounts.bindings if binding.profile.fund == account
        )
        profile = binding.profile
        assert isinstance(profile, SafeRoles)
        controls = [
            admitted(node, profile, f"{group.name}_{index}", call)
            for index, call in enumerate(group.controls)
        ]
        rejected = []
        for label, call in group.attacks:
            expected = (
                2
                if label == "arbitrary_outer_target"
                else 3
                if label == "arbitrary_outer_function"
                else None
            )
            rejected.append(denied(node, profile, label, call, expected_status=expected))
        records.append({"template": group.name, "controls": controls, "rejected": rejected})
    return {"method": "eth_call", "roles_entrypoint": ROLES_ENTRYPOINT, "templates": records}
