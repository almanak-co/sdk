"""Post-entry Robinhood V3 authority probes for the supervised reference harness."""

import ast
import json
from types import SimpleNamespace

from eth_abi import decode, encode
from eth_abi.exceptions import DecodingError
from eth_utils import keccak

from almanak.caip import AssetId
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue import operations as morpho_operations
from almanak.connectors.uniswap_v3 import abi
from almanak.connectors.uniswap_v3.models import Target
from almanak.connectors.uniswap_v3.operations import Close, Collect
from almanak.connectors.uniswap_v3.validation import validate_operation
from almanak.dex_intents import CollectLiquidityFees
from almanak.gateway.safe_fixture import calldata
from almanak.models import AssetAmount, SafeRoles
from tests.strategy_fork.pool_lending_authority import wrapped

ROLES_CONDITION_VIOLATION = "0xd0a9bf58"
RPC_ERROR_PREFIX = "fork RPC failed: eth_call: "


def _inner(payload: bytes) -> str:
    return "0x" + payload.hex()


def _wrapped(profile, target, inner, *, value=0, operation=0):
    return wrapped(profile, target, inner, value, operation)


def _wrapped_result(profile, target, inner, *, value=0, operation=0):
    return {
        "from": profile.operator.address,
        "to": profile.roles_module,
        "data": calldata(
            "execTransactionWithRoleReturnData(address,uint256,bytes,uint8,bytes32,bool)",
            ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
            (
                target,
                value,
                bytes.fromhex(inner[2:]),
                operation,
                bytes.fromhex(profile.role_key[2:]),
                False,
            ),
        ),
    }


def _allowed(node, label, call, *, return_data=False):
    """Require Roles admission and retain any returned inner-call outcome."""
    try:
        result = node.rpc("eth_call", [call, "latest"])
    except RuntimeError as error:
        raise AssertionError(f"valid {label} control did not execute") from error
    if not return_data:
        return {"case": label, "call": call, "status": "executed"}
    try:
        raw = bytes.fromhex(result[2:])
        success, returned = decode(("bool", "bytes"), raw)
    except (AttributeError, ValueError, DecodingError) as error:
        raise AssertionError(f"valid {label} control returned malformed Roles data") from error
    if encode(("bool", "bytes"), (success, returned)) != raw:
        raise AssertionError(f"valid {label} control returned noncanonical Roles data")
    return {
        "case": label,
        "call": call,
        "status": "inner_executed" if success else "authorized_inner_refusal",
        "roles_entrypoint": "execTransactionWithRoleReturnData",
        "inner_success": success,
        "inner_return_data": _inner(returned),
    }


def _roles_condition_violation(error):
    message = str(error)
    if not message.startswith(RPC_ERROR_PREFIX):
        raise AssertionError("malformed call returned an unstructured RPC error") from error
    try:
        payload = ast.literal_eval(message.removeprefix(RPC_ERROR_PREFIX))
        data = payload["data"]
        bytes.fromhex(data[2:])
    except (KeyError, TypeError, SyntaxError, ValueError) as parse_error:
        raise AssertionError("malformed call returned an unstructured RPC error") from parse_error
    if (
        not isinstance(payload, dict)
        or payload.get("code") != 3
        or not isinstance(payload.get("message"), str)
        or not payload["message"].startswith("execution reverted")
        or not isinstance(data, str)
        or not data.startswith(ROLES_CONDITION_VIOLATION)
    ):
        raise AssertionError("malformed call did not return a Roles ConditionViolation") from error
    return data


def _denied(node, label, call):
    try:
        node.rpc("eth_call", [call, "latest"])
    except RuntimeError as error:
        revert_data = _roles_condition_violation(error)
    else:
        raise AssertionError(f"malformed {label} call was admitted")
    return {
        "case": label,
        "call": call,
        "error_selector": ROLES_CONDITION_VIOLATION,
        "revert_data": revert_data,
    }


def _swap(target, values, *, count=1):
    exact = abi.calldata(
        f"exactInputSingle({abi.SWAP_PARAMS})", (abi.SWAP_PARAMS,), (tuple(values),)
    )
    return _inner(
        abi.calldata(
            "multicall(uint256,bytes[])",
            ("uint256", "bytes[]"),
            (2**64, (exact,) * count),
        )
    )


def _approval(spender, amount):
    return _inner(
        abi.calldata("approve(address,uint256)", ("address", "uint256"), (spender, amount))
    )


def exchange_permissions(node, state, allocation):
    """Probe the reverse swap scope with held inventory and, for LP, live exit templates."""
    target = state.config.target(allocation)
    profile = state.config.accounts.binding(target.account).profile
    if not isinstance(profile, SafeRoles):
        raise TypeError("Robinhood V3 authority probes require a Safe Roles profile")
    reverse = target.counter_currency
    owned = next(value for value in state.owned if value.target == target)
    amount = next(
        value.amount for value in (owned.inventory0, owned.inventory1) if value.asset == reverse
    )
    values = [
        reverse.reference,
        target.funding_currency.reference,
        target.pool.fee,
        target.fund.address,
        amount,
        1,
        0,
    ]
    # Scope-only grants (blueprint 27): an approval of any amount to the exact router.
    allowed = [
        _allowed(
            node,
            label,
            _wrapped(profile, reverse.reference, _approval(target.contracts.router, value)),
        )
        for label, value in (("router_approval", 1), ("router_approval_any_amount", 2**256 - 1))
    ]
    rejected = []
    insufficiencies = []
    if amount:
        allowed.append(
            _allowed(
                node,
                "reverse_swap_scope",
                _wrapped_result(profile, target.contracts.router, _swap(target, values)),
                return_data=True,
            )
        )
        changes = (
            ("reverse_swap_other_output", 1, reverse.reference),
            ("reverse_swap_other_fee", 2, target.pool.fee + 1),
            ("reverse_swap_other_recipient", 3, profile.submitter.address),
            ("reverse_swap_zero_input", 4, 0),
            ("reverse_swap_price_limit", 6, 1),
        )
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
                "reverse_swap_extra_nested_call",
                _wrapped(profile, target.contracts.router, _swap(target, values, count=2)),
            )
        )
    else:
        insufficiencies.append(
            "no retained counter-asset wallet inventory; reverse swap admission was not probed"
        )
    rejected.append(
        _denied(
            node,
            "router_approval_other_spender",
            _wrapped(profile, reverse.reference, _approval(profile.submitter.address, 1)),
        )
    )
    result = {
        "method": "eth_call",
        "allowed_controls": allowed,
        "rejected": rejected,
        "insufficiencies": insufficiencies,
        "qualification_status": "incomplete" if insufficiencies else "complete",
        "not_isolated": [],
    }
    if target.range is not None:
        result["liquidity_exit"] = liquidity_exit_permissions(node, state, allocation)
    return result


def _latest_observation(state, target):
    observations = [
        outcome.after
        for outcome in state.outcomes
        if getattr(outcome.operation, "target", None) == target
    ]
    if not observations:
        raise AssertionError("LP authority probe requires a retained successful V3 outcome")
    return observations[-1]


def _multicall(*calls):
    return _inner(abi.calldata("multicall(bytes[])", ("bytes[]",), (calls,)))


def liquidity_exit_permissions(node, state, allocation):
    target = state.config.target(allocation)
    profile = state.config.accounts.binding(target.account).profile
    owned = next(value for value in state.owned if value.target == target)
    position = owned.position
    if position is None:
        raise AssertionError("LP hold authority probe requires the retained strategy-owned NFT")
    deadline = 2**64
    collect = Collect(target=target, nft=position.nft, deadline=deadline)
    close = Close(
        target=target,
        nft=position.nft,
        liquidity=position.liquidity,
        minimum0=AssetAmount(asset=target.assets[0], amount=0),
        minimum1=AssetAmount(asset=target.assets[1], amount=0),
        deadline=deadline,
    )
    allowed = [
        _allowed(
            node,
            "position_manager_approval_any_amount",
            _wrapped(
                profile,
                target.assets[0].reference,
                _approval(target.contracts.position_manager, 2**256 - 1),
            ),
        ),
        _allowed(
            node,
            "retained_nft_collect",
            _wrapped(profile, target.contracts.position_manager, abi.call(collect).data),
        ),
        _allowed(
            node,
            "retained_nft_close",
            _wrapped(profile, target.contracts.position_manager, abi.call(close).data),
        ),
    ]
    token = int(position.nft.token_id)
    wrong_collect = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((token, profile.submitter.address, abi.MAX128, abi.MAX128),),
    )
    limited_collect = abi.calldata(
        f"collect({abi.COLLECT_PARAMS})",
        (abi.COLLECT_PARAMS,),
        ((token, target.fund.address, abi.MAX128 - 1, abi.MAX128),),
    )
    rejected = [
        _denied(
            node,
            "position_manager_approval_other_spender",
            _wrapped(
                profile,
                target.assets[0].reference,
                _approval(profile.submitter.address, 1),
            ),
        ),
        _denied(
            node,
            "collect_other_recipient",
            _wrapped(profile, target.contracts.position_manager, _inner(wrong_collect)),
        ),
        _denied(
            node,
            "collect_nonmax_amount",
            _wrapped(profile, target.contracts.position_manager, _inner(limited_collect)),
        ),
    ]
    close_payload = bytes.fromhex(abi.call(close).data[2:])
    rejected.extend(
        (
            _denied(
                node,
                "close_extra_nested_call",
                _wrapped(
                    profile,
                    target.contracts.position_manager,
                    _multicall(close_payload, close_payload),
                ),
            ),
            _denied(
                node,
                "close_native_value",
                _wrapped(
                    profile,
                    target.contracts.position_manager,
                    abi.call(close).data,
                    value=1,
                ),
            ),
        )
    )
    return {
        "allowed_controls": allowed,
        "rejected": rejected,
        "gateway_foreign_nft": foreign_nft_gateway_refusal(state, owned),
        "accepted_onchain_scope": (
            "Roles intentionally leaves compatible NFT IDs unrestricted; the gateway binds the "
            "operation to the retained strategy-owned NFT"
        ),
    }


def foreign_nft_gateway_refusal(state, owned):
    """Exercise gateway admission without replacing the retained ownership checkpoint."""
    position = owned.position
    retained = position.nft
    foreign = AssetId(f"{retained.asset_type}/{int(retained.token_id) + 1}")
    before = _latest_observation(state, owned.target)
    if before.owned != owned or before.position != position:
        raise AssertionError("latest V3 outcome does not retain the held NFT ownership")
    intent = CollectLiquidityFees[Target](
        target=owned.target,
        deadline=(
            before.block.timestamp
            + state.config.allocation(owned.target.allocation).deadline_seconds
        ),
    )
    decision = SimpleNamespace(intent=intent, observations=(before,))
    view = SimpleNamespace(
        config=state.config,
        owned=state.owned,
        capabilities=state.capabilities,
        decision=decision,
        outcomes=state.outcomes,
        mode=state.mode,
    )
    operation = Collect(target=owned.target, nft=foreign, deadline=intent.deadline)
    try:
        validate_operation(SimpleNamespace(config=state.config), view, operation, before)
    except ValueError as error:
        if "retained economic intent or NFT" not in str(error):
            raise
    else:
        raise AssertionError("gateway admitted a foreign NFT in place of retained ownership")
    retained_after = next(value for value in state.owned if value.target == owned.target)
    if retained_after.position.nft != retained:
        raise AssertionError("foreign NFT probe changed retained ownership")
    return {
        "status": "rejected",
        "retained_nft": str(retained),
        "foreign_nft": str(foreign),
        "ownership_source": "unchanged retained checkpoint and latest successful V3 outcome",
        "error": "operation differs from its retained economic intent or NFT",
    }


def morpho_exit_permissions(node, state):
    """Probe Morpho repay/withdraw grants whose bounds remain available after entry."""
    component = state.config.component(ConnectorId("morpho_blue"))
    profile = component.accounts.binding("fund").profile
    if not isinstance(profile, SafeRoles):
        raise TypeError("Morpho authority probes require a Safe Roles profile")
    loan = component.allocation("loan")
    collateral = component.allocation("collateral")
    loan_owned = next(value for value in state.owned if value.target == component.target("loan"))
    collateral_owned = next(
        value for value in state.owned if value.target == component.target("collateral")
    )
    if not loan_owned.borrow_shares or not collateral_owned.collateral:
        raise AssertionError("Morpho exit probes require retained debt and collateral")

    repay = morpho_operations.RepayOperation(
        target=loan_owned.target,
        amount=AssetAmount(asset=loan_owned.target.asset, amount=1),
        shares=0,
    )
    withdraw = morpho_operations.WithdrawOperation(
        target=collateral_owned.target,
        amount=AssetAmount(asset=collateral_owned.target.asset, amount=1),
        full=False,
    )
    controls = []
    for label, operation in (
        ("retained_debt_repay", repay),
        ("retained_collateral_withdraw", withdraw),
    ):
        call = morpho_operations.plan_call(operation)
        controls.append(
            _allowed(
                node,
                label,
                _wrapped_result(profile, call.to, call.data),
                return_data=True,
            )
        )
    controls.append(
        _allowed(
            node,
            "bounded_morpho_approval",
            _wrapped(
                profile,
                loan.reserve.asset.reference,
                _approval(loan.reserve.market.core, 1),
            ),
        )
    )

    rejected = []
    repay_name, repay_types, repay_values = morpho_operations.call_arguments(repay)
    withdraw_name, withdraw_types, withdraw_values = morpho_operations.call_arguments(withdraw)
    malformed = (
        (
            "repay_other_owner",
            loan.reserve.market.core,
            repay_name,
            repay_types,
            (*repay_values[:3], profile.submitter.address, repay_values[4]),
        ),
        (
            "repay_nonempty_callback",
            loan.reserve.market.core,
            repay_name,
            repay_types,
            (*repay_values[:4], b"\x01"),
        ),
        (
            "repay_assets_and_shares",
            loan.reserve.market.core,
            repay_name,
            repay_types,
            (repay_values[0], 1, 1, *repay_values[3:]),
        ),
        (
            "repay_zero",
            loan.reserve.market.core,
            repay_name,
            repay_types,
            (repay_values[0], 0, 0, *repay_values[3:]),
        ),
        (
            "withdraw_other_receiver",
            collateral.reserve.market.core,
            withdraw_name,
            withdraw_types,
            (*withdraw_values[:3], profile.submitter.address),
        ),
        (
            "withdraw_other_owner",
            collateral.reserve.market.core,
            withdraw_name,
            withdraw_types,
            (*withdraw_values[:2], profile.submitter.address, withdraw_values[3]),
        ),
    )
    for label, target, name, types, values in malformed:
        inner = keccak(text=f"{name}({','.join(types)})")[:4]
        inner += encode(types, values)
        rejected.append(_denied(node, label, _wrapped(profile, target, _inner(inner))))
    rejected.append(
        _denied(
            node,
            "morpho_approval_other_spender",
            _wrapped(
                profile,
                loan.reserve.asset.reference,
                _approval(profile.submitter.address, 1),
            ),
        )
    )
    return {
        "method": "eth_call",
        "allowed_controls": controls,
        "rejected": rejected,
        "not_isolated": [
            "supply and borrow predicates use entry-consumed non-refilling quotas; this hold "
            "probe does not attribute their refusal to individual calldata predicates"
        ],
    }


def retain_observation(journal, stage, observation):
    path = journal.path.parent / "permission-budget-observations.json"
    observations = json.loads(path.read_text()) if path.is_file() else []
    observations.append({"stage": stage, **observation})
    path.write_text(json.dumps(observations, indent=2) + "\n")


def exercise_hold(node, journal, process, allocation, *, morpho=False):
    state = journal.load()
    if process.poll() is not None:
        raise AssertionError("coordinator exited before the authority hold probe")
    target = state.config.target(allocation)
    profile = state.config.accounts.binding(target.account).profile
    if isinstance(profile, SafeRoles):
        uniswap = exchange_permissions(node, state, allocation)
        observation = {
            "profile": "safe_roles",
            "status": ("passed" if uniswap["qualification_status"] == "complete" else "incomplete"),
            "uniswap_v3": uniswap,
        }
        if morpho:
            observation["morpho_blue"] = morpho_exit_permissions(node, state)
    else:
        observation = {
            "profile": "eoa",
            "status": "not_applicable",
            "reason": "EOA execution has no installed Roles authority",
        }
    retain_observation(journal, "paused_and_resumed_hold", observation)
