from typing import NamedTuple, assert_never

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.connectors.aave_v4.domain import (
    AaveReserve,
    PositionObservation,
    ReserveObservation,
)
from almanak.connectors.aave_v4.models import Allocation, DebtAllocation, Intent, LendingSnapshot
from almanak.connectors.aave_v4.operations import (
    AaveOperation,
    ApproveOperation,
    BorrowOperation,
    CollateralOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.repayment import require_repayment_funding
from almanak.gateway.accounts import ContractCall
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import erc20_address
from almanak.lending_intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.models import Account, AssetAmount, Block, address


class ReserveData(NamedTuple):
    underlying: str
    hub: str
    hub_asset_id: int
    decimals: int
    collateral_risk_bps: int
    flags: int
    dynamic_config_key: int


def read_reserve_data(reader, spoke, reserve_id, block) -> ReserveData:
    return ReserveData(
        *reader.call(
            spoke,
            "getReserve(uint256)",
            block,
            outputs=("address", "address", "uint16", "uint8", "uint24", "uint8", "uint32"),
            inputs=("uint256",),
            args=(reserve_id,),
        )
    )


class ReserveConfig(NamedTuple):
    collateral_risk_bps: int
    paused: bool
    frozen: bool
    borrowable: bool
    receive_shares_enabled: bool


def read_reserve_config(reader, spoke, reserve_id, block) -> ReserveConfig:
    return ReserveConfig(
        *reader.call(
            spoke,
            "getReserveConfig(uint256)",
            block,
            outputs=("uint24", "bool", "bool", "bool", "bool"),
            inputs=("uint256",),
            args=(reserve_id,),
        )
    )


class DynamicReserveConfig(NamedTuple):
    collateral_factor_bps: int
    max_liquidation_bonus_bps: int
    liquidation_fee_bps: int


def read_dynamic_reserve_config(reader, spoke, reserve_id, key, block) -> DynamicReserveConfig:
    """One versioned collateral config of a reserve; a position keeps the key it opened with."""
    return DynamicReserveConfig(
        *reader.call(
            spoke,
            "getDynamicReserveConfig(uint256,uint32)",
            block,
            inputs=("uint256", "uint32"),
            args=(reserve_id, key),
            outputs=("uint16", "uint32", "uint16"),
        )
    )


class SpokeLiquidationConfig(NamedTuple):
    target_health_factor: int
    health_factor_for_max_bonus: int
    bonus_factor_bps: int


def read_liquidation_config(reader, spoke, block) -> SpokeLiquidationConfig:
    return SpokeLiquidationConfig(
        *reader.call(
            spoke, "getLiquidationConfig()", block, outputs=("uint128", "uint64", "uint16")
        )
    )


def read_reserve(reader: EthereumReader, source: AaveReserve, block: Block) -> ReserveObservation:
    if source.chain != reader.chain or block.chain != source.chain:
        raise DataError("chain_mismatch", "reserve source has a different chain/environment")
    # The authored asset identity is rejected before any RPC; the read cannot repair it.
    erc20_address(source.asset, source.chain.chain_id)
    data = read_reserve_data(reader, source.spoke, source.reserve_id, block)
    return observe_reserve(reader, source, block, data)


def observe_reserve(reader, source, block, data: ReserveData) -> ReserveObservation:
    token = erc20_address(source.asset, source.chain.chain_id)
    if (address(data.underlying), address(data.hub)) != (
        token,
        source.hub,
    ):
        raise DataError("source_mismatch", "reserve does not match the requested asset and Hub")
    config = read_reserve_config(reader, source.spoke, source.reserve_id, block)
    asset = reader.asset(source.asset, block)
    if asset.decimals != data.decimals or config.collateral_risk_bps != data.collateral_risk_bps:
        raise DataError(
            "source_mismatch", "reserve metadata does not reconcile at the pinned block"
        )
    return ReserveObservation(
        source=source,
        block=block,
        asset=asset,
        hub_asset_id=data.hub_asset_id,
        collateral_risk_bps=data.collateral_risk_bps,
        dynamic_config_key=data.dynamic_config_key,
        paused=config.paused,
        frozen=config.frozen,
        borrowable=config.borrowable,
        receive_shares_enabled=config.receive_shares_enabled,
    )


def read_position(
    reader: EthereumReader,
    source: AaveReserve,
    account: Account,
    block: Block,
) -> PositionObservation:
    if account.chain != source.chain:
        raise DataError("chain_mismatch", "position account has a different chain/environment")
    reserve = read_reserve(reader, source, block)
    inputs = ("uint256", "address")
    args = (source.reserve_id, account.address)
    (supplied,) = reader.call(
        source.spoke, "getUserSuppliedAssets(uint256,address)", block, inputs=inputs, args=args
    )
    (shares,) = reader.call(
        source.spoke, "getUserSuppliedShares(uint256,address)", block, inputs=inputs, args=args
    )
    drawn, premium = reader.call(
        source.spoke,
        "getUserDebt(uint256,address)",
        block,
        outputs=("uint256", "uint256"),
        inputs=inputs,
        args=args,
    )
    (total,) = reader.call(
        source.spoke, "getUserTotalDebt(uint256,address)", block, inputs=inputs, args=args
    )
    if drawn + premium != total:
        raise DataError("inconsistent", "drawn and premium debt do not reconcile with total debt")
    return PositionObservation(
        reserve=reserve,
        account=account,
        supplied_assets=supplied,
        supplied_shares=shares,
        drawn_debt=drawn,
        premium_debt=premium,
        total_debt=total,
    )


def plan_operation(intent: Intent, before: LendingSnapshot, allocation=None) -> AaveOperation:
    if intent.target != before.target:
        raise ValueError("intent observation has a different position scope")
    if isinstance(intent, SupplyIntent):
        if before.allowance not in (0, intent.amount.amount):
            raise DataError("inconsistent", "supply allowance differs from the requested amount")
        operation = SupplyOperation if before.allowance else ApproveOperation
        return operation(target=intent.target, amount=intent.amount)
    if isinstance(intent, WithdrawIntent):
        if before.position.supplied_shares:
            return WithdrawOperation(target=intent.target)
        if before.allowance:
            return RevokeOperation(
                target=intent.target, amount=AssetAmount(asset=intent.target.asset, amount=0)
            )
        raise ValueError("withdraw requires owned supply or residual allowance")
    if isinstance(intent, BorrowIntent):
        for collateral in before.collateral:
            if not collateral.enabled:
                return CollateralOperation(
                    target=intent.target, collateral=collateral.target, enabled=True
                )
        return BorrowOperation(target=intent.target, amount=intent.amount)
    if isinstance(intent, RepayIntent):
        return plan_repayment(intent, before, allocation)
    assert_never(intent)


def plan_repayment(intent, before, allocation):
    """Approve the held balance, repay the whole debt, revoke, then release collateral.

    The Spoke clamps the sentinel repayment to the debt at inclusion, so approving the
    held balance rather than the observed debt absorbs interest accrued in between.
    """
    if not isinstance(allocation, DebtAllocation):
        raise ValueError("repayment requires its configured debt allocation")
    debt = before.position.total_debt
    if debt:
        target = intent.target
        require_repayment_funding(target.allocation, target.asset, debt, before.wallet_balance)
        if before.allowance >= debt:
            amount = AssetAmount(asset=target.asset, amount=before.allowance)
            return RepayOperation(target=target, amount=amount)
        if before.allowance == 0:
            amount = AssetAmount(asset=target.asset, amount=before.wallet_balance)
            return ApproveOperation(target=target, amount=amount)
    if before.allowance:
        return RevokeOperation(
            target=intent.target, amount=AssetAmount(asset=intent.target.asset, amount=0)
        )
    for collateral in before.collateral:
        if collateral.enabled:
            return CollateralOperation(
                target=intent.target, collateral=collateral.target, enabled=False
            )
    raise ValueError("repayment has no remaining debt, allowance or collateral obligation")


def plan_call(allocation: Allocation, fund: Account, operation: AaveOperation) -> ContractCall:
    source = allocation.reserve
    if (
        fund != operation.target.fund
        or source != operation.target.reserve
        or operation.account != allocation.account
        or operation.allocation != allocation.name
    ):
        raise ValueError("Aave operation has the wrong fund/chain allocation")
    token = erc20_address(source.asset, source.chain.chain_id)
    types: tuple[str, ...]
    args: tuple[str | int, ...]
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        target = token
        signature = "approve(address,uint256)"
        types = ("address", "uint256")
        args = (source.spoke, operation.amount.amount)
    elif isinstance(operation, CollateralOperation):
        target = operation.collateral.reserve.spoke
        signature = "setUsingAsCollateral(uint256,bool,address)"
        types = ("uint256", "bool", "address")
        args = (operation.collateral.reserve.reserve_id, operation.enabled, fund.address)
    elif isinstance(
        operation, (SupplyOperation, WithdrawOperation, BorrowOperation, RepayOperation)
    ):
        target = source.spoke
        signature = operation.kind + "(uint256,uint256,address)"
        types = ("uint256", "uint256", "address")
        args = (
            source.reserve_id,
            # Full withdrawal and repayment send the sentinel; the Spoke settles the
            # owned balance or debt at inclusion.
            2**256 - 1
            if operation.amount is None or isinstance(operation, RepayOperation)
            else operation.amount.amount,
            fund.address,
        )
    else:
        assert_never(operation)
    return ContractCall(
        fund=fund,
        to=target,
        value=0,
        data="0x" + (keccak(text=signature)[:4] + encode(types, args)).hex(),
    )


def receipt_movement(
    receipt: dict, allocation: Allocation, fund: Account, operation: AaveOperation
) -> tuple[int, int]:
    if fund.chain != allocation.reserve.chain:
        raise ValueError("Aave receipt belongs to a different fund chain/environment")
    token = erc20_address(allocation.reserve.asset, allocation.reserve.chain.chain_id)
    expected: tuple[str | int, ...]
    types: tuple[str, ...]
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        event, emitter = (
            "Approval(address,address,uint256)",
            token,
        )
        expected = (fund.address, allocation.reserve.spoke)
        types = ("uint256",)
    elif isinstance(operation, CollateralOperation):
        event = "SetUsingAsCollateral(uint256,address,address,bool)"
        emitter = operation.collateral.reserve.spoke
        expected = (operation.collateral.reserve.reserve_id, fund.address, fund.address)
        types = ("bool",)
    elif isinstance(
        operation, (SupplyOperation, WithdrawOperation, BorrowOperation, RepayOperation)
    ):
        event = operation.kind.title() + "(uint256,address,address,uint256,uint256)"
        emitter = allocation.reserve.spoke
        expected = (allocation.reserve.reserve_id, fund.address, fund.address)
        types = ("uint256", "uint256")
        if isinstance(operation, RepayOperation):
            event = "Repay(uint256,address,address,uint256,uint256,(int256,int256,uint256))"
            types = (*types, "(int256,int256,uint256)")
    else:
        assert_never(operation)
    topic = "0x" + keccak(text=event).hex()
    logs = [
        entry
        for entry in receipt["logs"]
        if address(entry["address"]) == emitter and entry["topics"][:1] == [topic]
    ]
    if len(logs) != 1:
        raise DataError("inconsistent", "receipt must contain exactly one matching protocol event")
    log = logs[0]
    indexed = [int(value, 16) for value in log["topics"][1:]]
    if indexed != [int(v, 16) if isinstance(v, str) else v for v in expected]:
        raise DataError("source_mismatch", "receipt event belongs to another fund/reserve")
    raw = bytes.fromhex(log["data"][2:])
    values = decode(types, raw)
    if encode(types, values) != raw:
        raise DataError("malformed", "noncanonical event data")
    if isinstance(operation, (ApproveOperation, RevokeOperation)):
        if values[0] != operation.amount.amount:
            raise DataError("inconsistent", "approval event differs from the requested allowance")
        return 0, 0
    if isinstance(operation, CollateralOperation):
        if values != (operation.enabled,):
            raise DataError("inconsistent", "collateral event differs from the planned Boolean")
        return 0, 0
    shares, assets = values[:2]
    return assets, shares
