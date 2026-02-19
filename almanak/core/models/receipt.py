import uuid
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from almanak.core.enums import ActionType, SwapSide


class Receipt(BaseModel, ABC):
    type: ActionType
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    model_config = {
        "arbitrary_types_allowed": True,
    }

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        exclude: set[str] = kwargs.pop("exclude", None) or set()
        kwargs.setdefault("exclude_unset", True)
        data = super().model_dump(exclude=exclude, **kwargs)
        if "type" not in exclude:
            data["type"] = self.type.value
        if "action_id" not in exclude:
            data["action_id"] = str(self.action_id)
        if "bundle_id" not in exclude and self.bundle_id is not None:
            data["bundle_id"] = str(self.bundle_id)
        return data

    @abstractmethod
    def __str__(self) -> str:
        pass

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Receipt":
        receipt_type = ActionType(data.pop("type"))
        if receipt_type == ActionType.WRAP:
            return WrapReceipt(**data)
        elif receipt_type == ActionType.UNWRAP:
            return UnwrapReceipt(**data)
        elif receipt_type == ActionType.APPROVE:
            return ApproveReceipt(**data)
        elif receipt_type == ActionType.SWAP:
            return SwapReceipt(**data)
        elif receipt_type == ActionType.OPEN_LP_POSITION:
            return OpenPositionReceipt(**data)
        elif receipt_type == ActionType.CLOSE_LP_POSITION:
            return ClosePositionReceipt(**data)
        elif receipt_type == ActionType.SETTLE_VAULT_DEPOSIT:
            return SettleDepositReceipt(**data)
        elif receipt_type == ActionType.SETTLE_VAULT_REDEEM:
            return SettleRedeemReceipt(**data)
        elif receipt_type == ActionType.PROPOSE_VAULT_VALUATION:
            return UpdateTotalAssetsReceipt(**data)
        elif receipt_type == ActionType.DEPOSIT:
            return DepositReceipt(**data)
        elif receipt_type == ActionType.WITHDRAW:
            return WithdrawReceipt(**data)
        elif receipt_type == ActionType.SUPPLY:
            return SupplyReceipt(**data)
        elif receipt_type == ActionType.BORROW:
            return BorrowReceipt(**data)
        elif receipt_type == ActionType.REPAY:
            return RepayReceipt(**data)
        elif receipt_type == ActionType.CUSTOM:
            return CustomReceipt(**data)
        raise ValueError(f"Unknown Receipt type: {receipt_type}")


class WrapReceipt(Receipt):
    type: ActionType = ActionType.WRAP
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int

    def __str__(self) -> str:
        return f"WrapReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class UnwrapReceipt(Receipt):
    type: ActionType = ActionType.UNWRAP
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int

    def __str__(self) -> str:
        return f"UnwrapReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class ApproveReceipt(Receipt):
    type: ActionType = ActionType.APPROVE
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    def __str__(self):
        return f"ApproveReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}"


class OpenPositionReceipt(Receipt):
    type: ActionType = ActionType.OPEN_LP_POSITION
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    token0_symbol: str
    token1_symbol: str
    amount0: int
    amount1: int
    position_id: int
    bound_tick_lower: int
    bound_tick_upper: int
    bound_price_lower: float
    bound_price_upper: float
    pool_tick: int | None
    pool_spot_rate: float | None

    def __str__(self) -> str:
        return (
            f"OpenPositionReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  Position ID: {self.position_id}\n"
            f"  Token0: {self.token0_symbol}\n"
            f"  Token1: {self.token1_symbol}\n"
            f"  Amount0: {self.amount0}\n"
            f"  Amount1: {self.amount1}\n"
            f"  Bound Tick Lower: {self.bound_tick_lower}\n"
            f"  Bound Tick Upper: {self.bound_tick_upper}\n"
            f"  Bound Price Lower: {self.bound_price_lower}\n"
            f"  Bound Price Upper: {self.bound_price_upper}"
        )


class SwapReceipt(Receipt):
    type: ActionType = ActionType.SWAP
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    side: SwapSide | None = None
    tokenIn_symbol: str
    tokenOut_symbol: str
    amountIn: int
    amountOut: int | None = None

    def __str__(self) -> str:
        side_str = f"  Side: {self.side.value}\n" if self.side is not None else ""
        return (
            f"SwapReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  {side_str}"
            f"  TokenIn: {self.tokenIn_symbol}\n"
            f"  TokenOut: {self.tokenOut_symbol}\n"
            f"  AmountIn: {self.amountIn}\n"
            f"  AmountOut: {self.amountOut}"
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        d["side"] = self.side.value if self.side is not None else None
        return d


class ClosePositionReceipt(Receipt):
    type: ActionType = ActionType.CLOSE_LP_POSITION
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    position_id: int
    token0_symbol: str
    token1_symbol: str
    amount0: int
    amount1: int
    liquidity0: int
    liquidity1: int
    fees0: int
    fees1: int
    pool_tick: int | None
    pool_spot_rate: float | None

    def __str__(self) -> str:
        return (
            f"ClosePositionReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  Position ID: {self.position_id}\n"
            f"  Token0: {self.token0_symbol}\n"
            f"  Token1: {self.token1_symbol}\n"
            f"  Amount0: {self.amount0}\n"
            f"  Amount1: {self.amount1}\n"
            f"  Liquidity0: {self.liquidity0}\n"
            f"  Liquidity1: {self.liquidity1}\n"
            f"  Fees0: {self.fees0}\n"
            f"  Fees1: {self.fees1}"
        )


class SettleDepositReceipt(Receipt):
    type: ActionType = ActionType.SETTLE_VAULT_DEPOSIT
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    vault_address: str
    total_assets: int
    deposit_assets: int
    deposit_shares_minted: int
    deposit_total_supply: int | None
    deposit_total_assets: int | None
    redeem_assets_withdrawn: int
    redeem_shares_burned: int
    redeem_total_supply: int | None
    redeem_total_assets: int | None
    protocol_fee_shares_minted: int
    strategist_fee_shares_minted: int
    old_high_water_mark: int | None
    new_high_water_mark: int | None

    def __str__(self) -> str:
        return (
            f"SettleDepositReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  Total Assets: {self.total_assets}\n"
            f"  Deposited Amount: {self.deposit_assets}\n"
            f"  Shares Minted: {self.deposit_shares_minted}\n"
            f"  Protocol Fee Shares: {self.protocol_fee_shares_minted}\n"
            f"  Strategist Fee Shares: {self.strategist_fee_shares_minted}\n"
            f"  Old High Water Mark: {self.old_high_water_mark}\n"
            f"  New High Water Mark: {self.new_high_water_mark}"
        )


class SettleRedeemReceipt(Receipt):
    type: ActionType = ActionType.SETTLE_VAULT_REDEEM
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    vault_address: str
    total_assets: int
    redeem_assets_withdrawn: int
    redeem_shares_burned: int
    redeem_total_supply: int | None
    redeem_total_assets: int | None
    protocol_fee_shares_minted: int
    strategist_fee_shares_minted: int
    old_high_water_mark: int | None
    new_high_water_mark: int | None

    def __str__(self) -> str:
        return (
            f"SettleRedeemReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  Total Assets: {self.total_assets}\n"
            f"  Redeemed Amount: {self.redeem_assets_withdrawn}\n"
            f"  Shares Burned: {self.redeem_shares_burned}\n"
            f"  Protocol Fee Shares: {self.protocol_fee_shares_minted}\n"
            f"  Strategist Fee Shares: {self.strategist_fee_shares_minted}\n"
            f"  Old High Water Mark: {self.old_high_water_mark}\n"
            f"  New High Water Mark: {self.new_high_water_mark}"
        )


class UpdateTotalAssetsReceipt(Receipt):
    type: ActionType = ActionType.PROPOSE_VAULT_VALUATION
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int

    vault_address: str
    valuator_address: str
    total_assets: int

    def __str__(self) -> str:
        return (
            f"UpdateTotalAssetsReceipt action_id={self.action_id},\n"
            f"  TX Cost: {self.tx_cost}\n"
            f"  Total Assets: {self.total_assets}"
        )


class DepositReceipt(Receipt):
    type: ActionType = ActionType.DEPOSIT
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int
    # TODO: add asset here (get from transfer event)

    def __str__(self) -> str:
        return f"DepositReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class WithdrawReceipt(Receipt):
    type: ActionType = ActionType.WITHDRAW
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int
    # TODO: add asset here (get from transfer event)

    def __str__(self) -> str:
        return f"WithdrawReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class SupplyReceipt(Receipt):
    type: ActionType = ActionType.SUPPLY
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int
    # TODO: add asset here (get from transfer event)

    def __str__(self) -> str:
        return f"SupplyReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class BorrowReceipt(Receipt):
    type: ActionType = ActionType.BORROW
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int
    # TODO: add asset here (get from transfer event)

    def __str__(self) -> str:
        return f"BorrowReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class RepayReceipt(Receipt):
    type: ActionType = ActionType.REPAY
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    amount: int
    # TODO: add asset here (get from transfer event)

    def __str__(self) -> str:
        return f"RepayReceipt action_id={self.action_id},\n  TX Cost: {self.tx_cost}\n  Amount: {self.amount}"


class CustomReceipt(Receipt):
    type: ActionType = ActionType.CUSTOM
    action_id: uuid.UUID
    bundle_id: uuid.UUID | None
    tx_hash: str
    tx_cost: int
    gas_used: int
    block_number: int
    function: str  # e.g., "setAuthorization"
    target_protocol: str  # e.g., "morpho-markets-v1"

    def __str__(self) -> str:
        return (
            f"CustomReceipt action_id={self.action_id},\n"
            f"  Function: {self.function}\n"
            f"  Protocol: {self.target_protocol}\n"
            f"  TX Cost: {self.tx_cost}"
        )
