from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from almanak.core.enums import ActionType, SwapSide


class Params(BaseModel, ABC):
    type: ActionType
    context: dict[str, Any] = {}

    @abstractmethod
    def validate_params(self) -> None:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("exclude_unset", True)
        data = super().model_dump(**kwargs)
        data["type"] = self.type.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Params":
        param_type = ActionType(data.pop("type"))
        if param_type == ActionType.TRANSFER:
            return TransferParams(**data)
        elif param_type == ActionType.WRAP:
            return WrapParams(**data)
        elif param_type == ActionType.UNWRAP:
            return UnwrapParams(**data)
        elif param_type == ActionType.APPROVE:
            return ApproveParams(**data)
        elif param_type == ActionType.SWAP:
            return SwapParams(**data)
        elif param_type == ActionType.OPEN_LP_POSITION:
            return OpenPositionParams(**data)
        elif param_type == ActionType.CLOSE_LP_POSITION:
            return ClosePositionParams(**data)
        elif param_type == ActionType.PROPOSE_VAULT_VALUATION:
            return UpdateTotalAssetsParams(**data)
        elif param_type == ActionType.SETTLE_VAULT_DEPOSIT:
            return SettleDepositParams(**data)
        elif param_type == ActionType.SETTLE_VAULT_REDEEM:
            return SettleRedeemParams(**data)
        elif param_type == ActionType.DEPOSIT:
            return DepositParams(**data)
        elif param_type == ActionType.WITHDRAW:
            return WithdrawParams(**data)
        elif param_type == ActionType.SUPPLY:
            return SupplyParams(**data)
        elif param_type == ActionType.BORROW:
            return BorrowParams(**data)
        elif param_type == ActionType.REPAY:
            return RepayParams(**data)
        elif param_type == ActionType.CUSTOM:
            return CustomParams(**data)
        raise ValueError(f"Unknown Params type: {param_type}")


class TransferParams(Params):
    type: ActionType = ActionType.TRANSFER
    from_address: str
    to_address: str
    amount: int  # token decimal unit (wei)
    nonce_counter: int | None = Field(default=None)

    @field_validator("from_address", "to_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"TransferParams(from_address={self.from_address}, to_address={self.to_address}, amount={self.amount}, nonce_counter={self.nonce_counter})"


class WrapParams(Params):
    type: ActionType = ActionType.WRAP
    from_address: str
    amount: int  # token decimal unit (wei)

    @field_validator("from_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"WrapParams(from_address={self.from_address}, amount={self.amount})"


class UnwrapParams(Params):
    type: ActionType = ActionType.UNWRAP
    from_address: str
    token_address: str
    amount: int

    @field_validator("from_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"UnwrapParams(from_address={self.from_address}, amount={self.amount})"


class ApproveParams(Params):
    type: ActionType = ActionType.APPROVE
    token_address: str
    spender_address: str
    from_address: str
    amount: int | None = None  # token decimal unit (wei)

    @field_validator("token_address", "spender_address", "from_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        # Allow amount=0 for resetting allowance (required for non-standard tokens like USDT)
        if self.amount is not None and self.amount < 0:
            raise ValueError("amount must be non-negative")

    def __str__(self):
        return f"ApproveParams(token_address={self.token_address}, spender_address={self.spender_address}, from_address={self.from_address}, amount={self.amount})"


class SwapParams(Params):
    type: ActionType = ActionType.SWAP
    tokenIn: str
    tokenOut: str
    recipient: str
    amount: int
    side: SwapSide | None = None
    fee: int | None = None
    stable: bool | None = Field(default=None)  # For Aerodrome: True=stable pool, False=volatile pool
    slippage: float | None = Field(default=None)  # Float percentage (e.g. 0.02 is 2%)
    amountOutMinimum: int | None = Field(default=None)
    amountInMaximum: int | None = Field(default=None)
    transfer_eth_in: bool | None = Field(default=False)  # optional
    sqrtPriceLimitX96: int | None = Field(default=None)  # not used for now
    max_price_impact_bp: int | None = Field(default=None)

    @field_validator("tokenIn", "tokenOut", "recipient")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    @field_validator("amount")
    @classmethod
    def must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("amount must be greater than 0")
        return v

    def validate_params(self):
        if self.amountOutMinimum is not None and self.amountOutMinimum < 0:
            raise ValueError("amountOutMinimum must be non-negative if provided")
        if self.amountInMaximum is not None and self.amountInMaximum < 0:
            raise ValueError("amountInMaximum must be non-negative if provided")
        if self.sqrtPriceLimitX96 is not None and self.sqrtPriceLimitX96 < 0:
            raise ValueError("sqrtPriceLimitX96 must be non-negative if provided")
        if self.amountInMaximum is not None and self.side == SwapSide.SELL:
            raise ValueError("amountInMaximum should not be provided for sell side")
        if self.amountOutMinimum is not None and self.side == SwapSide.BUY:
            raise ValueError("amountOutMinimum should not be provided for buy side")
        if self.amountOutMinimum is not None and self.slippage is not None:
            raise ValueError("Only one of amountOutMinimum or slippage should be provided, not both")
        if self.amountInMaximum is not None and self.slippage is not None:
            raise ValueError("Only one of amountInMaximum or slippage should be provided, not both")
        if self.amountOutMinimum is None and self.side == SwapSide.SELL and self.slippage is None:
            raise ValueError("Either amountOutMinimum or slippage must be provided")
        if self.amountInMaximum is None and self.side == SwapSide.BUY and self.slippage is None:
            raise ValueError("Either amountInMaximum or slippage must be provided")

    def __str__(self):
        side_str = f"side={self.side.value}" if self.side else "side=None"
        fee_str = f"fee={self.fee}" if self.fee is not None else "fee=None"
        slippage_str = f"slippage={self.slippage}" if self.slippage is not None else "slippage=None"

        return (
            f"SwapParams({side_str}, tokenIn={self.tokenIn}, tokenOut={self.tokenOut}, {fee_str}, recipient={self.recipient}, "
            f"amount={self.amount}, {slippage_str}, transfer_eth_in={self.transfer_eth_in}, "
            f"amountOutMinimum={self.amountOutMinimum}, amountInMaximum={self.amountInMaximum}, sqrtPriceLimitX96={self.sqrtPriceLimitX96})"
        )

    def model_dump(self, *args, **kwargs):
        d = super().model_dump(*args, **kwargs)
        if self.side is not None:
            d["side"] = self.side.value
        return d


class OpenPositionParams(Params):
    type: ActionType = ActionType.OPEN_LP_POSITION
    token0: str
    token1: str
    fee: int
    stable: bool | None = Field(default=None)  # For Aerodrome: True=stable pool, False=volatile pool
    price_lower: float
    price_upper: float
    amount0_desired: int
    amount1_desired: int
    recipient: str
    amount0_min: int | None = Field(default=None)
    amount1_min: int | None = Field(default=None)
    slippage: float | None = Field(default=None)
    pool_address: str | None = Field(default=None)  # For TraderJoe V2: LBPair address

    @field_validator("token0", "token1", "fee", "price_lower", "price_upper", "recipient")
    @classmethod
    def must_not_be_empty(cls, v: Any) -> Any:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    @field_validator("price_lower", "price_upper")
    @classmethod
    def must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Field must be greater than 0")
        return v

    @field_validator("amount0_desired", "amount1_desired")
    @classmethod
    def amount_desired_must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Desired amount cannot be negative")
        return v

    def validate_params(self):
        if not self.token0 or not self.token1 or self.amount0_desired < 0 or self.amount1_desired < 0:
            raise ValueError("Invalid parameters for Open Position action")
        if (self.amount0_min is not None and self.amount1_min is None) or (
            self.amount0_min is None and self.amount1_min is not None
        ):
            raise ValueError("Both amount_min should be provided for open position")
        if (self.amount0_min is not None and self.amount1_min is not None) and self.slippage is not None:
            raise ValueError("Only one of amount_min or slippage should be provided for open position, not both")
        if self.amount0_min is None and self.amount1_min is None and self.slippage is None:
            raise ValueError("Either amount_min or slippage must be provided")

    def __str__(self):
        return (
            f"OpenPositionParams(token0={self.token0}, token1={self.token1}, fee={self.fee}, price_lower={self.price_lower}, "
            f"price_upper={self.price_upper}, amount0_desired={self.amount0_desired}, amount1_desired={self.amount1_desired}, "
            f"recipient={self.recipient}, amount0_min={self.amount0_min}, amount1_min={self.amount1_min}, slippage={self.slippage})"
        )


class ClosePositionParams(Params):
    type: ActionType = ActionType.CLOSE_LP_POSITION
    position_id: int
    recipient: str
    token0: str
    token1: str
    stable: bool | None = Field(default=None)  # For Aerodrome: True=stable pool, False=volatile pool
    liquidity: int | None = Field(default=None)  # For Aerodrome: LP token amount to burn
    amount0_min: int | None = Field(default=None)
    amount1_min: int | None = Field(default=None)
    slippage: float | None = Field(default=None)
    pool_address: str | None = Field(default=None)

    @field_validator("position_id", "recipient", "token0", "token1")
    @classmethod
    def must_not_be_empty(cls, v: Any) -> Any:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if not self.position_id:
            raise ValueError("Invalid parameters for Close Position action")
        if (self.amount0_min is not None and self.amount1_min is None) or (
            self.amount0_min is None and self.amount1_min is not None
        ):
            raise ValueError("Both amount_min should be provided for open position")
        if (self.amount0_min is not None and self.amount1_min is not None) and self.slippage is not None:
            raise ValueError("Only one of amount_min or slippage should be provided for open position, not both")
        if self.amount0_min is None and self.amount1_min is None and self.slippage is None:
            raise ValueError("Either amount_min or slippage must be provided")

    def __str__(self):
        res = f"ClosePositionParams(position_id={self.position_id}, recipient={self.recipient}, token0={self.token0}, token1={self.token1})"
        if self.amount0_min:
            res += f", amount0_min={self.amount0_min}"
        if self.amount1_min:
            res += f", amount1_min={self.amount1_min}"
        if self.slippage:
            res += f", slippage={self.slippage}"
        if self.pool_address:
            res += f", pool_address={self.pool_address}"
        return res


class UpdateTotalAssetsParams(Params):
    type: ActionType = ActionType.PROPOSE_VAULT_VALUATION
    vault_address: str
    valuator_address: str
    new_total_assets: int
    pending_deposits: int

    @field_validator("valuator_address", "vault_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.new_total_assets < 0:
            raise ValueError("new_total_assets must be non-negative")
        if self.pending_deposits < 0:
            raise ValueError("pending_deposits must be non-negative")

    def __str__(self):
        return f"UpdateTotalAssetsParams(vault_address={self.vault_address}, valuator_address={self.valuator_address}, new_total_assets={self.new_total_assets})"


class SettleDepositParams(Params):
    type: ActionType = ActionType.SETTLE_VAULT_DEPOSIT
    vault_address: str
    safe_address: str
    total_assets: int

    @field_validator("safe_address", "vault_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.total_assets < 0:
            raise ValueError("total_assets must be non-negative")

    def __str__(self):
        return f"SettleDepositParams(vault_address={self.vault_address}, safe_address={self.safe_address}, total_assets={self.total_assets})"


class SettleRedeemParams(Params):
    type: ActionType = ActionType.SETTLE_VAULT_REDEEM
    vault_address: str
    safe_address: str
    total_assets: int

    @field_validator("safe_address", "vault_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.total_assets < 0:
            raise ValueError("total_assets must be non-negative")

    def __str__(self):
        return f"SettleRedeemParams(vault_address={self.vault_address}, safe_address={self.safe_address}, total_assets={self.total_assets})"


class DepositParams(Params):
    type: ActionType = ActionType.DEPOSIT
    token_address: str
    amount: int  # token decimal unit (wei)
    on_behalf_of: str | None = None
    referral_code: int = 0
    from_address: str

    # Optional protocol-specific fields for Enso/Morpho/Aave
    protocol: str | None = None  # e.g., "morpho-markets-v1", "aave-v3"
    positionId: str | None = None  # Morpho market ID
    primaryAddress: str | None = None  # Morpho Blue contract address
    routing_strategy: str | None = None  # "router" or "delegate"
    market_id: str | None = None  # Alternative to positionId
    # gas_override: Optional[int] = None  # Manual gas limit override

    @field_validator("token_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"DepositParams(token_address={self.token_address}, amount={self.amount}, on_behalf_of={self.on_behalf_of}, referral_code={self.referral_code}, from_address={self.from_address})"


class WithdrawParams(Params):
    type: ActionType = ActionType.WITHDRAW
    token_address: str  # For backward compatibility
    asset_address: str | None = None  # Token to withdraw
    amount: int
    to: str  # For backward compatibility
    to_address: str | None = None  # Where to send the withdrawn tokens
    from_address: str
    on_behalf_of: str | None = None

    # Optional protocol-specific fields for Enso/Morpho/Aave
    protocol: str | None = None
    positionId: str | None = None
    primaryAddress: str | None = None
    routing_strategy: str | None = None
    # gas_override: Optional[int] = None  # Manual gas limit override

    @field_validator("token_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    @field_validator("asset_address", mode="before")
    @classmethod
    def set_asset_address(cls, v: str | None, info: ValidationInfo) -> str | None:
        # If asset_address is not set, use token_address
        if not v and info.data.get("token_address"):
            return info.data["token_address"]
        return v

    @field_validator("to_address", mode="before")
    @classmethod
    def set_to_address(cls, v: str | None, info: ValidationInfo) -> str | None:
        # If to_address is not set, use to
        if not v and info.data.get("to"):
            return info.data["to"]
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"WithdrawParams(token_address={self.token_address}, amount={self.amount}, to={self.to}, from_address={self.from_address})"


class SupplyParams(Params):
    type: ActionType = ActionType.SUPPLY
    token_address: str
    amount: int
    referral_code: int = 0
    from_address: str

    @field_validator("token_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"SupplyParams(token_address={self.token_address}, amount={self.amount}, referral_code={self.referral_code}, from_address={self.from_address})"


class BorrowParams(Params):
    type: ActionType = ActionType.BORROW
    token_address: str
    amount: int
    interest_rate_mode: int
    on_behalf_of: str | None = None
    referral_code: int = 0
    from_address: str

    # Optional protocol-specific fields for Enso/Morpho/Aave
    protocol: str | None = None  # e.g., "morpho-markets-v1", "aave-v3"
    positionId: str | None = None  # Morpho market ID
    primaryAddress: str | None = None  # Morpho Blue contract address
    routing_strategy: str | None = None  # "router" or "delegate"
    collateral: str | None = None  # Collateral token address (for Morpho)
    market_id: str | None = None  # Alternative to positionId
    # gas_override: Optional[int] = None  # Manual gas limit override

    @field_validator("token_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"BorrowParams(token_address={self.token_address}, amount={self.amount}, interest_rate_mode={self.interest_rate_mode}, referral_code={self.referral_code}, on_behalf_of={self.on_behalf_of}, from_address={self.from_address})"


class RepayParams(Params):
    type: ActionType = ActionType.REPAY
    token_address: str
    amount: int
    interest_rate_mode: int
    on_behalf_of: str | None = None
    from_address: str

    # Optional protocol-specific fields for Enso/Morpho/Aave
    protocol: str | None = None
    positionId: str | None = None
    primaryAddress: str | None = None
    routing_strategy: str | None = None
    market_id: str | None = None
    debtToCover: int | None = None
    collateralAsset: str | None = None
    userAddress: str | None = None
    # gas_override: Optional[int] = None  # Manual gas limit override

    @field_validator("token_address")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("Field must not be empty")
        return v

    def validate_params(self):
        if self.amount <= 0:
            raise ValueError("amount must be greater than 0")

    def __str__(self):
        return f"RepayParams(token_address={self.token_address}, amount={self.amount}, interest_rate_mode={self.interest_rate_mode}, on_behalf_of={self.on_behalf_of}, from_address={self.from_address}"


class CustomParams(Params):
    type: ActionType = ActionType.CUSTOM
    protocol: str  # e.g., "ENSO"
    target_protocol: str  # e.g., "morpho-markets-v1"
    function: str  # e.g., "setAuthorization"
    params: dict[str, Any]  # Function-specific params
    contract_address: str  # Target contract address
    from_address: str

    def validate_params(self):
        if not self.protocol:
            raise ValueError("protocol must be specified")
        if not self.function:
            raise ValueError("function must be specified")

    def __str__(self):
        return f"CustomParams(protocol={self.protocol}, target_protocol={self.target_protocol}, function={self.function}, contract_address={self.contract_address}, from_address={self.from_address})"
