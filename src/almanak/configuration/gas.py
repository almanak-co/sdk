"""Author gas economics use named accounts; concrete payer authority stays trusted."""

from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.configuration.input_values import Duration, Quantity, resolve_quantity
from almanak.gateway.evm import native_metadata
from almanak.gateway.operator_config import Name
from almanak.gateway.production_config import GasPolicy, Positive, ReplacementPolicy
from almanak.models import AssetAmount, Model, UInt


class ReplacementInput(Model):
    interval_seconds: Duration
    minimum_bump_bps: Positive
    max_candidates: Annotated[UInt, Field(ge=2)]
    max_elapsed_seconds: Duration

    def resolve(self):
        return ReplacementPolicy.model_validate(self.model_dump())


# Defaults for the fields that tune how execution bids, not what it may spend. The
# amounts (`max_transaction_fee`, `exit_reserve`, `total_budget`) are spending
# authority and never default. A default enters the authored digest, so changing one
# changes the digest of every file that relies on it: add chains, keep existing values.
DEFAULT_REWARD_PERCENTILE = 50
DEFAULT_GAS_LIMIT_BUFFER_BPS = 2500
# Covers every action measured through Safe/Roles in connectors/gas_references.json,
# with the default buffer, except GMX order creation and Uniswap V4 liquidity on native
# pools, which `almanak check` flags; within 2M operator ceilings.
DEFAULT_MAX_GAS_LIMIT = 1_200_000
DEFAULT_BASE_FEE_MULTIPLIER_BPS = 20000
# Arbitrum Nitro, and Robinhood Chain on it, charge the base fee and refund a legacy
# bid down to it, so a 2x multiplier only consumes the price ceiling.
BASE_FEE_MULTIPLIER_BPS = {"eip155:42161": 12500, "eip155:4663": 12500}
# Avalanche's fee history reports only the observed block's base fee, never the next.
OBSERVED_BASE_FEE_CHAINS = frozenset({"eip155:43114"})
DEFAULTED_FIELDS = (
    "source",
    "base_fee_basis",
    "reward_percentile",
    "base_fee_multiplier_bps",
    "max_gas_limit",
    "max_gas_price_wei",
    "gas_limit_buffer_bps",
)


def chain_multiplier(data: dict[str, Any]) -> int:
    return BASE_FEE_MULTIPLIER_BPS.get(str(data.get("chain")), DEFAULT_BASE_FEE_MULTIPLIER_BPS)


BaseFeeBasis = Literal["next_block", "observed_block"]


def chain_base_fee_basis(data: dict[str, Any]) -> BaseFeeBasis:
    return "observed_block" if str(data.get("chain")) in OBSERVED_BASE_FEE_CHAINS else "next_block"


def fee_bound_price(data: dict[str, Any]) -> int:
    """The highest price at which `max_gas_limit` gas fits `max_transaction_fee`.

    Pydantic does not locate an exception raised here, so a fee or chain that cannot
    give a price yields 0 and `positive_price` names the cause on its own field.
    """
    try:
        decimals = native_metadata(data["chain"])[1].decimals
        return resolve_quantity(data["max_transaction_fee"], decimals) // data["max_gas_limit"]
    except (KeyError, ValueError):
        return 0


class GasInput(Model):
    """An author gas policy; only the payers, the chain and the amounts are required.

    `max_gas_price_wei` defaults to the highest price at which `max_gas_limit` gas
    still fits `max_transaction_fee`, so the ceilings never contradict the fee the
    author chose. Above it every transaction, exits included, waits for gas to fall.
    """

    accounts: Annotated[tuple[Name, ...], Field(min_length=1)]
    chain: ChainId
    source: Literal["network_rpc_fee_history"] = "network_rpc_fee_history"
    base_fee_basis: BaseFeeBasis = Field(default_factory=chain_base_fee_basis)
    reward_percentile: Annotated[UInt, Field(le=100)] = DEFAULT_REWARD_PERCENTILE
    base_fee_multiplier_bps: Annotated[UInt, Field(ge=10000, le=50000)] = Field(
        default_factory=chain_multiplier
    )
    max_gas_limit: Positive = DEFAULT_MAX_GAS_LIMIT
    max_transaction_fee: Quantity
    # After the fee and the limit, which its default reads. Zero is refused below,
    # with a reason that also fits a derived price.
    max_gas_price_wei: Annotated[UInt, Field(lt=2**256)] = Field(default_factory=fee_bound_price)
    total_budget: Quantity | None = None
    exit_reserve: Quantity
    gas_limit_buffer_bps: Annotated[UInt, Field(le=10000)] = DEFAULT_GAS_LIMIT_BUFFER_BPS
    replacement: ReplacementInput | None = None

    @model_validator(mode="after")
    def positive_price(self):
        if self.max_gas_price_wei:
            return self
        try:
            decimals = native_metadata(self.chain)[1].decimals
        except ValueError as error:
            raise ValueError(f"gas chain {self.chain}: {error}") from None
        try:
            resolve_quantity(self.max_transaction_fee, decimals)
        except ValueError as error:
            raise ValueError(f"max_transaction_fee: {error}") from None
        raise ValueError(
            "max_gas_price_wei must be positive; when omitted it is max_transaction_fee "
            "in wei divided by max_gas_limit, so raise max_transaction_fee"
        )

    def resolve(self, name: str) -> GasPolicy:
        identity, native = native_metadata(self.chain)
        values = self.model_dump(exclude={"chain", "replacement"})
        values.update(name=name, asset=identity)
        for field in ("max_transaction_fee", "total_budget", "exit_reserve"):
            if getattr(self, field) is None:
                values.pop(field)
                continue
            values[field] = AssetAmount(
                asset=identity,
                amount=resolve_quantity(getattr(self, field), native.decimals),
            )
        values["replacement"] = self.replacement.resolve() if self.replacement else None
        return GasPolicy.model_validate(values)
