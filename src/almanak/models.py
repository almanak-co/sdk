from typing import Annotated, Literal, Self

from eth_utils import is_checksum_address, is_hex_address
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

from almanak.caip import AccountReference, AssetId, AssetType, ChainId


def address(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("0x") or not is_hex_address(value):
        raise ValueError("expected a 20-byte EVM address")
    body = value[2:]
    if body != body.lower() and body != body.upper() and not is_checksum_address(value):
        raise ValueError("invalid address checksum")
    return value.lower()


Address = Annotated[str, BeforeValidator(address)]
UInt = Annotated[int, Field(strict=True, ge=0)]
Hash32 = Annotated[str, Field(pattern=r"^0x[0-9a-f]{64}$")]
ZERO_ADDRESS = "0x" + "00" * 20


class Model(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", validate_default=True)


class AssetMetadata(Model):
    decimals: Annotated[int, Field(strict=True, ge=0, le=255)]
    symbol: str | None = Field(default=None, exclude_if=lambda value: value is None)


class Chain(Model):
    chain_id: ChainId
    environment: Annotated[str, Field(min_length=1)]


class Account(Model):
    chain: Chain
    address: AccountReference

    @model_validator(mode="after")
    def nonzero(self) -> Self:
        if self.chain.chain_id.namespace == "eip155":
            object.__setattr__(self, "address", AccountReference(address(self.address)))
            if self.address == ZERO_ADDRESS:
                raise ValueError("an account cannot be the zero address")
        return self


class Eoa(Model):
    kind: Literal["eoa"]
    fund: Account

    @model_validator(mode="after")
    def evm(self) -> Self:
        if self.fund.chain.chain_id.namespace != "eip155":
            raise ValueError("EOA execution profile requires an EVM chain")
        return self

    @property
    def operator(self) -> Account:
        return self.fund

    @property
    def submitter(self) -> Account:
        return self.fund


class SafeRoles(Model):
    kind: Literal["safe_roles"]
    fund: Account
    operator: Account
    roles_module: Address
    role_key: Hash32
    safe_deployment_block: UInt | None = None
    # Not read by verification, which uses Roles storage only. Retained fixtures and
    # evidence captures carry it, so the model still accepts it.
    roles_deployment_block: UInt | None = None

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.fund.chain.chain_id.namespace != "eip155":
            raise ValueError("Safe/Roles execution profile requires an EVM chain")
        if self.fund.chain != self.operator.chain:
            raise ValueError("fund and operator must be on the same chain/environment")
        if self.role_key == "0x" + "00" * 32:
            raise ValueError("Safe execution requires an explicit nonzero role key")
        if (
            len({self.fund.address, self.operator.address, self.roles_module}) != 3
            or self.roles_module == ZERO_ADDRESS
        ):
            raise ValueError("Safe fund, operator and nonzero Roles module must be distinct")
        return self

    @property
    def submitter(self) -> Account:
        """The initial profile uses a direct operator call to Roles."""
        return self.operator


class SimulationAccount(Model):
    """An adapter identity without signing or installed account authority."""

    kind: Literal["simulation"] = "simulation"
    fund: Account
    payer: Account | None = None

    @property
    def submitter(self) -> Account:
        return self.payer or self.fund


AccountProfile = Annotated[Eoa | SafeRoles | SimulationAccount, Field(discriminator="kind")]


class AccountBinding(Model):
    name: Annotated[str, Field(min_length=1, pattern=r"^\S+$")]
    profile: AccountProfile


class StrategyAccounts(Model):
    bindings: tuple[AccountBinding, ...]

    @model_validator(mode="after")
    def unique_bindings(self) -> Self:
        if len({binding.name for binding in self.bindings}) != len(self.bindings):
            raise ValueError("account binding names must be unique")
        if len({binding.profile.fund for binding in self.bindings}) != len(self.bindings):
            raise ValueError("each fund account must have one execution profile per strategy")
        return self

    def binding(self, name: str) -> AccountBinding:
        for binding in self.bindings:
            if binding.name == name:
                return binding
        raise KeyError(f"unknown account binding: {name}")


class Block(Model):
    chain: Chain
    number: UInt
    hash: Hash32
    timestamp: UInt


class Asset(Model):
    chain: Chain
    identifier: AssetType | AssetId
    decimals: Annotated[int, Field(strict=True, ge=0, le=255)]

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.identifier.chain != self.chain.chain_id:
            raise ValueError("asset identity and environment scope have different chains")
        return self


class AssetAmount(Model):
    """Nonnegative base-unit amount with explicit asset identity."""

    asset: AssetType
    amount: UInt
