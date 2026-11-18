"""CAIP value types. Syntax admits identities, never execution authority.

CAIP-2 and CAIP-19 are case sensitive. EVM contract references alone are
canonicalized to lowercase to deduplicate their equivalent hexadecimal forms;
other references and individual token identifiers retain their exact case.
"""

import re
from typing import ClassVar

from pydantic_core import core_schema

CHAIN = r"[-a-z0-9]{3,8}:[-_a-zA-Z0-9]{1,32}"
ASSET_TYPE = CHAIN + r"/[-a-z0-9]{3,8}:[-.%a-zA-Z0-9]{1,128}"
ASSET_ID = ASSET_TYPE + r"/[-.%a-zA-Z0-9]{1,78}"


class AccountReference(str):
    """Address component; namespace-specific semantics are validated by Account."""

    def __new__(cls, value):
        if not isinstance(value, str) or re.fullmatch(r"[-.%a-zA-Z0-9]{1,128}", value) is None:
            raise ValueError("invalid account reference")
        return super().__new__(cls, value)

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_after_validator_function(cls, core_schema.str_schema())


class Identifier:
    __slots__ = ("value",)
    value: str
    pattern: ClassVar[str]

    def __init__(self, value: str):
        object.__setattr__(self, "value", value)
        self.__post_init__()

    def __setattr__(self, name, value):
        raise AttributeError("CAIP identifiers are immutable")

    def __delattr__(self, name):
        raise AttributeError("CAIP identifiers are immutable")

    def __eq__(self, other):
        return type(self) is type(other) and self.value == other.value

    def __hash__(self):
        return hash((type(self), self.value))

    def __repr__(self):
        return f"{type(self).__name__}({self.value!r})"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __post_init__(self):
        if type(self.value) is not str or re.fullmatch(self.pattern, self.value) is None:
            raise ValueError(f"invalid {type(self).__name__} syntax")

    def __str__(self) -> str:
        return self.value

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.json_or_python_schema(
            json_schema=core_schema.no_info_after_validator_function(
                cls, core_schema.str_schema(strict=True, pattern="^" + cls.pattern + "$")
            ),
            python_schema=core_schema.is_instance_schema(cls),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda value, info: str(value) if info.mode == "json" else value,
                info_arg=True,
            ),
        )


class ChainId(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = CHAIN

    def __post_init__(self):
        Identifier.__post_init__(self)
        namespace, reference = self.namespace, self.reference
        if namespace == "eip155" and re.fullmatch(r"0|[1-9][0-9]*", reference) is None:
            raise ValueError("EIP-155 reference must be canonical unsigned decimal")
        if namespace == "bip122" and re.fullmatch(r"[0-9a-f]{32}", reference) is None:
            raise ValueError("BIP-122 reference must be 32 lowercase hexadecimal characters")
        if namespace == "cosmos":
            pattern = (
                r"hashed-[0-9a-f]{16}" if reference.startswith("hashed-") else r"[-a-zA-Z0-9]{1,32}"
            )
            if re.fullmatch(pattern, reference) is None:
                raise ValueError("invalid Cosmos direct or hashed reference")
        if namespace == "solana" and re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32}", reference) is None:
            raise ValueError("Solana reference must be the 32-character base58 genesis prefix")

    @property
    def namespace(self) -> str:
        return self.value.split(":", 1)[0]

    @property
    def reference(self) -> str:
        return self.value.split(":", 1)[1]


class AssetType(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = ASSET_TYPE

    def __post_init__(self):
        Identifier.__post_init__(self)
        chain = self.chain
        if self.namespace == "slip44":
            if re.fullmatch(r"0|[1-9][0-9]{0,63}", self.reference) is None:
                raise ValueError(
                    "SLIP-44 reference must be canonical unsigned decimal (64 digits maximum)"
                )
        elif chain.namespace == "eip155" and self.namespace.startswith("erc"):
            if (
                re.fullmatch(r"erc[0-9]{2,5}", self.namespace) is None
                or re.fullmatch(r"0x[0-9a-fA-F]{40}", self.reference) is None
            ):
                raise ValueError(
                    "EVM ERC assets require a numeric ERC namespace and contract address"
                )
            object.__setattr__(self, "value", f"{chain}/{self.namespace}:{self.reference.lower()}")

    @property
    def chain(self) -> ChainId:
        return ChainId(self.value.split("/", 1)[0])

    @property
    def namespace(self) -> str:
        return self.value.split("/", 1)[1].split(":", 1)[0]

    @property
    def reference(self) -> str:
        return self.value.split("/", 1)[1].split(":", 1)[1]


class AssetId(Identifier):
    __slots__ = ()
    pattern: ClassVar[str] = ASSET_ID

    def __post_init__(self):
        Identifier.__post_init__(self)
        asset_type = self.asset_type
        if asset_type.namespace in ("slip44", "erc20"):
            raise ValueError("fungible asset types do not identify individual tokens")
        if asset_type.chain.namespace == "eip155" and asset_type.namespace.startswith("erc"):
            if (
                re.fullmatch(r"0|[1-9][0-9]*", self.token_id) is None
                or int(self.token_id) >= 2**256
            ):
                raise ValueError("EVM individual token ID must be a canonical uint256 decimal")
        object.__setattr__(self, "value", f"{asset_type}/{self.token_id}")

    @property
    def asset_type(self) -> AssetType:
        return AssetType(self.value.rsplit("/", 1)[0])

    @property
    def token_id(self) -> str:
        return self.value.rsplit("/", 1)[1]

    @property
    def chain(self) -> ChainId:
        return self.asset_type.chain
