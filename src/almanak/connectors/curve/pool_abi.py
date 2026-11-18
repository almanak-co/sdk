"""Pool-family argument layouts shared by encoding, returns and Roles predicates."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PoolABI:
    index: str
    amounts: str
    array_parameter: int
    admin_flag: bool
    native_flag: bool = False

    def receiver(self, address):
        return ((False,) if self.native_flag else ()) + (address,)

    @property
    def receiver_types(self):
        return (("bool",) if self.native_flag else ()) + ("address",)

    @property
    def exchange(self):
        return (self.index, self.index, "uint256", "uint256") + self.receiver_types

    @property
    def addition(self):
        return (self.amounts, "uint256") + self.receiver_types

    @property
    def proportional(self):
        return (
            ("uint256", self.amounts) + self.receiver_types + (("bool",) if self.admin_flag else ())
        )

    @property
    def single_coin(self):
        return ("uint256", self.index, "uint256") + self.receiver_types


def layout(source):
    if source.family == "stableswap_ng":
        return PoolABI("int128", "uint256[]", 4, True)
    if source.family == "twocrypto_ng":
        # Roles decodes an inline fixed array as a tuple of its static elements.
        return PoolABI("uint256", "uint256[2]", 3, False)
    if source.family == "tricrypto_ng":
        if source.implementation_revision == "tricrypto-ng-2024-01-04":
            return PoolABI("uint256", "uint256[3]", 3, False)
        return PoolABI("uint256", "uint256[3]", 3, True, True)
    raise ValueError("unsupported Curve pool ABI family")


def signature(name, types):
    return name + "(" + ",".join(types) + ")"
