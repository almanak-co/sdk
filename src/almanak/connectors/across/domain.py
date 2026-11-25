"""Pinned SpokePools, one-direction routes and the two-fund bridge target."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.bridge_intents import BridgeTarget
from almanak.caip import AssetType, ChainId
from almanak.chains import evm_network
from almanak.gateway.evm import erc20_address
from almanak.models import Address, Chain, Hash32, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Uint32 = Annotated[UInt, Field(lt=2**32)]
FeeBps = Annotated[int, Field(strict=True, ge=0, le=10_000)]
Decimals = Annotated[int, Field(strict=True, ge=0, le=255)]
QUOTE_SOURCE = "across_api"


class SpokePoolPin(Model):
    """The proxy and the implementation it must run; another implementation refuses deposits."""

    address: Address
    implementation: Address
    implementation_code_hash: Hash32


# Read from each chain on 23 November 2026: the proxy's EIP-1967 implementation slot and
# the keccak of that implementation's runtime code.
SPOKE_POOLS: dict[ChainId, SpokePoolPin] = {
    ChainId("eip155:42161"): SpokePoolPin(
        address="0xe35e9842fceaca96570b734083f4a58e8f7c5f2a",
        implementation="0xcfcda84333431bcc9155f2368b8362f0d1dff8c9",
        implementation_code_hash="0xa860f20748abfdf98f4e55411b5db7630457bec1abfb5d88f1ecd5f25b4ec24b",
    ),
    ChainId("eip155:8453"): SpokePoolPin(
        address="0x09aea4b2242abc8bb4bb78d537a67a245a7bec64",
        implementation="0xf23c6c04a2b88e8651fe99bbdccbb5c9d306e6b0",
        implementation_code_hash="0xb36f3bbdffcc931890a4354aa13c9756f032cf6f968d1d1a9604cb3ece9eb480",
    ),
}


def spoke_pool(chain: ChainId) -> SpokePoolPin:
    try:
        return SPOKE_POOLS[chain]
    except KeyError:
        raise ValueError(f"Across has no pinned SpokePool on {chain}") from None


class Route(Model):
    """One direction of one asset pair. The reverse direction is another route."""

    kind: Literal["across"] = "across"
    chain: Chain
    asset: AssetType
    # CAIP-2 only: the destination's environment is its bound account's, never the route's.
    destination_chain: ChainId
    destination_asset: AssetType
    # Of the sent asset, read on its chain. The fee ceiling compares base units of both
    # assets, so the received asset must have the same decimals; the quote is held to it.
    decimals: Decimals
    spoke_pool: Address
    # Both pools are the ones resolution read, and stay the deployment's own: its
    # transfers in flight live in them, and its approval names the source pool, so every
    # reader, the revocation and closure use these addresses whatever the pin in code
    # says today. Only a new deposit compares them with the pin (`require_pinned`). The
    # destination pool is compared as retained, with no destination read, so the source
    # keeps working while the destination is unavailable.
    destination_pool: SpokePoolPin
    # The author's ceiling on the whole bridge fee, in basis points of the amount sent.
    max_fee_bps: FeeBps
    # The one source of the output amount: the venue's own quote. Nothing substitutes it.
    quote: Literal["across_api"]

    @model_validator(mode="after")
    def scope(self):
        erc20_address(self.asset, self.chain.chain_id)
        erc20_address(self.destination_asset, self.destination_chain)
        if self.chain.chain_id == self.destination_chain:
            raise ValueError("an Across route requires two different chains")
        # A chain must still be one Across is pinned on; which pool it names is not held.
        spoke_pool(self.chain.chain_id)
        spoke_pool(self.destination_chain)
        if self.asset == evm_network(self.chain.chain_id).wrapped_native:
            # A wrapped native deposit refunds as the wrapped token but can arrive as the
            # native asset, which this scope does not record.
            raise ValueError("Across routes of the wrapped native asset are not supported")
        return self


class Target(BridgeTarget):
    route: Route

    @property
    def asset(self) -> AssetType:
        return self.route.asset

    @property
    def destination_asset(self) -> AssetType:
        return self.route.destination_asset

    @model_validator(mode="after")
    def scope(self):
        if (self.fund.chain, self.destination_fund.chain.chain_id) != (
            self.route.chain,
            self.route.destination_chain,
        ):
            raise ValueError("Across target funds differ from the route's chains")
        return self


def refuse_native_output(route: Route, recipient_kind: str) -> None:
    """An EOA is paid the native asset for a wrapped native output; only a contract keeps it."""
    wrapped = evm_network(route.destination_chain).wrapped_native
    if route.destination_asset == wrapped and recipient_kind == "eoa":
        raise ValueError(
            "Across pays an EOA the native asset for a wrapped native output; "
            "this route needs a contract account as recipient"
        )
