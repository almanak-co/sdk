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


# Read from each chain by `scripts/across_pins.py` on 25 November 2026: the proxy's
# EIP-1967 implementation slot and the keccak of that implementation's runtime code.
# Each chain runs its own SpokePool variant. Every one here holds the selectors, events
# and views this connector uses, and reports its own chain number. A pin table, not an
# allowlist of routes: which assets a route moves is the author's configuration.
#
# HyperEVM has a SpokePool that passes the same checks and is left out. A deposit is
# prepared at the head, and an arrival page's status read is at the head too. The
# chain's one provider runs a read pinned near its head at a later block, which the
# reader refuses as `provider_context`: on 25 November 2026 the SpokePool read was
# refused at the head and 64 blocks behind it, and served from 140 blocks behind it,
# where the venue's quote is stamped after the pinned block. A pin here would also make
# HyperEVM a destination of production routes whose arrival could not be recorded.
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
    ChainId("eip155:1"): SpokePoolPin(
        address="0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5",
        implementation="0x456ac26e5ec083ee9889eba0d1a0a582502b8e84",
        implementation_code_hash="0x94cc890a705ae8f4b973b6531b201fcd53c6bcbefba7caa12d1812f6fcede5bf",
    ),
    ChainId("eip155:10"): SpokePoolPin(
        address="0x6f26bf09b1c792e3228e5467807a900a503c0281",
        implementation="0x60674c53a4cc4ed350659c95fb8020b5ac61ca1d",
        implementation_code_hash="0xcf5c48ec3d2a4cbd9e097f08e8888e4c07d8698e626414e284f52b66515e2b21",
    ),
    ChainId("eip155:56"): SpokePoolPin(
        address="0x4e8e101924ede233c13e2d8622dc8aed2872d505",
        implementation="0xc7b7029373f504949553106c9eb2dafdd48ef086",
        implementation_code_hash="0x536bd9288be6c656bfc28f182d249a6483cbaa3c645190eaa71a17ea6aca9dfa",
    ),
    ChainId("eip155:137"): SpokePoolPin(
        address="0x9295ee1d8c5b022be115a2ad3c30c72e34e7f096",
        implementation="0x304bed95daecd3e680073eab385193da711d59d4",
        implementation_code_hash="0x9fccb54e8a69629d8cb50bb3e00042456418de9c600dc73763e81b80e7262518",
    ),
    ChainId("eip155:4663"): SpokePoolPin(
        address="0xd29c85f15df544ba632c9e25829fd29d767d7978",
        implementation="0x1771c470d41b8c39338450c380bf2c080a2cedd8",
        implementation_code_hash="0x14f3a4a73c4a0aa5499d7ae7c3a11c0195bc769d46ff8e62b0c614faee8a95ed",
    ),
    ChainId("eip155:43114"): SpokePoolPin(
        address="0xfe9d541c92e4e90437c7152a00244886de37a658",
        implementation="0x7a6122b88da543b5b62ef3453070d1929b035146",
        implementation_code_hash="0x4af1b35fe2ff46e1aef984d9dae2d25466b6b39751525a66b77553c89001842b",
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
