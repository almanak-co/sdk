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


# SLIP-44 coin type 60 is ETH. A native route needs it on both chains: the SpokePool
# wraps the deposit's value into its wrapped native token, and pays a plain recipient
# the destination pool's wrapped native token unwrapped. Where another coin is native
# (BNB, POL, AVAX) the pool's wrapped native token is that coin's, and ETH is a token.
ETH_COIN = "60"
NATIVE_DECIMALS = 18
NATIVE_DIRECT_ONLY = (
    "a native Across route is sent by the direct command `almanak ax bridge` only, from a "
    "plain wallet to a stated recipient; a strategy route names ERC-20 assets"
)
NATIVE_PAIR = (
    "an Across route names the native asset on both chains or on neither: a native "
    "deposit delivered as a token, or a token delivered as the native asset, is not "
    "supported"
)


def names_native(*assets: AssetType) -> bool:
    return any(asset.namespace == "slip44" for asset in assets)


def require_native_eth(chain: ChainId, asset: AssetType) -> None:
    """Refuse an asset that is not ETH as the native coin of its own chain."""
    native = evm_network(chain).native_asset
    if asset.namespace != "slip44":
        raise ValueError(NATIVE_PAIR)
    if asset != native:
        raise ValueError(f"{asset} is not the native asset of {chain}, which is {native}")
    if asset.reference != ETH_COIN:
        raise ValueError(
            f"a native Across route needs ETH as the native coin of both chains, and the "
            f"native asset of {chain} is {native}; ETH is an ERC-20 token there"
        )


def require_native_pair(
    chain: ChainId, asset: AssetType, destination_chain: ChainId, destination_asset: AssetType
) -> None:
    """A native route sends native ETH and delivers native ETH, or it is refused."""
    require_native_eth(chain, asset)
    require_native_eth(destination_chain, destination_asset)


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
    # A native route only: the wrapped native token each SpokePool named in its own
    # `wrappedNativeToken()` at resolution, on the source and on the destination chain.
    # The route's identity stays the native asset; these addresses are what the quote
    # request and the deposit's calldata carry in its place.
    wrapped: tuple[Address, Address] | None = Field(default=None, exclude_if=lambda v: v is None)

    @property
    def native(self) -> bool:
        """Whether the route sends the chain's native coin as the deposit's own value."""
        return self.wrapped is not None

    @property
    def input_token(self) -> str:
        """The token address the SpokePool takes for the sent asset."""
        return self.asset.reference if self.wrapped is None else self.wrapped[0]

    @property
    def output_token(self) -> str:
        """The token address the SpokePool takes for the received asset."""
        return self.destination_asset.reference if self.wrapped is None else self.wrapped[1]

    @model_validator(mode="after")
    def scope(self):
        if self.chain.chain_id == self.destination_chain:
            raise ValueError("an Across route requires two different chains")
        # A chain must still be one Across is pinned on; which pool it names is not held.
        spoke_pool(self.chain.chain_id)
        spoke_pool(self.destination_chain)
        if names_native(self.asset, self.destination_asset):
            self.native_scope()
            return self
        if self.wrapped is not None:
            raise ValueError("only a native Across route retains wrapped native tokens")
        erc20_address(self.asset, self.chain.chain_id)
        erc20_address(self.destination_asset, self.destination_chain)
        if self.asset == evm_network(self.chain.chain_id).wrapped_native:
            # A wrapped native deposit refunds as the wrapped token but can arrive as the
            # native asset, which this scope does not record.
            raise ValueError("Across routes of the wrapped native asset are not supported")
        return self

    def native_scope(self):
        require_native_pair(
            self.chain.chain_id, self.asset, self.destination_chain, self.destination_asset
        )
        if self.decimals != NATIVE_DECIMALS:
            raise ValueError("a native Across route sends an asset of 18 decimals")
        if self.wrapped != (
            evm_network(self.chain.chain_id).wrapped_native.reference,
            evm_network(self.destination_chain).wrapped_native.reference,
        ):
            raise ValueError(
                "a native Across route retains the wrapped native token of each chain, as "
                "its SpokePool named it at resolution"
            )


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
    """An EOA is paid the native asset for a wrapped native output; only a contract keeps it.

    A Safe is a contract: the SpokePool pays it the wrapped token, which the arrival
    reader measures as it does any other token. A bound Safe account is verified on its
    own chain, with its Roles wiring, before a deployment is created.

    A native route is the other case and passes here: it names the native asset as
    received, its recipient is a plain account read on the destination chain before
    anything is signed, and only an operator deployment may hold one.
    """
    wrapped = evm_network(route.destination_chain).wrapped_native
    if route.destination_asset == wrapped and recipient_kind == "eoa":
        raise ValueError(
            "Across pays an EOA the native asset for a wrapped native output; "
            "this route needs a contract account as recipient"
        )
