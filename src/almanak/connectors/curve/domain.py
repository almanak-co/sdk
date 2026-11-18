"""Configured Curve coin order, LP identity and pinned protocol quantities."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.connectors.curve.stable_math import StableRevision, StableState
from almanak.connectors.identifiers import ProtocolVariant
from almanak.gateway.evm import erc20_address
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Chain, Model, UInt

Uint256 = Annotated[UInt, Field(lt=2**256)]
Coins = Annotated[tuple[AssetType, ...], Field(min_length=2, max_length=8)]
TriCryptoRevision = Literal["tricrypto-ng-v2.0.0", "tricrypto-ng-2024-01-04"]


def protocol_variant(family, revision):
    if family == "tricrypto_ng" and revision == "tricrypto-ng-2024-01-04":
        return ProtocolVariant("tricrypto_ng_2024")
    return ProtocolVariant(family)


class CurvePool(Model):
    kind: Literal["curve"] = "curve"
    family: Literal["stableswap_ng", "twocrypto_ng", "tricrypto_ng"]
    chain: Chain
    factory: Address
    pool: Address
    coins: Coins
    implementation_revision: TriCryptoRevision | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def identity(self):
        self.validate_revision()
        addresses = tuple(erc20_address(coin, self.chain.chain_id) for coin in self.coins)
        if self.family == "twocrypto_ng" and len(addresses) != 2:
            raise ValueError("TwoCrypto NG requires exactly two configured coins")
        if self.family == "tricrypto_ng" and len(addresses) != 3:
            raise ValueError("TriCrypto NG requires exactly three configured coins")
        if len(set(addresses)) != len(addresses):
            raise ValueError("Curve pool coins must be distinct in their configured order")
        if self.factory == self.pool or ZERO_ADDRESS in (self.factory, self.pool, *addresses):
            raise ValueError("Curve requires distinct nonzero factory and pool contracts")
        if self.pool in addresses:
            raise ValueError("a Curve pool cannot use its own LP asset as a coin")
        return self

    def validate_revision(self):
        if self.implementation_revision is not None and self.family != "tricrypto_ng":
            raise ValueError("the configured implementation revision requires TriCrypto NG")

    @property
    def variant(self) -> ProtocolVariant:
        return protocol_variant(self.family, self.implementation_revision)

    @property
    def shares_asset(self) -> AssetType:
        return AssetType(f"{self.chain.chain_id}/erc20:{self.pool}")


class Amplification(Model):
    initial: Annotated[Uint256, Field(gt=0, le=10**8)]
    future: Annotated[Uint256, Field(gt=0, le=10**8)]
    initial_time: UInt
    future_time: UInt

    @model_validator(mode="after")
    def times(self):
        if self.future_time < self.initial_time:
            raise ValueError("Curve amplification ramp has reversed timestamps")
        return self

    def at(self, timestamp):
        if timestamp >= self.future_time:
            return self.future
        if timestamp < self.initial_time:
            raise ValueError("Curve amplification cannot use a future ramp configuration")
        elapsed = timestamp - self.initial_time
        duration = self.future_time - self.initial_time
        delta = abs(self.future - self.initial) * elapsed // duration
        return self.initial + delta if self.future >= self.initial else self.initial - delta


class PoolObservation(Model):
    source: CurvePool
    block: Block
    currencies: tuple[Asset, ...]
    shares: Asset
    balances: tuple[Uint256, ...]
    admin_balances: tuple[Uint256, ...]
    rates: tuple[Uint256, ...]
    asset_types: tuple[Literal[0], ...]
    total_supply: Uint256
    amplification: Amplification
    fee: Annotated[Uint256, Field(le=5 * 10**9)]
    offpeg_multiplier: Uint256
    implementation: Address
    views: Address
    revision: StableRevision

    @model_validator(mode="after")
    def scope(self):
        if self.source.family != "stableswap_ng":
            raise ValueError("StableSwap observations cannot describe a Crypto pool")
        count = len(self.source.coins)
        vectors = (
            self.currencies,
            self.balances,
            self.admin_balances,
            self.rates,
            self.asset_types,
        )
        if any(len(vector) != count for vector in vectors):
            raise ValueError("Curve observation vectors must retain every configured coin")
        assets = (*self.currencies, self.shares)
        if self.block.chain != self.source.chain or any(
            asset.chain != self.source.chain for asset in assets
        ):
            raise ValueError("Curve observation belongs to another chain or environment")
        if tuple(asset.identifier for asset in assets) != (
            *self.source.coins,
            self.source.shares_asset,
        ):
            raise ValueError("Curve observation asset identities differ from its configured pool")
        if self.shares.decimals != 18 or any(asset.decimals > 18 for asset in self.currencies):
            raise ValueError(
                "this Curve revision requires 18-decimal LP shares and coin decimals at most 18"
            )
        if ZERO_ADDRESS in (self.implementation, self.views):
            raise ValueError(
                "Curve observations require verified implementation and view contracts"
            )
        self.math_state()
        return self

    def math_state(self, *, timestamp=None):
        time = self.block.timestamp if timestamp is None else timestamp
        return StableState(
            revision=self.revision,
            balances=self.balances,
            rates=self.rates,
            amplification=self.amplification.at(time),
            fee=self.fee,
            offpeg_multiplier=self.offpeg_multiplier,
            total_supply=self.total_supply,
        )
