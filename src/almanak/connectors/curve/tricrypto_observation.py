"""Revision-bound TriCrypto state retains stored prices and profit accounting."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.connectors.curve.crypto_prices import oracle
from almanak.connectors.curve.domain import CurvePool, TriCryptoRevision, Uint256
from almanak.connectors.curve.tricrypto_liquidity import AdminState, LiquidityState
from almanak.connectors.curve.tricrypto_math import invariant, parameters
from almanak.connectors.curve.tricrypto_token_accounting import TokenAccounting
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, UInt

Positive = Annotated[Uint256, Field(gt=0)]


class TriCryptoRamp(Model):
    initial_amplification: Positive
    initial_gamma: Positive
    future_amplification: Positive
    future_gamma: Positive
    initial_time: UInt
    future_time: UInt

    @model_validator(mode="after")
    def valid(self):
        parameters(self.initial_amplification, self.initial_gamma)
        parameters(self.future_amplification, self.future_gamma)
        if self.future_time < self.initial_time:
            raise ValueError("TriCrypto ramp has reversed timestamps")
        return self

    def at(self, timestamp):
        if timestamp < self.initial_time:
            raise ValueError("TriCrypto cannot apply a future ramp configuration")
        if timestamp >= self.future_time:
            return self.future_amplification, self.future_gamma
        elapsed = timestamp - self.initial_time
        duration = self.future_time - self.initial_time
        # The contract rounds the weighted sum, including a decreasing ramp.
        return tuple(
            (initial * (duration - elapsed) + future * elapsed) // duration
            for initial, future in (
                (self.initial_amplification, self.future_amplification),
                (self.initial_gamma, self.future_gamma),
            )
        )


class TriCryptoAccounting(Model):
    virtual_price: Positive
    profit: Positive
    claimed_profit: Positive
    receiver: Address

    def math_state(self):
        return AdminState(
            virtual_price=self.virtual_price,
            profit=self.profit,
            claimed_profit=self.claimed_profit,
            receiver_enabled=self.receiver != ZERO_ADDRESS,
        )


class TriCryptoObservation(Model):
    source: CurvePool
    block: Block
    currencies: tuple[Asset, Asset, Asset]
    shares: Asset
    balances: tuple[Positive, Positive, Positive]
    actual_balances: tuple[Uint256, Uint256, Uint256]
    precisions: tuple[Positive, Positive, Positive]
    total_supply: Positive
    ramp: TriCryptoRamp
    invariant: Positive
    packed_fees: Uint256
    packed_rebalancing: Annotated[Uint256, Field(lt=2**192)]
    price_scales: tuple[Positive, Positive]
    cached_price_oracle: tuple[Positive, Positive]
    price_oracle: tuple[Positive, Positive]
    last_prices: tuple[Positive, Positive]
    last_timestamp: UInt
    accounting: TriCryptoAccounting
    implementation: Address
    math: Address
    views: Address
    revision: TriCryptoRevision = "tricrypto-ng-v2.0.0"
    token_accounting: TokenAccounting | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def scope(self):
        if self.source.family != "tricrypto_ng":
            raise ValueError("TriCrypto observation requires its own configured family")
        assets = (*self.currencies, self.shares)
        if self.block.chain != self.source.chain or any(
            asset.chain != self.source.chain for asset in assets
        ):
            raise ValueError("TriCrypto observation belongs to another execution scope")
        if tuple(asset.identifier for asset in assets) != (
            *self.source.coins,
            self.source.shares_asset,
        ):
            raise ValueError("TriCrypto observed assets differ from configured coin order")
        self.validate_units()
        self.validate_revision()
        self.validate_times()
        self.validate_prices()
        self.math_state()
        return self

    def validate_revision(self):
        expected = self.source.implementation_revision or "tricrypto-ng-v2.0.0"
        if self.revision != expected or (self.token_accounting is not None) != (
            self.revision == "tricrypto-ng-2024-01-04"
        ):
            raise ValueError(
                "TriCrypto accounting revision differs from its configured implementation"
            )
        if (
            self.token_accounting is not None
            and max(self.token_accounting.xcp_timestamp, self.token_accounting.last_claim_time)
            > self.block.timestamp
        ):
            raise ValueError("TriCrypto token accounting cannot come from the future")

    def validate_units(self):
        if self.shares.decimals != 18 or any(coin.decimals > 18 for coin in self.currencies):
            raise ValueError("TriCrypto requires 18-decimal shares and supported coin decimals")
        if self.precisions != tuple(10 ** (18 - coin.decimals) for coin in self.currencies):
            raise ValueError("TriCrypto immutable precisions differ from measured coin units")
        if ZERO_ADDRESS in (self.implementation, self.math, self.views):
            raise ValueError("TriCrypto requires verified executable identities")
        if self.packed_rebalancing & (2**64 - 1) == 0:
            raise ValueError("TriCrypto moving-average duration must be positive")

    def validate_times(self):
        if self.last_timestamp > self.block.timestamp:
            raise ValueError("TriCrypto retained state cannot come from the future")
        self.ramp.at(self.block.timestamp)

    def validate_prices(self):
        expected = tuple(
            oracle(
                cached=cached,
                last_price=last,
                scale=scale,
                last_timestamp=self.last_timestamp,
                timestamp=self.block.timestamp,
                duration=self.packed_rebalancing & (2**64 - 1),
            )
            for cached, last, scale in zip(
                self.cached_price_oracle, self.last_prices, self.price_scales, strict=True
            )
        )
        if expected != self.price_oracle:
            raise ValueError("TriCrypto oracle view differs from retained EMA inputs")

    def math_state(self, *, timestamp=None):
        time = self.block.timestamp if timestamp is None else timestamp
        amplification, gamma = self.ramp.at(time)
        state = LiquidityState(
            balances=self.balances,
            precisions=self.precisions,
            price_scales=self.price_scales,
            amplification=amplification,
            gamma=gamma,
            invariant=self.invariant,
            packed_fees=self.packed_fees,
            total_supply=self.total_supply,
        )
        return state

    def ramp_invariant(self, *, timestamp):
        state = self.math_state(timestamp=timestamp)
        return invariant(state.amplification, state.gamma, state.normalized(state.balances))
