"""Revision-bound TwoCrypto state retains stored prices and profit accounting."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.curve.domain import CurvePool, Uint256
from almanak.connectors.curve.twocrypto_liquidity import AdminState, LiquidityState
from almanak.connectors.curve.twocrypto_math import invariant, parameters
from almanak.connectors.curve.twocrypto_prices import oracle
from almanak.models import ZERO_ADDRESS, Address, Asset, Block, Model, UInt

Positive = Annotated[Uint256, Field(gt=0)]


class CryptoRamp(Model):
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
            raise ValueError("TwoCrypto ramp has reversed timestamps")
        return self

    def at(self, timestamp):
        if timestamp < self.initial_time:
            raise ValueError("TwoCrypto cannot apply a future ramp configuration")
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


class CryptoAccounting(Model):
    virtual_price: Positive
    profit: Positive
    claimed_profit: Positive
    virtual_shares: Uint256
    last_claim_time: UInt
    receiver: Address

    def math_state(self):
        return AdminState(
            virtual_price=self.virtual_price,
            profit=self.profit,
            claimed_profit=self.claimed_profit,
            virtual_shares=self.virtual_shares,
            last_claim_time=self.last_claim_time,
            receiver_enabled=self.receiver != ZERO_ADDRESS,
        )


class CryptoObservation(Model):
    source: CurvePool
    block: Block
    currencies: tuple[Asset, Asset]
    shares: Asset
    balances: tuple[Positive, Positive]
    precisions: tuple[Positive, Positive]
    total_supply: Positive
    ramp: CryptoRamp
    invariant: Positive
    packed_fees: Uint256
    packed_rebalancing: Annotated[Uint256, Field(lt=2**192)]
    price_scale: Positive
    cached_price_oracle: Positive
    price_oracle: Positive
    last_prices: Positive
    last_timestamp: UInt
    accounting: CryptoAccounting
    implementation: Address
    math: Address
    views: Address
    revision: Literal["twocrypto-ng-v2.1.0"] = "twocrypto-ng-v2.1.0"

    @model_validator(mode="after")
    def scope(self):
        if self.source.family != "twocrypto_ng":
            raise ValueError("TwoCrypto observation requires its own configured family")
        assets = (*self.currencies, self.shares)
        if self.block.chain != self.source.chain or any(
            asset.chain != self.source.chain for asset in assets
        ):
            raise ValueError("TwoCrypto observation belongs to another execution scope")
        if tuple(asset.identifier for asset in assets) != (
            *self.source.coins,
            self.source.shares_asset,
        ):
            raise ValueError("TwoCrypto observed assets differ from configured coin order")
        self.validate_units()
        self.validate_times()
        self.validate_prices()
        self.math_state()
        return self

    def validate_units(self):
        if self.shares.decimals != 18 or any(coin.decimals > 18 for coin in self.currencies):
            raise ValueError("TwoCrypto requires 18-decimal shares and supported coin decimals")
        if self.precisions != tuple(10 ** (18 - coin.decimals) for coin in self.currencies):
            raise ValueError("TwoCrypto immutable precisions differ from measured coin units")
        if ZERO_ADDRESS in (self.implementation, self.math, self.views):
            raise ValueError("TwoCrypto requires verified executable identities")
        if self.packed_rebalancing & (2**64 - 1) == 0:
            raise ValueError("TwoCrypto moving-average duration must be positive")

    def validate_times(self):
        if max(self.last_timestamp, self.accounting.last_claim_time) > self.block.timestamp:
            raise ValueError("TwoCrypto retained state cannot come from the future")
        self.ramp.at(self.block.timestamp)

    def validate_prices(self):
        expected = oracle(
            cached=self.cached_price_oracle,
            last_price=self.last_prices,
            scale=self.price_scale,
            last_timestamp=self.last_timestamp,
            timestamp=self.block.timestamp,
            duration=self.packed_rebalancing & (2**64 - 1),
        )
        if expected != self.price_oracle:
            raise ValueError("TwoCrypto oracle view differs from retained EMA inputs")

    def math_state(self, *, timestamp=None):
        time = self.block.timestamp if timestamp is None else timestamp
        amplification, gamma = self.ramp.at(time)
        state = LiquidityState(
            balances=self.balances,
            precisions=self.precisions,
            price_scale=self.price_scale,
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
