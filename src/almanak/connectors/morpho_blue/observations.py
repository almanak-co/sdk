"""Morpho observations keep loan shares and collateral separate from strategy memory."""

from datetime import UTC, datetime

from pydantic import model_validator

from almanak.connectors.morpho_blue.domain import AccountObservation, Target, Uint128, Uint256
from almanak.connectors.morpho_blue.reader import read_account, read_market
from almanak.gateway.evm import native_asset
from almanak.lending_intents import OwnedCash
from almanak.models import Account, Asset, AssetAmount, Model


def target_asset(target: Target, account: AccountObservation) -> Asset:
    if target.fund != account.fund or target.reserve.market != account.market.source:
        raise ValueError("Morpho target differs from its observed fund or market")
    return account.market.loan_asset if target.reserve.is_loan else account.market.collateral_asset


class Snapshot(Model):
    target: Target
    account: AccountObservation
    wallet_balance: Uint256
    allowance: Uint256
    native_balance: AssetAmount
    payer: Account
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def block(self):
        return self.account.market.block

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def asset(self):
        return target_asset(self.target, self.account)

    @model_validator(mode="after")
    def scope(self):
        target_asset(self.target, self.account)
        if self.payer.chain != self.block.chain or self.native_balance.asset != native_asset(
            self.block.chain.chain_id
        ):
            raise ValueError("Morpho snapshot gas evidence differs from its payer chain")
        return self


class Ownership(Model):
    target: Target
    supply_shares: Uint256
    borrow_shares: Uint128
    collateral: Uint128
    inventory: OwnedCash = None

    @model_validator(mode="after")
    def scope(self):
        if self.target.reserve.is_loan:
            if self.collateral:
                raise ValueError("Morpho loan ownership cannot contain collateral units")
        elif self.supply_shares or self.borrow_shares:
            raise ValueError("Morpho collateral ownership cannot contain loan shares")
        return self

    @property
    def empty(self):
        return self.supply_shares == self.borrow_shares == self.collateral == 0


def owned(snapshot: Snapshot) -> Ownership:
    target, position = snapshot.target, snapshot.account.position
    if target.reserve.is_loan:
        return Ownership(
            target=target,
            supply_shares=position.supply_shares,
            borrow_shares=position.borrow_shares,
            collateral=0,
        )
    return Ownership(
        target=target, supply_shares=0, borrow_shares=0, collateral=position.collateral
    )


def snapshot(backend, name, block):
    from almanak.configuration.metadata import verify

    target = backend.config.target(name)
    profile = backend.config.accounts.binding(target.account).profile
    reader = backend.reader
    market = read_market(reader, target.reserve.market, block)
    account = read_account(reader, market, target.fund)
    verify(backend.config, target_asset(target, account))
    wallet = reader.call(
        target.asset.reference,
        "balanceOf(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )[0]
    allowance = reader.call(
        target.asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, market.source.core),
    )[0]
    native = int(
        reader._rpc(
            "eth_getBalance",
            [profile.submitter.address, {"blockHash": block.hash, "requireCanonical": True}],
        ),
        16,
    )
    return Snapshot(
        target=target,
        account=account,
        wallet_balance=wallet,
        allowance=allowance,
        native_balance=AssetAmount(asset=native_asset(block.chain.chain_id), amount=native),
        payer=profile.submitter,
    )
