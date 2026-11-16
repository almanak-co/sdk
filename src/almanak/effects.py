"""Scoped asset and connector position effects shared by live and historical execution."""

from typing import Self

from pydantic import model_validator

from almanak.caip import AssetType
from almanak.connectors.identifiers import ConnectorId, Name
from almanak.models import Chain, Model, UInt


class WalletAsset(Model):
    account: Name
    chain: Chain
    asset: AssetType

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.chain.chain_id != self.asset.chain:
            raise ValueError("wallet asset belongs to another chain")
        return self


class Balance(Model):
    key: WalletAsset
    amount: UInt


class AssetDelta(Model):
    key: WalletAsset
    amount: int


class PositionKey(Model):
    connector: ConnectorId
    chain: Chain
    account: Name
    reference: Name


class Position[Payload: Model](Model):
    key: PositionKey
    payload: Payload


class PositionChange[Payload: Model](Model):
    before: Position[Payload] | None
    after: Position[Payload] | None

    @model_validator(mode="after")
    def identity(self) -> Self:
        if self.before is None and self.after is None:
            raise ValueError("a position change requires a prior or resulting position")
        if self.before and self.after and self.before.key != self.after.key:
            raise ValueError("position changes cannot change ownership identity")
        return self


class Effects[Payload: Model](Model):
    assets: tuple[AssetDelta, ...] = ()
    positions: tuple[PositionChange[Payload], ...] = ()


class Portfolio[Payload: Model](Model):
    balances: tuple[Balance, ...]
    positions: tuple[Position[Payload], ...] = ()

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len({b.key for b in self.balances}) != len(self.balances):
            raise ValueError("duplicate wallet asset balance")
        if len({p.key for p in self.positions}) != len(self.positions):
            raise ValueError("duplicate connector position")
        return self

    def balance(self, key: WalletAsset) -> int:
        for balance in self.balances:
            if balance.key == key:
                return balance.amount
        raise ValueError("wallet asset is not explicitly initialized")

    def apply(self, effects: Effects[Payload]) -> Self:
        balances = apply_assets(self.balances, effects.assets)
        positions = {position.key: position for position in self.positions}
        seen = set()
        for change in effects.positions:
            position = change.after or change.before
            assert position is not None
            if position.key in seen or positions.get(position.key) != change.before:
                raise ValueError("position effects conflict with owned state")
            seen.add(position.key)
            if change.after is None:
                positions.pop(position.key)
            else:
                positions[position.key] = change.after
        return type(self)(balances=balances, positions=tuple(positions.values()))


def apply_assets(balances: tuple[Balance, ...], changes: tuple[AssetDelta, ...]):
    amounts = {balance.key: balance.amount for balance in balances}
    for delta in changes:
        if delta.key not in amounts:
            raise ValueError("effect addresses an unbound wallet asset")
        amounts[delta.key] += delta.amount
        if amounts[delta.key] < 0:
            raise ValueError("insufficient simulated wallet balance")
    return tuple(Balance(key=key, amount=amount) for key, amount in amounts.items())
