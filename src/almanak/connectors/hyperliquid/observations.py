"""The Safe's HyperEVM balances and the operator's gas, observed at one block.

A snapshot holds only facts the gateway can read again at the same block: the Safe's
USDC, its allowance of CoreDepositWallet and the operator's HYPE. The gateway verifies a
decision's observations by re-snapshotting at their block and comparing for equality,
and an `/info` answer is HyperCore's state when it was served, never repeatable, so no
HyperCore read is part of a snapshot (blueprint 30, section 13.1). What the strategy sees
of its HyperCore account is the checkpoint's reconciled documents, projected into
`Ownership` (`positions.projected`), so a snapshot carries the same `effective_at`,
`owned.activated`, `owned.leverage`, `owned.holding` and `withdrawable` a backtest's
does; the venue is read only inside the gateway, by planning, admission and
reconciliation.
"""

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import model_validator

from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.models import Target
from almanak.connectors.hyperliquid.positions import Ownership, initial_owned
from almanak.models import Account, AssetAmount, Block, Model, UInt


class Snapshot(Model):
    target: Target
    block: Block
    owned: Ownership
    # The Safe's HyperEVM balances of the owned inventory's assets, in order.
    wallets: tuple[AssetAmount, ...]
    # The Safe's USDC allowance of CoreDepositWallet; a deposit revokes its residual.
    allowance: UInt
    native_balance: AssetAmount
    payer: Account

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def needs_cleanup(self):
        return self.allowance != 0

    @property
    def position(self) -> Decimal:
        """The strategy's own position in the target's market, from the reconciled book."""
        return self.owned.position

    @property
    def holds_usdc(self) -> bool:
        """Whether the reconciled book shows USDC on the Safe's HyperCore account."""
        return self.owned.holds_usdc

    @property
    def withdrawable(self) -> int:
        """USDC base units a withdrawal of everything could return, as a backtest's snapshot
        names it; projected from the documents with the ownership it retains."""
        return self.owned.withdrawable

    @model_validator(mode="after")
    def scope(self):
        if self.owned.target != self.target or self.block.chain != self.target.fund.chain:
            raise ValueError("Hyperliquid snapshot must retain its exact target and scope")
        if self.payer.chain != self.block.chain:
            raise ValueError("Hyperliquid submitter observation must retain the execution chain")
        if tuple(a.asset for a in self.wallets) != tuple(a.asset for a in self.owned.inventory):
            raise ValueError("Hyperliquid wallet observations must cover every owned asset")
        return self


def snapshot(backend, name, block, reference=None) -> Snapshot:
    """The Safe's USDC and allowance and the operator's HYPE at `block`, which the caller
    has pinned; `reference` is the retained ownership or an earlier snapshot of it."""
    config, reader = backend.config, backend.reader
    target = config.target(name)
    owned = ownership(reference) if reference is not None else initial_owned(config, name)
    if owned.target != target:
        raise ValueError("Hyperliquid snapshot reference differs from its configured target")
    payer = config.accounts.binding(target.account).profile.submitter
    native = config.allocation(name).native_asset
    return Snapshot(
        target=target,
        block=block,
        owned=owned,
        wallets=tuple(balance(reader, target.fund, a.asset, block) for a in owned.inventory),
        allowance=allowance(reader, target, block),
        native_balance=balance(reader, payer, native, block),
        payer=payer,
    )


def ownership(reference) -> Ownership:
    return reference if isinstance(reference, Ownership) else reference.owned


def allowance(reader, target, block) -> int:
    (amount,) = reader.call(
        target.margin_asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, CORE_DEPOSIT_WALLET),
        outputs=("uint256",),
    )
    return amount


def balance(reader, account, asset, block) -> AssetAmount:
    if asset.namespace == "slip44":
        selector = {"blockHash": block.hash, "requireCanonical": True}
        amount = int(reader._rpc("eth_getBalance", [account.address, selector]), 16)
    else:
        (amount,) = reader.call(
            asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(account.address,),
            outputs=("uint256",),
        )
    return AssetAmount(asset=asset, amount=amount)
