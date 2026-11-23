"""Value in flight between two chains: an owned claim, neither a balance nor pending work.

A transfer opens when its source-chain effect is reconciled and stays open until a
measured arrival on the destination chain or a measured refund on the source chain.
An open transfer carries no received amount: a missing outcome is not a measured zero.
"""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.connectors.identifiers import ConnectorId, Name
from almanak.effects import WalletAsset
from almanak.gateway.environments import same_environment
from almanak.models import AssetAmount, Chain, Model
from almanak.triggers import UtcInstant

Reference = Annotated[str, Field(min_length=1, max_length=512, pattern=r"^\S+$")]
# Connector, source chain and the connector's reference: unique within a checkpoint.
type Identity = tuple[ConnectorId, Chain, str]


class Arrived(Model):
    kind: Literal["arrived"] = "arrived"
    # Measured on the destination chain. It may differ from the offered amount.
    received: AssetAmount


class Refunded(Model):
    kind: Literal["refunded"] = "refunded"
    # Measured on the source chain.
    returned: AssetAmount


Resolution = Annotated[Arrived | Refunded, Field(discriminator="kind")]


class Transfer(Model):
    connector: ConnectorId
    allocation: Name
    source: WalletAsset
    sent: AssetAmount
    destination: WalletAsset
    offered: AssetAmount
    # Opaque here. The connector's adapter owns its meaning and its chain-native form.
    reference: Reference
    fill_deadline: UtcInstant
    resolution: Resolution | None = None

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.source.chain.chain_id == self.destination.chain.chain_id:
            raise ValueError("a transfer requires two different chains")
        # A fork never sends to production, nor production to a fork.
        if not same_environment(self.source.chain, self.destination.chain):
            raise ValueError("a transfer stays within one environment")
        if self.source.account == self.destination.account:
            raise ValueError("a transfer requires two different named accounts")
        if self.sent.asset != self.source.asset or self.offered.asset != self.destination.asset:
            raise ValueError("transfer amounts must identify their own wallet assets")
        if self.sent.amount == 0 or self.offered.amount == 0:
            raise ValueError("a transfer requires positive sent and offered amounts")
        validate_resolution(self)
        return self

    @property
    def identity(self) -> Identity:
        return self.connector, self.source.chain, self.reference

    @property
    def open(self) -> bool:
        return self.resolution is None


def validate_resolution(transfer: Transfer) -> None:
    resolution = transfer.resolution
    if resolution is None:
        return
    measured, wallet = (
        (resolution.received, transfer.destination)
        if isinstance(resolution, Arrived)
        else (resolution.returned, transfer.source)
    )
    if measured.asset != wallet.asset:
        raise ValueError("a transfer resolves in the asset of the wallet that received it")
    if measured.amount == 0:
        raise ValueError("a transfer resolves only by a measured positive amount")
    if isinstance(resolution, Refunded) and measured.amount > transfer.sent.amount:
        raise ValueError("a refund cannot exceed the amount sent")


def resolution_chain(transfer: Transfer) -> Chain:
    """The chain whose evidence resolved the transfer."""
    if transfer.resolution is None:
        raise ValueError("an open transfer has no resolving chain")
    if isinstance(transfer.resolution, Arrived):
        return transfer.destination.chain
    return transfer.source.chain


def validate_transfers(transfers: tuple[Transfer, ...]) -> None:
    identities = [transfer.identity for transfer in transfers]
    if len(set(identities)) != len(identities):
        raise ValueError("a transfer identity can be retained only once")


def opened(transfers: tuple[Transfer, ...], transfer: Transfer) -> tuple[Transfer, ...]:
    if not transfer.open:
        raise ValueError("a transfer opens without a resolution")
    changed = (*transfers, transfer)
    validate_transfers(changed)
    return changed


def resolved(
    transfers: tuple[Transfer, ...], identity: Identity, resolution: Arrived | Refunded
) -> tuple[Transfer, ...]:
    matches = [transfer for transfer in transfers if transfer.identity == identity]
    if len(matches) != 1:
        raise ValueError("a resolution requires exactly one retained transfer")
    if not matches[0].open:
        raise ValueError("a resolved transfer cannot be resolved again")
    # Revalidated, so a resolution in the wrong asset or of zero is refused here.
    closed = Transfer(**{**dict(matches[0]), "resolution": resolution})
    return tuple(closed if transfer.identity == identity else transfer for transfer in transfers)


def touches(transfer: Transfer, chain: Chain) -> bool:
    return chain in (transfer.source.chain, transfer.destination.chain)


def visible(transfers: tuple[Transfer, ...], chain: Chain) -> tuple[Transfer, ...]:
    return tuple(transfer for transfer in transfers if touches(transfer, chain))


def validate_retained(before: Transfer, after: Transfer, chain: Chain) -> None:
    if after == before:
        return
    if not before.open or after.model_copy(update={"resolution": None}) != before:
        raise ValueError("a scoped change cannot alter or reopen a retained transfer")
    if resolution_chain(after) != chain:
        raise ValueError("a transfer resolves only on the chain that measured its outcome")


def validate_appended(appended: tuple[Transfer, ...], chain: Chain) -> None:
    for transfer in appended:
        if transfer.source.chain != chain:
            raise ValueError("a transfer opens only on its source chain")
        if not transfer.open:
            raise ValueError("a transfer opens without a resolution")


def merged(
    original: tuple[Transfer, ...], changed: tuple[Transfer, ...], chain: Chain
) -> tuple[Transfer, ...]:
    """Apply one chain's scoped view back; transfers it cannot see are retained as they are."""
    before = visible(original, chain)
    # A scoped view built with `model_copy` skips validation; nothing unvalidated is retained.
    changed = tuple(Transfer.model_validate(item.model_dump()) for item in changed)
    if len(changed) < len(before):
        raise ValueError("a scoped change cannot drop a retained transfer")
    for prior, current in zip(before, changed, strict=False):
        validate_retained(prior, current, chain)
    validate_appended(changed[len(before) :], chain)
    replacements = iter(changed[: len(before)])
    result = (
        *(next(replacements) if touches(t, chain) else t for t in original),
        *changed[len(before) :],
    )
    validate_transfers(result)
    return result
