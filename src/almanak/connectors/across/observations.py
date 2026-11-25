"""What a deposit is planned against: the fund's balance and allowance and the SpokePool."""

from datetime import UTC, datetime

from eth_utils import keccak
from pydantic import Field, model_validator

from almanak.connectors.across.domain import Target, Uint32, Uint256, spoke_pool
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.lending_intents import OwnedCash
from almanak.models import Account, Address, AssetAmount, Block, Hash32, Model, address
from almanak.transfers import Transfer

# EIP-1967: keccak("eip1967.proxy.implementation") - 1.
IMPLEMENTATION_SLOT = "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"


class PoolState(Model):
    address: Address
    implementation: Address
    implementation_code_hash: Hash32
    # Immutables of the implementation, read at the snapshot's block and never assumed.
    quote_time_buffer: Uint32
    fill_deadline_buffer: Uint32
    deposits_paused: bool


class Snapshot(Model):
    target: Target
    block: Block
    wallet_balance: Uint256
    allowance: Uint256
    pool: PoolState
    native_balance: AssetAmount
    payer: Account
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def scope(self):
        chain = self.block.chain
        if self.target.fund.chain != chain or self.pool.address != self.target.route.spoke_pool:
            raise ValueError("Across snapshot differs from its target's chain or SpokePool")
        if self.payer.chain != chain or self.native_balance.asset != native_asset(chain.chain_id):
            raise ValueError("Across snapshot gas evidence differs from its payer chain")
        if self.target.route.native:
            self.native_scope()
        return self

    def native_scope(self):
        """A native route's sent asset is the payer's own native balance, read once.

        The wallet that sends is the one that pays gas, and the route gives the SpokePool
        no approval, so no allowance is read or stated.
        """
        if self.payer != self.target.fund:
            raise ValueError("a native Across route is sent by the wallet that pays its gas")
        if self.allowance or self.wallet_balance != self.native_balance.amount:
            raise ValueError("a native Across snapshot states the native balance and no allowance")


class Ownership(Model):
    """A bridge allocation holds nothing in the venue: value in flight is a transfer."""

    target: Target
    # The source fund's owned cash of the sent asset, and the allocation's transfers not
    # yet arrived or refunded. Only composed decision input carries them; retained
    # records leave them absent. Value in flight is in neither wallet's cash, and a
    # transfer states no received amount before its arrival is measured.
    inventory: OwnedCash = None
    in_flight: tuple[Transfer, ...] = Field(default=(), exclude_if=lambda value: not value)

    @property
    def empty(self):
        return True


def owned(snapshot: Snapshot) -> Ownership:
    return Ownership(target=snapshot.target)


def require_pinned(pool, chain) -> None:
    """Refuse new deposits through an implementation other than the reviewed one.

    `pool` is a pool as read, or the pin a route retains of one. This is held when a
    market is first resolved and when a deposit is planned, signed or sent. It is never
    held when retained facts are loaded or replayed: a reviewed pin change must leave a
    running deployment able to restart, reconcile its transfers, revoke and close.
    """
    pin = spoke_pool(chain)
    if pool.address != pin.address:
        raise DataError(
            "deployment_changed",
            f"the Across SpokePool this deployment uses on {chain} ({pool.address}) is not "
            f"the reviewed one ({pin.address}); new deposits wait for its review",
        )
    if (pool.implementation, pool.implementation_code_hash) != (
        pin.implementation,
        pin.implementation_code_hash,
    ):
        raise DataError(
            "deployment_changed",
            f"the Across SpokePool on {chain} runs another implementation than the reviewed "
            f"one ({pool.implementation}); new deposits wait for its review",
        )


def pinned(block: Block) -> dict:
    return {"blockHash": block.hash, "requireCanonical": True}


def read_pool(reader, pool_address: str, block: Block) -> PoolState:
    slot = reader._rpc("eth_getStorageAt", [pool_address, IMPLEMENTATION_SLOT, pinned(block)])
    implementation = address("0x" + slot[-40:])
    code = reader._rpc("eth_getCode", [implementation, pinned(block)])
    values = {
        name: reader.call(pool_address, f"{name}()", block, outputs=(kind,))[0]
        for name, kind in (
            ("depositQuoteTimeBuffer", "uint32"),
            ("fillDeadlineBuffer", "uint32"),
            ("pausedDeposits", "bool"),
        )
    }
    return PoolState(
        address=pool_address,
        implementation=implementation,
        implementation_code_hash="0x" + keccak(bytes.fromhex(code[2:])).hex(),
        quote_time_buffer=values["depositQuoteTimeBuffer"],
        fill_deadline_buffer=values["fillDeadlineBuffer"],
        deposits_paused=values["pausedDeposits"],
    )


def token_read(reader, target: Target, block: Block, signature: str, *accounts: str) -> int:
    return reader.call(
        target.asset.reference,
        signature,
        block,
        inputs=("address",) * len(accounts),
        args=accounts,
    )[0]


def snapshot(backend, name, block) -> Snapshot:
    target = backend.config.target(name)
    profile = backend.config.accounts.binding(target.account).profile
    reader = backend.reader
    fund, pool = target.fund.address, target.route.spoke_pool
    native = int(reader._rpc("eth_getBalance", [profile.submitter.address, pinned(block)]), 16)
    if target.route.native:
        # The sent asset is the native balance itself. An allowance of the wrapped token
        # plays no part in a native deposit and is not read.
        held, allowance = native, 0
    else:
        held = token_read(reader, target, block, "balanceOf(address)", fund)
        allowance = token_read(reader, target, block, "allowance(address,address)", fund, pool)
    return Snapshot(
        target=target,
        block=block,
        wallet_balance=held,
        allowance=allowance,
        pool=read_pool(reader, pool, block),
        native_balance=AssetAmount(asset=native_asset(block.chain.chain_id), amount=native),
        payer=profile.submitter,
    )
