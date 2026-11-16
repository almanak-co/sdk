"""Fund balances, separate payer costs and complete configured Pool risk evidence."""

from datetime import UTC, datetime

from pydantic import model_validator

from almanak.connectors.aave_v3.domain import PositionObservation, ScaledBalance, Target, Uint256
from almanak.connectors.aave_v3.reader import balance, optional_evidence, read_position
from almanak.connectors.aave_v3.risk import RiskInputs
from almanak.gateway.evm import native_asset
from almanak.lending_intents import OwnedCash
from almanak.models import Account, AssetAmount, Model


class Snapshot(Model):
    target: Target
    position: PositionObservation
    wallet_balance: Uint256
    allowance: Uint256
    native_balance: AssetAmount
    payer: Account
    pool_risk: RiskInputs | None
    pool_risk_error: str | None = None
    simulated: bool = False

    @property
    def allocation(self):
        return self.target.allocation

    @property
    def block(self):
        return self.position.reserve.block

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def scope(self):
        if (self.target.fund, self.target.reserve) != (
            self.position.fund,
            self.position.reserve.source,
        ):
            raise ValueError("snapshot position differs from its named target")
        if self.payer.chain != self.block.chain or self.native_balance.asset != native_asset(
            self.block.chain.chain_id
        ):
            raise ValueError("snapshot gas evidence differs from its payer chain")
        if (self.pool_risk is None) != (self.pool_risk_error is not None):
            raise ValueError("missing Pool risk must retain a visible reason")
        if self.pool_risk is not None and self.position not in self.pool_risk.positions:
            raise ValueError("Pool risk must include the same observed position")
        return self


class Ownership(Model):
    target: Target
    supply_scaled: ScaledBalance
    debt_scaled: ScaledBalance
    collateral_enabled: bool
    inventory: OwnedCash = None


def pool_risk(reader, config, target, position):
    block = position.reserve.block
    positions = []
    for allocation in config.positions:
        candidate = config.target(allocation.name)
        if (candidate.fund, candidate.reserve.pool) != (target.fund, target.reserve.pool):
            continue
        positions.append(
            position
            if candidate == target
            else read_position(reader, candidate.reserve, candidate.fund, block)
        )
    bitmap = reader.call(
        target.reserve.pool,
        "getUserConfiguration(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
        outputs=("(uint256)",),
    )[0][0]
    category = reader.call(
        target.reserve.pool,
        "getUserEMode(address)",
        block,
        inputs=("address",),
        args=(target.fund.address,),
    )[0]
    return RiskInputs(
        positions=tuple(positions), user_configuration=bitmap, emode_category=category
    )


def snapshot(backend, name, block):
    from almanak.configuration.metadata import verify

    target = backend.config.target(name)
    profile = backend.config.accounts.binding(target.account).profile
    reader = backend.reader
    position = read_position(reader, target.reserve, target.fund, block)
    verify(backend.config, position.reserve.asset)
    allowance = reader.call(
        target.asset.reference,
        "allowance(address,address)",
        block,
        inputs=("address", "address"),
        args=(target.fund.address, target.reserve.pool),
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
        position=position,
        wallet_balance=balance(reader, target.asset, target.fund.address, block),
        allowance=allowance,
        native_balance=AssetAmount(asset=native_asset(block.chain.chain_id), amount=native),
        payer=profile.submitter,
        **optional_evidence(
            "pool_risk", lambda: pool_risk(reader, backend.config, target, position)
        ),
    )
