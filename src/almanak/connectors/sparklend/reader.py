"""Pinned SparkLend revision-4 observations with explicit stable debt and physical liquidity."""

from almanak.caip import AssetType
from almanak.connectors.pool_lending_reader import (
    LEGACY_RESERVE,
    account_risk,
    balance,
    optional_evidence,
    oracle_values,
)
from almanak.connectors.sparklend.domain import (
    OracleQuote,
    PositionObservation,
    Reserve,
    ReserveConfiguration,
    ReserveObservation,
)
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Account, Block

TOKEN_PROXY_HASH = "0x077e8c88e5535988fd2aca6383174ed3c54c66c84388036e5b880ecfa9c85bb5"
REVISION_HASHES = {
    "pool": (
        "0xb31875340e8b4553fcf22d1cffde6accb743ccd40c31d85ad83ff32eeec712c2",
        "0x4aeabea6fae28c3449056986776ae14ec0e3ed81da608d2543cc7ef5b1d16249",
    ),
    "a_token": (
        TOKEN_PROXY_HASH,
        "0x141c8fc1d7189df98b8c924da357a63881985cd4be621e9785186a41b4cbe684",
    ),
    "variable_debt_token": (
        TOKEN_PROXY_HASH,
        "0x9b98d3402fefb3a25c9763574db1ab563f958c998a44f461cb949450eed36104",
    ),
    "stable_debt_token": (
        TOKEN_PROXY_HASH,
        "0xa3e44b699083a490b71becf0f23f36daa682a4ded5d948a50b2fe0a8cc795d4a",
    ),
}


def verify_revision(reader, address, block, role):
    observed = AuthorityReader(reader, block).identity(address)
    if (observed.code_hash, observed.implementation_hash) != REVISION_HASHES[role]:
        raise DataError("deployment_changed", f"SparkLend {role} has an unverified revision")
    return observed


def pool_provider(reader, source, block):
    verify_revision(reader, source.pool, block, "pool")
    if reader.call(source.pool, "POOL_REVISION()", block)[0] != 4:
        raise DataError("deployment_changed", "SparkLend Pool revision differs from its model")
    provider = reader.call(source.pool, "ADDRESSES_PROVIDER()", block, outputs=("address",))[0]
    if reader.call(provider, "getPool()", block, outputs=("address",))[0] != source.pool:
        raise DataError("deployment_changed", "SparkLend Pool and provider do not reconcile")
    return provider


def reserve_token(reader, source, block, address, role):
    verify_revision(reader, address, block, role)
    pool = reader.call(address, "POOL()", block, outputs=("address",))[0]
    underlying = reader.call(address, "UNDERLYING_ASSET_ADDRESS()", block, outputs=("address",))[0]
    if pool != source.pool or underlying != source.asset.reference:
        raise DataError("deployment_changed", "SparkLend token belongs to another Pool or reserve")
    return AssetType(f"{source.chain.chain_id}/erc20:{address}")


def oracle_quote(reader, source, block, provider):
    return OracleQuote(**oracle_values(reader, source, block, provider))


def read_reserve(reader, source: Reserve, block: Block) -> ReserveObservation:
    if source.chain != block.chain or reader.chain != source.chain:
        raise ValueError("SparkLend source and observation block have different scope")
    provider = pool_provider(reader, source, block)
    data = reader.call(
        source.pool,
        "getReserveData(address)",
        block,
        inputs=("address",),
        args=(source.asset.reference,),
        outputs=(LEGACY_RESERVE,),
    )[0]
    a_token = reserve_token(reader, source, block, data[8], "a_token")
    debt_token = reserve_token(reader, source, block, data[10], "variable_debt_token")
    stable_token = reserve_token(reader, source, block, data[9], "stable_debt_token")
    return ReserveObservation(
        source=source,
        block=block,
        asset=reader.asset(source.asset, block),
        configuration=ReserveConfiguration(raw=data[0][0]),
        reserve_id=data[7],
        liquidity_index_ray=data[1],
        liquidity_rate_ray=data[2],
        variable_borrow_index_ray=data[3],
        variable_borrow_rate_ray=data[4],
        last_update_timestamp=data[6],
        a_token=a_token,
        variable_debt_token=debt_token,
        stable_debt_token=stable_token,
        accrued_to_treasury_scaled=data[12],
        unbacked=data[13],
        available_underlying=balance(reader, source.asset, a_token.reference, block),
        total_scaled_supply=reader.call(a_token.reference, "scaledTotalSupply()", block)[0],
        total_scaled_debt=reader.call(debt_token.reference, "scaledTotalSupply()", block)[0],
        total_stable_debt=reader.call(stable_token.reference, "totalSupply()", block)[0],
        **optional_evidence("oracle", lambda: oracle_quote(reader, source, block, provider)),
    )


def read_position(reader, source: Reserve, fund: Account, block: Block) -> PositionObservation:
    if fund.chain != source.chain:
        raise ValueError("SparkLend fund and reserve have different scope")
    reserve = read_reserve(reader, source, block)
    configuration = reader.call(
        source.pool,
        "getUserConfiguration(address)",
        block,
        inputs=("address",),
        args=(fund.address,),
        outputs=("(uint256)",),
    )[0][0]
    return PositionObservation(
        reserve=reserve,
        fund=fund,
        supplied_assets=balance(reader, reserve.a_token, fund.address, block),
        supplied_scaled=balance(reader, reserve.a_token, fund.address, block, scaled=True),
        debt_assets=balance(reader, reserve.variable_debt_token, fund.address, block),
        debt_scaled=balance(reader, reserve.variable_debt_token, fund.address, block, scaled=True),
        stable_debt_assets=balance(reader, reserve.stable_debt_token, fund.address, block),
        collateral_enabled=bool(configuration & (1 << (2 * reserve.reserve_id + 1))),
        user_configuration=configuration,
        **optional_evidence("risk", lambda: account_risk(reader, source, fund, block)),
    )
