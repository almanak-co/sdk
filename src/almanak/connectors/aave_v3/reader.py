"""Pinned V3 observations without signing or deployment-address defaults."""

from almanak.caip import AssetType
from almanak.connectors.aave_v3.domain import (
    OracleQuote,
    PositionObservation,
    Reserve,
    ReserveConfiguration,
    ReserveObservation,
)
from almanak.connectors.pool_lending_reader import LEGACY_RESERVE as LEGACY_RESERVE
from almanak.connectors.pool_lending_reader import account_risk as account_risk
from almanak.connectors.pool_lending_reader import balance as balance
from almanak.connectors.pool_lending_reader import optional_evidence as optional_evidence
from almanak.connectors.pool_lending_reader import oracle_values
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_storage import AuthorityReader
from almanak.models import Account, Block

# Revisions identify executable semantics, not a pool or asset selection policy.
# Ethereum, Base, Arbitrum, Optimism, BNB, Polygon and Avalanche have source evidence.
# Arbitrum USDC and BNB reserve proxy metadata differ from compiled references.
# Admission still requires each complete observed proxy and implementation hash.
#
# Each verified Pool, as (proxy code hash, implementation code hash), with the
# POOL_REVISION that implementation reports. Revision 12 adds a transient reentrancy guard
# to the Pool's entry points. It keeps every function signature and the storage layout,
# and links the SupplyLogic, BorrowLogic, LiquidationLogic and PoolLogic deployments that
# revision 11 links, so one accounting model serves both. The reserve tokens are the same
# implementations under either Pool revision.
POOL_REVISIONS = {
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x92b4bfbdb85278f47d05c6daa11efc7665f5827bd5cfc2363105469efc067153",
    ): 11,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x79a6f319f85da0a4ab13195e76d5c96ac48aa7b2c1f741a129844095458cb6e3",
    ): 11,
    (
        "0xa2aac688c1cbeae86c0b468a926632c775c7a40f1b5dec59948f9f1562c53e9e",
        "0x7266d9c5bf4d60b3c0dbccb254e6e9c06880db859bc3a5cd82b38e42212dd2cb",
    ): 11,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x34b10035786e78f682a30e11cd04de5228ee452bde734354b335bacdd918da77",
    ): 11,
    (
        "0x96107dc4006b4c7fecd1827cfb275ffeef31e6194cd50466f85f8eb24ccf2679",
        "0x530cdbba5eb9487cd5d041bb74b7a1936ad3230bf9e361893ecd025373c7fbe5",
    ): 11,
    (
        "0xffcb26fbebbe09d9b0d8baef76a1fa218989be6c279b7acf9865d8fb6e0718ce",
        "0x46a99baf41f90e39818ded14f4d45885fe6293496e72c1cea2014e814701a994",
    ): 11,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x9018e99643e0c9b7506f6c8233da851241f5bfddb06158e978fda41861fdea50",
    ): 11,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x39d977584f7dddf2887834de21197d89e0033900776973c2d5212b62d8df6f7b",
    ): 12,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x9d993b3df52c0cd3ae308a701605adefb43a79fe12e766c01637f7bf92dd7f33",
    ): 12,
    (
        "0xa2aac688c1cbeae86c0b468a926632c775c7a40f1b5dec59948f9f1562c53e9e",
        "0x3bdf20b81c093002d1f7a544e55dcb85f63032750cd94ba8b27e75a509384189",
    ): 12,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x9875343964bc428629b4c2829163a267050f38ff34a141a1a350c610ad9f4d42",
    ): 12,
    (
        "0x96107dc4006b4c7fecd1827cfb275ffeef31e6194cd50466f85f8eb24ccf2679",
        "0xe334f592d8ff872871487568216f48dfbe98822bbf8c9c2ec55242db3a07028e",
    ): 12,
    (
        "0xffcb26fbebbe09d9b0d8baef76a1fa218989be6c279b7acf9865d8fb6e0718ce",
        "0xa4d2855cd851dc69a5bef2dfc99504ab6b101f29c5353a4b043eece444780771",
    ): 12,
    (
        "0xf168c2e9e4d04292c7d5d526a9a917175a44369ab63a2f9997537acf0ffaaf2e",
        "0x1f369913d01c3141dff9de25fe35d096c468b1d43d7996b3a1ba7fa0d759a0ec",
    ): 12,
}
REVISION_HASHES = {
    "pool": tuple(POOL_REVISIONS),
    "a_token": (
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x472945bd53fa2ce9c77aa4820a53f48f9c059d80a7bd676739f68b763f0dcbde",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x823495f280a75bddc78cbed331acdf5fb97e9836df67313501c59c24e94b3667",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0x823495f280a75bddc78cbed331acdf5fb97e9836df67313501c59c24e94b3667",
        ),
        (
            "0xb77c9461b60312343a5626629c0667c9fb5059f3e7187d85ea826b723d9ae9c9",
            "0x4e14a89da74d0851a96459258d7a99712f20b89a90d8f2c2c12bdb2f3d2466cb",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0xc6551b001022268f9212e7ad5ff3350522fbc1e853b8fc34a159a12e8655d68b",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0xc6551b001022268f9212e7ad5ff3350522fbc1e853b8fc34a159a12e8655d68b",
        ),
        (
            "0x82c6d153799b3226525e3b7ec27b843ef44c5f6bca21fcf8b3c80db61ba64881",
            "0x3bd38f9cd664b4169375c69f362dc585ed97adf6da6f8b6d5569ecb8690d9eb5",
        ),
        (
            "0x59d2fd2a4bad76f979bc2c1da50504e072f4b3bb64f5429302a384ad9c0706f2",
            "0xb766d075565868563fa6712eed3c6c90afb71dc004230146eb77d8c6ed721145",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0xc730301c56de6c1ab7feeecf24fd64795555b2e6c936e40e3f1882cac6e307a6",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0xc730301c56de6c1ab7feeecf24fd64795555b2e6c936e40e3f1882cac6e307a6",
        ),
    ),
    "variable_debt_token": (
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x1ef51940e044b1a04e7a40ddd2bc3bfb79c151498ea99846cba17adcecaef1a4",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x0b41bd329e882e5fee5697c517936d96cf7acc20bfb0feed3c809a22fa92509c",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0x0b41bd329e882e5fee5697c517936d96cf7acc20bfb0feed3c809a22fa92509c",
        ),
        (
            "0xb77c9461b60312343a5626629c0667c9fb5059f3e7187d85ea826b723d9ae9c9",
            "0x06b12c2bd325a3c1b13b0f84d5e6d91e349e68a13281b918e4b94afee8c35d9d",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x68cd611a56e7a9d3a7a5a8d73332445fe3e632cc3f6eeac41d06bf7973c25c8d",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0x68cd611a56e7a9d3a7a5a8d73332445fe3e632cc3f6eeac41d06bf7973c25c8d",
        ),
        (
            "0x82c6d153799b3226525e3b7ec27b843ef44c5f6bca21fcf8b3c80db61ba64881",
            "0xb17b28c6f03e856dc7dd8ffbfc05a1643797cf20343ca511b4e6064236f38033",
        ),
        (
            "0x59d2fd2a4bad76f979bc2c1da50504e072f4b3bb64f5429302a384ad9c0706f2",
            "0x40d68592b28ba299f483d469b05db7b9061a66f21964587f38057da64f8915cd",
        ),
        (
            "0x3ebcd59bd9d51bd49cafbb0419423ba025e8e477f34cefa1a71ea3c29b78fa1f",
            "0x9fe871be0b3aeeea4c8b9147b0e1b8fda4c0d8d467499329a01610a28f3c02f1",
        ),
        (
            "0xc3aa4c3619e0f36aac996ce2308856466850ac4ef7033e5fef29a8807d2ae6e8",
            "0x9fe871be0b3aeeea4c8b9147b0e1b8fda4c0d8d467499329a01610a28f3c02f1",
        ),
    ),
}


def verify_revision(reader, address, block, role):
    observed = AuthorityReader(reader, block).identity(address)
    if (observed.code_hash, observed.implementation_hash) not in REVISION_HASHES[role]:
        raise DataError("deployment_changed", f"Aave V3 {role} has an unverified revision")
    return observed


def verify_pool(reader, pool, block):
    """The Pool's pinned code, reporting the revision that code was verified as."""
    observed = verify_revision(reader, pool, block, "pool")
    revision = POOL_REVISIONS[(observed.code_hash, observed.implementation_hash)]
    if reader.call(pool, "POOL_REVISION()", block)[0] != revision:
        raise DataError("deployment_changed", "Aave V3 Pool revision differs from its model")
    return observed


def pool_provider(reader, source, block):
    verify_pool(reader, source.pool, block)
    provider = reader.call(source.pool, "ADDRESSES_PROVIDER()", block, outputs=("address",))[0]
    if reader.call(provider, "getPool()", block, outputs=("address",))[0] != source.pool:
        raise DataError("deployment_changed", "Aave V3 Pool and provider do not reconcile")
    return provider


def reserve_token(reader, source, block, address, role):
    verify_revision(reader, address, block, role)
    pool = reader.call(address, "POOL()", block, outputs=("address",))[0]
    underlying = reader.call(address, "UNDERLYING_ASSET_ADDRESS()", block, outputs=("address",))[0]
    if pool != source.pool or underlying != source.asset.reference:
        raise DataError("deployment_changed", "Aave V3 token belongs to another Pool or reserve")
    return AssetType(f"{source.chain.chain_id}/erc20:{address}")


def oracle_quote(reader, source, block, provider):
    return OracleQuote(**oracle_values(reader, source, block, provider))


def read_reserve(reader, source: Reserve, block: Block) -> ReserveObservation:
    if source.chain != block.chain or reader.chain != source.chain:
        raise ValueError("Aave V3 source and observation block have different scope")
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
        accrued_to_treasury_scaled=data[12],
        virtual_underlying_balance=reader.call(
            source.pool,
            "getVirtualUnderlyingBalance(address)",
            block,
            inputs=("address",),
            args=(source.asset.reference,),
            outputs=("uint128",),
        )[0],
        available_underlying=balance(reader, source.asset, a_token.reference, block),
        total_scaled_supply=reader.call(a_token.reference, "scaledTotalSupply()", block)[0],
        total_scaled_debt=reader.call(debt_token.reference, "scaledTotalSupply()", block)[0],
        **optional_evidence("oracle", lambda: oracle_quote(reader, source, block, provider)),
    )


def read_position(reader, source: Reserve, fund: Account, block: Block) -> PositionObservation:
    if fund.chain != source.chain:
        raise ValueError("Aave V3 fund and reserve have different scope")
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
        collateral_enabled=bool(configuration & (1 << (2 * reserve.reserve_id + 1))),
        user_configuration=configuration,
        **optional_evidence("risk", lambda: account_risk(reader, source, fund, block)),
    )
